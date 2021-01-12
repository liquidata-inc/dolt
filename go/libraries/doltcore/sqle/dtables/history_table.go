// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dtables

import (
	"context"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	// DoltHistoryTablePrefix is the name prefix for each history table

	// CommitHashCol is the name of the column containing the commit hash in the result set
	CommitHashCol = "commit_hash"

	// CommitterCol is the name of the column containing the committer in the result set
	CommitterCol = "committer"

	// CommitDateCol is the name of the column containing the commit date in the result set
	CommitDateCol = "commit_date"
)

var _ sql.Table = &HistoryTable{}

// HistoryTable is a system table that shows the history of rows over time
type HistoryTable struct {
	name                  string
	ddb                   *doltdb.DoltDB
	ss                    *schema.SuperSchema
	sqlSch                sql.Schema
	commitFilters         []sql.Expression
	rowFilters            []sql.Expression
	cmItr                 doltdb.CommitItr
	readerCreateFuncCache *ThreadSafeCRFuncCache
}

// NewHistoryTable creates a history table
func NewHistoryTable(ctx *sql.Context, tblName string, ddb *doltdb.DoltDB, root *doltdb.RootValue, head *doltdb.Commit) (sql.Table, error) {
	ss, err := calcSuperSchema(ctx, root, tblName)

	if err != nil {
		return nil, err
	}

	_ = ss.AddColumn(schema.NewColumn(CommitHashCol, schema.HistoryCommitHashTag, types.StringKind, false))
	_ = ss.AddColumn(schema.NewColumn(CommitterCol, schema.HistoryCommitterTag, types.StringKind, false))
	_ = ss.AddColumn(schema.NewColumn(CommitDateCol, schema.HistoryCommitDateTag, types.TimestampKind, false))

	sch, err := ss.GenerateSchema()

	if err != nil {
		return nil, err
	}

	if sch.GetAllCols().Size() <= 3 {
		return nil, sql.ErrTableNotFound.New(doltdb.DoltHistoryTablePrefix + tblName)
	}

	tableName := doltdb.DoltHistoryTablePrefix + tblName
	sqlSch, err := sqlutil.FromDoltSchema(tableName, sch)

	if err != nil {
		return nil, err
	}

	cmItr := doltdb.CommitItrForRoots(ddb, head)
	return &HistoryTable{
		name:                  tblName,
		ddb:                   ddb,
		ss:                    ss,
		sqlSch:                sqlSch,
		cmItr:                 cmItr,
		readerCreateFuncCache: NewThreadSafeCRFuncCache(),
	}, nil
}

// HandledFilters returns the list of filters that will be handled by the table itself
func (ht *HistoryTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	ht.commitFilters, ht.rowFilters = splitCommitFilters(filters)
	return ht.commitFilters
}

// Filters returns the list of filters that are applied to this table.
func (ht *HistoryTable) Filters() []sql.Expression {
	return ht.commitFilters
}

// WithFilters returns a new sql.Table instance with the filters applied
func (ht *HistoryTable) WithFilters(filters []sql.Expression) sql.Table {
	if ht.commitFilters == nil {
		ht.commitFilters, ht.rowFilters = splitCommitFilters(filters)
	}

	if len(ht.commitFilters) > 0 {
		commitCheck, err := getCommitFilterFunc(ht.commitFilters)

		if err != nil {
			return sqlutil.NewStaticErrorTable(ht, err)
		}

		ht.cmItr = doltdb.NewFilteringCommitItr(ht.cmItr, commitCheck)
	}

	return ht
}

var commitFilterCols = set.NewStrSet([]string{CommitHashCol, CommitDateCol, CommitterCol})

func getColumnFilterCheck(colNameSet *set.StrSet) func(sql.Expression) bool {
	return func(filter sql.Expression) bool {
		isCommitFilter := true
		sql.Inspect(filter, func(e sql.Expression) (cont bool) {
			if e == nil {
				return true
			}

			switch val := e.(type) {
			case *expression.GetField:
				if !colNameSet.Contains(strings.ToLower(val.Name())) {
					isCommitFilter = false
					return false
				}
			}

			return true
		})

		return isCommitFilter
	}
}

func splitFilters(filters []sql.Expression, filterCheck func(filter sql.Expression) bool) (matching, notMatching []sql.Expression) {
	matching = make([]sql.Expression, 0, len(filters))
	notMatching = make([]sql.Expression, 0, len(filters))
	for _, f := range filters {
		if filterCheck(f) {
			matching = append(matching, f)
		} else {
			notMatching = append(notMatching, f)
		}
	}
	return matching, notMatching
}

func splitCommitFilters(filters []sql.Expression) (commitFilters, rowFilters []sql.Expression) {
	return splitFilters(filters, getColumnFilterCheck(commitFilterCols))
}

func getCommitFilterFunc(filters []sql.Expression) (doltdb.CommitFilter, error) {
	filters = transformFilters(filters...)

	return func(ctx context.Context, h hash.Hash, cm *doltdb.Commit) (filterOut bool, err error) {
		meta, err := cm.GetCommitMeta()

		if err != nil {
			return false, err
		}

		sc := sql.NewContext(ctx)
		r := sql.Row{h.String(), meta.Name, meta.Time()}

		for _, filter := range filters {
			res, err := filter.Eval(sc, r)
			if err != nil {
				return false, err
			}
			b, ok := res.(bool)
			if ok && !b {
				return true, nil
			}
		}

		return false, err
	}, nil
}

func transformFilters(filters ...sql.Expression) []sql.Expression {
	for i := range filters {
		filters[i], _ = expression.TransformUp(filters[i], func(e sql.Expression) (sql.Expression, error) {
			gf, ok := e.(*expression.GetField)
			if !ok {
				return e, nil
			}
			switch gf.Name() {
			case CommitHashCol:
				return gf.WithIndex(0), nil
			case CommitterCol:
				return gf.WithIndex(1), nil
			case CommitDateCol:
				return gf.WithIndex(2), nil
			default:
				return gf, nil
			}
		})
	}
	return filters
}

func (ht *HistoryTable) WithProjection(colNames []string) sql.Table {
	return ht
}

func (ht *HistoryTable) Projection() []string {
	return []string{}
}

// Name returns the name of the history table
func (ht *HistoryTable) Name() string {
	return doltdb.DoltHistoryTablePrefix + ht.name
}

// String returns the name of the history table
func (ht *HistoryTable) String() string {
	return doltdb.DoltHistoryTablePrefix + ht.name
}

// Schema returns the schema for the history table, which will be the super set of the schemas from the history
func (ht *HistoryTable) Schema() sql.Schema {
	return ht.sqlSch
}

// Partitions returns a PartitionIter which will be used in getting partitions each of which is used to create RowIter.
func (ht *HistoryTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return &commitPartitioner{ctx, ht.cmItr}, nil
}

// PartitionRows takes a partition and returns a row iterator for that partition
func (ht *HistoryTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	cp := part.(*commitPartition)

	return newRowItrForTableAtCommit(ctx, cp.h, cp.cm, ht.name, ht.ss, ht.rowFilters, ht.readerCreateFuncCache)
}

// commitPartition is a single commit
type commitPartition struct {
	h  hash.Hash
	cm *doltdb.Commit
}

// Key returns the hash of the commit for this partition which is used as the partition key
func (cp *commitPartition) Key() []byte {
	return cp.h[:]
}

// commitPartitioner creates partitions from a CommitItr
type commitPartitioner struct {
	ctx   *sql.Context
	cmItr doltdb.CommitItr
}

// Next returns the next partition and nil, io.EOF when complete
func (cp commitPartitioner) Next() (sql.Partition, error) {
	h, cm, err := cp.cmItr.Next(cp.ctx)

	if err != nil {
		return nil, err
	}

	return &commitPartition{h, cm}, nil
}

// Close closes the partitioner
func (cp commitPartitioner) Close() error {
	return nil
}

type rowItrForTableAtCommit struct {
	ctx            context.Context
	rd             table.TableReadCloser
	sch            schema.Schema
	toSuperSchConv *rowconv.RowConverter
	extraVals      map[uint64]types.Value
	empty          bool
}

func newRowItrForTableAtCommit(
	ctx context.Context,
	h hash.Hash,
	cm *doltdb.Commit,
	tblName string,
	ss *schema.SuperSchema,
	filters []sql.Expression,
	readerCreateFuncCache *ThreadSafeCRFuncCache) (*rowItrForTableAtCommit, error) {
	root, err := cm.GetRootValue()

	if err != nil {
		return nil, err
	}

	tbl, _, ok, err := root.GetTableInsensitive(ctx, tblName)

	if err != nil {
		return nil, err
	}

	if !ok {
		return &rowItrForTableAtCommit{empty: true}, nil
	}

	m, err := tbl.GetRowData(ctx)

	if err != nil {
		return nil, err
	}

	schRef, err := tbl.GetSchemaRef()
	schHash := schRef.TargetHash()

	if err != nil {
		return nil, err
	}

	tblSch, err := doltdb.RefToSchema(ctx, root.VRW(), schRef)

	if err != nil {
		return nil, err
	}

	vrw := types.NewInternalValueStore() // We're displaying here, so all values that require a VRW will use an internal one

	toSuperSchConv, err := rowConvForSchema(ctx, vrw, ss, tblSch)

	if err != nil {
		return nil, err
	}

	createReaderFunc, err := readerCreateFuncCache.GetOrCreate(schHash, tbl.Format(), tblSch, filters)

	if err != nil {
		return nil, err
	}

	rd, err := createReaderFunc(ctx, m)

	if err != nil {
		return nil, err
	}

	sch, err := ss.GenerateSchema()

	if err != nil {
		return nil, err
	}

	hashCol, hashOK := sch.GetAllCols().GetByName(CommitHashCol)
	dateCol, dateOK := sch.GetAllCols().GetByName(CommitDateCol)
	committerCol, commiterOK := sch.GetAllCols().GetByName(CommitterCol)

	if !hashOK || !dateOK || !commiterOK {
		panic("Bug: History table super schema should always have commit_hash")
	}

	meta, err := cm.GetCommitMeta()

	if err != nil {
		return nil, err
	}

	return &rowItrForTableAtCommit{
		ctx:            ctx,
		rd:             rd,
		sch:            sch,
		toSuperSchConv: toSuperSchConv,
		extraVals: map[uint64]types.Value{
			hashCol.Tag:      types.String(h.String()),
			dateCol.Tag:      types.Timestamp(meta.Time()),
			committerCol.Tag: types.String(meta.Name),
		},
		empty: false,
	}, nil
}

// Next retrieves the next row. It will return io.EOF if it's the last row. After retrieving the last row, Close
// will be automatically closed.
func (tblItr *rowItrForTableAtCommit) Next() (sql.Row, error) {
	if tblItr.empty {
		return nil, io.EOF
	}

	r, err := tblItr.rd.ReadRow(tblItr.ctx)

	if err != nil {
		return nil, err
	}

	r, err = tblItr.toSuperSchConv.Convert(r)

	if err != nil {
		return nil, err
	}

	for tag, val := range tblItr.extraVals {
		r, err = r.SetColVal(tag, val, tblItr.sch)

		if err != nil {
			return nil, err
		}
	}

	return sqlutil.DoltRowToSqlRow(r, tblItr.sch)
}

// Close the iterator.
func (tblItr *rowItrForTableAtCommit) Close() error {
	if tblItr.rd != nil {
		return tblItr.rd.Close(tblItr.ctx)
	}

	return nil
}

func calcSuperSchema(ctx context.Context, wr *doltdb.RootValue, tblName string) (*schema.SuperSchema, error) {
	ss, found, err := wr.GetSuperSchema(ctx, tblName)

	if err != nil {
		return nil, err
	} else if !found {
		return nil, doltdb.ErrTableNotFound
	}

	return ss, nil
}
