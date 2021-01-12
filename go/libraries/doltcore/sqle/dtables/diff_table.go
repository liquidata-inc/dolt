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
	"errors"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/parse"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/expreval"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	toCommit       = "to_commit"
	fromCommit     = "from_commit"
	toCommitDate   = "to_commit_date"
	fromCommitDate = "from_commit_date"

	diffTypeColName  = "diff_type"
	diffTypeAdded    = "added"
	diffTypeModified = "modified"
	diffTypeRemoved  = "removed"
)

func toNamer(name string) string {
	return diff.To + "_" + name
}

func fromNamer(name string) string {
	return diff.From + "_" + name
}

var _ sql.Table = (*DiffTable)(nil)

type DiffTable struct {
	name        string
	ddb         *doltdb.DoltDB
	workingRoot *doltdb.RootValue
	head        *doltdb.Commit

	ss               *schema.SuperSchema
	joiner           *rowconv.Joiner
	sqlSch           sql.Schema
	partitionFilters []sql.Expression
	rowFilters       []sql.Expression
}

func NewDiffTable(ctx *sql.Context, tblName string, ddb *doltdb.DoltDB, root *doltdb.RootValue, head *doltdb.Commit) (sql.Table, error) {
	diffTblName := doltdb.DoltDiffTablePrefix + tblName

	ss, err := calcSuperSchema(ctx, root, tblName)
	if err != nil {
		return nil, err
	}

	_ = ss.AddColumn(schema.NewColumn("commit", schema.DiffCommitTag, types.StringKind, false))
	_ = ss.AddColumn(schema.NewColumn("commit_date", schema.DiffCommitDateTag, types.TimestampKind, false))

	sch, err := ss.GenerateSchema()

	if err != nil {
		return nil, err
	}

	if sch.GetAllCols().Size() <= 1 {
		return nil, sql.ErrTableNotFound.New(diffTblName)
	}

	j, err := rowconv.NewJoiner(
		[]rowconv.NamedSchema{{Name: diff.To, Sch: sch}, {Name: diff.From, Sch: sch}},
		map[string]rowconv.ColNamingFunc{
			diff.To:   toNamer,
			diff.From: fromNamer,
		})

	if err != nil {
		return nil, err
	}

	sqlSch, err := sqlutil.FromDoltSchema(diffTblName, j.GetSchema())

	if err != nil {
		return nil, err
	}

	// parses to literal, no need to pass through analyzer
	defaultVal, err := parse.StringToColumnDefaultValue(ctx, fmt.Sprintf(`"%s"`, diffTypeModified))
	if err != nil {
		return nil, err
	}

	sqlSch = append(sqlSch, &sql.Column{
		Name:     diffTypeColName,
		Type:     sql.Text,
		Default:  defaultVal,
		Nullable: false,
		Source:   diffTblName,
	})

	return &DiffTable{
		name:             tblName,
		ddb:              ddb,
		workingRoot:      root,
		head:             head,
		ss:               ss,
		joiner:           j,
		sqlSch:           sqlSch,
		partitionFilters: nil,
		rowFilters:       nil,
	}, nil
}

func (dt *DiffTable) Name() string {
	return doltdb.DoltDiffTablePrefix + dt.name
}

func (dt *DiffTable) String() string {
	return doltdb.DoltDiffTablePrefix + dt.name
}

func (dt *DiffTable) Schema() sql.Schema {
	return dt.sqlSch
}

func (dt *DiffTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	cmItr := doltdb.CommitItrForRoots(dt.ddb, dt.head)

	sf, err := selectFuncForFilters(dt.ddb.Format(), dt.partitionFilters)

	if err != nil {
		return nil, err
	}

	return newDiffPartitions(ctx, cmItr, dt.workingRoot, dt.name, sf)
}

var partitionFilterCols = set.NewStrSet([]string{toCommit, fromCommit, toCommitDate, fromCommitDate})

func splitPartitionFilters(filters []sql.Expression) (commitFilters, rowFilters []sql.Expression) {
	return splitFilters(filters, getColumnFilterCheck(partitionFilterCols))
}

// HandledFilters returns the list of filters that will be handled by the table itself
func (dt *DiffTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	dt.partitionFilters, dt.rowFilters = splitPartitionFilters(filters)
	return dt.partitionFilters
}

// Filters returns the list of filters that are applied to this table.
func (dt *DiffTable) Filters() []sql.Expression {
	return dt.partitionFilters
}

// WithFilters returns a new sql.Table instance with the filters applied
func (dt *DiffTable) WithFilters(filters []sql.Expression) sql.Table {
	if dt.partitionFilters == nil {
		dt.partitionFilters, dt.rowFilters = splitPartitionFilters(filters)
	}

	return dt
}

func (dt *DiffTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	dp := part.(diffPartition)
	return dp.getRowIter(ctx, dt.ddb, dt.ss, dt.joiner)
}

func tableData(ctx *sql.Context, tbl *doltdb.Table, ddb *doltdb.DoltDB) (types.Map, schema.Schema, error) {
	var data types.Map
	var err error
	if tbl == nil {
		data, err = types.NewMap(ctx, ddb.ValueReadWriter())
	} else {
		data, err = tbl.GetRowData(ctx)

		if err != nil {
			return types.EmptyMap, nil, err
		}
	}

	var sch schema.Schema
	if tbl == nil {
		sch = schema.EmptySchema
	} else {
		sch, err = tbl.GetSchema(ctx)

		if err != nil {
			return types.EmptyMap, nil, err
		}
	}

	return data, sch, nil
}

var _ sql.RowIter = (*diffRowItr)(nil)

type diffRowItr struct {
	ad             diff.RowDiffer
	diffSrc        *diff.RowDiffSource
	joiner         *rowconv.Joiner
	sch            schema.Schema
	fromCommitInfo commitInfo
	toCommitInfo   commitInfo
}

type commitInfo struct {
	name    types.String
	date    *types.Timestamp
	nameTag uint64
	dateTag uint64
}

// Next returns the next row
func (itr *diffRowItr) Next() (sql.Row, error) {
	r, _, err := itr.diffSrc.NextDiff()

	if err != nil {
		return nil, err
	}

	toAndFromRows, err := itr.joiner.Split(r)
	_, hasTo := toAndFromRows[diff.To]
	_, hasFrom := toAndFromRows[diff.From]

	r, err = r.SetColVal(itr.toCommitInfo.nameTag, types.String(itr.toCommitInfo.name), itr.sch)

	if err != nil {
		return nil, err
	}

	r, err = r.SetColVal(itr.fromCommitInfo.nameTag, types.String(itr.fromCommitInfo.name), itr.sch)

	if err != nil {
		return nil, err
	}

	if itr.toCommitInfo.date != nil {
		r, err = r.SetColVal(itr.toCommitInfo.dateTag, *itr.toCommitInfo.date, itr.sch)

		if err != nil {
			return nil, err
		}
	}

	if itr.fromCommitInfo.date != nil {
		r, err = r.SetColVal(itr.fromCommitInfo.dateTag, *itr.fromCommitInfo.date, itr.sch)

		if err != nil {
			return nil, err
		}
	}

	sqlRow, err := sqlutil.DoltRowToSqlRow(r, itr.sch)

	if err != nil {
		return nil, err
	}

	if hasTo && hasFrom {
		sqlRow = append(sqlRow, diffTypeModified)
	} else if hasTo && !hasFrom {
		sqlRow = append(sqlRow, diffTypeAdded)
	} else {
		sqlRow = append(sqlRow, diffTypeRemoved)
	}

	return sqlRow, nil
}

// Close closes the iterator
func (itr *diffRowItr) Close() (err error) {
	defer itr.ad.Close()
	defer func() {
		closeErr := itr.diffSrc.Close()

		if err == nil {
			err = closeErr
		}
	}()

	return nil
}

type tblInfoAtCommit struct {
	name    string
	date    *types.Timestamp
	tbl     *doltdb.Table
	tblHash hash.Hash
}

// data partitioned into pairs of table states which get compared
type diffPartition struct {
	to       *doltdb.Table
	from     *doltdb.Table
	toName   string
	fromName string
	toDate   *types.Timestamp
	fromDate *types.Timestamp
}

func (dp diffPartition) Key() []byte {
	return []byte(dp.toName + dp.fromName)
}

func (dp diffPartition) getRowIter(ctx *sql.Context, ddb *doltdb.DoltDB, ss *schema.SuperSchema, joiner *rowconv.Joiner) (sql.RowIter, error) {
	fromData, fromSch, err := tableData(ctx, dp.from, ddb)

	if err != nil {
		return nil, err
	}

	toData, toSch, err := tableData(ctx, dp.to, ddb)

	if err != nil {
		return nil, err
	}

	vrw := types.NewInternalValueStore() // We're displaying here, so all values that require a VRW will use an internal one

	fromConv, err := rowConvForSchema(ctx, vrw, ss, fromSch)

	if err != nil {
		return nil, err
	}

	toConv, err := rowConvForSchema(ctx, vrw, ss, toSch)

	if err != nil {
		return nil, err
	}

	sch := joiner.GetSchema()
	toCol, _ := sch.GetAllCols().GetByName(toCommit)
	fromCol, _ := sch.GetAllCols().GetByName(fromCommit)
	toDateCol, _ := sch.GetAllCols().GetByName(toCommitDate)
	fromDateCol, _ := sch.GetAllCols().GetByName(fromCommitDate)

	fromCmInfo := commitInfo{types.String(dp.fromName), dp.fromDate, fromCol.Tag, fromDateCol.Tag}
	toCmInfo := commitInfo{types.String(dp.toName), dp.toDate, toCol.Tag, toDateCol.Tag}

	rd := diff.NewRowDiffer(ctx, fromSch, toSch, 1024)
	rd.Start(ctx, fromData, toData)

	src := diff.NewRowDiffSource(rd, joiner)
	src.AddInputRowConversion(fromConv, toConv)

	return &diffRowItr{
		ad:             rd,
		diffSrc:        src,
		joiner:         joiner,
		sch:            joiner.GetSchema(),
		fromCommitInfo: fromCmInfo,
		toCommitInfo:   toCmInfo,
	}, nil
}

type partitionSelectFunc func(*sql.Context, diffPartition) (bool, error)

func selectFuncForFilters(nbf *types.NomsBinFormat, filters []sql.Expression) (partitionSelectFunc, error) {
	const (
		toCommitTag uint64 = iota
		fromCommitTag
		toCommitDateTag
		fromCommitDateTag
	)

	colColl, err := schema.NewColCollection(
		schema.NewColumn(toCommit, toCommitTag, types.StringKind, false),
		schema.NewColumn(fromCommit, fromCommitTag, types.StringKind, false),
		schema.NewColumn(toCommitDate, toCommitDateTag, types.TimestampKind, false),
		schema.NewColumn(fromCommitDate, fromCommitDateTag, types.TimestampKind, false),
	)

	if err != nil {
		return nil, err
	}

	expFunc, err := expreval.ExpressionFuncFromSQLExpressions(nbf, schema.UnkeyedSchemaFromCols(colColl), filters)

	if err != nil {
		return nil, err
	}

	return func(ctx *sql.Context, partition diffPartition) (bool, error) {
		vals := row.TaggedValues{
			toCommitTag:   types.String(partition.toName),
			fromCommitTag: types.String(partition.fromName),
		}

		if partition.toDate != nil {
			vals[toCommitDateTag] = *partition.toDate
		}

		if partition.fromDate != nil {
			vals[fromCommitDateTag] = *partition.fromDate
		}

		return expFunc(ctx, vals)
	}, nil
}

var _ sql.PartitionIter = &diffPartitions{}

// collection of paratitions. Implements PartitionItr
type diffPartitions struct {
	// TODO change the sql.PartitionIterator interface so that Next receives the context rather than caching it.
	ctx             *sql.Context
	tblName         string
	cmItr           doltdb.CommitItr
	cmHashToTblInfo map[hash.Hash]tblInfoAtCommit
	selectFunc      partitionSelectFunc
}

func newDiffPartitions(ctx *sql.Context, cmItr doltdb.CommitItr, wr *doltdb.RootValue, tblName string, selectFunc partitionSelectFunc) (*diffPartitions, error) {
	t, exactName, ok, err := wr.GetTableInsensitive(ctx, tblName)

	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, errors.New(fmt.Sprintf("table: %s does not exist", tblName))
	}

	wrTblHash, _, err := wr.GetTableHash(ctx, exactName)

	if err != nil {
		return nil, err
	}

	cmHash, _, err := cmItr.Next(ctx)

	if err != nil {
		return nil, err
	}

	cmHashToTblInfo := make(map[hash.Hash]tblInfoAtCommit)
	cmHashToTblInfo[cmHash] = tblInfoAtCommit{"WORKING", nil, t, wrTblHash}

	err = cmItr.Reset(ctx)

	if err != nil {
		return nil, err
	}

	return &diffPartitions{
		ctx:             ctx,
		tblName:         tblName,
		cmItr:           cmItr,
		cmHashToTblInfo: cmHashToTblInfo,
		selectFunc:      selectFunc,
	}, nil
}

// called in a commit iteration loop. Adds partitions when it finds a commit and it's parent that have different values
// for the hash of the table being looked at.
func (dp *diffPartitions) processCommit(ctx *sql.Context, cmHash hash.Hash, cm *doltdb.Commit, root *doltdb.RootValue, tbl *doltdb.Table) (*diffPartition, error) {
	tblHash, _, err := root.GetTableHash(ctx, dp.tblName)

	if err != nil {
		return nil, err
	}

	toInfoForCommit := dp.cmHashToTblInfo[cmHash]
	cmHashStr := cmHash.String()
	meta, err := cm.GetCommitMeta()

	if err != nil {
		return nil, err
	}

	ts := types.Timestamp(meta.Time())

	var nextPartition *diffPartition
	if tblHash != toInfoForCommit.tblHash {
		partition := diffPartition{toInfoForCommit.tbl, tbl, toInfoForCommit.name, cmHashStr, toInfoForCommit.date, &ts}
		selected, err := dp.selectFunc(ctx, partition)

		if err != nil {
			return nil, err
		}

		if selected {
			nextPartition = &partition
		}
	}

	newInfo := tblInfoAtCommit{cmHashStr, &ts, tbl, tblHash}
	parentHashes, err := cm.ParentHashes(ctx)

	if err != nil {
		return nil, err
	}

	for _, h := range parentHashes {
		dp.cmHashToTblInfo[h] = newInfo
	}

	return nextPartition, nil
}

func (dp *diffPartitions) Next() (sql.Partition, error) {
	for {
		cmHash, cm, err := dp.cmItr.Next(dp.ctx)

		if err != nil {
			return nil, err
		}

		root, err := cm.GetRootValue()

		if err != nil {
			return nil, err
		}

		tbl, _, _, err := root.GetTableInsensitive(dp.ctx, dp.tblName)

		if err != nil {
			return nil, err
		}

		next, err := dp.processCommit(dp.ctx, cmHash, cm, root, tbl)

		if err != nil {
			return nil, err
		}

		if next != nil {
			return *next, nil
		}
	}
}

func (dp *diffPartitions) Close() error {
	return nil
}

// creates a RowConverter for transforming rows with the the given schema to this super schema.
func rowConvForSchema(ctx context.Context, vrw types.ValueReadWriter, ss *schema.SuperSchema, sch schema.Schema) (*rowconv.RowConverter, error) {
	eq, err := schema.SchemasAreEqual(sch, schema.EmptySchema)
	if err != nil {
		return nil, err
	}
	if eq {
		return rowconv.IdentityConverter, nil
	}

	inNameToOutName, err := ss.NameMapForSchema(sch)

	if err != nil {
		return nil, err
	}

	ssch, err := ss.GenerateSchema()

	if err != nil {
		return nil, err
	}

	fm, err := rowconv.NameMapping(sch, ssch, inNameToOutName)

	if err != nil {
		return nil, err
	}

	return rowconv.NewRowConverter(ctx, vrw, fm)
}
