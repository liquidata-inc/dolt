// Copyright 2020 Dolthub, Inc.
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

package sqle

import (
	"context"
	"errors"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/lookup"
	"github.com/dolthub/dolt/go/store/types"
)

type DoltIndex interface {
	sql.Index
	sql.AscendIndex
	sql.DescendIndex
	sql.NegateIndex
	Schema() schema.Schema
	IndexSchema() schema.Schema
	TableData() types.Map
	IndexRowData() types.Map
}

type doltIndex struct {
	cols         []schema.Column
	db           sql.Database
	id           string
	indexRowData types.Map
	indexSch     schema.Schema
	table        *doltdb.Table
	tableData    types.Map
	tableName    string
	tableSch     schema.Schema
	unique       bool
	comment      string
}

//TODO: have queries using IS NULL make use of indexes
var _ DoltIndex = (*doltIndex)(nil)

// AscendGreaterOrEqual implements sql.AscendIndex
func (di *doltIndex) AscendGreaterOrEqual(keys ...interface{}) (sql.IndexLookup, error) {
	tpl, err := di.keysToTuple(keys)
	if err != nil {
		return nil, err
	}
	return &doltIndexLookup{
		idx: di,
		ranges: []lookup.Range{
			lookup.GreaterOrEqualRange(tpl),
		},
	}, nil
}

// AscendLessThan implements sql.AscendIndex
func (di *doltIndex) AscendLessThan(keys ...interface{}) (sql.IndexLookup, error) {
	tpl, err := di.keysToTuple(keys)
	if err != nil {
		return nil, err
	}
	return &doltIndexLookup{
		idx: di,
		ranges: []lookup.Range{
			lookup.LessThanRange(tpl),
		},
	}, nil
}

// AscendRange implements sql.AscendIndex
// TODO: rename this from AscendRange to BetweenRange or something
func (di *doltIndex) AscendRange(greaterOrEqual, lessThanOrEqual []interface{}) (sql.IndexLookup, error) {
	greaterTpl, err := di.keysToTuple(greaterOrEqual)
	if err != nil {
		return nil, err
	}
	lessTpl, err := di.keysToTuple(lessThanOrEqual)
	if err != nil {
		return nil, err
	}
	r, err := lookup.ClosedRange(greaterTpl, lessTpl)
	if err != nil {
		return nil, err
	}
	return &doltIndexLookup{
		idx: di,
		ranges: []lookup.Range{
			r,
		},
	}, nil
}

// DescendGreater implements sql.DescendIndex
func (di *doltIndex) DescendGreater(keys ...interface{}) (sql.IndexLookup, error) {
	tpl, err := di.keysToTuple(keys)
	if err != nil {
		return nil, err
	}
	r, err := lookup.GreaterThanRange(tpl)
	if err != nil {
		return nil, err
	}
	return &doltIndexLookup{
		idx: di,
		ranges: []lookup.Range{
			r,
		},
	}, nil
}

// DescendLessOrEqual implements sql.DescendIndex
func (di *doltIndex) DescendLessOrEqual(keys ...interface{}) (sql.IndexLookup, error) {
	tpl, err := di.keysToTuple(keys)
	if err != nil {
		return nil, err
	}
	r, err := lookup.LessOrEqualRange(tpl)
	if err != nil {
		return nil, err
	}
	return &doltIndexLookup{
		idx: di,
		ranges: []lookup.Range{
			r,
		},
	}, nil
}

// DescendRange implements sql.DescendIndex
// TODO: fix go-mysql-server to remove this duplicate function
func (di *doltIndex) DescendRange(lessOrEqual, greaterOrEqual []interface{}) (sql.IndexLookup, error) {
	return di.AscendRange(greaterOrEqual, lessOrEqual)
}

// Database implement sql.Index
func (di *doltIndex) Database() string {
	return di.db.Name()
}

// Expressions implements sql.Index
func (di *doltIndex) Expressions() []string {
	strs := make([]string, len(di.cols))
	for i, col := range di.cols {
		strs[i] = di.tableName + "." + col.Name
	}
	return strs
}

// Get implements sql.Index
func (di *doltIndex) Get(keys ...interface{}) (sql.IndexLookup, error) {
	tpl, err := di.keysToTuple(keys)
	if err != nil {
		return nil, err
	}
	r, err := lookup.ClosedRange(tpl, tpl)
	if err != nil {
		return nil, err
	}
	return &doltIndexLookup{
		idx: di,
		ranges: []lookup.Range{
			r,
		},
	}, nil
}

// Not implements sql.NegateIndex
func (di *doltIndex) Not(keys ...interface{}) (sql.IndexLookup, error) {
	tpl, err := di.keysToTuple(keys)
	if err != nil {
		return nil, err
	}
	r1 := lookup.LessThanRange(tpl)
	r2, err := lookup.GreaterThanRange(tpl)
	if err != nil {
		return nil, err
	}
	return &doltIndexLookup{
		idx: di,
		ranges: []lookup.Range{
			r1,
			r2,
		},
	}, nil
}

// Has implements sql.Index
func (*doltIndex) Has(partition sql.Partition, key ...interface{}) (bool, error) {
	return false, errors.New("unimplemented")
}

// ID implements sql.Index
func (di *doltIndex) ID() string {
	return di.id
}

// IsUnique implements sql.Index
func (di *doltIndex) IsUnique() bool {
	return di.unique
}

// Comment implements sql.Index
func (di *doltIndex) Comment() string {
	return di.comment
}

// IndexType implements sql.Index
func (di *doltIndex) IndexType() string {
	return "BTREE"
}

// Schema returns the dolt table schema of this index.
func (di *doltIndex) Schema() schema.Schema {
	return di.tableSch
}

// Schema returns the dolt index schema.
func (di *doltIndex) IndexSchema() schema.Schema {
	return di.indexSch
}

// Table implements sql.Index
func (di *doltIndex) Table() string {
	return di.tableName
}

// TableData returns the map of table data for this index (the map of the target table, not the index storage table)
func (di *doltIndex) TableData() types.Map {
	return di.tableData
}

// IndexRowData returns the map of index row data.
func (di *doltIndex) IndexRowData() types.Map {
	return di.indexRowData
}

func (di *doltIndex) keysToTuple(keys []interface{}) (types.Tuple, error) {
	nbf := di.indexRowData.Format()
	if len(di.cols) != len(keys) {
		return types.EmptyTuple(nbf), errors.New("keys must specify all columns for an index")
	}
	var vals []types.Value
	for i, col := range di.cols {
		// As an example, if our TypeInfo is Int8, we should not fail to create a tuple if we are returning all keys
		// that have a value of less than 9001, thus we promote the TypeInfo to the widest type.
		vrw := types.NewInternalValueStore() // We are creating index keys, therefore we can use an internal store
		val, err := col.TypeInfo.Promote().ConvertValueToNomsValue(context.Background(), vrw, keys[i])
		if err != nil {
			return types.EmptyTuple(nbf), err
		}
		vals = append(vals, types.Uint(col.Tag), val)
	}
	return types.NewTuple(nbf, vals...)
}
