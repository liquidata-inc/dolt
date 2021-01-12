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

package table_test

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	dtu "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/store/types"
)

func TestKeylessTableReader(t *testing.T) {
	sch := dtu.CreateSchema(
		schema.NewColumn("c0", 0, types.IntKind, false),
		schema.NewColumn("c1", 1, types.IntKind, false))

	type bagRow struct {
		vals        sql.Row
		cardinality uint64
	}

	makeBag := func(vrw types.ValueReadWriter, sch schema.Schema, rows ...bagRow) types.Map {
		var tups []types.Value
		for _, r := range rows {
			k, v, err := encodeKeylessSqlRows(vrw, sch, r.vals, r.cardinality)
			require.NoError(t, err)
			require.NotNil(t, k)
			require.NotNil(t, v)

			tups = append(tups, k, v)
		}
		return dtu.MustMap(t, vrw, tups...)
	}

	tests := []struct {
		name string
		sch  schema.Schema
		rows []bagRow
		// read order is pseudorandom, based on hash of values
		expected []sql.Row
	}{
		{
			name: "read empty map",
			sch:  sch,
		},
		{
			name: "read non-duplicate map",
			sch:  sch,
			rows: []bagRow{
				{sql.NewRow(int64(0), int64(0)), 1},
				{sql.NewRow(int64(1), int64(1)), 1},
				{sql.NewRow(int64(2), int64(2)), 1},
			},
			expected: []sql.Row{
				sql.NewRow(int64(2), int64(2)),
				sql.NewRow(int64(0), int64(0)),
				sql.NewRow(int64(1), int64(1)),
			},
		},
		{
			name: "read duplicate map",
			sch:  sch,
			rows: []bagRow{
				{sql.NewRow(int64(0), int64(0)), 1},
				{sql.NewRow(int64(1), int64(1)), 2},
				{sql.NewRow(int64(2), int64(2)), 3},
			},
			expected: []sql.Row{
				sql.NewRow(int64(2), int64(2)),
				sql.NewRow(int64(2), int64(2)),
				sql.NewRow(int64(2), int64(2)),
				sql.NewRow(int64(0), int64(0)),
				sql.NewRow(int64(1), int64(1)),
				sql.NewRow(int64(1), int64(1)),
			},
		},
		{
			name: "read order independent of write order",
			sch:  sch,
			rows: []bagRow{
				{sql.NewRow(int64(2), int64(2)), 1},
				{sql.NewRow(int64(1), int64(1)), 1},
				{sql.NewRow(int64(0), int64(0)), 1},
			},
			expected: []sql.Row{
				sql.NewRow(int64(2), int64(2)),
				sql.NewRow(int64(0), int64(0)),
				sql.NewRow(int64(1), int64(1)),
			},
		},
	}

	dEnv := dtu.CreateTestEnv()
	ctx := context.Background()
	vrw := dEnv.DoltDB.ValueReadWriter()
	schVal, err := encoding.MarshalSchemaAsNomsValue(ctx, vrw, sch)
	require.NoError(t, err)
	empty := dtu.MustMap(t, vrw)

	compareRows := func(t *testing.T, expected []sql.Row, rdr table.SqlTableReader) {
		for _, exp := range expected {
			act, err := rdr.ReadSqlRow(ctx)
			assert.NoError(t, err)
			assert.Equal(t, exp, act)
		}
		r, err := rdr.ReadSqlRow(ctx)
		assert.Equal(t, io.EOF, err)
		assert.Nil(t, r)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rowMap := makeBag(vrw, sch, test.rows...)
			tbl, err := doltdb.NewTable(ctx, vrw, schVal, rowMap, empty)
			require.NoError(t, err)
			rdr, err := table.NewTableReader(ctx, tbl)
			require.NoError(t, err)
			compareRows(t, test.expected, rdr)
		})
		t.Run(test.name+"_buffered", func(t *testing.T) {
			rowMap := makeBag(vrw, sch, test.rows...)
			tbl, err := doltdb.NewTable(ctx, vrw, schVal, rowMap, empty)
			require.NoError(t, err)
			rdr, err := table.NewBufferedTableReader(ctx, tbl)
			require.NoError(t, err)
			compareRows(t, test.expected, rdr)
		})
	}
}

func encodeKeylessSqlRows(vrw types.ValueReadWriter, sch schema.Schema, r sql.Row, cardinality uint64) (key, val types.Tuple, err error) {
	if len(r) != sch.GetAllCols().Size() {
		rl, sl := len(r), sch.GetAllCols().Size()
		return key, val, fmt.Errorf("row length (%d) != schema length (%d)", rl, sl)
	}

	size := 0
	for _, v := range r {
		// skip NULLS
		if v != nil {
			size++
		}
	}

	// { Uint(count), Uint(tag1), Value(val1), ..., Uint(tagN), Value(valN) }
	vals := make([]types.Value, 2+(size*2))
	vals[0] = types.Uint(schema.KeylessRowCardinalityTag)
	vals[1] = types.Uint(cardinality)

	idx := 0
	err = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		v := r[idx]
		if v != nil {
			vals[2*idx+2] = types.Uint(tag)
			vals[2*idx+3], err = col.TypeInfo.ConvertValueToNomsValue(context.Background(), vrw, v)
		}
		idx++

		stop = err != nil
		return
	})
	if err != nil {
		return key, val, err
	}

	id, err := types.UUIDHashedFromValues(vrw.Format(), vals[2:]...)
	if err != nil {
		return key, val, err
	}

	key, err = types.NewTuple(vrw.Format(), id)
	if err != nil {
		return key, val, err
	}

	val, err = types.NewTuple(vrw.Format(), vals...)
	if err != nil {
		return key, val, err
	}

	return key, val, nil
}
