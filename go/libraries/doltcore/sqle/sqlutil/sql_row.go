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

package sqlutil

import (
	"context"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

// DoltRowToSqlRow constructs a go-mysql-server sql.Row from a Dolt row.Row.
func DoltRowToSqlRow(doltRow row.Row, sch schema.Schema) (sql.Row, error) {
	colVals := make(sql.Row, sch.GetAllCols().Size())
	i := 0

	_, err := doltRow.IterSchema(sch, func(tag uint64, val types.Value) (stop bool, err error) {
		col, _ := sch.GetAllCols().GetByTag(tag)
		colVals[i], err = col.TypeInfo.ConvertNomsValueToValue(val)
		i++

		stop = err != nil
		return
	})
	if err != nil {
		return nil, err
	}

	return sql.NewRow(colVals...), nil
}

// SqlRowToDoltRow constructs a Dolt row.Row from a go-mysql-server sql.Row.
func SqlRowToDoltRow(ctx context.Context, vrw types.ValueReadWriter, r sql.Row, doltSchema schema.Schema) (row.Row, error) {
	if schema.IsKeyless(doltSchema) {
		return keylessDoltRowFromSqlRow(ctx, vrw, r, doltSchema)
	}
	return pkDoltRowFromSqlRow(ctx, vrw, r, doltSchema)
}

func pkDoltRowFromSqlRow(ctx context.Context, vrw types.ValueReadWriter, r sql.Row, doltSchema schema.Schema) (row.Row, error) {
	taggedVals := make(row.TaggedValues)
	allCols := doltSchema.GetAllCols()
	for i, val := range r {
		tag := allCols.Tags[i]
		schCol := allCols.TagToCol[tag]
		if val != nil {
			var err error
			taggedVals[tag], err = schCol.TypeInfo.ConvertValueToNomsValue(ctx, vrw, val)
			if err != nil {
				return nil, err
			}
		} else if !schCol.IsNullable() {
			// TODO: this isn't an error in the case of result set construction (where non-null columns can indeed be null)
			return nil, fmt.Errorf("column <%v> received nil but is non-nullable", schCol.Name)
		}
	}
	return row.New(vrw.Format(), doltSchema, taggedVals)
}

func keylessDoltRowFromSqlRow(ctx context.Context, vrw types.ValueReadWriter, sqlRow sql.Row, sch schema.Schema) (row.Row, error) {
	j := 0
	vals := make([]types.Value, sch.GetAllCols().Size()*2)

	for idx, val := range sqlRow {
		if val != nil {
			col := sch.GetAllCols().GetByIndex(idx)
			nv, err := col.TypeInfo.ConvertValueToNomsValue(ctx, vrw, val)
			if err != nil {
				return nil, err
			}

			vals[j] = types.Uint(col.Tag)
			vals[j+1] = nv
			j += 2
		}
	}

	return row.KeylessRow(vrw.Format(), vals[:j]...)
}
