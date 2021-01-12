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

package sqlutil

import (
	"context"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

// ApplyDefaults applies the default values to the given indices, returning the resulting row.
func ApplyDefaults(ctx context.Context, vrw types.ValueReadWriter, doltSchema schema.Schema, sqlSchema sql.Schema, indicesOfColumns []int, dRow row.Row) (row.Row, error) {
	if len(indicesOfColumns) == 0 {
		return dRow, nil
	}
	sqlCtx, ok := ctx.(*sql.Context)
	if !ok {
		sqlCtx = sql.NewContext(ctx)
	}
	doltCols := doltSchema.GetAllCols()
	oldSqlRow := make(sql.Row, len(sqlSchema))
	for i, tag := range doltCols.Tags {
		val, ok := dRow.GetColVal(tag)
		if ok {
			var err error
			oldSqlRow[i], err = doltCols.TagToCol[tag].TypeInfo.ConvertNomsValueToValue(val)
			if err != nil {
				return nil, err
			}
		} else {
			oldSqlRow[i] = nil
		}
	}
	newSqlRow, err := sqle.ApplyDefaults(sqlCtx, sqlSchema, indicesOfColumns, oldSqlRow)
	if err != nil {
		return nil, err
	}
	newRow := make(row.TaggedValues)
	for i, tag := range doltCols.Tags {
		if newSqlRow[i] == nil {
			continue
		}
		val, err := doltCols.TagToCol[tag].TypeInfo.ConvertValueToNomsValue(ctx, vrw, newSqlRow[i])
		if err != nil {
			return nil, err
		}
		newRow[tag] = val
	}
	return row.New(dRow.Format(), doltSchema, newRow)
}

// ParseCreateTableStatement will parse a CREATE TABLE ddl statement and use it to create a Dolt Schema. A RootValue
// is used to generate unique tags for the Schema
func ParseCreateTableStatement(ctx context.Context, root *doltdb.RootValue, query string) (string, schema.Schema, error) {
	// todo: verify create table statement
	ddl, err := sqlparser.ParseStrictDDL(query)

	if err != nil {
		return "", nil, err
	}

	ts := ddl.(*sqlparser.DDL).TableSpec
	s, err := parse.TableSpecToSchema(sql.NewContext(ctx), ts)

	if err != nil {
		return "", nil, err
	}

	tn := ddl.(*sqlparser.DDL).Table
	buf := sqlparser.NewTrackedBuffer(nil)
	tn.Format(buf)
	tableName := buf.String()
	sch, err := ToDoltSchema(ctx, root, tableName, s)

	if err != nil {
		return "", nil, err
	}

	return tableName, sch, err
}
