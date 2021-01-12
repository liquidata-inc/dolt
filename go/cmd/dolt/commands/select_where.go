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

package commands

import (
	"context"
	"errors"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/pipeline"
)

type FilterFn = func(r row.Row) (matchesFilter bool)

func ParseWhere(sch schema.Schema, whereClause string) (FilterFn, error) {
	if whereClause == "" {
		return func(r row.Row) bool {
			return true
		}, nil
	} else {
		tokens := strings.Split(whereClause, "=")

		if len(tokens) != 2 {
			return nil, errors.New("'" + whereClause + "' is not in the format key=value")
		}

		key := tokens[0]
		valStr := tokens[1]

		col, ok := sch.GetAllCols().GetByName(key)

		var cols []schema.Column
		if !ok {
			toCol, toOk := sch.GetAllCols().GetByName("to_" + key)
			fromCol, fromOk := sch.GetAllCols().GetByName("from_" + key)

			if !(toOk && fromOk) {
				return nil, errors.New("where clause is invalid. '" + key + "' is not a known column.")
			}

			if fromCol.Kind != toCol.Kind {
				panic("to col and from col are different types.")
			}

			cols = []schema.Column{toCol, fromCol}
		} else {
			cols = []schema.Column{col}
		}

		var tags []uint64
		for _, curr := range cols {
			tags = append(tags, curr.Tag)
		}

		var val types.Value
		if typeinfo.IsStringType(cols[0].TypeInfo) {
			val = types.String(valStr)
		} else {
			var err error
			vrw := types.NewInternalValueStore() // We don't want to persist anything, so we use an internal VRW
			val, err = cols[0].TypeInfo.ParseValue(context.Background(), vrw, &valStr)
			if err != nil {
				return nil, errors.New("unable to convert '" + valStr + "' to " + col.TypeInfo.String())
			}
		}

		return func(r row.Row) bool {
			for _, tag := range tags {
				rowVal, ok := r.GetColVal(tag)

				if !ok {
					continue
				}

				if val.Equals(rowVal) {
					return true
				}
			}

			return false
		}, nil
	}
}

type SelectTransform struct {
	Pipeline *pipeline.Pipeline
	filter   FilterFn
	limit    int
	count    int
}

func NewSelTrans(filter FilterFn, limit int) *SelectTransform {
	return &SelectTransform{filter: filter, limit: limit}
}

func (st *SelectTransform) LimitAndFilter(inRow row.Row, props pipeline.ReadableMap) ([]*pipeline.TransformedRowResult, string) {
	if st.limit <= 0 || st.count < st.limit {
		if st.filter(inRow) {
			st.count++
			return []*pipeline.TransformedRowResult{{RowData: inRow, PropertyUpdates: nil}}, ""
		}
	} else if st.count == st.limit {
		st.Pipeline.NoMore()
	}

	return nil, ""
}
