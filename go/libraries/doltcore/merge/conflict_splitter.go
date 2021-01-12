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

package merge

import (
	"context"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped"
	"github.com/dolthub/dolt/go/store/types"
)

// MergeVersion defines which version a value of a row corresponds to
type MergeVersion int

const (
	// BaseVersion represents the state of a row at the most recent ancestor
	BaseVersion MergeVersion = iota
	// OurVersion represents the state of a row on our branch that is being merged into
	OurVersion
	// TheirVersion represents the state of a row on their branch which we are merging
	TheirVersion
	// Blank is used for displaying a row without a version label
	Blank
)

var typeToMergeVersion = map[string]MergeVersion{
	oursStr:   OurVersion,
	theirsStr: TheirVersion,
	baseStr:   BaseVersion,
}

// ConflictsSplitter splits a conflict into base, ours, and their version of a row
type ConflictSplitter struct {
	joiner     *rowconv.Joiner
	sch        schema.Schema
	converters map[string]*rowconv.RowConverter
}

// NewConflictSplitter creates a new ConflictSplitter
func NewConflictSplitter(ctx context.Context, vrw types.ValueReadWriter, joiner *rowconv.Joiner) (ConflictSplitter, error) {
	baseSch := joiner.SchemaForName(baseStr)
	ourSch := joiner.SchemaForName(baseStr)
	theirSch := joiner.SchemaForName(theirsStr)

	sch, err := untyped.UntypedSchemaUnion(baseSch, ourSch, theirSch)

	if err != nil {
		return ConflictSplitter{}, err
	}

	converters := make(map[string]*rowconv.RowConverter)
	converters[oursStr], err = tagMappingConverter(ctx, vrw, ourSch, sch)

	if err != nil {
		return ConflictSplitter{}, err
	}

	converters[theirsStr], err = tagMappingConverter(ctx, vrw, theirSch, sch)

	if err != nil {
		return ConflictSplitter{}, err
	}

	converters[baseStr], err = tagMappingConverter(ctx, vrw, baseSch, sch)

	if err != nil {
		return ConflictSplitter{}, err
	}

	return ConflictSplitter{joiner: joiner, sch: sch, converters: converters}, nil
}

// GetSchema returns the common schema which all rows will share
func (ds ConflictSplitter) GetSchema() schema.Schema {
	return ds.sch
}

// SplitConflicts takes a conflict row and splits it into ours, theirs, and base versions and provides pipeline properties
// which can be used to distinguished which is which and what type of conflict occurred.
func (ds ConflictSplitter) SplitConflicts(inRow row.Row, _ pipeline.ReadableMap) (rowData []*pipeline.TransformedRowResult, badRowDetails string) {
	rows, err := ds.joiner.Split(inRow)
	if err != nil {
		return nil, err.Error()
	}

	var baseRow row.Row
	has := make(map[string]bool)
	baseRow, has[baseStr] = rows[baseStr]
	_, has[oursStr] = rows[oursStr]
	_, has[theirsStr] = rows[theirsStr]

	if has[baseStr] {
		baseRow, err = ds.converters[baseStr].Convert(baseRow)

		if err != nil {
			return nil, err.Error()
		}
	}

	rowData = make([]*pipeline.TransformedRowResult, 0, 3)
	for _, rowType := range []string{baseStr, oursStr, theirsStr} {
		row, ok := rows[rowType]
		props := map[string]interface{}{mergeVersionProp: typeToMergeVersion[rowType]}

		if ok {
			converted, err := ds.converters[rowType].Convert(row)

			if err != nil {
				return nil, err.Error()
			}

			if !has[baseStr] {
				props[mergeRowOperation] = types.DiffChangeAdded
			} else {
				props[mergeRowOperation] = types.DiffChangeModified
			}

			rowData = append(rowData, &pipeline.TransformedRowResult{RowData: converted, PropertyUpdates: props})
		} else if rowType != baseStr {
			props[mergeRowOperation] = types.DiffChangeRemoved
			rowData = append(rowData, &pipeline.TransformedRowResult{RowData: baseRow, PropertyUpdates: props})
		}
	}

	return rowData, ""
}
