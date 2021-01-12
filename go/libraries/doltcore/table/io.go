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

package table

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/types"
)

// GetRow returns a row from |tbl| corresponding to |key| if it exists.
func GetRow(ctx context.Context, tbl *doltdb.Table, sch schema.Schema, key types.Tuple) (r row.Row, ok bool, err error) {
	rowMap, err := tbl.GetRowData(ctx)
	if err != nil {
		return nil, false, err
	}

	var fields types.Value
	fields, ok, err = rowMap.MaybeGet(ctx, key)
	if err != nil || !ok {
		return nil, ok, err
	}

	r, err = row.FromNoms(sch, key, fields.(types.Tuple))
	return
}

// PipeRows will read a row from given TableReader and write it to the provided TableWriter.  It will do this
// for every row until the TableReader's ReadRow method returns io.EOF or encounters an error in either reading
// or writing.  The caller will need to handle closing the tables as necessary. If contOnBadRow is true, errors reading
// or writing will be ignored and the pipe operation will continue.
//
// Returns a tuple: (number of rows written, number of errors ignored, error). In the case that err is non-nil, the
// row counter fields in the tuple will be set to -1.
func PipeRows(ctx context.Context, rd TableReader, wr TableWriter, contOnBadRow bool) (int, int, error) {
	var numBad, numGood int
	for {
		r, err := rd.ReadRow(ctx)

		if err != nil && err != io.EOF {
			if IsBadRow(err) && contOnBadRow {
				numBad++
				continue
			}

			return -1, -1, err
		} else if err == io.EOF && r == nil {
			break
		} else if r == nil {
			// row equal to nil should
			return -1, -1, errors.New("reader returned nil row with err==nil")
		}

		err = wr.WriteRow(ctx, r)

		if err != nil {
			return -1, -1, err
		} else {
			numGood++
		}
	}

	return numGood, numBad, nil
}

// ReadAllRows reads all rows from a TableReader and returns a slice containing those rows.  Usually this is used
// for testing, or with very small data sets.
func ReadAllRows(ctx context.Context, rd TableReader, contOnBadRow bool) ([]row.Row, int, error) {
	var rows []row.Row
	var err error

	badRowCount := 0
	for {
		var r row.Row
		r, err = rd.ReadRow(ctx)

		if err != nil && err != io.EOF || r == nil {
			if IsBadRow(err) {
				badRowCount++

				if contOnBadRow {
					continue
				}
			}

			break
		}

		rows = append(rows, r)
	}

	if err == nil || err == io.EOF {
		return rows, badRowCount, nil
	}

	return nil, badRowCount, err
}

// ForeignKeyIsSatisfied ensures that the foreign key is valid by comparing the index data from the given table
// against the index data from the referenced table.
func ForeignKeyIsSatisfied(ctx context.Context, fk doltdb.ForeignKey, childIdx, parentIdx types.Map, childDef, parentDef schema.Index) error {
	if fk.ReferencedTableIndex != parentDef.Name() {
		return fmt.Errorf("cannot validate data as wrong referenced index was given: expected `%s` but received `%s`",
			fk.ReferencedTableIndex, parentDef.Name())
	}

	tagMap := make(map[uint64]uint64, len(fk.TableColumns))
	for i, childTag := range fk.TableColumns {
		tagMap[childTag] = fk.ReferencedTableColumns[i]
	}

	// FieldMappings ignore columns not in the tagMap
	fm, err := rowconv.NewFieldMapping(childDef.Schema(), parentDef.Schema(), tagMap)
	if err != nil {
		return err
	}

	vrw := types.NewInternalValueStore() // We are checking fks rather than persisting any values, so an internal VRW can be used
	rc, err := rowconv.NewRowConverter(ctx, vrw, fm)
	if err != nil {
		return err
	}

	rdr, err := noms.NewNomsMapReader(ctx, childIdx, childDef.Schema())
	if err != nil {
		return err
	}

	for {
		childIdxRow, err := rdr.ReadRow(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		parentIdxRow, err := rc.Convert(childIdxRow)
		if err != nil {
			return err
		}
		if row.IsEmpty(parentIdxRow) {
			continue
		}

		partial, err := row.ReduceToIndexPartialKey(parentDef, parentIdxRow)
		if err != nil {
			return err
		}

		indexIter := noms.NewNomsRangeReader(parentDef.Schema(), parentIdx,
			[]*noms.ReadRange{{Start: partial, Inclusive: true, Reverse: false, Check: func(tuple types.Tuple) (bool, error) {
				return tuple.StartsWith(partial), nil
			}}},
		)

		switch _, err = indexIter.ReadRow(ctx); err {
		case nil:
			continue // parent table contains child key
		case io.EOF:
			indexKeyStr, _ := types.EncodedValue(ctx, partial)
			return fmt.Errorf("foreign key violation on `%s`.`%s`: `%s`", fk.Name, fk.TableName, indexKeyStr)
		default:
			return err
		}
	}

	return nil
}
