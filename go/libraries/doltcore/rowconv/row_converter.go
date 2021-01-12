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

package rowconv

import (
	"context"
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/types"
)

var IdentityConverter = &RowConverter{nil, true, nil}

// RowConverter converts rows from one schema to another
type RowConverter struct {
	// FieldMapping is a mapping from source column to destination column
	*FieldMapping
	// IdentityConverter is a bool which is true if the converter is doing nothing.
	IdentityConverter bool
	ConvFuncs         map[uint64]types.MarshalCallback
}

func newIdentityConverter(mapping *FieldMapping) *RowConverter {
	return &RowConverter{mapping, true, nil}
}

// NewRowConverter creates a row converter from a given FieldMapping.
func NewRowConverter(ctx context.Context, vrw types.ValueReadWriter, mapping *FieldMapping) (*RowConverter, error) {
	if nec, err := isNecessary(mapping.SrcSch, mapping.DestSch, mapping.SrcToDest); err != nil {
		return nil, err
	} else if !nec {
		return newIdentityConverter(mapping), nil
	}

	convFuncs := make(map[uint64]types.MarshalCallback, len(mapping.SrcToDest))
	for srcTag, destTag := range mapping.SrcToDest {
		destCol, destOk := mapping.DestSch.GetAllCols().GetByTag(destTag)
		srcCol, srcOk := mapping.SrcSch.GetAllCols().GetByTag(srcTag)

		if !destOk || !srcOk {
			return nil, fmt.Errorf("Could not find column being mapped. src tag: %d, dest tag: %d", srcTag, destTag)
		}

		if srcCol.TypeInfo.Equals(destCol.TypeInfo) {
			convFuncs[srcTag] = func(v types.Value) (types.Value, error) {
				return v, nil
			}
		}
		if typeinfo.IsStringType(destCol.TypeInfo) {
			convFuncs[srcTag] = func(v types.Value) (types.Value, error) {
				val, err := srcCol.TypeInfo.FormatValue(v)
				if err != nil {
					return nil, err
				}
				if val == nil {
					return types.NullValue, nil
				}
				return types.String(*val), nil
			}
		} else {
			convFuncs[srcTag] = func(v types.Value) (types.Value, error) {
				return typeinfo.Convert(ctx, vrw, v, srcCol.TypeInfo, destCol.TypeInfo)
			}
		}
	}

	return &RowConverter{mapping, false, convFuncs}, nil
}

// NewImportRowConverter creates a row converter from a given FieldMapping specifically for importing.
func NewImportRowConverter(ctx context.Context, vrw types.ValueReadWriter, mapping *FieldMapping) (*RowConverter, error) {
	if nec, err := isNecessary(mapping.SrcSch, mapping.DestSch, mapping.SrcToDest); err != nil {
		return nil, err
	} else if !nec {
		return newIdentityConverter(mapping), nil
	}

	convFuncs := make(map[uint64]types.MarshalCallback, len(mapping.SrcToDest))
	for srcTag, destTag := range mapping.SrcToDest {
		destCol, destOk := mapping.DestSch.GetAllCols().GetByTag(destTag)
		srcCol, srcOk := mapping.SrcSch.GetAllCols().GetByTag(srcTag)

		if !destOk || !srcOk {
			return nil, fmt.Errorf("Could not find column being mapped. src tag: %d, dest tag: %d", srcTag, destTag)
		}

		if srcCol.TypeInfo.Equals(destCol.TypeInfo) {
			convFuncs[srcTag] = func(v types.Value) (types.Value, error) {
				return v, nil
			}
		}
		if typeinfo.IsStringType(destCol.TypeInfo) {
			convFuncs[srcTag] = func(v types.Value) (types.Value, error) {
				val, err := srcCol.TypeInfo.FormatValue(v)
				if err != nil {
					return nil, err
				}
				if val == nil {
					return types.NullValue, nil
				}
				return types.String(*val), nil
			}
		} else if destCol.TypeInfo.Equals(typeinfo.PseudoBoolType) || destCol.TypeInfo.Equals(typeinfo.Int8Type) {
			// BIT(1) and BOOLEAN (MySQL alias for TINYINT or Int8) are both logical stand-ins for a bool type
			convFuncs[srcTag] = func(v types.Value) (types.Value, error) {
				intermediateVal, err := typeinfo.Convert(ctx, vrw, v, srcCol.TypeInfo, typeinfo.BoolType)
				if err != nil {
					return nil, err
				}
				return typeinfo.Convert(ctx, vrw, intermediateVal, typeinfo.BoolType, destCol.TypeInfo)
			}
		} else {
			convFuncs[srcTag] = func(v types.Value) (types.Value, error) {
				return typeinfo.Convert(ctx, vrw, v, srcCol.TypeInfo, destCol.TypeInfo)
			}
		}
	}

	return &RowConverter{mapping, false, convFuncs}, nil
}

// Convert takes a row maps its columns to their destination columns, and performs any type conversion needed to create
// a row of the expected destination schema.
func (rc *RowConverter) Convert(inRow row.Row) (row.Row, error) {
	if rc.IdentityConverter {
		return inRow, nil
	}

	outTaggedVals := make(row.TaggedValues, len(rc.SrcToDest))
	_, err := inRow.IterCols(func(tag uint64, val types.Value) (stop bool, err error) {
		convFunc, ok := rc.ConvFuncs[tag]

		if ok {
			outTag := rc.SrcToDest[tag]
			outVal, err := convFunc(val)

			if err != nil {
				return false, err
			}

			outTaggedVals[outTag] = outVal
		}

		return false, nil
	})

	if err != nil {
		return nil, err
	}

	return row.New(inRow.Format(), rc.DestSch, outTaggedVals)
}

func isNecessary(srcSch, destSch schema.Schema, destToSrc map[uint64]uint64) (bool, error) {
	srcCols := srcSch.GetAllCols()
	destCols := destSch.GetAllCols()

	if len(destToSrc) != srcCols.Size() || len(destToSrc) != destCols.Size() {
		return true, nil
	}

	for k, v := range destToSrc {
		if k != v {
			return true, nil
		}

		srcCol, srcOk := srcCols.GetByTag(v)
		destCol, destOk := destCols.GetByTag(k)

		if !srcOk || !destOk {
			panic("There is a bug.  FieldMapping creation should prevent this from happening")
		}

		if srcCol.IsPartOfPK != destCol.IsPartOfPK {
			return true, nil
		}

		if !srcCol.TypeInfo.Equals(destCol.TypeInfo) {
			return true, nil
		}
	}

	srcPKCols := srcSch.GetPKCols()
	destPKCols := destSch.GetPKCols()

	if srcPKCols.Size() != destPKCols.Size() {
		return true, nil
	}

	i := 0
	err := destPKCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		srcPKCol := srcPKCols.GetByIndex(i)

		if srcPKCol.Tag != col.Tag {
			return true, nil
		}

		i++
		return false, nil
	})

	if err != nil {
		return false, err
	}

	return false, nil
}
