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

package typeinfo

import (
	"context"
	"fmt"
	"strconv"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/store/types"
)

type FloatWidth int8

const (
	floatTypeParam_Width    = "width"
	floatTypeParam_Width_32 = "32"
	floatTypeParam_Width_64 = "64"
)

type floatType struct {
	sqlFloatType sql.NumberType
}

var _ TypeInfo = (*floatType)(nil)
var (
	Float32Type = &floatType{sql.Float32}
	Float64Type = &floatType{sql.Float64}
)

func CreateFloatTypeFromParams(params map[string]string) (TypeInfo, error) {
	if width, ok := params[floatTypeParam_Width]; ok {
		switch width {
		case floatTypeParam_Width_32:
			return Float32Type, nil
		case floatTypeParam_Width_64:
			return Float64Type, nil
		default:
			return nil, fmt.Errorf(`create float type info has "%v" param with value "%v"`, floatTypeParam_Width, width)
		}
	}
	return nil, fmt.Errorf(`create float type info is missing "%v" param`, floatTypeParam_Width)
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *floatType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.Float); ok {
		switch ti.sqlFloatType {
		case sql.Float32:
			return float32(val), nil
		case sql.Float64:
			return float64(val), nil
		}
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *floatType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	if v == nil {
		return types.NullValue, nil
	}
	fltVal, err := ti.sqlFloatType.Convert(v)
	if err != nil {
		return nil, err
	}
	switch val := fltVal.(type) {
	case float32:
		return types.Float(val), nil
	case float64:
		return types.Float(val), nil
	default:
		return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v)
	}
}

// Equals implements TypeInfo interface.
func (ti *floatType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*floatType); ok {
		return ti.sqlFloatType.Type() == ti2.sqlFloatType.Type()
	}
	return false
}

// FormatValue implements TypeInfo interface.
func (ti *floatType) FormatValue(v types.Value) (*string, error) {
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	fltVal, err := ti.ConvertNomsValueToValue(v)
	if err != nil {
		return nil, err
	}
	switch val := fltVal.(type) {
	case float32:
		res := strconv.FormatFloat(float64(val), 'f', -1, 64)
		return &res, nil
	case float64:
		res := strconv.FormatFloat(val, 'f', -1, 64)
		return &res, nil
	default:
		return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v)
	}
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *floatType) GetTypeIdentifier() Identifier {
	return FloatTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *floatType) GetTypeParams() map[string]string {
	sqlParam := ""
	switch ti.sqlFloatType.Type() {
	case sqltypes.Float32:
		sqlParam = floatTypeParam_Width_32
	case sqltypes.Float64:
		sqlParam = floatTypeParam_Width_64
	default:
		panic(fmt.Errorf(`unknown float type info sql type "%v"`, ti.sqlFloatType.Type().String()))
	}
	return map[string]string{floatTypeParam_Width: sqlParam}
}

// IsValid implements TypeInfo interface.
func (ti *floatType) IsValid(v types.Value) bool {
	if val, ok := v.(types.Float); ok {
		_, err := ti.sqlFloatType.Convert(float64(val))
		if err != nil {
			return false
		}
		return true
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return true
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *floatType) NomsKind() types.NomsKind {
	return types.FloatKind
}

// ParseValue implements TypeInfo interface.
func (ti *floatType) ParseValue(ctx context.Context, vrw types.ValueReadWriter, str *string) (types.Value, error) {
	if str == nil || *str == "" {
		return types.NullValue, nil
	}
	return ti.ConvertValueToNomsValue(context.Background(), nil, *str)
}

// Promote implements TypeInfo interface.
func (ti *floatType) Promote() TypeInfo {
	return &floatType{ti.sqlFloatType.Promote().(sql.NumberType)}
}

// String implements TypeInfo interface.
func (ti *floatType) String() string {
	switch ti.sqlFloatType.Type() {
	case sqltypes.Float32:
		return "Float32"
	case sqltypes.Float64:
		return "Float64"
	default:
		panic(fmt.Errorf(`unknown float type info sql type "%v"`, ti.sqlFloatType.Type().String()))
	}
}

// ToSqlType implements TypeInfo interface.
func (ti *floatType) ToSqlType() sql.Type {
	return ti.sqlFloatType
}
