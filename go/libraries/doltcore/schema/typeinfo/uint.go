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

const (
	uintTypeParam_Width    = "width"
	uintTypeParam_Width_8  = "8"
	uintTypeParam_Width_16 = "16"
	uintTypeParam_Width_24 = "24"
	uintTypeParam_Width_32 = "32"
	uintTypeParam_Width_64 = "64"
)

type uintType struct {
	sqlUintType sql.NumberType
}

var _ TypeInfo = (*uintType)(nil)
var (
	Uint8Type  = &uintType{sql.Uint8}
	Uint16Type = &uintType{sql.Uint16}
	Uint24Type = &uintType{sql.Uint24}
	Uint32Type = &uintType{sql.Uint32}
	Uint64Type = &uintType{sql.Uint64}
)

func CreateUintTypeFromParams(params map[string]string) (TypeInfo, error) {
	if width, ok := params[uintTypeParam_Width]; ok {
		switch width {
		case uintTypeParam_Width_8:
			return Uint8Type, nil
		case uintTypeParam_Width_16:
			return Uint16Type, nil
		case uintTypeParam_Width_24:
			return Uint24Type, nil
		case uintTypeParam_Width_32:
			return Uint32Type, nil
		case uintTypeParam_Width_64:
			return Uint64Type, nil
		default:
			return nil, fmt.Errorf(`create uint type info has "%v" param with value "%v"`, uintTypeParam_Width, width)
		}
	}
	return nil, fmt.Errorf(`create uint type info is missing "%v" param`, uintTypeParam_Width)
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *uintType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.Uint); ok {
		switch ti.sqlUintType {
		case sql.Uint8:
			return uint8(val), nil
		case sql.Uint16:
			return uint16(val), nil
		case sql.Uint24:
			return uint32(val), nil
		case sql.Uint32:
			return uint32(val), nil
		case sql.Uint64:
			return uint64(val), nil
		}
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *uintType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	if v == nil {
		return types.NullValue, nil
	}
	uintVal, err := ti.sqlUintType.Convert(v)
	if err != nil {
		return nil, err
	}
	switch val := uintVal.(type) {
	case uint8:
		return types.Uint(val), nil
	case uint16:
		return types.Uint(val), nil
	case uint32:
		return types.Uint(val), nil
	case uint64:
		return types.Uint(val), nil
	default:
		return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v)
	}
}

// Equals implements TypeInfo interface.
func (ti *uintType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*uintType); ok {
		return ti.sqlUintType.Type() == ti2.sqlUintType.Type()
	}
	return false
}

// FormatValue implements TypeInfo interface.
func (ti *uintType) FormatValue(v types.Value) (*string, error) {
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	uintVal, err := ti.ConvertNomsValueToValue(v)
	if err != nil {
		return nil, err
	}
	switch val := uintVal.(type) {
	case uint8:
		res := strconv.FormatUint(uint64(val), 10)
		return &res, nil
	case uint16:
		res := strconv.FormatUint(uint64(val), 10)
		return &res, nil
	case uint32:
		res := strconv.FormatUint(uint64(val), 10)
		return &res, nil
	case uint64:
		res := strconv.FormatUint(val, 10)
		return &res, nil
	default:
		return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v)
	}
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *uintType) GetTypeIdentifier() Identifier {
	return UintTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *uintType) GetTypeParams() map[string]string {
	sqlParam := ""
	switch ti.sqlUintType.Type() {
	case sqltypes.Uint8:
		sqlParam = uintTypeParam_Width_8
	case sqltypes.Uint16:
		sqlParam = uintTypeParam_Width_16
	case sqltypes.Uint24:
		sqlParam = uintTypeParam_Width_24
	case sqltypes.Uint32:
		sqlParam = uintTypeParam_Width_32
	case sqltypes.Uint64:
		sqlParam = uintTypeParam_Width_64
	default:
		panic(fmt.Errorf(`unknown uint type info sql type "%v"`, ti.sqlUintType.Type().String()))
	}
	return map[string]string{uintTypeParam_Width: sqlParam}
}

// IsValid implements TypeInfo interface.
func (ti *uintType) IsValid(v types.Value) bool {
	if val, ok := v.(types.Uint); ok {
		_, err := ti.sqlUintType.Convert(uint64(val))
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
func (ti *uintType) NomsKind() types.NomsKind {
	return types.UintKind
}

// ParseValue implements TypeInfo interface.
func (ti *uintType) ParseValue(ctx context.Context, vrw types.ValueReadWriter, str *string) (types.Value, error) {
	if str == nil || *str == "" {
		return types.NullValue, nil
	}
	return ti.ConvertValueToNomsValue(context.Background(), nil, *str)
}

// Promote implements TypeInfo interface.
func (ti *uintType) Promote() TypeInfo {
	return &uintType{ti.sqlUintType.Promote().(sql.NumberType)}
}

// String implements TypeInfo interface.
func (ti *uintType) String() string {
	switch ti.sqlUintType.Type() {
	case sqltypes.Uint8:
		return "Uint8"
	case sqltypes.Uint16:
		return "Uint16"
	case sqltypes.Uint24:
		return "Uint24"
	case sqltypes.Uint32:
		return "Uint32"
	case sqltypes.Uint64:
		return "Uint64"
	default:
		panic(fmt.Errorf(`unknown uint type info sql type "%v"`, ti.sqlUintType.Type().String()))
	}
}

// ToSqlType implements TypeInfo interface.
func (ti *uintType) ToSqlType() sql.Type {
	return ti.sqlUintType
}
