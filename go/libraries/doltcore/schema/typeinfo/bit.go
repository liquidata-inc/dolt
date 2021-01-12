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

	"github.com/dolthub/dolt/go/store/types"
)

const (
	bitTypeParam_Bits = "bits"
)

// This is a dolt implementation of the MySQL type Bit, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type bitType struct {
	sqlBitType sql.BitType
}

var _ TypeInfo = (*bitType)(nil)

var PseudoBoolType TypeInfo = &bitType{sql.MustCreateBitType(1)}

func CreateBitTypeFromParams(params map[string]string) (TypeInfo, error) {
	if bitStr, ok := params[bitTypeParam_Bits]; ok {
		bitUint, err := strconv.ParseUint(bitStr, 10, 8)
		if err != nil {
			return nil, err
		}
		sqlBitType, err := sql.CreateBitType(uint8(bitUint))
		if err != nil {
			return nil, err
		}
		return &bitType{sqlBitType}, nil
	} else {
		return nil, fmt.Errorf(`create bit type info is missing param "%v"`, bitTypeParam_Bits)
	}
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *bitType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.Uint); ok {
		return uint64(val), nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *bitType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	if v == nil {
		return types.NullValue, nil
	}
	uintVal, err := ti.sqlBitType.Convert(v)
	if err != nil {
		return nil, err
	}
	val, ok := uintVal.(uint64)
	if ok {
		return types.Uint(val), nil
	}
	return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v)
}

// Equals implements TypeInfo interface.
func (ti *bitType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*bitType); ok {
		return ti.sqlBitType.NumberOfBits() == ti2.sqlBitType.NumberOfBits()
	}
	return false
}

// FormatValue implements TypeInfo interface.
func (ti *bitType) FormatValue(v types.Value) (*string, error) {
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	uintVal, err := ti.ConvertNomsValueToValue(v)
	if err != nil {
		return nil, err
	}
	val, ok := uintVal.(uint64)
	if !ok {
		return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v)
	}
	res := strconv.FormatUint(val, 10)
	return &res, nil
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *bitType) GetTypeIdentifier() Identifier {
	return BitTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *bitType) GetTypeParams() map[string]string {
	return map[string]string{
		bitTypeParam_Bits: strconv.FormatInt(int64(ti.sqlBitType.NumberOfBits()), 10),
	}
}

// IsValid implements TypeInfo interface.
func (ti *bitType) IsValid(v types.Value) bool {
	if val, ok := v.(types.Uint); ok {
		_, err := ti.sqlBitType.Convert(uint64(val))
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
func (ti *bitType) NomsKind() types.NomsKind {
	return types.UintKind
}

// ParseValue implements TypeInfo interface.
func (ti *bitType) ParseValue(ctx context.Context, vrw types.ValueReadWriter, str *string) (types.Value, error) {
	if str == nil || *str == "" {
		return types.NullValue, nil
	}
	if val, err := strconv.ParseUint(*str, 10, 64); err == nil {
		uintVal, err := ti.sqlBitType.Convert(val)
		if err != nil {
			return nil, err
		}
		if val, ok := uintVal.(uint64); ok {
			return types.Uint(val), nil
		}
	}
	strVal, err := ti.sqlBitType.Convert(*str)
	if err != nil {
		return nil, err
	}
	if val, ok := strVal.(uint64); ok {
		return types.Uint(val), nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert the string "%v" to a value`, ti.String(), str)
}

// Promote implements TypeInfo interface.
func (ti *bitType) Promote() TypeInfo {
	return &bitType{ti.sqlBitType.Promote().(sql.BitType)}
}

// String implements TypeInfo interface.
func (ti *bitType) String() string {
	return fmt.Sprintf("Bit(%v)", ti.sqlBitType.NumberOfBits())
}

// ToSqlType implements TypeInfo interface.
func (ti *bitType) ToSqlType() sql.Type {
	return ti.sqlBitType
}
