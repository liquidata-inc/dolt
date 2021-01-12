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

// This is a dolt implementation of the MySQL type Year, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type yearType struct {
	sqlYearType sql.YearType
}

var _ TypeInfo = (*yearType)(nil)

var YearType = &yearType{sql.Year}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *yearType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.Int); ok {
		return int16(val), nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *yearType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	if v == nil {
		return types.NullValue, nil
	}
	intVal, err := ti.sqlYearType.Convert(v)
	if err != nil {
		return nil, err
	}
	val, ok := intVal.(int16)
	if ok {
		return types.Int(val), nil
	}
	return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v)
}

// Equals implements TypeInfo interface.
func (ti *yearType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	_, ok := other.(*yearType)
	return ok
}

// FormatValue implements TypeInfo interface.
func (ti *yearType) FormatValue(v types.Value) (*string, error) {
	if val, ok := v.(types.Int); ok {
		convVal, err := ti.ConvertNomsValueToValue(val)
		if err != nil {
			return nil, err
		}
		val, ok := convVal.(int16)
		if !ok {
			return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v)
		}
		res := strconv.FormatInt(int64(val), 10)
		return &res, nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a string`, ti.String(), v.Kind())
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *yearType) GetTypeIdentifier() Identifier {
	return YearTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *yearType) GetTypeParams() map[string]string {
	return nil
}

// IsValid implements TypeInfo interface.
func (ti *yearType) IsValid(v types.Value) bool {
	if val, ok := v.(types.Int); ok {
		_, err := ti.sqlYearType.Convert(int64(val))
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
func (ti *yearType) NomsKind() types.NomsKind {
	return types.IntKind
}

// ParseValue implements TypeInfo interface.
func (ti *yearType) ParseValue(ctx context.Context, vrw types.ValueReadWriter, str *string) (types.Value, error) {
	if str == nil || *str == "" {
		return types.NullValue, nil
	}
	intVal, err := ti.sqlYearType.Convert(*str)
	if err != nil {
		return nil, err
	}
	if val, ok := intVal.(int16); ok {
		return types.Int(val), nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert the string "%v" to a value`, ti.String(), str)
}

// Promote implements TypeInfo interface.
func (ti *yearType) Promote() TypeInfo {
	return &yearType{ti.sqlYearType.Promote().(sql.YearType)}
}

// String implements TypeInfo interface.
func (ti *yearType) String() string {
	return "Year"
}

// ToSqlType implements TypeInfo interface.
func (ti *yearType) ToSqlType() sql.Type {
	return ti.sqlYearType
}
