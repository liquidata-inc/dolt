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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/store/types"
)

// This is a dolt implementation of the MySQL type Time, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type timeType struct {
	sqlTimeType sql.TimeType
}

var _ TypeInfo = (*timeType)(nil)

var TimeType = &timeType{sql.Time}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *timeType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.Int); ok {
		return ti.sqlTimeType.Unmarshal(int64(val)), nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *timeType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	if v == nil {
		return types.NullValue, nil
	}
	val, err := ti.sqlTimeType.Marshal(v)
	if err != nil {
		return nil, err
	}
	return types.Int(val), nil
}

// Equals implements TypeInfo interface.
func (ti *timeType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	_, ok := other.(*timeType)
	return ok
}

// FormatValue implements TypeInfo interface.
func (ti *timeType) FormatValue(v types.Value) (*string, error) {
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	strVal, err := ti.ConvertNomsValueToValue(v)
	if err != nil {
		return nil, err
	}
	val, ok := strVal.(string)
	if !ok {
		return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v)
	}
	return &val, nil
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *timeType) GetTypeIdentifier() Identifier {
	return TimeTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *timeType) GetTypeParams() map[string]string {
	return nil
}

// IsValid implements TypeInfo interface.
func (ti *timeType) IsValid(v types.Value) bool {
	if _, ok := v.(types.Int); ok {
		return true
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return true
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *timeType) NomsKind() types.NomsKind {
	return types.IntKind
}

// ParseValue implements TypeInfo interface.
func (ti *timeType) ParseValue(ctx context.Context, vrw types.ValueReadWriter, str *string) (types.Value, error) {
	if str == nil || *str == "" {
		return types.NullValue, nil
	}
	val, err := ti.sqlTimeType.Marshal(*str)
	if err != nil {
		return nil, err
	}
	return types.Int(val), nil
}

// Promote implements TypeInfo interface.
func (ti *timeType) Promote() TypeInfo {
	return &timeType{ti.sqlTimeType.Promote().(sql.TimeType)}
}

// String implements TypeInfo interface.
func (ti *timeType) String() string {
	return "Time"
}

// ToSqlType implements TypeInfo interface.
func (ti *timeType) ToSqlType() sql.Type {
	return ti.sqlTimeType
}
