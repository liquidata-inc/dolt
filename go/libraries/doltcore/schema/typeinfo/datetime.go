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
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/store/types"
)

const (
	datetimeTypeParam_SQL           = "sql"
	datetimeTypeParam_SQL_Date      = "date"
	datetimeTypeParam_SQL_Datetime  = "datetime"
	datetimeTypeParam_SQL_Timestamp = "timestamp"
)

type datetimeType struct {
	sqlDatetimeType sql.DatetimeType
}

var _ TypeInfo = (*datetimeType)(nil)
var (
	DateType      = &datetimeType{sql.Date}
	DatetimeType  = &datetimeType{sql.Datetime}
	TimestampType = &datetimeType{sql.Timestamp}
)

func CreateDatetimeTypeFromParams(params map[string]string) (TypeInfo, error) {
	if sqlType, ok := params[datetimeTypeParam_SQL]; ok {
		switch sqlType {
		case datetimeTypeParam_SQL_Date:
			return DateType, nil
		case datetimeTypeParam_SQL_Datetime:
			return DatetimeType, nil
		case datetimeTypeParam_SQL_Timestamp:
			return TimestampType, nil
		default:
			return nil, fmt.Errorf(`create datetime type info has invalid param "%v"`, sqlType)
		}
	} else {
		return nil, fmt.Errorf(`create datetime type info is missing param "%v"`, datetimeTypeParam_SQL)
	}
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *datetimeType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.Timestamp); ok {
		return time.Time(val).UTC(), nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *datetimeType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	//TODO: handle the zero value as a special case that is valid for all ranges
	if v == nil {
		return types.NullValue, nil
	}
	timeVal, err := ti.sqlDatetimeType.Convert(v)
	if err != nil {
		return nil, err
	}
	val, ok := timeVal.(time.Time)
	if ok {
		return types.Timestamp(val), nil
	}
	return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v)
}

// Equals implements TypeInfo interface.
func (ti *datetimeType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*datetimeType); ok {
		return ti.sqlDatetimeType.Type() == ti2.sqlDatetimeType.Type()
	}
	return false
}

// FormatValue implements TypeInfo interface.
func (ti *datetimeType) FormatValue(v types.Value) (*string, error) {
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	timeVal, err := ti.ConvertNomsValueToValue(v)
	if err != nil {
		return nil, err
	}
	val, ok := timeVal.(time.Time)
	if !ok {
		return nil, fmt.Errorf(`"%v" has unexpectedly encountered a value of type "%T" from embedded type`, ti.String(), v)
	}
	if ti.sqlDatetimeType.Type() == sqltypes.Date {
		res := val.Format(sql.DateLayout)
		return &res, nil
	} else {
		res := val.Format(sql.TimestampDatetimeLayout)
		return &res, nil
	}
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *datetimeType) GetTypeIdentifier() Identifier {
	return DatetimeTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *datetimeType) GetTypeParams() map[string]string {
	sqlParam := ""
	switch ti.sqlDatetimeType.Type() {
	case sqltypes.Date:
		sqlParam = datetimeTypeParam_SQL_Date
	case sqltypes.Datetime:
		sqlParam = datetimeTypeParam_SQL_Datetime
	case sqltypes.Timestamp:
		sqlParam = datetimeTypeParam_SQL_Timestamp
	default:
		panic(fmt.Errorf(`unknown datetime type info sql type "%v"`, ti.sqlDatetimeType.Type().String()))
	}
	return map[string]string{datetimeTypeParam_SQL: sqlParam}
}

// IsValid implements TypeInfo interface.
func (ti *datetimeType) IsValid(v types.Value) bool {
	if val, ok := v.(types.Timestamp); ok {
		_, err := ti.sqlDatetimeType.Convert(time.Time(val))
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
func (ti *datetimeType) NomsKind() types.NomsKind {
	return types.TimestampKind
}

// ParseValue implements TypeInfo interface.
func (ti *datetimeType) ParseValue(ctx context.Context, vrw types.ValueReadWriter, str *string) (types.Value, error) {
	if str == nil || *str == "" {
		return types.NullValue, nil
	}
	strVal, err := ti.sqlDatetimeType.Convert(*str)
	if err != nil {
		return nil, err
	}
	if val, ok := strVal.(time.Time); ok {
		return types.Timestamp(val), nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert the string "%v" to a value`, ti.String(), str)
}

// Promote implements TypeInfo interface.
func (ti *datetimeType) Promote() TypeInfo {
	return &datetimeType{ti.sqlDatetimeType.Promote().(sql.DatetimeType)}
}

// String implements TypeInfo interface.
func (ti *datetimeType) String() string {
	return fmt.Sprintf(`Datetime(SQL: "%v")`, ti.sqlDatetimeType.String())
}

// ToSqlType implements TypeInfo interface.
func (ti *datetimeType) ToSqlType() sql.Type {
	return ti.sqlDatetimeType
}
