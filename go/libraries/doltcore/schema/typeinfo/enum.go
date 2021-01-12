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
	"encoding/gob"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/store/types"
)

const (
	enumTypeParam_Collation = "collate"
	enumTypeParam_Values    = "vals"
)

// This is a dolt implementation of the MySQL type Enum, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type enumType struct {
	sqlEnumType sql.EnumType
}

var _ TypeInfo = (*enumType)(nil)

func CreateEnumTypeFromParams(params map[string]string) (TypeInfo, error) {
	var collation sql.Collation
	var err error
	if collationStr, ok := params[enumTypeParam_Collation]; ok {
		collation, err = sql.ParseCollation(nil, &collationStr, false)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf(`create enum type info is missing param "%v"`, enumTypeParam_Collation)
	}
	var values []string
	if valuesStr, ok := params[enumTypeParam_Values]; ok {
		dec := gob.NewDecoder(strings.NewReader(valuesStr))
		err = dec.Decode(&values)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf(`create enum type info is missing param "%v"`, enumTypeParam_Values)
	}
	sqlEnumType, err := sql.CreateEnumType(values, collation)
	if err != nil {
		return nil, err
	}
	return &enumType{sqlEnumType}, nil
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *enumType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.Uint); ok {
		res, err := ti.sqlEnumType.Unmarshal(int64(val))
		if err != nil {
			return nil, fmt.Errorf(`"%v" cannot convert "%v" to value`, ti.String(), val)
		}
		return res, nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *enumType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	if v == nil {
		return types.NullValue, nil
	}
	val, err := ti.sqlEnumType.Marshal(v)
	if err != nil {
		return nil, err
	}
	return types.Uint(val), nil
}

// Equals implements TypeInfo interface.
func (ti *enumType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*enumType); ok && ti.sqlEnumType.NumberOfElements() == ti2.sqlEnumType.NumberOfElements() {
		tiVals := ti.sqlEnumType.Values()
		ti2Vals := ti2.sqlEnumType.Values()
		for i := range tiVals {
			if tiVals[i] != ti2Vals[i] {
				return false
			}
		}
		return true
	}
	return false
}

// FormatValue implements TypeInfo interface.
func (ti *enumType) FormatValue(v types.Value) (*string, error) {
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
func (ti *enumType) GetTypeIdentifier() Identifier {
	return EnumTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *enumType) GetTypeParams() map[string]string {
	var sb strings.Builder
	enc := gob.NewEncoder(&sb)
	err := enc.Encode(ti.sqlEnumType.Values())
	// this should never error, encoding an array of strings should always succeed
	if err != nil {
		panic(err)
	}
	return map[string]string{
		enumTypeParam_Collation: ti.sqlEnumType.Collation().String(),
		enumTypeParam_Values:    sb.String(),
	}
}

// IsValid implements TypeInfo interface.
func (ti *enumType) IsValid(v types.Value) bool {
	if val, ok := v.(types.Uint); ok {
		_, err := ti.sqlEnumType.Unmarshal(int64(val))
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
func (ti *enumType) NomsKind() types.NomsKind {
	return types.UintKind
}

// ParseValue implements TypeInfo interface.
func (ti *enumType) ParseValue(ctx context.Context, vrw types.ValueReadWriter, str *string) (types.Value, error) {
	if str == nil || *str == "" {
		return types.NullValue, nil
	}
	val, err := ti.sqlEnumType.Marshal(*str)
	if err != nil {
		return nil, err
	}
	return types.Uint(val), nil
}

// Promote implements TypeInfo interface.
func (ti *enumType) Promote() TypeInfo {
	return &enumType{ti.sqlEnumType.Promote().(sql.EnumType)}
}

// String implements TypeInfo interface.
func (ti *enumType) String() string {
	return fmt.Sprintf(`Enum(Collation: %v, Values: %v)`, ti.sqlEnumType.Collation().String(), strings.Join(ti.sqlEnumType.Values(), ", "))
}

// ToSqlType implements TypeInfo interface.
func (ti *enumType) ToSqlType() sql.Type {
	return ti.sqlEnumType
}
