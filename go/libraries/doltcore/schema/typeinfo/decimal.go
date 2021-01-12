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
	"github.com/shopspring/decimal"

	"github.com/dolthub/dolt/go/store/types"
)

const (
	decimalTypeParam_Precision = "prec"
	decimalTypeParam_Scale     = "scale"
)

// This is a dolt implementation of the MySQL type Decimal, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type decimalType struct {
	sqlDecimalType sql.DecimalType
}

var _ TypeInfo = (*decimalType)(nil)

func CreateDecimalTypeFromParams(params map[string]string) (TypeInfo, error) {
	var precision uint8
	if precisionStr, ok := params[decimalTypeParam_Precision]; ok {
		precisionUint, err := strconv.ParseUint(precisionStr, 10, 8)
		if err != nil {
			return nil, err
		}
		precision = uint8(precisionUint)
	} else {
		return nil, fmt.Errorf(`create decimal type info is missing param "%v"`, decimalTypeParam_Precision)
	}
	var scale uint8
	if scaleStr, ok := params[decimalTypeParam_Scale]; ok {
		scaleUint, err := strconv.ParseUint(scaleStr, 10, 8)
		if err != nil {
			return nil, err
		}
		scale = uint8(scaleUint)
	} else {
		return nil, fmt.Errorf(`create decimal type info is missing param "%v"`, decimalTypeParam_Scale)
	}
	sqlDecimalType, err := sql.CreateDecimalType(precision, scale)
	if err != nil {
		return nil, err
	}
	return &decimalType{sqlDecimalType}, nil
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *decimalType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.Decimal); ok {
		return decimal.Decimal(val).StringFixed(int32(ti.sqlDecimalType.Scale())), nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *decimalType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	if v == nil {
		return types.NullValue, nil
	}
	decVal, err := ti.sqlDecimalType.ConvertToDecimal(v)
	if err != nil {
		return nil, err
	}
	if !decVal.Valid {
		return nil, fmt.Errorf(`"%v" has unexpectedly encountered a null value from embedded type`, ti.String())
	}
	return types.Decimal(decVal.Decimal), nil
}

// Equals implements TypeInfo interface.
func (ti *decimalType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*decimalType); ok {
		return ti.sqlDecimalType.Precision() == ti2.sqlDecimalType.Precision() &&
			ti.sqlDecimalType.Scale() == ti2.sqlDecimalType.Scale()
	}
	return false
}

// FormatValue implements TypeInfo interface.
func (ti *decimalType) FormatValue(v types.Value) (*string, error) {
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
func (ti *decimalType) GetTypeIdentifier() Identifier {
	return DecimalTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *decimalType) GetTypeParams() map[string]string {
	return map[string]string{
		decimalTypeParam_Precision: strconv.FormatUint(uint64(ti.sqlDecimalType.Precision()), 10),
		decimalTypeParam_Scale:     strconv.FormatUint(uint64(ti.sqlDecimalType.Scale()), 10),
	}
}

// IsValid implements TypeInfo interface.
func (ti *decimalType) IsValid(v types.Value) bool {
	if val, ok := v.(types.Decimal); ok {
		_, err := ti.sqlDecimalType.Convert(decimal.Decimal(val))
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
func (ti *decimalType) NomsKind() types.NomsKind {
	return types.DecimalKind
}

// ParseValue implements TypeInfo interface.
func (ti *decimalType) ParseValue(ctx context.Context, vrw types.ValueReadWriter, str *string) (types.Value, error) {
	if str == nil || *str == "" {
		return types.NullValue, nil
	}
	return ti.ConvertValueToNomsValue(context.Background(), nil, *str)
}

// Promote implements TypeInfo interface.
func (ti *decimalType) Promote() TypeInfo {
	return &decimalType{ti.sqlDecimalType.Promote().(sql.DecimalType)}
}

// String implements TypeInfo interface.
func (ti *decimalType) String() string {
	return fmt.Sprintf("Decimal(%v, %v)", ti.sqlDecimalType.Precision(), ti.sqlDecimalType.Scale())
}

// ToSqlType implements TypeInfo interface.
func (ti *decimalType) ToSqlType() sql.Type {
	return ti.sqlDecimalType
}
