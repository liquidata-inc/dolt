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

type unknownImpl struct{}

var _ TypeInfo = (*unknownImpl)(nil)

var UnknownType TypeInfo = &unknownImpl{}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *unknownImpl) ConvertNomsValueToValue(types.Value) (interface{}, error) {
	return nil, fmt.Errorf(`"Unknown" cannot convert any Noms value to a go value`)
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *unknownImpl) ConvertValueToNomsValue(context.Context, types.ValueReadWriter, interface{}) (types.Value, error) {
	return nil, fmt.Errorf(`"Unknown" cannot convert any go value to a Noms value`)
}

// Equals implements TypeInfo interface.
func (ti *unknownImpl) Equals(TypeInfo) bool {
	return false
}

// FormatValue implements TypeInfo interface.
func (ti *unknownImpl) FormatValue(types.Value) (*string, error) {
	return nil, fmt.Errorf(`"Unknown" cannot convert any Noms value to a string`)
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *unknownImpl) GetTypeIdentifier() Identifier {
	return UnknownTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *unknownImpl) GetTypeParams() map[string]string {
	panic("cannot persist unknown type")
}

// IsValid implements TypeInfo interface.
func (ti *unknownImpl) IsValid(types.Value) bool {
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *unknownImpl) NomsKind() types.NomsKind {
	return types.UnknownKind
}

// ParseValue implements TypeInfo interface.
func (ti *unknownImpl) ParseValue(context.Context, types.ValueReadWriter, *string) (types.Value, error) {
	return nil, fmt.Errorf(`"Unknown" cannot convert any strings to a Noms value`)
}

// Promote implements TypeInfo interface.
func (ti *unknownImpl) Promote() TypeInfo {
	return ti
}

// String implements TypeInfo interface.
func (ti *unknownImpl) String() string {
	return "Unknown"
}

// ToSqlType implements TypeInfo interface.
func (ti *unknownImpl) ToSqlType() sql.Type {
	panic(fmt.Errorf("unknown type info does not have a relevant SQL type"))
}
