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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/store/types"
)

const (
	varBinaryTypeParam_Length = "length"
)

// As a type, this is modeled more after MySQL's story for binary data. There, it's treated
// as a string that is interpreted as raw bytes, rather than as a bespoke data structure,
// and thus this is mirrored here in its implementation. This will minimize any differences
// that could arise.
type varBinaryType struct {
	sqlBinaryType sql.StringType
}

var _ TypeInfo = (*varBinaryType)(nil)

func CreateVarBinaryTypeFromParams(params map[string]string) (TypeInfo, error) {
	var length int64
	var err error
	if lengthStr, ok := params[varBinaryTypeParam_Length]; ok {
		length, err = strconv.ParseInt(lengthStr, 10, 64)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf(`create varbinary type info is missing param "%v"`, varBinaryTypeParam_Length)
	}
	sqlType, err := sql.CreateBinary(sqltypes.Blob, length)
	if err != nil {
		return nil, err
	}
	return &varBinaryType{sqlType}, nil
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *varBinaryType) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	if val, ok := v.(types.Blob); ok {
		s, err := ti.fromBlob(val)
		if err != nil {
			return nil, err
		}
		return string(s), nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *varBinaryType) ConvertValueToNomsValue(ctx context.Context, vrw types.ValueReadWriter, v interface{}) (types.Value, error) {
	if v == nil {
		return types.NullValue, nil
	}
	strVal, err := ti.sqlBinaryType.Convert(v)
	if err != nil {
		return nil, err
	}
	val, ok := strVal.(string)
	if ok {
		return ti.toBlob(ctx, vrw, val)
	}
	return nil, fmt.Errorf(`"%v" cannot convert value "%v" of type "%T" as it is invalid`, ti.String(), v, v)
}

// Equals implements TypeInfo interface.
func (ti *varBinaryType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*varBinaryType); ok {
		return ti.sqlBinaryType.MaxCharacterLength() == ti2.sqlBinaryType.MaxCharacterLength()
	}
	return false
}

// FormatValue implements TypeInfo interface.
func (ti *varBinaryType) FormatValue(v types.Value) (*string, error) {
	if val, ok := v.(types.Blob); ok {
		valStr, err := ti.fromBlob(val)
		if err != nil {
			return nil, err
		}
		resStr := string(valStr)
		return &resStr, nil
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a string`, ti.String(), v.Kind())
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *varBinaryType) GetTypeIdentifier() Identifier {
	return VarBinaryTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *varBinaryType) GetTypeParams() map[string]string {
	return map[string]string{
		varBinaryTypeParam_Length: strconv.FormatInt(ti.sqlBinaryType.MaxCharacterLength(), 10),
	}
}

// IsValid implements TypeInfo interface.
func (ti *varBinaryType) IsValid(v types.Value) bool {
	if _, ok := v.(types.Blob); ok {
		// This is only for this test branch
		return true
	}
	if _, ok := v.(types.Ref); ok {
		// This is only for this test branch
		return true
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return true
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *varBinaryType) NomsKind() types.NomsKind {
	return types.BlobKind
	//return types.RefKind
}

// ParseValue implements TypeInfo interface.
func (ti *varBinaryType) ParseValue(ctx context.Context, vrw types.ValueReadWriter, str *string) (types.Value, error) {
	if str == nil {
		return types.NullValue, nil
	}
	strVal, err := ti.sqlBinaryType.Convert(*str)
	if err != nil {
		return nil, err
	}
	if val, ok := strVal.(string); ok {
		return ti.toBlob(ctx, vrw, val)
	}
	return nil, fmt.Errorf(`"%v" cannot convert the string "%v" to a value`, ti.String(), str)
}

// Promote implements TypeInfo interface.
func (ti *varBinaryType) Promote() TypeInfo {
	return &varBinaryType{ti.sqlBinaryType.Promote().(sql.StringType)}
}

// String implements TypeInfo interface.
func (ti *varBinaryType) String() string {
	return fmt.Sprintf(`VarBinary(%v)`, ti.sqlBinaryType.MaxCharacterLength())
}

// ToSqlType implements TypeInfo interface.
func (ti *varBinaryType) ToSqlType() sql.Type {
	return ti.sqlBinaryType
}

func (ti *varBinaryType) fromBlob(b types.Blob) (types.String, error) {
	countBytes := make([]byte, 8)
	n, err := b.ReadAt(context.Background(), countBytes, 0)
	if err == io.EOF {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if n != 8 {
		return "", fmt.Errorf("wanted 8 bytes from blob for count, got %d", n)
	}
	count := binary.LittleEndian.Uint64(countBytes)
	str := make([]byte, count)
	n, err = b.ReadAt(context.Background(), str, 8)
	if err != nil && err != io.EOF {
		return "", err
	}
	if uint64(n) != count {
		return "", fmt.Errorf("wanted %d bytes from blob for data, got %d", count, n)
	}
	return types.String(str), nil
}

func (ti *varBinaryType) toBlob(ctx context.Context, vrw types.ValueReadWriter, s string) (types.Blob, error) {
	data := make([]byte, 8+len(s))
	binary.LittleEndian.PutUint64(data[:8], uint64(len(s)))
	copy(data[8:], s)
	b, e := types.NewBlob(ctx, vrw, bytes.NewReader(data))
	return b, e
}

func (ti *varBinaryType) toRef(ctx context.Context, vrw types.ValueReadWriter, s string) (types.Ref, error) {
	val, err := ti.toBlob(ctx, vrw, s)
	if err != nil {
		return types.Ref{}, err
	}
	valRef, err := types.NewRef(val, vrw.Format())
	if err != nil {
		return types.Ref{}, err
	}
	targetVal, err := valRef.TargetValue(ctx, vrw)
	if err != nil {
		return types.Ref{}, err
	}
	if targetVal == nil {
		_, err = vrw.WriteValue(ctx, val)
		if err != nil {
			return types.Ref{}, err
		}
	}
	return valRef, err
}
