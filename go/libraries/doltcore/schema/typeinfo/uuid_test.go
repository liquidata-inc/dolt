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
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/types"
)

func TestUuidConvertNomsValueToValue(t *testing.T) {
	tests := []struct {
		input  types.UUID
		output string
	}{
		{
			types.UUID(uuid.UUID{0}),
			"00000000-0000-0000-0000-000000000000",
		},
		{
			types.UUID(uuid.UUID{1, 2, 3, 4}),
			"01020304-0000-0000-0000-000000000000",
		},
		{
			types.UUID(uuid.UUID{11, 22, 33, 44, 55, 66, 77, 88, 99}),
			"0b16212c-3742-4d58-6300-000000000000",
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v %v`, UuidType.String(), test.input), func(t *testing.T) {
			output, err := UuidType.ConvertNomsValueToValue(test.input)
			require.NoError(t, err)
			require.Equal(t, test.output, output)
		})
	}
}

func TestUuidConvertValueToNomsValue(t *testing.T) {
	tests := []struct {
		input       interface{}
		output      types.UUID
		expectedErr bool
	}{
		{
			uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			types.UUID(uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}),
			false,
		},
		{
			"01020304-0506-0708-090a-0b0c0d0e0f10",
			types.UUID(uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}),
			false,
		},
		{
			int8(1),
			types.UUID{},
			true,
		},
		{
			int16(1),
			types.UUID{},
			true,
		},
		{
			int32(1),
			types.UUID{},
			true,
		},
		{
			int64(1),
			types.UUID{},
			true,
		},
		{
			uint8(1),
			types.UUID{},
			true,
		},
		{
			uint16(1),
			types.UUID{},
			true,
		},
		{
			uint32(1),
			types.UUID{},
			true,
		},
		{
			uint64(1),
			types.UUID{},
			true,
		},
		{
			false,
			types.UUID{},
			true,
		},
		{
			"something",
			types.UUID{},
			true,
		},
		{
			"",
			types.UUID{},
			true,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v %v`, UuidType.String(), test.input), func(t *testing.T) {
			vrw := types.NewInternalValueStore()
			output, err := UuidType.ConvertValueToNomsValue(context.Background(), vrw, test.input)
			if !test.expectedErr {
				require.NoError(t, err)
				assert.Equal(t, test.output, output, "%v\n%v", test.output, output)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestUuidFormatValue(t *testing.T) {
	tests := []struct {
		input  types.UUID
		output string
	}{
		{
			types.UUID(uuid.UUID{0}),
			"00000000-0000-0000-0000-000000000000",
		},
		{
			types.UUID(uuid.UUID{1, 2, 3, 4}),
			"01020304-0000-0000-0000-000000000000",
		},
		{
			types.UUID(uuid.UUID{11, 22, 33, 44, 55, 66, 77, 88, 99}),
			"0b16212c-3742-4d58-6300-000000000000",
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v %v`, UuidType.String(), test.input), func(t *testing.T) {
			output, err := UuidType.FormatValue(test.input)
			require.NoError(t, err)
			require.Equal(t, test.output, *output)
		})
	}
}

func TestUuidParseValue(t *testing.T) {
	tests := []struct {
		input       string
		output      types.UUID
		expectedErr bool
	}{
		{
			"01020304-0000-0000-0000-000000000000",
			types.UUID(uuid.UUID{1, 2, 3, 4}),
			false,
		},
		{
			"01020304-0506-0708-090a-0b0c0d0e0f10",
			types.UUID(uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}),
			false,
		},
		{
			"something",
			types.UUID{},
			true,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v %v`, UuidType.String(), test.input), func(t *testing.T) {
			vrw := types.NewInternalValueStore()
			output, err := UuidType.ParseValue(context.Background(), vrw, &test.input)
			if !test.expectedErr {
				require.NoError(t, err)
				assert.Equal(t, test.output, output)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
