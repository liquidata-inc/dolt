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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/types"
)

func TestEnumConvertNomsValueToValue(t *testing.T) {
	tests := []struct {
		typ         *enumType
		input       types.Uint
		output      string
		expectedErr bool
	}{
		{
			generateEnumType(t, 3),
			1,
			"aaaa",
			false,
		},
		{
			generateEnumType(t, 5),
			2,
			"aaab",
			false,
		},
		{
			generateEnumType(t, 8),
			3,
			"aaac",
			false,
		},
		{
			generateEnumType(t, 7),
			7,
			"aaag",
			false,
		},
		{
			generateEnumType(t, 2),
			0,
			"",
			true,
		},
		{
			generateEnumType(t, 3),
			4,
			"",
			true,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v %v`, test.typ.String(), test.input), func(t *testing.T) {
			output, err := test.typ.ConvertNomsValueToValue(test.input)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.output, output)
			}
		})
	}
}

func TestEnumConvertValueToNomsValue(t *testing.T) {
	tests := []struct {
		typ         *enumType
		input       interface{}
		output      types.Uint
		expectedErr bool
	}{
		{
			generateEnumType(t, 4),
			"aaac",
			3,
			false,
		},
		{
			generateEnumType(t, 7),
			uint64(3),
			3,
			false,
		},
		{
			generateEnumType(t, 4),
			"dog",
			0,
			true,
		},
		{
			generateEnumType(t, 3),
			true,
			0,
			true,
		},
		{
			generateEnumType(t, 10),
			time.Unix(137849, 0),
			0,
			true,
		},
		{
			generateEnumType(t, 5),
			complex128(14i),
			0,
			true,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v %v`, test.typ.String(), test.input), func(t *testing.T) {
			vrw := types.NewInternalValueStore()
			output, err := test.typ.ConvertValueToNomsValue(context.Background(), vrw, test.input)
			if !test.expectedErr {
				require.NoError(t, err)
				assert.Equal(t, test.output, output)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestEnumFormatValue(t *testing.T) {
	tests := []struct {
		typ         *enumType
		input       types.Uint
		output      string
		expectedErr bool
	}{
		{
			generateEnumType(t, 3),
			1,
			"aaaa",
			false,
		},
		{
			generateEnumType(t, 5),
			2,
			"aaab",
			false,
		},
		{
			generateEnumType(t, 8),
			3,
			"aaac",
			false,
		},
		{
			generateEnumType(t, 7),
			7,
			"aaag",
			false,
		},
		{
			generateEnumType(t, 2),
			0,
			"",
			true,
		},
		{
			generateEnumType(t, 3),
			4,
			"",
			true,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v %v`, test.typ.String(), test.input), func(t *testing.T) {
			output, err := test.typ.FormatValue(test.input)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.output, *output)
			}
		})
	}
}

func TestEnumParseValue(t *testing.T) {
	tests := []struct {
		typ         *enumType
		input       string
		output      types.Uint
		expectedErr bool
	}{
		{
			generateEnumType(t, 3),
			"aaaa",
			1,
			false,
		},
		{
			generateEnumType(t, 5),
			"aaab",
			2,
			false,
		},
		{
			generateEnumType(t, 8),
			"aaac",
			3,
			false,
		},
		{
			generateEnumType(t, 7),
			"aaag",
			7,
			false,
		},
		{
			generateEnumType(t, 2),
			"dog",
			0,
			true,
		},
		{
			generateEnumType(t, 3),
			"aaad",
			4,
			true,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v %v`, test.typ.String(), test.input), func(t *testing.T) {
			vrw := types.NewInternalValueStore()
			output, err := test.typ.ParseValue(context.Background(), vrw, &test.input)
			if !test.expectedErr {
				require.NoError(t, err)
				assert.Equal(t, test.output, output)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
