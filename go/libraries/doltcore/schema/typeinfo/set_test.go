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

func TestSetConvertNomsValueToValue(t *testing.T) {
	tests := []struct {
		typ         *setType
		input       types.Uint
		output      string
		expectedErr bool
	}{
		{
			generateSetType(t, 2),
			0,
			"",
			false,
		},
		{
			generateSetType(t, 3),
			1,
			"aa",
			false,
		},
		{
			generateSetType(t, 5),
			2,
			"ab",
			false,
		},
		{
			generateSetType(t, 8),
			3,
			"aa,ab",
			false,
		},
		{
			generateSetType(t, 7),
			4,
			"ac",
			false,
		},
		{
			generateSetType(t, 4),
			7,
			"aa,ab,ac",
			false,
		},
		{
			generateSetType(t, 3),
			8,
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

func TestSetConvertValueToNomsValue(t *testing.T) {
	tests := []struct {
		typ         *setType
		input       interface{}
		output      types.Uint
		expectedErr bool
	}{
		{
			generateSetType(t, 4),
			"aa,ab",
			3,
			false,
		},
		{
			generateSetType(t, 7),
			uint64(3),
			3,
			false,
		},
		{
			generateSetType(t, 3),
			true,
			0,
			true,
		},
		{
			generateSetType(t, 10),
			time.Unix(137849, 0),
			0,
			true,
		},
		{
			generateSetType(t, 5),
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

func TestSetFormatValue(t *testing.T) {
	tests := []struct {
		typ         *setType
		input       types.Uint
		output      string
		expectedErr bool
	}{
		{
			generateSetType(t, 2),
			0,
			"",
			false,
		},
		{
			generateSetType(t, 3),
			1,
			"aa",
			false,
		},
		{
			generateSetType(t, 5),
			2,
			"ab",
			false,
		},
		{
			generateSetType(t, 8),
			3,
			"aa,ab",
			false,
		},
		{
			generateSetType(t, 7),
			4,
			"ac",
			false,
		},
		{
			generateSetType(t, 4),
			7,
			"aa,ab,ac",
			false,
		},
		{
			generateSetType(t, 3),
			8,
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

func TestSetParseValue(t *testing.T) {
	tests := []struct {
		typ         *setType
		input       string
		output      types.Uint
		expectedErr bool
	}{
		{
			generateSetType(t, 2),
			"",
			0,
			false,
		},
		{
			generateSetType(t, 3),
			"aa",
			1,
			false,
		},
		{
			generateSetType(t, 5),
			"ab",
			2,
			false,
		},
		{
			generateSetType(t, 8),
			"aa,ab",
			3,
			false,
		},
		{
			generateSetType(t, 7),
			"ac",
			4,
			false,
		},
		{
			generateSetType(t, 4),
			"aa,ab,ac",
			7,
			false,
		},
		{
			generateSetType(t, 3),
			"ad",
			0,
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
