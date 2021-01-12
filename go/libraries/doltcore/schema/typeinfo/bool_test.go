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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/types"
)

func TestBoolConvertNomsValueToValue(t *testing.T) {
	tests := []struct {
		input       types.Bool
		output      uint64
		expectedErr bool
	}{
		{
			false,
			0,
			false,
		},
		{
			true,
			1,
			false,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v %v`, BoolType.String(), test.input), func(t *testing.T) {
			output, err := BoolType.ConvertNomsValueToValue(test.input)
			require.NoError(t, err)
			require.Equal(t, test.output, output)
		})
	}
}

func TestBoolConvertValueToNomsValue(t *testing.T) {
	tests := []struct {
		input       interface{}
		output      types.Bool
		expectedErr bool
	}{
		{
			true,
			true,
			false,
		},
		{
			int16(25),
			true,
			false,
		},
		{
			uint32(84875),
			true,
			false,
		},
		{
			"FALSE",
			false,
			false,
		},
		{
			"0",
			false,
			false,
		},
		{
			"44",
			true,
			false,
		},
		{
			[]byte{32, 1, 84},
			false,
			true,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v %v`, BoolType.String(), test.input), func(t *testing.T) {
			vrw := types.NewInternalValueStore()
			output, err := BoolType.ConvertValueToNomsValue(context.Background(), vrw, test.input)
			if !test.expectedErr {
				require.NoError(t, err)
				assert.Equal(t, test.output, output)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestBoolFormatValue(t *testing.T) {
	tests := []struct {
		input       types.Bool
		output      string
		expectedErr bool
	}{
		{
			false,
			"0",
			false,
		},
		{
			true,
			"1",
			false,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v %v`, BoolType.String(), test.input), func(t *testing.T) {
			output, err := BoolType.FormatValue(test.input)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.output, *output)
			}
		})
	}
}

func TestBoolParseValue(t *testing.T) {
	tests := []struct {
		input       string
		output      types.Bool
		expectedErr bool
	}{
		{
			"0",
			false,
			false,
		},
		{
			"25",
			true,
			false,
		},
		{
			"true",
			true,
			false,
		},
		{
			"something",
			false,
			true,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v %v`, BoolType.String(), test.input), func(t *testing.T) {
			output, err := BoolType.ParseValue(context.Background(), nil, &test.input)
			if !test.expectedErr {
				require.NoError(t, err)
				assert.Equal(t, test.output, output)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
