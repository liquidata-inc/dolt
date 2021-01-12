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
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/types"
)

func TestIntConvertNomsValueToValue(t *testing.T) {
	tests := []struct {
		typ         *intType
		input       types.Int
		output      interface{}
		expectedErr bool
	}{
		{
			Int8Type,
			120,
			int8(120),
			false,
		},
		{
			Int16Type,
			30000,
			int16(30000),
			false,
		},
		{
			Int24Type,
			7000000,
			int32(7000000),
			false,
		},
		{
			Int32Type,
			2000000000,
			int32(2000000000),
			false,
		},
		{
			Int64Type,
			math.MaxInt64,
			int64(math.MaxInt64),
			false,
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

func TestIntConvertValueToNomsValue(t *testing.T) {
	tests := []struct {
		typ         *intType
		input       interface{}
		output      types.Int
		expectedErr bool
	}{
		{
			Int8Type,
			true,
			1,
			false,
		},
		{
			Int16Type,
			int16(25),
			25,
			false,
		},
		{
			Int24Type,
			uint64(184035),
			184035,
			false,
		},
		{
			Int32Type,
			float32(312.1235),
			312,
			false,
		},
		{
			Int64Type,
			"184035",
			184035,
			false,
		},
		{
			Int24Type,
			int32(math.MaxInt32),
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

func TestIntFormatValue(t *testing.T) {
	tests := []struct {
		typ         *intType
		input       types.Int
		output      string
		expectedErr bool
	}{
		{
			Int8Type,
			120,
			"120",
			false,
		},
		{
			Int16Type,
			30000,
			"30000",
			false,
		},
		{
			Int24Type,
			7000000,
			"7000000",
			false,
		},
		{
			Int32Type,
			2000000000,
			"2000000000",
			false,
		},
		{
			Int64Type,
			math.MaxInt64,
			strconv.FormatInt(math.MaxInt64, 10),
			false,
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

func TestIntParseValue(t *testing.T) {
	tests := []struct {
		typ         *intType
		input       string
		output      types.Int
		expectedErr bool
	}{
		{
			Int8Type,
			"120",
			120,
			false,
		},
		{
			Int16Type,
			"30000",
			30000,
			false,
		},
		{
			Int24Type,
			"7000000",
			7000000,
			false,
		},
		{
			Int32Type,
			"2000000000",
			2000000000,
			false,
		},
		{
			Int64Type,
			strconv.FormatInt(math.MaxInt64, 10),
			math.MaxInt64,
			false,
		},
		{
			Int32Type,
			"something",
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
