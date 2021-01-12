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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/types"
)

func TestFloatConvertNomsValueToValue(t *testing.T) {
	tests := []struct {
		typ         *floatType
		input       types.Float
		output      interface{}
		expectedErr bool
	}{
		{
			Float32Type,
			0,
			float32(0),
			false,
		},
		{
			Float64Type,
			1,
			float64(1),
			false,
		},
		{
			Float32Type,
			250,
			float32(250),
			false,
		},
		{
			Float64Type,
			math.MaxFloat64,
			float64(math.MaxFloat64),
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

func TestFloatConvertValueToNomsValue(t *testing.T) {
	tests := []struct {
		typ         *floatType
		input       interface{}
		output      types.Float
		expectedErr bool
	}{
		{
			Float32Type,
			true,
			1,
			false,
		},
		{
			Float64Type,
			"25",
			25,
			false,
		},
		{
			Float64Type,
			uint64(287946293486),
			287946293486,
			false,
		},
		{
			Float32Type,
			time.Unix(137849, 0),
			137849,
			false,
		},
		{
			Float64Type,
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

func TestFloatFormatValue(t *testing.T) {
	tests := []struct {
		typ         *floatType
		input       types.Float
		output      string
		expectedErr bool
	}{
		{
			Float32Type,
			0,
			"0",
			false,
		},
		{
			Float64Type,
			1,
			"1",
			false,
		},
		{
			Float32Type,
			250,
			"250",
			false,
		},
		{
			Float64Type,
			math.MaxFloat64,
			strconv.FormatFloat(math.MaxFloat64, 'f', -1, 64),
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

func TestFloatParseValue(t *testing.T) {
	tests := []struct {
		typ         *floatType
		input       string
		output      types.Float
		expectedErr bool
	}{
		{
			Float32Type,
			"423.5",
			423.5,
			false,
		},
		{
			Float64Type,
			"81277392850347",
			81277392850347,
			false,
		},
		{
			Float64Type,
			"12345.03125",
			12345.03125,
			false,
		},
		{
			Float64Type,
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
