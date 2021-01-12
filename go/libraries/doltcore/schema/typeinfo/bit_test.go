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

func TestBitConvertNomsValueToValue(t *testing.T) {
	tests := []struct {
		typ         *bitType
		input       types.Uint
		output      uint64
		expectedErr bool
	}{
		{
			generateBitType(t, 1),
			0,
			0,
			false,
		},
		{
			generateBitType(t, 1),
			1,
			1,
			false,
		},
		{
			generateBitType(t, 11),
			250,
			250,
			false,
		},
		{
			generateBitType(t, 64),
			math.MaxUint64,
			math.MaxUint64,
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

func TestBitConvertValueToNomsValue(t *testing.T) {
	tests := []struct {
		typ         *bitType
		input       interface{}
		output      types.Uint
		expectedErr bool
	}{
		{
			generateBitType(t, 1),
			true,
			1,
			false,
		},
		{
			generateBitType(t, 5),
			int16(25),
			25,
			false,
		},
		{
			generateBitType(t, 64),
			uint64(287946293486),
			287946293486,
			false,
		},
		{
			generateBitType(t, 3),
			float32(78.3),
			0,
			true,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v %v`, test.typ.String(), test.input), func(t *testing.T) {
			output, err := test.typ.ConvertValueToNomsValue(context.Background(), nil, test.input)
			if !test.expectedErr {
				require.NoError(t, err)
				assert.Equal(t, test.output, output)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestBitFormatValue(t *testing.T) {
	tests := []struct {
		typ         *bitType
		input       types.Uint
		output      string
		expectedErr bool
	}{
		{
			generateBitType(t, 1),
			0,
			"0",
			false,
		},
		{
			generateBitType(t, 1),
			1,
			"1",
			false,
		},
		{
			generateBitType(t, 11),
			250,
			"250",
			false,
		},
		{
			generateBitType(t, 64),
			math.MaxUint64,
			strconv.FormatUint(math.MaxUint64, 10),
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

func TestBitParseValue(t *testing.T) {
	tests := []struct {
		typ         *bitType
		input       string
		output      types.Uint
		expectedErr bool
	}{
		{
			generateBitType(t, 1),
			"0",
			0,
			false,
		},
		{
			generateBitType(t, 5),
			"25",
			25,
			false,
		},
		{
			generateBitType(t, 64),
			"287946293486",
			287946293486,
			false,
		},
		{
			generateBitType(t, 3),
			"78.3",
			0,
			true,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v %v`, test.typ.String(), test.input), func(t *testing.T) {
			output, err := test.typ.ParseValue(context.Background(), nil, &test.input)
			if !test.expectedErr {
				require.NoError(t, err)
				assert.Equal(t, test.output, output)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
