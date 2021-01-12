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

func TestYearConvertNomsValueToValue(t *testing.T) {
	tests := []struct {
		input       types.Int
		output      int16
		expectedErr bool
	}{
		{
			0,
			0,
			false,
		},
		{
			1901,
			1901,
			false,
		},
		{
			2000,
			2000,
			false,
		},
		{
			2155,
			2155,
			false,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v %v`, YearType.String(), test.input), func(t *testing.T) {
			output, err := YearType.ConvertNomsValueToValue(test.input)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.output, output)
			}
		})
	}
}

func TestYearConvertValueToNomsValue(t *testing.T) {
	tests := []struct {
		input       interface{}
		output      types.Int
		expectedErr bool
	}{
		{
			time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC),
			2001,
			false,
		},
		{
			int8(70),
			1970,
			false,
		},
		{
			uint64(89),
			1989,
			false,
		},
		{
			"5",
			2005,
			false,
		},
		{
			float32(7884.3),
			0,
			true,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v %v`, YearType.String(), test.input), func(t *testing.T) {
			vrw := types.NewInternalValueStore()
			output, err := YearType.ConvertValueToNomsValue(context.Background(), vrw, test.input)
			if !test.expectedErr {
				require.NoError(t, err)
				assert.Equal(t, test.output, output)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestYearFormatValue(t *testing.T) {
	tests := []struct {
		input       types.Int
		output      string
		expectedErr bool
	}{
		{
			2001,
			"2001",
			false,
		},
		{
			1901,
			"1901",
			false,
		},
		{
			2000,
			"2000",
			false,
		},
		{
			1989,
			"1989",
			false,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v %v`, YearType.String(), test.input), func(t *testing.T) {
			output, err := YearType.FormatValue(test.input)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.output, *output)
			}
		})
	}
}

func TestYearParseValue(t *testing.T) {
	tests := []struct {
		input       string
		output      types.Int
		expectedErr bool
	}{
		{
			"2001",
			2001,
			false,
		},
		{
			"1901",
			1901,
			false,
		},
		{
			"2000",
			2000,
			false,
		},
		{
			"89",
			1989,
			false,
		},
		{
			"3000",
			0,
			true,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v %v`, YearType.String(), test.input), func(t *testing.T) {
			vrw := types.NewInternalValueStore()
			output, err := YearType.ParseValue(context.Background(), vrw, &test.input)
			if !test.expectedErr {
				require.NoError(t, err)
				assert.Equal(t, test.output, output)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
