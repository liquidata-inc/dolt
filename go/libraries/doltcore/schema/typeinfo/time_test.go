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

func TestTimeConvertNomsValueToValue(t *testing.T) {
	tests := []struct {
		input       types.Int
		output      string
		expectedErr bool
	}{
		{
			1000000,
			"00:00:01",
			false,
		},
		{
			113000000,
			"00:01:53",
			false,
		},
		{
			247019000000,
			"68:36:59",
			false,
		},
		{
			458830485214,
			"127:27:10.485214",
			false,
		},
		{
			-3020399000000,
			"-838:59:59",
			false,
		},
		{ // no integer can cause an error, values beyond the max/min are set equal to the max/min
			922337203685477580,
			"838:59:59",
			false,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v`, test.input), func(t *testing.T) {
			output, err := TimeType.ConvertNomsValueToValue(test.input)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.output, output)
			}
		})
	}
}

func TestTimeConvertValueToNomsValue(t *testing.T) {
	tests := []struct {
		input       interface{}
		output      types.Int
		expectedErr bool
	}{
		{
			153,
			113000000,
			false,
		},
		{
			1.576,
			1576000,
			false,
		},
		{
			"68:36:59",
			247019000000,
			false,
		},
		{
			"683659",
			247019000000,
			false,
		},
		{
			"dog",
			0,
			true,
		},
		{
			true,
			0,
			true,
		},
		{
			time.Unix(137849, 0),
			0,
			true,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v`, test.input), func(t *testing.T) {
			vrw := types.NewInternalValueStore()
			output, err := TimeType.ConvertValueToNomsValue(context.Background(), vrw, test.input)
			if !test.expectedErr {
				require.NoError(t, err)
				assert.Equal(t, test.output, output)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestTimeFormatValue(t *testing.T) {
	tests := []struct {
		input       types.Int
		output      string
		expectedErr bool
	}{
		{
			1000000,
			"00:00:01",
			false,
		},
		{
			113000000,
			"00:01:53",
			false,
		},
		{
			247019000000,
			"68:36:59",
			false,
		},
		{
			458830485214,
			"127:27:10.485214",
			false,
		},
		{
			-3020399000000,
			"-838:59:59",
			false,
		},
		{
			922337203685477580,
			"838:59:59",
			false,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v`, test.input), func(t *testing.T) {
			output, err := TimeType.FormatValue(test.input)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.output, *output)
			}
		})
	}
}

func TestTimeParseValue(t *testing.T) {
	tests := []struct {
		input       string
		output      types.Int
		expectedErr bool
	}{
		{
			"683659",
			247019000000,
			false,
		},
		{
			"127:27:10.485214",
			458830485214,
			false,
		},
		{
			"-838:59:59",
			-3020399000000,
			false,
		},
		{
			"850:00:00",
			3020399000000,
			false,
		},
		{
			"dog",
			0,
			true,
		},
		{
			"2030-01-02 04:06:03.472382",
			0,
			true,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v`, test.input), func(t *testing.T) {
			vrw := types.NewInternalValueStore()
			output, err := TimeType.ParseValue(context.Background(), vrw, &test.input)
			if !test.expectedErr {
				require.NoError(t, err)
				assert.Equal(t, test.output, output)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
