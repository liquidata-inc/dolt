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

func TestDatetimeConvertNomsValueToValue(t *testing.T) {
	tests := []struct {
		typ         *datetimeType
		input       types.Timestamp
		output      time.Time
		expectedErr bool
	}{
		{
			DateType,
			types.Timestamp(time.Date(1880, 1, 2, 0, 0, 0, 0, time.UTC)),
			time.Date(1880, 1, 2, 0, 0, 0, 0, time.UTC),
			false,
		},
		{
			TimestampType,
			types.Timestamp(time.Date(2030, 1, 2, 4, 6, 3, 472382485, time.UTC)),
			time.Date(2030, 1, 2, 4, 6, 3, 472382485, time.UTC),
			false,
		},
		{
			DatetimeType,
			types.Timestamp(time.Date(5800, 1, 2, 4, 6, 3, 472382485, time.UTC)),
			time.Date(5800, 1, 2, 4, 6, 3, 472382485, time.UTC),
			false,
		},
		{
			DatetimeType,
			types.Timestamp(time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC)),
			time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC),
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

func TestDatetimeConvertValueToNomsValue(t *testing.T) {
	tests := []struct {
		typ         *datetimeType
		input       interface{}
		output      types.Timestamp
		expectedErr bool
	}{
		{
			DateType,
			time.Date(1880, 1, 2, 4, 6, 3, 472382485, time.UTC),
			types.Timestamp(time.Date(1880, 1, 2, 0, 0, 0, 0, time.UTC)),
			false,
		},
		{
			TimestampType,
			time.Date(2030, 1, 2, 4, 6, 3, 472382485, time.UTC),
			types.Timestamp(time.Date(2030, 1, 2, 4, 6, 3, 472382485, time.UTC)),
			false,
		},
		{
			DatetimeType,
			time.Date(5800, 1, 2, 4, 6, 3, 472382485, time.UTC),
			types.Timestamp(time.Date(5800, 1, 2, 4, 6, 3, 472382485, time.UTC)),
			false,
		},
		{
			DatetimeType,
			time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC),
			types.Timestamp(time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC)),
			false,
		},
		{
			TimestampType,
			time.Date(2039, 1, 2, 4, 6, 3, 472382485, time.UTC),
			types.Timestamp{},
			true,
		},
		{
			DatetimeType,
			time.Date(5, 1, 2, 4, 6, 3, 472382485, time.UTC),
			types.Timestamp{},
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

func TestDatetimeFormatValue(t *testing.T) {
	tests := []struct {
		typ         *datetimeType
		input       types.Timestamp
		output      string
		expectedErr bool
	}{
		{
			DateType,
			types.Timestamp(time.Date(1880, 1, 2, 4, 6, 3, 472382485, time.UTC)),
			"1880-01-02",
			false,
		},
		{
			TimestampType,
			types.Timestamp(time.Date(2030, 1, 2, 4, 6, 3, 472382485, time.UTC)),
			"2030-01-02 04:06:03.472382",
			false,
		},
		{
			DatetimeType,
			types.Timestamp(time.Date(5800, 1, 2, 4, 6, 3, 472382485, time.UTC)),
			"5800-01-02 04:06:03.472382",
			false,
		},
		{
			DatetimeType,
			types.Timestamp(time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC)),
			"9999-12-31 23:59:59.999999",
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

func TestDatetimeParseValue(t *testing.T) {
	tests := []struct {
		typ         *datetimeType
		input       string
		output      types.Timestamp
		expectedErr bool
	}{
		{
			DateType,
			"1880-01-02 04:06:03.472382",
			types.Timestamp(time.Date(1880, 1, 2, 0, 0, 0, 0, time.UTC)),
			false,
		},
		{
			DateType,
			"1880-01-02",
			types.Timestamp(time.Date(1880, 1, 2, 0, 0, 0, 0, time.UTC)),
			false,
		},
		{
			TimestampType,
			"2030-01-02 04:06:03.472382",
			types.Timestamp(time.Date(2030, 1, 2, 4, 6, 3, 472382000, time.UTC)),
			false,
		},
		{
			DatetimeType,
			"5800-01-02 04:06:03.472382",
			types.Timestamp(time.Date(5800, 1, 2, 4, 6, 3, 472382000, time.UTC)),
			false,
		},
		{
			DatetimeType,
			"9999-12-31 23:59:59.999999",
			types.Timestamp(time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC)),
			false,
		},
		{
			TimestampType,
			"2039-01-02 04:06:03.472382",
			types.Timestamp{},
			true,
		},
		{
			DatetimeType,
			"0005-01-02 04:06:03.472382",
			types.Timestamp{},
			true,
		},
		//{
		//  todo: this doesn't error
		//	DatetimeType,
		//	"",
		//	types.Timestamp{},
		//	true,
		//},
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
