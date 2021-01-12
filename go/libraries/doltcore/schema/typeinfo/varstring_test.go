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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/types"
)

func TestVarStringConvertNomsValueToValue(t *testing.T) {
	tests := []struct {
		typ         *varStringType
		input       types.String
		output      string
		expectedErr bool
	}{
		{
			generateVarStringType(t, 10, false),
			"0  ",
			"0  ",
			false,
		},
		{
			generateVarStringType(t, 10, true),
			"0  ",
			"0",
			false,
		},
		{
			generateVarStringType(t, 80, false),
			"this is some text that will be returned",
			"this is some text that will be returned",
			false,
		},
		{
			&varStringType{sql.CreateLongText(sql.Collation_Default)},
			"  This is a sentence.  ",
			"  This is a sentence.  ",
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

func TestVarStringConvertValueToNomsValue(t *testing.T) {
	tests := []struct {
		typ         *varStringType
		input       interface{}
		output      types.String
		expectedErr bool
	}{
		{
			generateVarStringType(t, 10, false),
			"0  ",
			"0  ",
			false,
		},
		{
			generateVarStringType(t, 10, true),
			[]byte("0  "),
			"0  ", // converting to NomsValue counts as storage, thus we don't trim then
			false,
		},
		{
			generateVarStringType(t, 80, false),
			int64(28354),
			"28354",
			false,
		},
		{
			&varStringType{sql.CreateLongText(sql.Collation_Default)},
			float32(3724.75),
			"3724.75",
			false,
		},
		{
			generateVarStringType(t, 80, false),
			time.Date(2030, 1, 2, 4, 6, 3, 472382485, time.UTC),
			"2030-01-02 04:06:03.472382",
			false,
		},
		{
			generateVarStringType(t, 2, true),
			"yey",
			"",
			true,
		},
		{
			generateVarStringType(t, 2, true),
			int32(382),
			"",
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

func TestVarStringFormatValue(t *testing.T) {
	tests := []struct {
		typ         *varStringType
		input       types.String
		output      string
		expectedErr bool
	}{
		{
			generateVarStringType(t, 10, false),
			"0  ",
			"0  ",
			false,
		},
		{
			generateVarStringType(t, 10, true),
			"0  ",
			"0",
			false,
		},
		{
			generateVarStringType(t, 80, false),
			"this is some text that will be returned",
			"this is some text that will be returned",
			false,
		},
		{
			&varStringType{sql.CreateLongText(sql.Collation_Default)},
			"  This is a sentence.  ",
			"  This is a sentence.  ",
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

func TestVarStringParseValue(t *testing.T) {
	tests := []struct {
		typ         *varStringType
		input       string
		output      types.String
		expectedErr bool
	}{
		{
			generateVarStringType(t, 10, false),
			"0  ",
			"0  ",
			false,
		},
		{
			generateVarStringType(t, 10, true),
			"0  ",
			"0  ", // converting to NomsValue counts as storage, thus we don't trim then
			false,
		},
		{
			generateVarStringType(t, 80, false),
			"this is some text that will be returned",
			"this is some text that will be returned",
			false,
		},
		{
			&varStringType{sql.CreateLongText(sql.Collation_Default)},
			"  This is a sentence.  ",
			"  This is a sentence.  ",
			false,
		},
		{
			generateVarStringType(t, 2, false),
			"yay",
			"",
			true,
		},
		{
			generateVarStringType(t, 2, true),
			"yey",
			"",
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
