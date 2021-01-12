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
	"math/big"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/types"
)

func TestDecimalConvertNomsValueToValue(t *testing.T) {
	tests := []struct {
		typ         *decimalType
		input       types.Decimal
		output      string
		expectedErr bool
	}{
		{
			generateDecimalType(t, 1, 0),
			types.Decimal(decimal.RequireFromString("0")),
			"0",
			false,
		},
		{
			generateDecimalType(t, 1, 0),
			types.Decimal(decimal.RequireFromString("-1.5")),
			"-2",
			false,
		},
		{
			generateDecimalType(t, 2, 1),
			types.Decimal(decimal.RequireFromString("-1.5")),
			"-1.5",
			false,
		},
		{
			generateDecimalType(t, 5, 4),
			types.Decimal(decimal.RequireFromString("-5.7159")),
			"-5.7159",
			false,
		},
		{
			generateDecimalType(t, 9, 2),
			types.Decimal(decimal.RequireFromString("4723245")),
			"4723245.00",
			false,
		},
		{
			generateDecimalType(t, 9, 2),
			types.Decimal(decimal.RequireFromString("4723245.01")),
			"4723245.01",
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

func TestDecimalConvertValueToNomsValue(t *testing.T) {
	tests := []struct {
		typ         *decimalType
		input       interface{}
		output      types.Decimal
		expectedErr bool
	}{
		{
			generateDecimalType(t, 1, 0),
			7,
			types.Decimal(decimal.RequireFromString("7")),
			false,
		},
		{
			generateDecimalType(t, 5, 1),
			-4.5,
			types.Decimal(decimal.RequireFromString("-4.5")),
			false,
		},
		{
			generateDecimalType(t, 10, 0),
			"77",
			types.Decimal(decimal.RequireFromString("77")),
			false,
		},
		{
			generateDecimalType(t, 5, 0),
			"dog",
			types.Decimal{},
			true,
		},
		{
			generateDecimalType(t, 15, 7),
			true,
			types.Decimal{},
			true,
		},
		{
			generateDecimalType(t, 20, 5),
			time.Unix(137849, 0),
			types.Decimal{},
			true,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v %v`, test.typ.String(), test.input), func(t *testing.T) {
			vrw := types.NewInternalValueStore()
			output, err := test.typ.ConvertValueToNomsValue(context.Background(), vrw, test.input)
			if !test.expectedErr {
				require.NoError(t, err)
				assert.True(t, test.output.Equals(output))
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestDecimalFormatValue(t *testing.T) {
	tests := []struct {
		typ         *decimalType
		input       types.Decimal
		output      string
		expectedErr bool
	}{
		{
			generateDecimalType(t, 1, 0),
			types.Decimal(decimal.RequireFromString("0")),
			"0",
			false,
		},
		{
			generateDecimalType(t, 1, 0),
			types.Decimal(decimal.RequireFromString("-1.5")),
			"-2",
			false,
		},
		{
			generateDecimalType(t, 2, 1),
			types.Decimal(decimal.RequireFromString("-1.5")),
			"-1.5",
			false,
		},
		{
			generateDecimalType(t, 5, 4),
			types.Decimal(decimal.RequireFromString("-5.7159")),
			"-5.7159",
			false,
		},
		{
			generateDecimalType(t, 9, 2),
			types.Decimal(decimal.RequireFromString("4723245")),
			"4723245.00",
			false,
		},
		{
			generateDecimalType(t, 9, 2),
			types.Decimal(decimal.RequireFromString("4723245.01")),
			"4723245.01",
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

func TestDecimalParseValue(t *testing.T) {
	tests := []struct {
		typ         *decimalType
		input       string
		output      types.Decimal
		expectedErr bool
	}{
		{
			generateDecimalType(t, 1, 0),
			"0",
			types.Decimal(decimal.RequireFromString("0")),
			false,
		},
		{
			generateDecimalType(t, 1, 0),
			"-1.5",
			types.Decimal(decimal.RequireFromString("-2")),
			false,
		},
		{
			generateDecimalType(t, 2, 1),
			"-1.5",
			types.Decimal(decimal.RequireFromString("-1.5")),
			false,
		},
		{
			generateDecimalType(t, 5, 4),
			"-5.7159",
			types.Decimal(decimal.RequireFromString("-5.7159")),
			false,
		},
		{
			generateDecimalType(t, 9, 2),
			"4723245.00",
			types.Decimal(decimal.RequireFromString("4723245.00")),
			false,
		},
		{
			generateDecimalType(t, 13, 2),
			"4723245.01",
			types.Decimal(decimal.RequireFromString("4723245.01")),
			false,
		},
		{
			generateDecimalType(t, 9, 2),
			"24723245.01",
			types.Decimal{},
			true,
		},
		{
			generateDecimalType(t, 5, 4),
			"-44.2841",
			types.Decimal{},
			true,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v %v`, test.typ.String(), test.input), func(t *testing.T) {
			vrw := types.NewInternalValueStore()
			output, err := test.typ.ParseValue(context.Background(), vrw, &test.input)
			if !test.expectedErr {
				require.NoError(t, err)
				assert.True(t, test.output.Equals(output))
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestDecimalMarshal(t *testing.T) {
	tests := []struct {
		precision   uint8
		scale       uint8
		val         interface{}
		expectedVal string
		expectedErr bool
	}{
		{1, 0, byte(0), "0", false},
		{1, 0, int8(3), "3", false},
		{1, 0, "-3.7e0", "-4", false},
		{1, 0, uint(4), "4", false},
		{1, 0, int16(9), "9", false},
		{1, 0, "0.00000000000000000003e20", "3", false},
		{1, 0, float64(-9.4), "-9", false},
		{1, 0, float32(9.5), "", true},
		{1, 0, int32(-10), "", true},

		{1, 1, 0, "0.0", false},
		{1, 1, .01, "0.0", false},
		{1, 1, .1, "0.1", false},
		{1, 1, ".22", "0.2", false},
		{1, 1, .55, "0.6", false},
		{1, 1, "-.7863294659345624", "-0.8", false},
		{1, 1, "2634193746329327479.32030573792e-19", "0.3", false},
		{1, 1, 1, "", true},
		{1, 1, new(big.Rat).SetInt64(2), "", true},

		{5, 0, 0, "0", false},
		{5, 0, -5, "-5", false},
		{5, 0, -99995, "-99995", false},
		{5, 0, 5000.2, "5000", false},
		{5, 0, "7742", "7742", false},
		{5, 0, new(big.Float).SetFloat64(-4723.875), "-4724", false},
		{5, 0, 99999, "99999", false},
		{5, 0, "0xf8e1", "63713", false},
		{5, 0, "0b1001110101100110", "40294", false},
		{5, 0, new(big.Rat).SetFrac64(999999, 10), "", true},
		{5, 0, 673927, "", true},

		{10, 5, 0, "0.00000", false},
		{10, 5, "25.1", "25.10000", false},
		{10, 5, "-25.1", "-25.10000", false},
		{10, 5, "-99205.8572", "-99205.85720", false},
		{10, 5, "99999.999994", "99999.99999", false},
		{10, 5, "5.5729136e3", "5572.91360", false},
		{10, 5, "600e-2", "6.00000", false},
		{10, 5, new(big.Rat).SetFrac64(-22, 7), "-3.14286", false},
		{10, 5, "-99995.1", "-99995.10000", false},
		{10, 5, 100000, "", true},
		{10, 5, "-99999.999995", "", true},

		{65, 0, "99999999999999999999999999999999999999999999999999999999999999999",
			"99999999999999999999999999999999999999999999999999999999999999999", false},
		{65, 0, "99999999999999999999999999999999999999999999999999999999999999999.1",
			"99999999999999999999999999999999999999999999999999999999999999999", false},
		{65, 0, "99999999999999999999999999999999999999999999999999999999999999999.99", "", true},

		{65, 12, "16976349273982359874209023948672021737840592720387475.2719128737543572927374503832837350563300243035038234972093785",
			"16976349273982359874209023948672021737840592720387475.271912873754", false},
		{65, 12, "99999999999999999999999999999999999999999999999999999.9999999999999", "", true},

		{20, 10, []byte{32}, "", true},
		{20, 10, time.Date(2019, 12, 12, 12, 12, 12, 0, time.UTC), "", true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v %v", test.precision, test.scale, test.val), func(t *testing.T) {
			typ := &decimalType{sql.MustCreateDecimalType(test.precision, test.scale)}
			vrw := types.NewInternalValueStore()
			val, err := typ.ConvertValueToNomsValue(context.Background(), vrw, test.val)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedVal, typ.sqlDecimalType.MustConvert(decimal.Decimal(val.(types.Decimal))))
				umar, err := typ.ConvertNomsValueToValue(val)
				require.NoError(t, err)
				testVal := typ.sqlDecimalType.MustConvert(test.val)
				cmp, err := typ.sqlDecimalType.Compare(testVal, umar)
				require.NoError(t, err)
				assert.Equal(t, 0, cmp)
			}
		})
	}
}

func TestDecimalRoundTrip(t *testing.T) {
	tests := []struct {
		typ         *decimalType
		input       string
		output      string
		expectedErr bool
	}{
		{
			generateDecimalType(t, 1, 0),
			"0",
			"0",
			false,
		},
		{
			generateDecimalType(t, 4, 1),
			"0",
			"0.0",
			false,
		},
		{
			generateDecimalType(t, 9, 4),
			"0",
			"0.0000",
			false,
		},
		{
			generateDecimalType(t, 26, 0),
			"0",
			"0",
			false,
		},
		{
			generateDecimalType(t, 48, 22),
			"0",
			"0.0000000000000000000000",
			false,
		},
		{
			generateDecimalType(t, 65, 30),
			"0",
			"0.000000000000000000000000000000",
			false,
		},
		{
			generateDecimalType(t, 1, 0),
			"-1.5",
			"-2",
			false,
		},
		{
			generateDecimalType(t, 4, 1),
			"-1.5",
			"-1.5",
			false,
		},
		{
			generateDecimalType(t, 9, 4),
			"-1.5",
			"-1.5000",
			false,
		},
		{
			generateDecimalType(t, 26, 0),
			"-1.5",
			"-2",
			false,
		},
		{
			generateDecimalType(t, 48, 22),
			"-1.5",
			"-1.5000000000000000000000",
			false,
		},
		{
			generateDecimalType(t, 65, 30),
			"-1.5",
			"-1.500000000000000000000000000000",
			false,
		},
		{
			generateDecimalType(t, 1, 0),
			"9351580",
			"",
			true,
		},
		{
			generateDecimalType(t, 4, 1),
			"9351580",
			"",
			true,
		},
		{
			generateDecimalType(t, 9, 4),
			"9351580",
			"",
			true,
		},
		{
			generateDecimalType(t, 26, 0),
			"9351580",
			"9351580",
			false,
		},
		{
			generateDecimalType(t, 48, 22),
			"9351580",
			"9351580.0000000000000000000000",
			false,
		},
		{
			generateDecimalType(t, 65, 30),
			"9351580",
			"9351580.000000000000000000000000000000",
			false,
		},
		{
			generateDecimalType(t, 1, 0),
			"-1076416.875",
			"",
			true,
		},
		{
			generateDecimalType(t, 4, 1),
			"-1076416.875",
			"",
			true,
		},
		{
			generateDecimalType(t, 9, 4),
			"-1076416.875",
			"",
			true,
		},
		{
			generateDecimalType(t, 26, 0),
			"-1076416.875",
			"-1076417",
			false,
		},
		{
			generateDecimalType(t, 48, 22),
			"-1076416.875",
			"-1076416.8750000000000000000000",
			false,
		},
		{
			generateDecimalType(t, 65, 30),
			"-1076416.875",
			"-1076416.875000000000000000000000000000",
			false,
		},
		{
			generateDecimalType(t, 1, 0),
			"198728394234798423466321.27349757",
			"",
			true,
		},
		{
			generateDecimalType(t, 4, 1),
			"198728394234798423466321.27349757",
			"",
			true,
		},
		{
			generateDecimalType(t, 9, 4),
			"198728394234798423466321.27349757",
			"",
			true,
		},
		{
			generateDecimalType(t, 26, 0),
			"198728394234798423466321.27349757",
			"198728394234798423466321",
			false,
		},
		{
			generateDecimalType(t, 48, 22),
			"198728394234798423466321.27349757",
			"198728394234798423466321.2734975700000000000000",
			false,
		},
		{
			generateDecimalType(t, 65, 30),
			"198728394234798423466321.27349757",
			"198728394234798423466321.273497570000000000000000000000",
			false,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf(`%v %v %v`, test.typ.String(), test.input, test.output), func(t *testing.T) {
			vrw := types.NewInternalValueStore()
			parsed, err := test.typ.ConvertValueToNomsValue(context.Background(), vrw, test.input)
			if !test.expectedErr {
				require.NoError(t, err)
				output, err := test.typ.ConvertNomsValueToValue(parsed)
				require.NoError(t, err)
				assert.Equal(t, test.output, output)
				parsed2, err := test.typ.ParseValue(context.Background(), vrw, &test.input)
				require.NoError(t, err)
				assert.Equal(t, parsed, parsed2)
				output2, err := test.typ.FormatValue(parsed2)
				require.NoError(t, err)
				assert.Equal(t, test.output, *output2)
			} else {
				assert.Error(t, err)
				_, err = test.typ.ParseValue(context.Background(), vrw, &test.input)
				assert.Error(t, err)
			}
		})
	}
}
