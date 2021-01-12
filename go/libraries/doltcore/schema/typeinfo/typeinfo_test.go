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
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/types"
)

func TestTypeInfoSuite(t *testing.T) {
	typeInfoArrays, validTypeValues := generateTypeInfoArrays(t)
	t.Run("VerifyArray", func(t *testing.T) {
		verifyTypeInfoArrays(t, typeInfoArrays, validTypeValues)
	})
	t.Run("ConvertRoundTrip", func(t *testing.T) {
		testTypeInfoConvertRoundTrip(t, typeInfoArrays, validTypeValues)
	})
	t.Run("Equals", func(t *testing.T) {
		testTypeInfoEquals(t, typeInfoArrays)
	})
	t.Run("ForeignKindHandling", func(t *testing.T) {
		testTypeInfoForeignKindHandling(t, typeInfoArrays, validTypeValues)
	})
	t.Run("FormatParseRoundTrip", func(t *testing.T) {
		testTypeInfoFormatParseRoundTrip(t, typeInfoArrays, validTypeValues)
	})
	t.Run("GetTypeParams", func(t *testing.T) {
		testTypeInfoGetTypeParams(t, typeInfoArrays)
	})
	t.Run("NullHandling", func(t *testing.T) {
		testTypeInfoNullHandling(t, typeInfoArrays)
	})
	t.Run("NomsKind", func(t *testing.T) {
		testTypeInfoNomsKind(t, typeInfoArrays, validTypeValues)
	})
	t.Run("ToSqlType", func(t *testing.T) {
		testTypeInfoToSqlType(t, typeInfoArrays)
	})
}

// verify that the TypeInfos and values are all consistent with each other, and cover the full range of types
func verifyTypeInfoArrays(t *testing.T, tiArrays [][]TypeInfo, vaArrays [][]types.Value) {
	require.Equal(t, len(tiArrays), len(vaArrays))

	seenTypeInfos := make(map[Identifier]bool)
	for identifier := range Identifiers {
		seenTypeInfos[identifier] = false
	}
	// delete any types that should not be tested
	delete(seenTypeInfos, UnknownTypeIdentifier)
	delete(seenTypeInfos, TupleTypeIdentifier)
	//TODO: determine the storage format for VarBinaryType
	delete(seenTypeInfos, VarBinaryTypeIdentifier)
	for _, tiArray := range tiArrays {
		// no row should be empty
		require.True(t, len(tiArray) > 0, `length of array "%v" should be greater than zero`, len(tiArray))
		firstIdentifier := tiArray[0].GetTypeIdentifier()
		t.Run(firstIdentifier.String(), func(t *testing.T) {
			seen, ok := seenTypeInfos[firstIdentifier]
			require.True(t, ok, `identifier "%v" is not recognized`, firstIdentifier)
			require.False(t, seen, `identifier "%v" is used by another type from the array`, firstIdentifier)
			seenTypeInfos[firstIdentifier] = true
			for _, ti := range tiArray {
				// verify that all of the types have the same identifier
				require.Equal(t, firstIdentifier, ti.GetTypeIdentifier(),
					`expected "%v" but got "%v"`, firstIdentifier, ti.GetTypeIdentifier())
			}
		})
	}
	// make sure that we are testing all of the types (unless deleted above)
	for seenti, seen := range seenTypeInfos {
		require.True(t, seen, `identifier "%v" does not have a relevant type being tested`, seenti)
	}
	for _, vaArray := range vaArrays {
		// no row should be empty
		require.True(t, len(vaArray) > 0, `length of array "%v" should be greater than zero`, len(vaArray))
		firstKind := vaArray[0].Kind()
		for _, val := range vaArray {
			// verify that all of the values in an row are of the same kind
			require.Equal(t, firstKind, val.Kind(), `expected kind "%v" but got "%v"`, firstKind, val.Kind())
		}
	}
}

// assuming valid data, verifies that the To-From interface{} functions can round trip
func testTypeInfoConvertRoundTrip(t *testing.T, tiArrays [][]TypeInfo, vaArrays [][]types.Value) {
	for rowIndex, tiArray := range tiArrays {
		t.Run(tiArray[0].GetTypeIdentifier().String(), func(t *testing.T) {
			for _, ti := range tiArray {
				atLeastOneValid := false
				t.Run(ti.String(), func(t *testing.T) {
					for _, val := range vaArrays[rowIndex] {
						t.Run(fmt.Sprintf(`types.%v(%v)`, val.Kind().String(), val.HumanReadableString()), func(t *testing.T) {
							vInterface, err := ti.ConvertNomsValueToValue(val)
							if ti.IsValid(val) {
								atLeastOneValid = true
								require.NoError(t, err)
								vrw := types.NewInternalValueStore()
								outVal, err := ti.ConvertValueToNomsValue(context.Background(), vrw, vInterface)
								require.NoError(t, err)
								if ti == DateType { // Special case as DateType removes the hh:mm:ss
									val = types.Timestamp(time.Time(val.(types.Timestamp)).Truncate(24 * time.Hour))
									require.True(t, val.Equals(outVal), "\"%v\"\n\"%v\"", val, outVal)
								} else if ti.GetTypeIdentifier() != DecimalTypeIdentifier { // Any Decimal's on-disk representation varies by precision/scale
									require.True(t, val.Equals(outVal), "\"%v\"\n\"%v\"", val, outVal)
								}
							}
						})
					}
				})
				require.True(t, atLeastOneValid, `all values reported false for "%v"`, ti.String())
			}
		})
	}
}

// each TypeInfo in tiArrays is unique, so all equality comparisons should fail when the indices don't match
func testTypeInfoEquals(t *testing.T, tiArrays [][]TypeInfo) {
	for tiArrayIndex, tiArray := range tiArrays {
		t.Run(tiArray[0].GetTypeIdentifier().String(), func(t *testing.T) {
			// check this TypeInfo against its own variations, EX: Int16 & Int32
			// a != b should also mean b != a
			for i := range tiArray {
				ti1 := tiArray[i]
				t.Run(ti1.String(), func(t *testing.T) {
					for j := range tiArray {
						ti2 := tiArray[j]
						t.Run(fmt.Sprintf(ti2.String()), func(t *testing.T) {
							equality := ti1.Equals(ti2)
							if i == j {
								assert.True(t, equality)
							} else {
								assert.False(t, equality)
							}
						})
					}
				})
			}
			// we just check the first element and assume it'll hold true for the other values
			firstTi := tiArray[0]
			t.Run(fmt.Sprintf(`%v Others`, firstTi), func(t *testing.T) {
				// check this TypeInfo against the other types, EX: Int16 & Float64
				for tiArrayIndex2, tiArray2 := range tiArrays {
					if tiArrayIndex == tiArrayIndex2 {
						// this is the for loop above
						continue
					}
					for _, otherTi := range tiArray2 {
						t.Run(fmt.Sprintf(otherTi.String()), func(t *testing.T) {
							equality := firstTi.Equals(otherTi)
							assert.False(t, equality)
						})
					}
				}
			})
		})
	}
}

// ConvertNomsValueToValue and FormatValue should fail if the kind does not match the TypeInfo kind
func testTypeInfoForeignKindHandling(t *testing.T, tiArrays [][]TypeInfo, vaArrays [][]types.Value) {
	for _, tiArray := range tiArrays {
		t.Run(tiArray[0].GetTypeIdentifier().String(), func(t *testing.T) {
			for _, ti := range tiArray {
				t.Run(ti.String(), func(t *testing.T) {
					for _, vaArray := range vaArrays {
						for _, val := range vaArray {
							t.Run(fmt.Sprintf(`types.%v(%v)`, val.Kind().String(), val.HumanReadableString()), func(t *testing.T) {
								if ti.NomsKind() != val.Kind() {
									_, err := ti.ConvertNomsValueToValue(val)
									assert.Error(t, err)
									_, err = ti.FormatValue(val)
									assert.Error(t, err)
								}
							})
						}
					}
				})
			}
		})
	}
}

// assuming valid data, verifies that the To-From string functions can round trip
func testTypeInfoFormatParseRoundTrip(t *testing.T, tiArrays [][]TypeInfo, vaArrays [][]types.Value) {
	for rowIndex, tiArray := range tiArrays {
		t.Run(tiArray[0].GetTypeIdentifier().String(), func(t *testing.T) {
			for _, ti := range tiArray {
				atLeastOneValid := false
				t.Run(ti.String(), func(t *testing.T) {
					for _, val := range vaArrays[rowIndex] {
						t.Run(fmt.Sprintf(`types.%v(%v)`, val.Kind().String(), val.HumanReadableString()), func(t *testing.T) {
							str, err := ti.FormatValue(val)
							if ti.IsValid(val) {
								atLeastOneValid = true
								require.NoError(t, err)
								vrw := types.NewInternalValueStore()
								outVal, err := ti.ParseValue(context.Background(), vrw, str)
								require.NoError(t, err)
								if ti == DateType { // special case as DateType removes the hh:mm:ss
									val = types.Timestamp(time.Time(val.(types.Timestamp)).Truncate(24 * time.Hour))
									require.True(t, val.Equals(outVal), "\"%v\"\n\"%v\"", val, outVal)
								} else if ti.GetTypeIdentifier() != DecimalTypeIdentifier { // Any Decimal's on-disk representation varies by precision/scale
									require.True(t, val.Equals(outVal), "\"%v\"\n\"%v\"", val, outVal)
								}
							}
						})
					}
				})
				require.True(t, atLeastOneValid, `all values reported false for "%v"`, ti.String())
			}
		})
	}
}

// verify that FromTypeParams can reconstruct the exact same TypeInfo from the params
func testTypeInfoGetTypeParams(t *testing.T, tiArrays [][]TypeInfo) {
	for _, tiArray := range tiArrays {
		t.Run(tiArray[0].GetTypeIdentifier().String(), func(t *testing.T) {
			for _, ti := range tiArray {
				t.Run(ti.String(), func(t *testing.T) {
					newTi, err := FromTypeParams(ti.GetTypeIdentifier(), ti.GetTypeParams())
					require.NoError(t, err)
					require.True(t, ti.Equals(newTi), "%v\n%v", ti.String(), newTi.String())
				})
			}
		})
	}
}

// makes sure that everything can handle nil and NullValue (if applicable)
func testTypeInfoNullHandling(t *testing.T, tiArrays [][]TypeInfo) {
	for _, tiArray := range tiArrays {
		t.Run(tiArray[0].GetTypeIdentifier().String(), func(t *testing.T) {
			for _, ti := range tiArray {
				t.Run(ti.String(), func(t *testing.T) {
					t.Run("ConvertNomsValueToValue", func(t *testing.T) {
						val, err := ti.ConvertNomsValueToValue(types.NullValue)
						require.NoError(t, err)
						require.Nil(t, val)
						val, err = ti.ConvertNomsValueToValue(nil)
						require.NoError(t, err)
						require.Nil(t, val)
					})
					t.Run("ConvertValueToNomsValue", func(t *testing.T) {
						vrw := types.NewInternalValueStore()
						tVal, err := ti.ConvertValueToNomsValue(context.Background(), vrw, nil)
						require.NoError(t, err)
						require.Equal(t, types.NullValue, tVal)
					})
					t.Run("FormatValue", func(t *testing.T) {
						tVal, err := ti.FormatValue(types.NullValue)
						require.NoError(t, err)
						require.Nil(t, tVal)
						tVal, err = ti.FormatValue(nil)
						require.NoError(t, err)
						require.Nil(t, tVal)
					})
					t.Run("IsValid", func(t *testing.T) {
						require.True(t, ti.IsValid(types.NullValue))
						require.True(t, ti.IsValid(nil))
					})
					t.Run("ParseValue", func(t *testing.T) {
						vrw := types.NewInternalValueStore()
						tVal, err := ti.ParseValue(context.Background(), vrw, nil)
						require.NoError(t, err)
						require.Equal(t, types.NullValue, tVal)
					})
				})
			}
		})
	}
}

// smoke test checking that the returned NomsKind is consistent and matches the values.
func testTypeInfoNomsKind(t *testing.T, tiArrays [][]TypeInfo, vaArrays [][]types.Value) {
	for rowIndex, tiArray := range tiArrays {
		t.Run(tiArray[0].GetTypeIdentifier().String(), func(t *testing.T) {
			nomsKind := tiArray[0].NomsKind()
			for _, ti := range tiArray {
				t.Run("Equality "+ti.String(), func(t *testing.T) {
					require.Equal(t, nomsKind, ti.NomsKind())
				})
			}
			t.Run("Values "+tiArray[0].NomsKind().String(), func(t *testing.T) {
				for _, val := range vaArrays[rowIndex] {
					require.Equal(t, nomsKind, val.Kind())
				}
			})
		})
	}
}

// smoke test so that there are no obvious panics when returning SQL types
func testTypeInfoToSqlType(t *testing.T, tiArrays [][]TypeInfo) {
	for _, tiArray := range tiArrays {
		t.Run(tiArray[0].GetTypeIdentifier().String(), func(t *testing.T) {
			for _, ti := range tiArray {
				t.Run(ti.String(), func(t *testing.T) {
					_ = ti.ToSqlType()
				})
			}
		})
	}
}

// generate unique TypeInfos for each type, and also values that are valid for at least one of the TypeInfos for the matching row
func generateTypeInfoArrays(t *testing.T) ([][]TypeInfo, [][]types.Value) {
	return [][]TypeInfo{
			generateBitTypes(t, 16),
			{BoolType},
			{DateType, DatetimeType, TimestampType},
			generateDecimalTypes(t, 16),
			generateEnumTypes(t, 16),
			{Float32Type, Float64Type},
			{DefaultInlineBlobType},
			{Int8Type, Int16Type, Int24Type, Int32Type, Int64Type},
			generateSetTypes(t, 16),
			{TimeType},
			{Uint8Type, Uint16Type, Uint24Type, Uint32Type, Uint64Type},
			{UuidType},
			//append(generateVarBinaryTypes(t, 12),
			//	&varBinaryType{sql.TinyBlob}, &varBinaryType{sql.Blob},
			//	&varBinaryType{sql.MediumBlob}, &varBinaryType{sql.LongBlob}),
			append(generateVarStringTypes(t, 12),
				&varStringType{sql.CreateTinyText(sql.Collation_Default)}, &varStringType{sql.CreateText(sql.Collation_Default)},
				&varStringType{sql.CreateMediumText(sql.Collation_Default)}, &varStringType{sql.CreateLongText(sql.Collation_Default)}),
			{YearType},
		},
		[][]types.Value{
			{types.Uint(1), types.Uint(207), types.Uint(79147), types.Uint(34845728), types.Uint(9274618927)}, //Bit
			{types.Bool(false), types.Bool(true)}, //Bool
			{types.Timestamp(time.Date(1000, 1, 1, 0, 0, 0, 0, time.UTC)), //Datetime
				types.Timestamp(time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC)),
				types.Timestamp(time.Date(2000, 2, 28, 14, 38, 43, 583395000, time.UTC)),
				types.Timestamp(time.Date(2038, 1, 19, 3, 14, 7, 999999000, time.UTC)),
				types.Timestamp(time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC))},
			{types.Decimal(decimal.RequireFromString("0")), //Decimal
				types.Decimal(decimal.RequireFromString("-1.5")),
				types.Decimal(decimal.RequireFromString("4723245")),
				types.Decimal(decimal.RequireFromString("-1076416.875")),
				types.Decimal(decimal.RequireFromString("198728394234798423466321.27349757"))},
			{types.Uint(1), types.Uint(3), types.Uint(5), types.Uint(7), types.Uint(8)},                                                                                                    //Enum
			{types.Float(1.0), types.Float(65513.75), types.Float(4293902592), types.Float(4.58e71), types.Float(7.172e285)},                                                               //Float
			{types.InlineBlob{0}, types.InlineBlob{21}, types.InlineBlob{1, 17}, types.InlineBlob{72, 42}, types.InlineBlob{21, 122, 236}},                                                 //InlineBlob
			{types.Int(20), types.Int(215), types.Int(237493), types.Int(2035753568), types.Int(2384384576063)},                                                                            //Int
			{types.Uint(1), types.Uint(5), types.Uint(64), types.Uint(42), types.Uint(192)},                                                                                                //Set
			{types.Int(0), types.Int(1000000 /*"00:00:01"*/), types.Int(113000000 /*"00:01:53"*/), types.Int(247019000000 /*"68:36:59"*/), types.Int(458830485214 /*"127:27:10.485214"*/)}, //Time
			{types.Uint(20), types.Uint(275), types.Uint(328395), types.Uint(630257298), types.Uint(93897259874)},                                                                          //Uint
			{types.UUID{3}, types.UUID{3, 13}, types.UUID{128, 238, 82, 12}, types.UUID{31, 54, 23, 13, 63, 43}, types.UUID{83, 64, 21, 14, 42, 6, 35, 7, 54, 234, 6, 32, 1, 4, 2, 4}},     //Uuid
			//{types.String([]byte{1}), types.String([]byte{42, 52}), types.String([]byte{84, 32, 13, 63, 12, 86}), //VarBinary
			//	types.String([]byte{1, 32, 235, 64, 32, 23, 45, 76}), types.String([]byte{123, 234, 34, 223, 76, 35, 32, 12, 84, 26, 15, 34, 65, 86, 45, 23, 43, 12, 76, 154, 234, 76, 34})},
			{types.String(""), types.String("a"), types.String("abc"), //VarString
				types.String("abcdefghijklmnopqrstuvwxyz"), types.String("هذا هو بعض نماذج النص التي أستخدمها لاختبار عناصر")},
			{types.Int(1901), types.Int(1950), types.Int(2000), types.Int(2080), types.Int(2155)}, //Year
		}
}
