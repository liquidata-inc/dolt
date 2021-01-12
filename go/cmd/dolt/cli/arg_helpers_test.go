// Copyright 2019 Dolthub, Inc.
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

package cli

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

func mustValue(v types.Value, err error) types.Value {
	if err != nil {
		panic(err)
	}

	return v
}

func mustString(str string, err error) string {
	if err != nil {
		panic(err)
	}

	return str
}

func TestParseKeyValues(t *testing.T) {
	ctx := context.Background()

	const (
		lnColName = "last"
		fnColName = "first"
		mnColName = "middle"
		lnColTag  = 1
		fnColTag  = 0
		mnColTag  = 2
	)

	testKeyColColl, _ := schema.NewColCollection(
		schema.NewColumn(lnColName, lnColTag, types.StringKind, true),
		schema.NewColumn(fnColName, fnColTag, types.StringKind, true),
		schema.NewColumn(mnColName, mnColTag, types.StringKind, true),
	)

	sch, err := schema.SchemaFromCols(testKeyColColl)
	require.NoError(t, err)

	singleKeyColColl, _ := schema.NewColCollection(
		schema.NewColumn(lnColName, lnColTag, types.StringKind, true),
	)

	singleKeySch, err := schema.SchemaFromCols(singleKeyColColl)
	require.NoError(t, err)

	tests := []struct {
		sch          schema.Schema
		args         []string
		expectedKeys []types.Value
		expectErr    bool
	}{
		{
			singleKeySch,
			[]string{"robertson"},
			[]types.Value{
				mustValue(row.TaggedValues{lnColTag: types.String("robertson")}.NomsTupleForPKCols(types.Format_7_18, singleKeyColColl).Value(ctx)),
			},
			false,
		},

		{
			singleKeySch,
			[]string{"last", "robertson"},
			[]types.Value{
				mustValue(row.TaggedValues{lnColTag: types.String("robertson")}.NomsTupleForPKCols(types.Format_7_18, singleKeyColColl).Value(ctx)),
			},
			false,
		},
		{
			singleKeySch,
			[]string{"last"},
			[]types.Value{},
			false,
		},
		{
			singleKeySch,
			[]string{"last", "last"},
			[]types.Value{
				mustValue(row.TaggedValues{lnColTag: types.String("last")}.NomsTupleForPKCols(types.Format_7_18, singleKeyColColl).Value(ctx)),
			},
			false,
		},
		{
			singleKeySch,
			[]string{"last", "robertson", "johnson"},
			[]types.Value{
				mustValue(row.TaggedValues{lnColTag: types.String("robertson")}.NomsTupleForPKCols(types.Format_7_18, singleKeyColColl).Value(ctx)),
				mustValue(row.TaggedValues{lnColTag: types.String("johnson")}.NomsTupleForPKCols(types.Format_7_18, singleKeyColColl).Value(ctx)),
			},
			false,
		},

		{
			sch,
			[]string{"last"},
			nil,
			false,
		},
		{
			sch,
			[]string{"last", "robertson", "johnson"},
			[]types.Value{
				mustValue(row.TaggedValues{lnColTag: types.String("robertson")}.NomsTupleForPKCols(types.Format_7_18, testKeyColColl).Value(ctx)),
				mustValue(row.TaggedValues{lnColTag: types.String("johnson")}.NomsTupleForPKCols(types.Format_7_18, testKeyColColl).Value(ctx)),
			},
			false,
		},
		{
			sch,
			[]string{"first,last", "robert,robertson", "john,johnson"},
			[]types.Value{
				mustValue(row.TaggedValues{lnColTag: types.String("robertson"), fnColTag: types.String("robert")}.NomsTupleForPKCols(types.Format_7_18, testKeyColColl).Value(ctx)),
				mustValue(row.TaggedValues{lnColTag: types.String("johnson"), fnColTag: types.String("john")}.NomsTupleForPKCols(types.Format_7_18, testKeyColColl).Value(ctx)),
			},
			false,
		},
	}

	for _, test := range tests {
		vrw := types.NewInternalValueStore()
		actual, err := ParseKeyValues(ctx, vrw, test.sch, test.args)

		if test.expectErr != (err != nil) {
			t.Error(test.args, "produced an unexpected error")
		} else {
			longer := len(actual)
			if len(test.expectedKeys) > longer {
				longer = len(test.expectedKeys)
			}

			for i := 0; i < longer; i++ {
				var currActual types.Value = types.NullValue
				var currExpected types.Value = types.NullValue

				if i < len(test.expectedKeys) {
					currExpected = test.expectedKeys[i]
				}

				if i < len(actual) {
					currActual = actual[i]
				}

				if !currActual.Equals(currExpected) {
					t.Error("actual:", mustString(types.EncodedValue(context.Background(), currActual)), "!= expected:", mustString(types.EncodedValue(context.Background(), currExpected)))
				}
			}
		}
	}
}
