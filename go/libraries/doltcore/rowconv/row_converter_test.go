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

package rowconv

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped"
	"github.com/dolthub/dolt/go/store/types"
)

var srcCols, _ = schema.NewColCollection(
	schema.NewColumn("uuidtostr", 0, types.UUIDKind, true),
	schema.NewColumn("floattostr", 1, types.FloatKind, false),
	schema.NewColumn("uinttostr", 2, types.UintKind, false),
	schema.NewColumn("booltostr", 3, types.BoolKind, false),
	schema.NewColumn("inttostr", 4, types.IntKind, false),
	schema.NewColumn("stringtostr", 5, types.StringKind, false),
	schema.NewColumn("timestamptostr", 6, types.TimestampKind, false),
)

var srcSch = schema.MustSchemaFromCols(srcCols)

func TestRowConverter(t *testing.T) {
	mapping, err := TypedToUntypedMapping(srcSch)

	assert.NoError(t, err)

	vrw := types.NewInternalValueStore()
	rConv, err := NewRowConverter(context.Background(), vrw, mapping)

	if err != nil {
		t.Fatal("Error creating row converter")
	}

	id, _ := uuid.NewRandom()
	tt := types.Timestamp(time.Now())
	inRow, err := row.New(vrw.Format(), srcSch, row.TaggedValues{
		0: types.UUID(id),
		1: types.Float(1.25),
		2: types.Uint(12345678),
		3: types.Bool(true),
		4: types.Int(-1234),
		5: types.String("string string string"),
		6: tt,
	})

	assert.NoError(t, err)
	outData, err := rConv.Convert(inRow)
	assert.NoError(t, err)

	destSch := mapping.DestSch
	expected, err := row.New(vrw.Format(), destSch, row.TaggedValues{
		0: types.String(id.String()),
		1: types.String("1.25"),
		2: types.String("12345678"),
		3: types.String("1"),
		4: types.String("-1234"),
		5: types.String("string string string"),
		6: types.String(tt.String()),
	})

	assert.NoError(t, err)

	if !row.AreEqual(outData, expected, destSch) {
		t.Error("\n", row.Fmt(context.Background(), expected, destSch), "!=\n", row.Fmt(context.Background(), outData, destSch))
	}
}

func TestUnneccessaryConversion(t *testing.T) {
	mapping, err := TagMapping(srcSch, srcSch)

	if err != nil {
		t.Error(err)
	}

	vrw := types.NewInternalValueStore()
	rconv, err := NewRowConverter(context.Background(), vrw, mapping)

	if !rconv.IdentityConverter {
		t.Error("expected identity converter")
	}
}

func TestSpecialBoolHandling(t *testing.T) {
	col1, err := schema.NewColumnWithTypeInfo("pk", 0, typeinfo.Int64Type, true, "", false, "")
	require.NoError(t, err)
	col2, err := schema.NewColumnWithTypeInfo("v", 1, typeinfo.PseudoBoolType, false, "", false, "")
	require.NoError(t, err)
	colColl, _ := schema.NewColCollection(col1, col2)
	sch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)
	untypedSch, err := untyped.UntypeSchema(sch)
	require.NoError(t, err)

	mapping, err := TagMapping(untypedSch, sch)
	require.NoError(t, err)
	vrw := types.NewInternalValueStore()
	rconv, err := NewImportRowConverter(context.Background(), vrw, mapping)
	require.NoError(t, err)
	inRow, err := row.New(vrw.Format(), untypedSch, row.TaggedValues{
		0: types.String("76"),
		1: types.String("true"),
	})
	require.NoError(t, err)
	outData, err := rconv.Convert(inRow)
	require.NoError(t, err)
	require.NotNil(t, outData)

	expected, err := row.New(vrw.Format(), mapping.DestSch, row.TaggedValues{
		0: types.Int(76),
		1: types.Uint(1),
	})
	require.NoError(t, err)
	assert.True(t, row.AreEqual(outData, expected, mapping.DestSch))

	rconvNoHandle, err := NewRowConverter(context.Background(), vrw, mapping)
	require.NoError(t, err)
	results, errStr := rconvNoHandle.Convert(inRow)
	assert.Nil(t, results)
	assert.NotEmpty(t, errStr)
}
