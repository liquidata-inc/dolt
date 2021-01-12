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

package xlsx

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped"
	"github.com/dolthub/dolt/go/store/types"
)

func TestDecodeXLSXRows(t *testing.T) {
	colNames := []string{"id", "first", "last", "age"}
	_, sch := untyped.NewUntypedSchema(colNames...)

	second := [][][]string{}
	first := [][]string{{"id", "first", "last", "age"}, {"1", "osheiza", "otori", "24"}}
	second = append(second, first)

	vrw := types.NewInternalValueStore()
	decoded, err := decodeXLSXRows(context.Background(), vrw, second, sch)
	if err != nil {
		fmt.Println(err)

	}

	taggedVals := make(row.TaggedValues, sch.GetAllCols().Size())
	str := "1"
	taggedVals[uint64(0)], _ = typeinfo.StringDefaultType.ParseValue(context.Background(), nil, &str)
	str = "osheiza"
	taggedVals[uint64(1)], _ = typeinfo.StringDefaultType.ParseValue(context.Background(), nil, &str)
	str = "otori"
	taggedVals[uint64(2)], _ = typeinfo.StringDefaultType.ParseValue(context.Background(), nil, &str)
	str = "24"
	taggedVals[uint64(3)], _ = typeinfo.StringDefaultType.ParseValue(context.Background(), nil, &str)

	newRow, err := row.New(vrw.Format(), sch, taggedVals)

	assert.NoError(t, err)

	if !reflect.DeepEqual(decoded[0], newRow) {
		t.Log("error!")
	}
}

func TestGetRows(t *testing.T) {
	path := "test_files/employees.xlsx"
	stateCols, _ := getXlsxRowsFromPath(path, "states")
	employeeCols, _ := getXlsxRowsFromPath(path, "employees")

	if stateCols != nil || employeeCols == nil {
		t.Fatal("error")
	}
}

func TestGetRowsFromBinary(t *testing.T) {
	xlsxBinary := getBytesFromXlsx()
	stateCols, _ := getXlsxRowsFromBinary(xlsxBinary, "states")
	employeeCols, _ := getXlsxRowsFromBinary(xlsxBinary, "employees")

	if stateCols != nil || employeeCols == nil {
		t.Fatal("error")
	}
}

func getBytesFromXlsx() []byte {
	f, err := os.Open("test_files/employees.xlsx")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	bs, err := ioutil.ReadAll(f)
	if err != nil {
		panic(err)
	}
	return bs
}
