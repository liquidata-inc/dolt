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
	"errors"
	"fmt"
	"strings"

	"github.com/tealeg/xlsx"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

var ErrTableNameMatchSheetName = errors.New("table name must match excel sheet name.")

func UnmarshalFromXLSX(path string) ([][][]string, error) {
	data, err := openFile(path)

	if err != nil {
		return nil, err
	}

	dataSlice, err := data.ToSlice()
	if err != nil {
		return nil, err
	}

	return dataSlice, nil
}

func openFile(path string) (*xlsx.File, error) {
	data, err := xlsx.OpenFile(path)

	if err != nil {
		msg := strings.ReplaceAll(err.Error(), "zip", "xlsx")
		return nil, fmt.Errorf(msg)
	}

	return data, nil
}

func openBinary(content []byte) (*xlsx.File, error) {
	data, err := xlsx.OpenBinary(content)

	if err != nil {
		msg := strings.ReplaceAll(err.Error(), "zip", "xlsx")
		return nil, fmt.Errorf(msg)
	}

	return data, nil
}

func decodeXLSXRows(ctx context.Context, vrw types.ValueReadWriter, xlData [][][]string, sch schema.Schema) ([]row.Row, error) {
	var rows []row.Row

	var err error

	cols := sch.GetAllCols()
	numSheets := len(xlData)
	dataVals := xlData[0]
	header := dataVals[0]
	numRows := len(dataVals) - 1

	taggedVals := make(row.TaggedValues, len(header))

	for j := 0; j < numSheets; j++ {
		for i := 0; i < numRows; i++ {
			for k, v := range header {
				col, ok := cols.GetByName(v)
				if !ok {
					return nil, errors.New(v + "is not a valid column")
				}
				valString := dataVals[i+1][k]
				taggedVals[col.Tag], err = col.TypeInfo.ParseValue(ctx, vrw, &valString)
				if err != nil {
					return nil, err
				}
			}
			r, err := row.New(vrw.Format(), sch, taggedVals)

			if err != nil {
				return nil, err
			}

			rows = append(rows, r)
			fmt.Println(rows)
		}

	}
	return rows, nil
}

func getXlsxRowsFromPath(path string, tblName string) ([][][]string, error) {
	data, err := openFile(path)
	if err != nil {
		return nil, err
	}

	return getXlsxRows(data, tblName)
}

func getXlsxRowsFromBinary(content []byte, tblName string) ([][][]string, error) {
	data, err := openBinary(content)
	if err != nil {
		return nil, err
	}

	return getXlsxRows(data, tblName)
}

func getXlsxRows(data *xlsx.File, tblName string) ([][][]string, error) {
	var rows [][]string
	var allRows [][][]string
	for _, sheet := range data.Sheets {
		if sheet.Name == tblName {
			for i := 0; i < len(sheet.Rows); i++ {
				var rowVals []string
				for j := 0; j < len(sheet.Rows[i].Cells); j++ {

					rowVals = append(rowVals, sheet.Rows[i].Cells[j].Value)
				}
				rows = append(rows, rowVals)
			}
			allRows = append(allRows, rows)
			return allRows, nil
		}

	}
	return nil, ErrTableNameMatchSheetName
}
