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

package mvdata

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/env"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/json"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/csv"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/sqlexport"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/xlsx"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

// DFFromString returns a data object from a string.
func DFFromString(dfStr string) DataFormat {
	switch strings.ToLower(dfStr) {
	case "csv", ".csv":
		return CsvFile
	case "psv", ".psv":
		return PsvFile
	case "xlsx", ".xlsx":
		return XlsxFile
	case "json", ".json":
		return JsonFile
	case "sql", ".sql":
		return SqlFile
	default:
		return InvalidDataFormat
	}
}

// FileDataLocation is a file that that can be imported from or exported to.
type FileDataLocation struct {
	// Path is the path of the file on the filesystem
	Path string

	// Format is the DataFormat of the file
	Format DataFormat
}

// String returns a string representation of the data location.
func (dl FileDataLocation) String() string {
	return dl.Format.ReadableStr() + ":" + dl.Path
}

// Exists returns true if the DataLocation already exists
func (dl FileDataLocation) Exists(ctx context.Context, root *doltdb.RootValue, fs filesys.ReadableFS) (bool, error) {
	exists, _ := fs.Exists(dl.Path)
	return exists, nil
}

// NewReader creates a TableReadCloser for the DataLocation
func (dl FileDataLocation) NewReader(ctx context.Context, root *doltdb.RootValue, fs filesys.ReadableFS, opts interface{}) (rdCl table.TableReadCloser, sorted bool, err error) {
	exists, isDir := fs.Exists(dl.Path)

	if !exists {
		return nil, false, os.ErrNotExist
	} else if isDir {
		return nil, false, filesys.ErrIsDir
	}

	switch dl.Format {
	case CsvFile:
		delim := ","

		if opts != nil {
			csvOpts, _ := opts.(CsvOptions)

			if len(csvOpts.Delim) != 0 {
				delim = csvOpts.Delim
			}
		}

		rd, err := csv.OpenCSVReader(root.VRW().Format(), dl.Path, fs, csv.NewCSVInfo().SetDelim(delim))

		return rd, false, err

	case PsvFile:
		rd, err := csv.OpenCSVReader(root.VRW().Format(), dl.Path, fs, csv.NewCSVInfo().SetDelim("|"))
		return rd, false, err

	case XlsxFile:
		xlsxOpts := opts.(XlsxOptions)
		rd, err := xlsx.OpenXLSXReader(ctx, root.VRW(), dl.Path, fs, &xlsx.XLSXFileInfo{SheetName: xlsxOpts.SheetName})
		return rd, false, err

	case JsonFile:
		var sch schema.Schema
		jsonOpts, _ := opts.(JSONOptions)
		if jsonOpts.SchFile != "" {
			tn, s, err := SchAndTableNameFromFile(ctx, jsonOpts.SchFile, fs, root)
			if err != nil {
				return nil, false, err
			}
			if tn != jsonOpts.TableName {
				return nil, false, fmt.Errorf("table name '%s' from schema file %s does not match table arg '%s'", tn, jsonOpts.SchFile, jsonOpts.TableName)
			}
			sch = s
		} else {
			if opts == nil {
				return nil, false, errors.New("Unable to determine table name on JSON import")
			}
			tbl, exists, err := root.GetTable(context.TODO(), jsonOpts.TableName)
			if !exists {
				return nil, false, errors.New(fmt.Sprintf("The following table could not be found:\n%v", jsonOpts.TableName))
			}
			if err != nil {
				return nil, false, errors.New(fmt.Sprintf("An error occurred attempting to read the table:\n%v", err.Error()))
			}
			sch, err = tbl.GetSchema(context.TODO())
			if err != nil {
				return nil, false, errors.New(fmt.Sprintf("An error occurred attempting to read the table schema:\n%v", err.Error()))
			}
		}

		rd, err := json.OpenJSONReader(root.VRW(), dl.Path, fs, sch)
		return rd, false, err
	}

	return nil, false, errors.New("unsupported format")
}

// NewCreatingWriter will create a TableWriteCloser for a DataLocation that will create a new table, or overwrite
// an existing table.
func (dl FileDataLocation) NewCreatingWriter(ctx context.Context, mvOpts DataMoverOptions, dEnv *env.DoltEnv, root *doltdb.RootValue, _ bool, outSch schema.Schema, _ noms.StatsCB, _ bool) (table.TableWriteCloser, error) {
	switch dl.Format {
	case CsvFile:
		return csv.OpenCSVWriter(dl.Path, dEnv.FS, outSch, csv.NewCSVInfo())
	case PsvFile:
		return csv.OpenCSVWriter(dl.Path, dEnv.FS, outSch, csv.NewCSVInfo().SetDelim("|"))
	case XlsxFile:
		panic("writing to xlsx files is not supported yet")
	case JsonFile:
		return json.OpenJSONWriter(dl.Path, dEnv.FS, outSch)
	case SqlFile:
		return sqlexport.OpenSQLExportWriter(ctx, dl.Path, dEnv.FS, root, mvOpts.SrcName(), outSch)
	}

	panic("Invalid Data Format." + string(dl.Format))
}

// NewUpdatingWriter will create a TableWriteCloser for a DataLocation that will update and append rows based on
// their primary key.
func (dl FileDataLocation) NewUpdatingWriter(_ context.Context, _ DataMoverOptions, _ *env.DoltEnv, _ *doltdb.RootValue, _ bool, _ schema.Schema, _ noms.StatsCB, _ bool) (table.TableWriteCloser, error) {
	panic("Updating of files is not supported")
}

// NewReplacingWriter will create a TableWriteCloser for a DataLocation that will overwrite an existing table while
// preserving schema
func (dl FileDataLocation) NewReplacingWriter(_ context.Context, _ DataMoverOptions, _ *env.DoltEnv, _ *doltdb.RootValue, _ bool, _ schema.Schema, _ noms.StatsCB, _ bool) (table.TableWriteCloser, error) {
	panic("Replacing files is not supported")
}
