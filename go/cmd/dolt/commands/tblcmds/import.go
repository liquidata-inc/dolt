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

package tblcmds

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/schcmds"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/mvdata"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/funcitr"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	createParam      = "create-table"
	updateParam      = "update-table"
	replaceParam     = "replace-table"
	tableParam       = "table"
	fileParam        = "file"
	schemaParam      = "schema"
	mappingFileParam = "map"
	forceParam       = "force"
	contOnErrParam   = "continue"
	primaryKeyParam  = "pk"
	fileTypeParam    = "file-type"
	delimParam       = "delim"
)

var importDocs = cli.CommandDocumentationContent{
	ShortDesc: `Imports data into a dolt table`,
	LongDesc: `If {{.EmphasisLeft}}--create-table | -c{{.EmphasisRight}} is given the operation will create {{.LessThan}}table{{.GreaterThan}} and import the contents of file into it.  If a table already exists at this location then the operation will fail, unless the {{.EmphasisLeft}}--force | -f{{.EmphasisRight}} flag is provided. The force flag forces the existing table to be overwritten.

The schema for the new table can be specified explicitly by providing a SQL schema definition file, or will be inferred from the imported file.  All schemas, inferred or explicitly defined must define a primary key.  If the file format being imported does not support defining a primary key, then the {{.EmphasisLeft}}--pk{{.EmphasisRight}} parameter must supply the name of the field that should be used as the primary key.

If {{.EmphasisLeft}}--update-table | -u{{.EmphasisRight}} is given the operation will update {{.LessThan}}table{{.GreaterThan}} with the contents of file. The table's existing schema will be used, and field names will be used to match file fields with table fields unless a mapping file is specified.

During import, if there is an error importing any row, the import will be aborted by default.  Use the {{.EmphasisLeft}}--continue{{.EmphasisRight}} flag to continue importing when an error is encountered.

If {{.EmphasisLeft}}--replace-table | -r{{.EmphasisRight}} is given the operation will replace {{.LessThan}}table{{.GreaterThan}} with the contents of the file. The table's existing schema will be used, and field names will be used to match file fields with table fields unless a mapping file is specified.

If the schema for the existing table does not match the schema for the new file, the import will be aborted by default. To overwrite both the table and the schema, use {{.EmphasisLeft}}-c -f{{.EmphasisRight}}.

A mapping file can be used to map fields between the file being imported and the table being written to. This can be used when creating a new table, or updating or replacing an existing table.

` + schcmds.MappingFileHelp +

		`
In create, update, and replace scenarios the file's extension is used to infer the type of the file.  If a file does not have the expected extension then the {{.EmphasisLeft}}--file-type{{.EmphasisRight}} parameter should be used to explicitly define the format of the file in one of the supported formats (csv, psv, json, xlsx).  For files separated by a delimiter other than a ',' (type csv) or a '|' (type psv), the --delim parameter can be used to specify a delimeter`,

	Synopsis: []string{
		"-c [-f] [--pk {{.LessThan}}field{{.GreaterThan}}] [--schema {{.LessThan}}file{{.GreaterThan}}] [--map {{.LessThan}}file{{.GreaterThan}}] [--continue] [--file-type {{.LessThan}}type{{.GreaterThan}}] {{.LessThan}}table{{.GreaterThan}} {{.LessThan}}file{{.GreaterThan}}",
		"-u [--map {{.LessThan}}file{{.GreaterThan}}] [--continue] [--file-type {{.LessThan}}type{{.GreaterThan}}] {{.LessThan}}table{{.GreaterThan}} {{.LessThan}}file{{.GreaterThan}}",
		"-r [--map {{.LessThan}}file{{.GreaterThan}}] [--file-type {{.LessThan}}type{{.GreaterThan}}] {{.LessThan}}table{{.GreaterThan}} {{.LessThan}}file{{.GreaterThan}}",
	},
}

type tableImportOp string

const (
	CreateOp  tableImportOp = "overwrite"
	ReplaceOp tableImportOp = "replace"
	UpdateOp  tableImportOp = "update"
	InvalidOp tableImportOp = "invalid"
)

type importOptions struct {
	operation   tableImportOp
	tableName   string
	contOnErr   bool
	force       bool
	schFile     string
	primaryKeys []string
	nameMapper  rowconv.NameMapper
	src         mvdata.DataLocation
	dest        mvdata.TableDataLocation
	srcOptions  interface{}
}

func (m importOptions) WritesToTable() bool {
	return true
}

func (m importOptions) SrcName() string {
	if t, tblSrc := m.src.(mvdata.TableDataLocation); tblSrc {
		return t.Name
	}
	if f, fileSrc := m.src.(mvdata.FileDataLocation); fileSrc {
		return f.Path
	}
	return m.src.String()
}

func (m importOptions) DestName() string {
	return m.dest.Name
}

func (m importOptions) ColNameMapper() rowconv.NameMapper {
	return m.nameMapper
}

func (m importOptions) FloatThreshold() float64 {
	return 0.0
}

func (m importOptions) checkOverwrite(ctx context.Context, root *doltdb.RootValue, fs filesys.ReadableFS) (bool, error) {
	if !m.force && m.operation == CreateOp {
		return m.dest.Exists(ctx, root, fs)
	}
	return false, nil
}

func (m importOptions) srcIsJson() bool {
	_, isJson := m.srcOptions.(mvdata.JSONOptions)
	return isJson
}

func (m importOptions) srcIsStream() bool {
	_, isStream := m.src.(mvdata.StreamDataLocation)
	return isStream
}

func getImportMoveOptions(ctx context.Context, apr *argparser.ArgParseResults, dEnv *env.DoltEnv) (*importOptions, errhand.VerboseError) {
	tableName := apr.Arg(0)

	path := ""
	if apr.NArg() > 1 {
		path = apr.Arg(1)
	}

	fType, _ := apr.GetValue(fileTypeParam)
	srcLoc := mvdata.NewDataLocation(path, fType)
	delim, hasDelim := apr.GetValue(delimParam)

	schemaFile, _ := apr.GetValue(schemaParam)
	force := apr.Contains(forceParam)
	contOnErr := apr.Contains(contOnErrParam)

	val, _ := apr.GetValue(primaryKeyParam)
	pks := funcitr.MapStrings(strings.Split(val, ","), strings.TrimSpace)
	pks = funcitr.FilterStrings(pks, func(s string) bool { return s != "" })

	mappingFile := apr.GetValueOrDefault(mappingFileParam, "")
	colMapper, err := rowconv.NameMapperFromFile(mappingFile, dEnv.FS)
	if err != nil {
		return nil, errhand.VerboseErrorFromError(err)
	}

	var srcOpts interface{}
	switch val := srcLoc.(type) {
	case mvdata.FileDataLocation:
		if hasDelim {
			if val.Format == mvdata.InvalidDataFormat {
				val = mvdata.FileDataLocation{Path: val.Path, Format: mvdata.CsvFile}
				srcLoc = val
			}

			srcOpts = mvdata.CsvOptions{Delim: delim}
		}

		if val.Format == mvdata.XlsxFile {
			// table name must match sheet name currently
			srcOpts = mvdata.XlsxOptions{SheetName: tableName}
		} else if val.Format == mvdata.JsonFile {
			srcOpts = mvdata.JSONOptions{TableName: tableName, SchFile: schemaFile}
		}

	case mvdata.StreamDataLocation:
		if val.Format == mvdata.InvalidDataFormat {
			val = mvdata.StreamDataLocation{Format: mvdata.CsvFile, Reader: os.Stdin, Writer: iohelp.NopWrCloser(cli.CliOut)}
			srcLoc = val
		}

		if hasDelim {
			srcOpts = mvdata.CsvOptions{Delim: delim}
		}
	}

	var moveOp tableImportOp
	switch {
	case apr.Contains(createParam):
		moveOp = CreateOp
	case apr.Contains(replaceParam):
		moveOp = ReplaceOp
	default:
		moveOp = UpdateOp
	}

	if moveOp != CreateOp {
		root, err := dEnv.WorkingRoot(ctx)
		if err != nil {
			return nil, errhand.VerboseErrorFromError(err)
		}
		_, exists, err := root.GetTable(ctx, tableName)
		if err != nil {
			return nil, errhand.VerboseErrorFromError(err)
		}
		if !exists {
			return nil, errhand.BuildDError("The following table could not be found: %s", tableName).Build()
		}
	}

	tableLoc := mvdata.TableDataLocation{Name: tableName}

	return &importOptions{
		operation:   moveOp,
		tableName:   tableName,
		contOnErr:   contOnErr,
		force:       force,
		schFile:     schemaFile,
		nameMapper:  colMapper,
		primaryKeys: pks,
		src:         srcLoc,
		dest:        tableLoc,
		srcOptions:  srcOpts,
	}, nil

}

func validateImportArgs(apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.NArg() == 0 || apr.NArg() > 2 {
		return errhand.BuildDError("expected 1 or 2 arguments").SetPrintUsage().Build()
	}

	if apr.Contains(schemaParam) && apr.Contains(primaryKeyParam) {
		return errhand.BuildDError("parameters %s and %s are mutually exclusive", schemaParam, primaryKeyParam).Build()
	}

	if !apr.Contains(createParam) && !apr.Contains(updateParam) && !apr.Contains(replaceParam) {
		return errhand.BuildDError("Must include '-c' for initial table import or -u to update existing table or -r to replace existing table.").Build()
	}

	if apr.Contains(schemaParam) && !apr.Contains(createParam) {
		return errhand.BuildDError("fatal: " + schemaParam + " is not supported for update or replace operations").Build()
	}

	tableName := apr.Arg(0)
	if err := schcmds.ValidateTableNameForCreate(tableName); err != nil {
		return err
	}

	path := ""
	if apr.NArg() > 1 {
		path = apr.Arg(1)
	}

	fType, hasFileType := apr.GetValue(fileTypeParam)
	if hasFileType && mvdata.DFFromString(fType) == mvdata.InvalidDataFormat {
		return errhand.BuildDError("'%s' is not a valid file type.", fType).Build()
	}

	_, hasDelim := apr.GetValue(delimParam)
	srcLoc := mvdata.NewDataLocation(path, fType)

	switch val := srcLoc.(type) {
	case mvdata.FileDataLocation:
		if !hasDelim && val.Format == mvdata.InvalidDataFormat {
			return errhand.BuildDError("Could not infer type file '%s'\nFile extensions should match supported file types, or should be explicitly defined via the file-type parameter", path).Build()
		}

	case mvdata.TableDataLocation:
		if hasDelim {
			return errhand.BuildDError("delim is not a valid parameter for this type of file").Build()
		}
	}

	if srcFileLoc, isFileType := srcLoc.(mvdata.FileDataLocation); isFileType {
		if srcFileLoc.Format == mvdata.SqlFile {
			return errhand.BuildDError("For SQL import, please pipe SQL input files to `dolt sql`").Build()
		}

		_, hasSchema := apr.GetValue(schemaParam)
		if srcFileLoc.Format == mvdata.JsonFile && apr.Contains(createParam) && !hasSchema {
			return errhand.BuildDError("Please specify schema file for .json tables.").Build()
		}
	}

	return nil
}

type ImportCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd ImportCmd) Name() string {
	return "import"
}

// Description returns a description of the command
func (cmd ImportCmd) Description() string {
	return "Creates, overwrites, replaces, or updates a table from the data in a file."
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd ImportCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return commands.CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, importDocs, ap))
}

func (cmd ImportCmd) createArgParser() *argparser.ArgParser {
	ap := createArgParser()
	return ap
}

// EventType returns the type of the event to log
func (cmd ImportCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_TABLE_IMPORT
}

// Exec executes the command
func (cmd ImportCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()

	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, importDocs, ap))
	apr := cli.ParseArgs(ap, args, help)

	dEnv, err := commands.MaybeMigrateEnv(ctx, dEnv)

	var verr errhand.VerboseError
	if err != nil {
		verr = errhand.BuildDError("could not load manifest for gc").AddCause(err).Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	verr = validateImportArgs(apr)
	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	mvOpts, verr := getImportMoveOptions(ctx, apr, dEnv)

	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	root, err := dEnv.WorkingRoot(ctx)

	if err != nil {
		verr = errhand.BuildDError("Unable to get the working root value for this data repository.").AddCause(err).Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	mover, nDMErr := newImportDataMover(ctx, root, dEnv, mvOpts, importStatsCB)

	if nDMErr != nil {

		verr = newDataMoverErrToVerr(mvOpts, nDMErr)
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	skipped, verr := mvdata.MoveData(ctx, dEnv, mover, mvOpts)

	if skipped > 0 {
		cli.PrintErrln(color.YellowString("Lines skipped: %d", skipped))
	}
	if verr == nil {
		cli.PrintErrln(color.CyanString("Import completed successfully."))
	}

	return commands.HandleVErrAndExitCode(verr, usage)
}

func createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{tableParam, "The new or existing table being imported to."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{fileParam, "The file being imported. Supported file types are csv, psv, and nbf."})
	ap.SupportsFlag(createParam, "c", "Create a new table, or overwrite an existing table (with the -f flag) from the imported data.")
	ap.SupportsFlag(updateParam, "u", "Update an existing table with the imported data.")
	ap.SupportsFlag(forceParam, "f", "If a create operation is being executed, data already exists in the destination, the force flag will allow the target to be overwritten.")
	ap.SupportsFlag(replaceParam, "r", "Replace existing table with imported data while preserving the original schema.")
	ap.SupportsFlag(contOnErrParam, "", "Continue importing when row import errors are encountered.")
	ap.SupportsString(schemaParam, "s", "schema_file", "The schema for the output data.")
	ap.SupportsString(mappingFileParam, "m", "mapping_file", "A file that lays out how fields should be mapped from input data to output data.")
	ap.SupportsString(primaryKeyParam, "pk", "primary_key", "Explicitly define the name of the field in the schema which should be used as the primary key.")
	ap.SupportsString(fileTypeParam, "", "file_type", "Explicitly define the type of the file if it can't be inferred from the file extension.")
	ap.SupportsString(delimParam, "", "delimiter", "Specify a delimeter for a csv style file with a non-comma delimiter.")
	return ap
}

var displayStrLen int

func importStatsCB(stats types.AppliedEditStats) {
	noEffect := stats.NonExistentDeletes + stats.SameVal
	total := noEffect + stats.Modifications + stats.Additions
	displayStr := fmt.Sprintf("Rows Processed: %d, Additions: %d, Modifications: %d, Had No Effect: %d", total, stats.Additions, stats.Modifications, noEffect)
	displayStrLen = cli.DeleteAndPrint(displayStrLen, displayStr)
}

func newImportDataMover(ctx context.Context, root *doltdb.RootValue, dEnv *env.DoltEnv, impOpts *importOptions, statsCB noms.StatsCB) (*mvdata.DataMover, *mvdata.DataMoverCreationError) {
	var err error

	ow, err := impOpts.checkOverwrite(ctx, root, dEnv.FS)
	if err != nil {
		return nil, &mvdata.DataMoverCreationError{ErrType: mvdata.CreateReaderErr, Cause: err}
	}
	if ow {
		return nil, &mvdata.DataMoverCreationError{ErrType: mvdata.CreateReaderErr, Cause: fmt.Errorf("%s already exists. Use -f to overwrite.", impOpts.DestName())}
	}

	wrSch, dmce := getImportSchema(ctx, root, dEnv.FS, impOpts)
	if dmce != nil {
		return nil, dmce
	}

	rd, srcIsSorted, err := impOpts.src.NewReader(ctx, root, dEnv.FS, impOpts.srcOptions)
	if err != nil {
		return nil, &mvdata.DataMoverCreationError{ErrType: mvdata.CreateReaderErr, Cause: err}
	}

	defer func() {
		if rd != nil {
			rd.Close(ctx)
		}
	}()

	err = wrSch.GetPKCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		preImage := impOpts.nameMapper.PreImage(col.Name)
		_, found := rd.GetSchema().GetAllCols().GetByName(preImage)
		if !found {
			err = fmt.Errorf("input primary keys do not match primary keys of existing table")
		}
		return err == nil, err
	})

	if err != nil {
		return nil, &mvdata.DataMoverCreationError{ErrType: mvdata.SchemaErr, Cause: err}
	}

	transforms, err := mvdata.NameMapTransform(ctx, root.VRW(), rd.GetSchema(), wrSch, impOpts.nameMapper)

	if err != nil {
		return nil, &mvdata.DataMoverCreationError{ErrType: mvdata.CreateMapperErr, Cause: err}
	}

	var wr table.TableWriteCloser
	switch impOpts.operation {
	case CreateOp:
		wr, err = impOpts.dest.NewCreatingWriter(ctx, impOpts, dEnv, root, srcIsSorted, wrSch, statsCB, true)
	case ReplaceOp:
		wr, err = impOpts.dest.NewReplacingWriter(ctx, impOpts, dEnv, root, srcIsSorted, wrSch, statsCB, true)
	case UpdateOp:
		wr, err = impOpts.dest.NewUpdatingWriter(ctx, impOpts, dEnv, root, srcIsSorted, wrSch, statsCB, true)
	default:
		err = errors.New("invalid move operation")
	}

	if err != nil {
		return nil, &mvdata.DataMoverCreationError{ErrType: mvdata.CreateWriterErr, Cause: err}
	}

	imp := &mvdata.DataMover{Rd: rd, Transforms: transforms, Wr: wr, ContOnErr: impOpts.contOnErr}
	rd = nil

	return imp, nil
}

func getImportSchema(ctx context.Context, root *doltdb.RootValue, fs filesys.Filesys, impOpts *importOptions) (schema.Schema, *mvdata.DataMoverCreationError) {
	if impOpts.schFile != "" {
		tn, out, err := mvdata.SchAndTableNameFromFile(ctx, impOpts.schFile, fs, root)

		if err == nil && tn != impOpts.tableName {
			err = fmt.Errorf("table name '%s' from schema file %s does not match table arg '%s'", tn, impOpts.schFile, impOpts.tableName)
		}

		if err != nil {
			return nil, &mvdata.DataMoverCreationError{ErrType: mvdata.SchemaErr, Cause: err}
		}

		return out, nil
	}

	if impOpts.operation == CreateOp {
		if impOpts.srcIsStream() {
			// todo: capture stream data to file so we can use schema inferrence
			return nil, nil
		}

		rd, _, err := impOpts.src.NewReader(ctx, root, fs, impOpts.srcOptions)
		if err != nil {
			return nil, &mvdata.DataMoverCreationError{ErrType: mvdata.CreateReaderErr, Cause: err}
		}
		defer rd.Close(ctx)

		if impOpts.srcIsJson() {
			return rd.GetSchema(), nil
		}

		outSch, err := mvdata.InferSchema(ctx, root, rd, impOpts.tableName, impOpts.primaryKeys, impOpts)
		if err != nil {
			return nil, &mvdata.DataMoverCreationError{ErrType: mvdata.SchemaErr, Cause: err}
		}

		return outSch, nil
	}

	// UpdateOp || ReplaceOp
	tblRd, _, err := impOpts.dest.NewReader(ctx, root, fs, nil)
	if err != nil {
		return nil, &mvdata.DataMoverCreationError{ErrType: mvdata.CreateReaderErr, Cause: err}
	}
	defer tblRd.Close(ctx)

	return tblRd.GetSchema(), nil
}

func newDataMoverErrToVerr(mvOpts *importOptions, err *mvdata.DataMoverCreationError) errhand.VerboseError {
	switch err.ErrType {
	case mvdata.CreateReaderErr:
		bdr := errhand.BuildDError("Error creating reader for %s.", mvOpts.src.String())
		bdr.AddDetails("When attempting to move data from %s to %s, could not open a reader.", mvOpts.src.String(), mvOpts.dest.String())
		return bdr.AddCause(err.Cause).Build()

	case mvdata.SchemaErr:
		bdr := errhand.BuildDError("Error determining the output schema.")
		bdr.AddDetails("When attempting to move data from %s to %s, could not determine the output schema.", mvOpts.src.String(), mvOpts.dest.String())
		bdr.AddDetails(`Schema File: "%s"`, mvOpts.schFile)
		bdr.AddDetails(`explicit pks: "%s"`, strings.Join(mvOpts.primaryKeys, ","))
		return bdr.AddCause(err.Cause).Build()

	case mvdata.MappingErr:
		bdr := errhand.BuildDError("Error determining the mapping from input fields to output fields.")
		bdr.AddDetails("When attempting to move data from %s to %s, determine the mapping from input fields t, output fields.", mvOpts.src.String(), mvOpts.dest.String())
		bdr.AddDetails(`Mapping File: "%s"`, mvOpts.nameMapper)
		return bdr.AddCause(err.Cause).Build()

	case mvdata.ReplacingErr:
		bdr := errhand.BuildDError("Error replacing table")
		bdr.AddDetails("When attempting to replace data with %s, could not validate schema.", mvOpts.src.String())
		return bdr.AddCause(err.Cause).Build()

	case mvdata.CreateMapperErr:
		bdr := errhand.BuildDError("Error creating input to output mapper.")
		details := fmt.Sprintf("When attempting to move data from %s to %s, could not create a mapper.", mvOpts.src.String(), mvOpts.dest.String())
		bdr.AddDetails(details)
		bdr.AddCause(err.Cause)

		return bdr.AddCause(err.Cause).Build()

	case mvdata.CreateWriterErr:
		if err.Cause == mvdata.ErrNoPK {
			builder := errhand.BuildDError("Attempting to write to %s with a schema that does not contain a primary key.", mvOpts.dest.String())
			builder.AddDetails("A primary key is required and can be specified by:\n" +
				"\tusing -pk option to designate a field as the primary key by name.\n" +
				"\tusing -schema to provide a schema descriptor file.")
			return builder.Build()
		} else {
			bdr := errhand.BuildDError("Error creating writer for %s.\n", mvOpts.dest.String())
			bdr.AddDetails("When attempting to move data from %s to %s, could not open a writer.", mvOpts.src.String(), mvOpts.dest.String())
			return bdr.AddCause(err.Cause).Build()
		}

	case mvdata.CreateSorterErr:
		bdr := errhand.BuildDError("Error creating sorting reader.")
		bdr.AddDetails("When attempting to move data from %s to %s, could not open create sorting reader.", mvOpts.src.String(), mvOpts.dest.String())
		return bdr.AddCause(err.Cause).Build()
	}

	panic("Unhandled Error type")
}
