package tblcmds

import (
	"fmt"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/libraries/argparser"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/mvdata"
	"os"
)

var exportShortDesc = `Export the contents of a table to a file.`
var exportLongDesc = `dolt table export will export the contents of <table> to <file>

See the help for <b>dolt table import</b> as the options are the same.`
var exportSynopsis = []string{
	"[-f] [-pk <field>] [-schema <file>] [-map <file>] [-continue] [-file-type <type>] <table> <file>",
}

func validateExportArgs(apr *argparser.ArgParseResults, usage cli.UsagePrinter) (*mvdata.DataLocation, *mvdata.DataLocation) {
	if apr.NArg() != 2 {
		fmt.Println("Invalid usage.")
		usage()
		return nil, nil
	}

	tableName := apr.Arg(0)
	if !doltdb.IsValidTableName(tableName) {
		fmt.Fprintln(
			os.Stderr,
			color.RedString("'%s' is not a valid table name\n", tableName),
			"table names must match the regular expression:", doltdb.TableNameRegexStr)
		return nil, nil
	}

	path := apr.Arg(1)
	fType, _ := apr.GetValue(fileTypeParam)
	fileLoc := mvdata.NewDataLocation(path, fType)

	if fileLoc.Format == mvdata.InvalidDataFormat {
		fmt.Fprintln(
			os.Stderr,
			color.RedString("Could not infer type file '%s'\n", path),
			"File extensions should match supported file types, or should be explicitly defined via the file-type parameter")
		return nil, nil
	}

	tableLoc := &mvdata.DataLocation{tableName, mvdata.DoltDB}

	return tableLoc, fileLoc
}

func parseExportArgs(commandStr string, args []string) (bool, *mvdata.MoveOptions) {
	ap := argparser.NewArgParser()
	ap.ArgListHelp["table"] = "The table being exported."
	ap.ArgListHelp["file"] = "The file being output to."
	ap.SupportsFlag(forceParam, "f", "If data already exists in the destination, the Force flag will allow the target to be overwritten.")
	ap.SupportsFlag(contOnErrParam, "", "Continue exporting when row export errors are encountered.")
	ap.SupportsString(outSchemaParam, "s", "schema_file", "The schema for the output data.")
	ap.SupportsString(mappingFileParam, "m", "mapping_file", "A file that lays out how fields should be mapped from input data to output data.")
	ap.SupportsString(primaryKeyParam, "pk", "primary_key", "Explicitly define the name of the field in the schema which should be used as the primary key.")
	ap.SupportsString(fileTypeParam, "", "file_type", "Explicitly define the type of the file if it can't be inferred from the file extension.")

	help, usage := cli.HelpAndUsagePrinters(commandStr, exportShortDesc, exportLongDesc, exportSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)
	tableLoc, fileLoc := validateExportArgs(apr, usage)

	if fileLoc == nil || tableLoc == nil {
		return false, nil
	}

	schemaFile, _ := apr.GetValue(outSchemaParam)
	mappingFile, _ := apr.GetValue(mappingFileParam)
	primaryKey, _ := apr.GetValue(primaryKeyParam)

	return apr.Contains(forceParam), &mvdata.MoveOptions{
		mvdata.OverwriteOp,
		apr.Contains(contOnErrParam),
		schemaFile,
		mappingFile,
		primaryKey,
		tableLoc,
		fileLoc,
	}
}

func Export(commandStr string, args []string, dEnv *env.DoltEnv) int {
	force, mvOpts := parseExportArgs(commandStr, args)

	if mvOpts == nil {
		return 1
	}

	result := executeMove(dEnv, force, mvOpts)

	if result == 0 {
		fmt.Println(color.CyanString("Successfully exported data."))
	}

	return result
}