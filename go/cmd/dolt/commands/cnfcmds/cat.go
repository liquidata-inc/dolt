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

package cnfcmds

import (
	"context"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/fwt"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/nullprinter"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/store/types"
)

var catDocs = cli.CommandDocumentationContent{
	ShortDesc: "print conflicts",
	LongDesc:  `The dolt conflicts cat command reads table conflicts and writes them to the standard output.`,
	Synopsis: []string{
		"[{{.LessThan}}commit{{.GreaterThan}}] {{.LessThan}}table{{.GreaterThan}}...",
	},
}

type CatCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd CatCmd) Name() string {
	return "cat"
}

// Description returns a description of the command
func (cmd CatCmd) Description() string {
	return "Writes out the table conflicts."
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd CatCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return commands.CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, catDocs, ap))
}

// EventType returns the type of the event to log
func (cmd CatCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_CONF_CAT
}

func (cmd CatCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "List of tables to be printed. '.' can be used to print conflicts for all tables."})

	return ap
}

// Exec executes the command
func (cmd CatCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, catDocs, ap))
	apr := cli.ParseArgs(ap, args, help)
	args = apr.Args()

	if len(args) == 0 {
		cli.Println("No tables specified")
		cli.Println(" Maybe you wanted to say 'dolt conflicts cat .'?")
		usage()
		return 1
	}

	root, verr := commands.GetWorkingWithVErr(dEnv)
	if verr != nil {
		return exitWithVerr(verr)
	}

	cm, verr := commands.MaybeGetCommitWithVErr(dEnv, args[0])
	if verr != nil {
		return exitWithVerr(verr)
	}

	// If no commit was resolved from the first argument, assume the args are all table names and print the conflicts
	if cm == nil {
		if verr := printConflicts(ctx, root, args); verr != nil {
			return exitWithVerr(verr)
		}

		return 0
	}

	tblNames := args[1:]
	if len(tblNames) == 0 {
		cli.Println("No tables specified")
		usage()
		return 1
	}

	root, err := cm.GetRootValue()
	if err != nil {
		return exitWithVerr(errhand.BuildDError("unable to get the root value").AddCause(err).Build())
	}

	if verr = printConflicts(ctx, root, tblNames); verr != nil {
		return exitWithVerr(verr)
	}

	return 0
}

func exitWithVerr(verr errhand.VerboseError) int {
	cli.PrintErrln(verr.Verbose())
	return 1
}

func printConflicts(ctx context.Context, root *doltdb.RootValue, tblNames []string) errhand.VerboseError {
	if len(tblNames) == 1 && tblNames[0] == "." {
		var err error
		tblNames, err = doltdb.UnionTableNames(ctx, root)

		if err != nil {
			return errhand.BuildDError("unable to read tables").AddCause(err).Build()
		}
	}

	for _, tblName := range tblNames {
		verr := func() errhand.VerboseError {
			if has, err := root.HasTable(ctx, tblName); err != nil {
				return errhand.BuildDError("error: unable to read database").AddCause(err).Build()
			} else if !has {
				return errhand.BuildDError("error: unknown table '%s'", tblName).Build()
			}

			tbl, _, err := root.GetTable(ctx, tblName)

			if err != nil {
				return errhand.BuildDError("error: unable to read database").AddCause(err).Build()
			}

			cnfRd, err := merge.NewConflictReader(ctx, tbl)

			if err == doltdb.ErrNoConflicts {
				return nil
			} else if err != nil {
				return errhand.BuildDError("failed to read conflicts").AddCause(err).Build()
			}

			defer cnfRd.Close()

			vrw := types.NewInternalValueStore() // Some types require a vrw but we're just displaying, so we use an internal store
			splitter, err := merge.NewConflictSplitter(ctx, vrw, cnfRd.GetJoiner())

			if err != nil {
				return errhand.BuildDError("error: unable to handle schemas").AddCause(err).Build()
			}

			cnfWr, err := merge.NewConflictSink(iohelp.NopWrCloser(cli.CliOut), splitter.GetSchema(), " | ")
			defer cnfWr.Close()

			if err != nil {
				return errhand.BuildDError("error: unable to read database").AddCause(err).Build()
			}

			nullPrinter := nullprinter.NewNullPrinter(splitter.GetSchema())
			fwtTr := fwt.NewAutoSizingFWTTransformer(splitter.GetSchema(), fwt.HashFillWhenTooLong, 1000)
			transforms := pipeline.NewTransformCollection(
				pipeline.NewNamedTransform("split", splitter.SplitConflicts),
				pipeline.NewNamedTransform(nullprinter.NullPrintingStage, nullPrinter.ProcessRow),
				pipeline.NamedTransform{Name: "fwt", Func: fwtTr.TransformToFWT},
			)

			// TODO: Pipeline should be contextified.
			srcProcFunc := pipeline.ProcFuncForSourceFunc(func() (row.Row, pipeline.ImmutableProperties, error) { return cnfRd.NextConflict(ctx) })
			sinkProcFunc := pipeline.ProcFuncForSinkFunc(cnfWr.ProcRowWithProps)
			p := pipeline.NewAsyncPipeline(srcProcFunc, sinkProcFunc, transforms, func(failure *pipeline.TransformRowFailure) (quit bool) {
				panic("")
			})

			colNames, err := schema.ExtractAllColNames(splitter.GetSchema())

			if err != nil {
				return errhand.BuildDError("error: failed to read columns from schema").AddCause(err).Build()
			}
			r, err := untyped.NewRowFromTaggedStrings(tbl.Format(), splitter.GetSchema(), colNames)

			if err != nil {
				return errhand.BuildDError("error: failed to create header row for printing").AddCause(err).Build()
			}

			p.InjectRow("fwt", r)

			p.Start()
			p.Wait()

			return nil
		}()

		if verr != nil {
			return verr
		}
	}

	return nil
}
