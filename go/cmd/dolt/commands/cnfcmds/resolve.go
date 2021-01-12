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

	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var resDocumentation = cli.CommandDocumentationContent{
	ShortDesc: "Removes rows from list of conflicts",
	LongDesc: `
When a merge operation finds conflicting changes, the rows with the conflicts are added to list of conflicts that must be resolved.  Once the value for the row is resolved in the working set of tables, then the conflict should be resolved.
		
In it's first form {{.EmphasisLeft}}dolt conflicts resolve <table> <key>...{{.EmphasisRight}}, resolve runs in manual merge mode resolving the conflicts whose keys are provided.

In it's second form {{.EmphasisLeft}}dolt conflicts resolve --ours|--theirs <table>...{{.EmphasisRight}}, resolve runs in auto resolve mode. Where conflicts are resolved using a rule to determine which version of a row should be used.
`,
	Synopsis: []string{
		`{{.LessThan}}table{{.GreaterThan}} [{{.LessThan}}key_definition{{.GreaterThan}}] {{.LessThan}}key{{.GreaterThan}}...`,
		`--ours|--theirs {{.LessThan}}table{{.GreaterThan}}...`,
	},
}

const (
	oursFlag   = "ours"
	theirsFlag = "theirs"
)

var autoResolvers = map[string]merge.AutoResolver{
	oursFlag:   merge.Ours,
	theirsFlag: merge.Theirs,
}

var autoResolverParams []string

func init() {
	autoResolverParams = make([]string, 0, len(autoResolvers))
	for k := range autoResolvers {
		autoResolverParams = append(autoResolverParams, k)
	}
}

type ResolveCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd ResolveCmd) Name() string {
	return "resolve"
}

// Description returns a description of the command
func (cmd ResolveCmd) Description() string {
	return "Removes rows from list of conflicts"
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd ResolveCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return commands.CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, resDocumentation, ap))
}

// EventType returns the type of the event to log
func (cmd ResolveCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_CONF_RESOLVE
}

func (cmd ResolveCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "List of tables to be printed. When in auto-resolve mode, '.' can be used to resolve all tables."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"key", "key(s) of rows within a table whose conflicts have been resolved"})
	ap.SupportsFlag("ours", "", "For all conflicts, take the version from our branch and resolve the conflict")
	ap.SupportsFlag("theirs", "", "Fol all conflicts, take the version from our branch and resolve the conflict")

	return ap
}

// Exec executes the command
func (cmd ResolveCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, resDocumentation, ap))
	apr := cli.ParseArgs(ap, args, help)

	var verr errhand.VerboseError
	if apr.ContainsAny(autoResolverParams...) {
		verr = autoResolve(ctx, apr, dEnv)
	} else {
		verr = manualResolve(ctx, apr, dEnv)
	}

	return commands.HandleVErrAndExitCode(verr, usage)
}

func autoResolve(ctx context.Context, apr *argparser.ArgParseResults, dEnv *env.DoltEnv) errhand.VerboseError {
	funcFlags := apr.FlagsEqualTo(autoResolverParams, true)

	if funcFlags.Size() > 1 {
		return errhand.BuildDError("").SetPrintUsage().Build()
	} else if apr.NArg() == 0 {
		return errhand.BuildDError("").SetPrintUsage().Build()
	}

	autoResolveFlag := funcFlags.AsSlice()[0]
	autoResolveFunc := autoResolvers[autoResolveFlag]

	var err error
	tbls := apr.Args()
	if len(tbls) == 1 && tbls[0] == "." {
		err = actions.AutoResolveAll(ctx, dEnv, autoResolveFunc)
	} else {
		err = actions.AutoResolveTables(ctx, dEnv, autoResolveFunc, tbls)
	}

	if err != nil {
		if err == doltdb.ErrNoConflicts {
			cli.Println("no conflicts to resolve.")
			return nil
		}

		return errhand.BuildDError("error: failed to resolve").AddCause(err).Build()
	}

	return saveDocsOnResolve(ctx, dEnv)
}

func manualResolve(ctx context.Context, apr *argparser.ArgParseResults, dEnv *env.DoltEnv) errhand.VerboseError {
	args := apr.Args()

	if len(args) < 2 {
		return errhand.BuildDError("").SetPrintUsage().Build()
	}

	root, verr := commands.GetWorkingWithVErr(dEnv)

	if verr != nil {
		return verr
	}

	tblName := args[0]

	if has, err := root.HasTable(ctx, tblName); err != nil {
		return errhand.BuildDError("error: could not read tables").AddCause(err).Build()
	} else if !has {
		return errhand.BuildDError("error: table '%s' not found", tblName).Build()
	}

	tbl, _, err := root.GetTable(ctx, tblName)

	if err != nil {
		return errhand.BuildDError("error: failed to get table '%s'", tblName).AddCause(err).Build()
	}

	sch, err := tbl.GetSchema(ctx)

	if err != nil {
		return errhand.BuildDError("error: failed to get schema").AddCause(err).Build()
	}

	keysToResolve, err := cli.ParseKeyValues(ctx, root.VRW(), sch, args[1:])

	if err != nil {
		return errhand.BuildDError("error: parsing command line").AddCause(err).Build()
	}

	invalid, notFound, updatedTbl, err := tbl.ResolveConflicts(ctx, keysToResolve)

	if err != nil {
		return errhand.BuildDError("fatal: Failed to resolve conflicts").AddCause(err).Build()
	}

	for _, key := range invalid {
		cli.Println(key, "is not a valid key")
	}

	for _, key := range notFound {
		cli.Println(key, "is not the primary key of a conflicting row")
	}

	updatedHash, err := updatedTbl.HashOf()

	if err != nil {
		return errhand.BuildDError("error: failed to get table hash").AddCause(err).Build()
	}

	hash, err := tbl.HashOf()

	if err != nil {
		return errhand.BuildDError("error: failed to get table hash").AddCause(err).Build()
	}

	if hash != updatedHash {
		root, err := root.PutTable(ctx, tblName, updatedTbl)

		if err != nil {
			return errhand.BuildDError("").AddCause(err).Build()
		}

		if verr := commands.UpdateWorkingWithVErr(dEnv, root); verr != nil {
			return verr
		}
	}

	valid := len(keysToResolve) - len(invalid) - len(notFound)
	cli.Println(valid, "rows resolved successfully")

	return saveDocsOnResolve(ctx, dEnv)
}

func saveDocsOnResolve(ctx context.Context, dEnv *env.DoltEnv) errhand.VerboseError {
	err := actions.SaveTrackedDocsFromWorking(ctx, dEnv)
	if err != nil {
		return errhand.BuildDError("error: failed to update docs on the filesystem").AddCause(err).Build()
	}
	return nil
}
