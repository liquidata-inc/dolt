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

package commands

import (
	"context"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff/querydiff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/fwt"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/nullprinter"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/store/types"
)

func diffQuery(ctx context.Context, dEnv *env.DoltEnv, fromRoot, toRoot *doltdb.RootValue, query string) errhand.VerboseError {
	qd, err := querydiff.MakeQueryDiffer(ctx, dEnv, fromRoot, toRoot, query)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	doltSch := doltSchWithPKFromSqlSchema(qd.Schema())

	joiner, err := rowconv.NewJoiner(
		[]rowconv.NamedSchema{
			{Name: diff.From, Sch: doltSch},
			{Name: diff.To, Sch: doltSch},
		},
		map[string]rowconv.ColNamingFunc{diff.To: toNamer, diff.From: fromNamer},
	)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	p, err := buildQueryDiffPipeline(qd, doltSch, joiner)

	if err != nil {
		return errhand.BuildDError("error building diff pipeline").AddCause(err).Build()
	}

	p.Start()

	return errhand.VerboseErrorFromError(p.Wait())
}

func doltSchWithPKFromSqlSchema(sch sql.Schema) schema.Schema {
	dSch, _ := sqlutil.ToDoltResultSchema(sch)
	// make the first col the PK
	pk := false
	newCC, _ := schema.MapColCollection(dSch.GetAllCols(), func(col schema.Column) (column schema.Column, err error) {
		if !pk {
			col.IsPartOfPK = true
			pk = true
		}
		return col, nil
	})
	return schema.MustSchemaFromCols(newCC)
}

func nextQueryDiff(vrw types.ValueReadWriter, qd *querydiff.QueryDiffer, joiner *rowconv.Joiner) (row.Row, pipeline.ImmutableProperties, error) {
	fromRow, toRow, err := qd.NextDiff()
	if err != nil {
		return nil, pipeline.ImmutableProperties{}, err
	}

	rows := make(map[string]row.Row)
	if fromRow != nil {
		sch := joiner.SchemaForName(diff.From)
		oldRow, err := sqlutil.SqlRowToDoltRow(context.Background(), vrw, fromRow, sch)
		if err != nil {
			return nil, pipeline.ImmutableProperties{}, err
		}
		rows[diff.From] = oldRow
	}

	if toRow != nil {
		sch := joiner.SchemaForName(diff.To)
		newRow, err := sqlutil.SqlRowToDoltRow(context.Background(), vrw, toRow, sch)
		if err != nil {
			return nil, pipeline.ImmutableProperties{}, err
		}
		rows[diff.To] = newRow
	}

	joinedRow, err := joiner.Join(rows)
	if err != nil {
		return nil, pipeline.ImmutableProperties{}, err
	}

	return joinedRow, pipeline.ImmutableProperties{}, nil
}

func buildQueryDiffPipeline(qd *querydiff.QueryDiffer, doltSch schema.Schema, joiner *rowconv.Joiner) (*pipeline.Pipeline, error) {
	vrw := types.NewInternalValueStore() // We don't want to persist anything, so we use an internal store
	unionSch, ds, verr := createSplitter(context.Background(), vrw, doltSch, doltSch, joiner, &diffArgs{diffOutput: TabularDiffOutput})
	if verr != nil {
		return nil, verr
	}

	transforms := pipeline.NewTransformCollection()
	nullPrinter := nullprinter.NewNullPrinter(unionSch)
	fwtTr := fwt.NewAutoSizingFWTTransformer(unionSch, fwt.HashFillWhenTooLong, 1000)
	transforms.AppendTransforms(
		pipeline.NewNamedTransform("split_diffs", ds.SplitDiffIntoOldAndNew),
		pipeline.NewNamedTransform(nullprinter.NullPrintingStage, nullPrinter.ProcessRow),
		pipeline.NamedTransform{Name: fwtStageName, Func: fwtTr.TransformToFWT},
	)

	badRowCB := func(trf *pipeline.TransformRowFailure) (quit bool) {
		verr := errhand.BuildDError("Failed transforming row").AddDetails(trf.TransformName).AddDetails(trf.Details).Build()
		cli.PrintErrln(verr.Error())
		return true
	}

	sink, err := diff.NewColorDiffSink(iohelp.NopWrCloser(cli.CliOut), doltSch, 1)
	if err != nil {
		return nil, err
	}
	sinkProcFunc := pipeline.ProcFuncForSinkFunc(sink.ProcRowWithProps)

	srcProcFunc := pipeline.ProcFuncForSourceFunc(func() (row.Row, pipeline.ImmutableProperties, error) {
		return nextQueryDiff(vrw, qd, joiner)
	})

	p := pipeline.NewAsyncPipeline(srcProcFunc, sinkProcFunc, transforms, badRowCB)

	names := make(map[uint64]string, doltSch.GetAllCols().Size())
	_ = doltSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		names[tag] = col.Name
		return false, nil
	})
	schRow, err := untyped.NewRowFromTaggedStrings(types.Format_Default, unionSch, names)
	if err != nil {
		return nil, err
	}
	p.InjectRow(fwtStageName, schRow)

	p.RunAfter(func() {
		err := sink.Close()
		if err != nil {
			cli.PrintErrln(err)
		}
		err = qd.Close()
		if err != nil {
			cli.PrintErrln(err)
		}
	})
	qd.Start()

	return p, nil
}
