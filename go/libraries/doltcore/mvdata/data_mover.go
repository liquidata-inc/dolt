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
	"sync/atomic"

	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/types"
)

type CsvOptions struct {
	Delim string
}

type XlsxOptions struct {
	SheetName string
}

type JSONOptions struct {
	TableName string
	SchFile   string
}

type DataMoverOptions interface {
	WritesToTable() bool
	SrcName() string
	DestName() string
}

type DataMoverCloser interface {
	table.TableWriteCloser
	Flush(context.Context) (*doltdb.RootValue, error)
}

type DataMover struct {
	Rd         table.TableReadCloser
	Transforms *pipeline.TransformCollection
	Wr         table.TableWriteCloser
	ContOnErr  bool
}

type DataMoverCreationErrType string

const (
	CreateReaderErr   DataMoverCreationErrType = "Create reader error"
	NomsKindSchemaErr DataMoverCreationErrType = "Invalid schema error"
	SchemaErr         DataMoverCreationErrType = "Schema error"
	MappingErr        DataMoverCreationErrType = "Mapping error"
	ReplacingErr      DataMoverCreationErrType = "Replacing error"
	CreateMapperErr   DataMoverCreationErrType = "Mapper creation error"
	CreateWriterErr   DataMoverCreationErrType = "Create writer error"
	CreateSorterErr   DataMoverCreationErrType = "Create sorter error"
)

var ErrProvidedPkNotFound = errors.New("provided primary key not found")

type DataMoverCreationError struct {
	ErrType DataMoverCreationErrType
	Cause   error
}

func (dmce *DataMoverCreationError) String() string {
	return string(dmce.ErrType) + ": " + dmce.Cause.Error()
}

type GCTableWriteCloser interface {
	table.TableWriteCloser
	GC(ctx context.Context) error
}

// Move is the method that executes the pipeline which will move data from the pipeline's source DataLocation to it's
// dest DataLocation.  It returns the number of bad rows encountered during import, and an error.
func (imp *DataMover) Move(ctx context.Context) (badRowCount int64, err error) {
	defer imp.Rd.Close(ctx)
	defer func() {
		closeErr := imp.Wr.Close(ctx)
		if err == nil {
			err = closeErr
		}

		if err == nil {
			if gcTWC, ok := imp.Wr.(GCTableWriteCloser); ok {
				err = gcTWC.GC(ctx)
			}
		}
	}()

	var badCount int64
	var rowErr error
	badRowCB := func(trf *pipeline.TransformRowFailure) (quit bool) {
		if !imp.ContOnErr {
			rowErr = trf
			return true
		}

		atomic.AddInt64(&badCount, 1)
		return false
	}

	p := pipeline.NewAsyncPipeline(
		pipeline.ProcFuncForReader(ctx, imp.Rd),
		pipeline.ProcFuncForWriter(ctx, imp.Wr),
		imp.Transforms,
		badRowCB)
	p.Start()

	err = p.Wait()

	if err != nil {
		return 0, err
	}

	if rowErr != nil {
		return 0, rowErr
	}

	return badCount, nil
}

func MoveDataToRoot(ctx context.Context, mover *DataMover, mvOpts DataMoverOptions, root *doltdb.RootValue, updateRoot func(c context.Context, r *doltdb.RootValue) error) (*doltdb.RootValue, int64, errhand.VerboseError) {
	var badCount int64
	var err error
	newRoot := &doltdb.RootValue{}

	badCount, err = mover.Move(ctx)

	if err != nil {
		if pipeline.IsTransformFailure(err) {
			bdr := errhand.BuildDError("\nA bad row was encountered while moving data.")

			r := pipeline.GetTransFailureRow(err)
			if r != nil {
				bdr.AddDetails("Bad Row:" + row.Fmt(ctx, r, mover.Rd.GetSchema()))
			}

			details := pipeline.GetTransFailureDetails(err)

			bdr.AddDetails(details)
			bdr.AddDetails("These can be ignored using the '--continue'")

			return nil, badCount, bdr.Build()
		}
		return nil, badCount, errhand.BuildDError("An error occurred moving data:\n").AddCause(err).Build()
	}

	if mvOpts.WritesToTable() {
		wr := mover.Wr.(DataMoverCloser)
		newRoot, err = wr.Flush(ctx)
		if err != nil {
			return nil, badCount, errhand.BuildDError("Failed to apply changes to the table.").AddCause(err).Build()
		}

		rootHash, err := root.HashOf()
		if err != nil {
			return nil, badCount, errhand.BuildDError("Failed to hash the working value.").AddCause(err).Build()
		}

		newRootHash, err := newRoot.HashOf()
		if rootHash != newRootHash {
			err = updateRoot(ctx, newRoot)
			if err != nil {
				return nil, badCount, errhand.BuildDError("Failed to update the working value.").AddCause(err).Build()
			}
		}
	}

	return newRoot, badCount, nil
}

func MoveData(ctx context.Context, dEnv *env.DoltEnv, mover *DataMover, mvOpts DataMoverOptions) (int64, errhand.VerboseError) {
	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return 0, errhand.BuildDError("Failed to fetch the working value.").AddCause(err).Build()
	}
	_, badCount, moveErr := MoveDataToRoot(ctx, mover, mvOpts, root, dEnv.UpdateWorkingRoot)
	if moveErr != nil {
		return badCount, moveErr
	}
	return badCount, nil
}

// NameMapTransform creates a pipeline transform that converts rows from inSch to outSch based on a name mapping.
func NameMapTransform(ctx context.Context, vrw types.ValueReadWriter, inSch schema.Schema, outSch schema.Schema, mapper rowconv.NameMapper) (*pipeline.TransformCollection, error) {
	mapping, err := rowconv.NameMapping(inSch, outSch, mapper)

	if err != nil {
		return nil, err
	}

	rconv, err := rowconv.NewImportRowConverter(ctx, vrw, mapping)

	if err != nil {
		return nil, err
	}

	transforms := pipeline.NewTransformCollection()
	if !rconv.IdentityConverter {
		nt := pipeline.NewNamedTransform("Mapping transform", pipeline.GetRowConvTransformFunc(rconv))
		transforms.AppendTransforms(nt)
	}

	return transforms, nil
}

// SchAndTableNameFromFile reads a SQL schema file and creates a Dolt schema from it.
func SchAndTableNameFromFile(ctx context.Context, path string, fs filesys.ReadableFS, root *doltdb.RootValue) (string, schema.Schema, error) {
	if path != "" {
		data, err := fs.ReadFile(path)

		if err != nil {
			return "", nil, err
		}

		tn, sch, err := sqlutil.ParseCreateTableStatement(ctx, root, string(data))

		if err != nil {
			return "", nil, fmt.Errorf("%s in schema file %s", err.Error(), path)
		}

		return tn, sch, nil
	} else {
		return "", nil, errors.New("no schema file to parse")
	}
}

func InferSchema(ctx context.Context, root *doltdb.RootValue, rd table.TableReadCloser, tableName string, pks []string, args actions.InferenceArgs) (schema.Schema, error) {
	var err error

	infCols, err := actions.InferColumnTypesFromTableReader(ctx, root, rd, args)
	if err != nil {
		return nil, err
	}

	pkSet := set.NewStrSet(pks)
	newCols, _ := schema.MapColCollection(infCols, func(col schema.Column) (schema.Column, error) {
		col.IsPartOfPK = pkSet.Contains(col.Name)
		if col.IsPartOfPK {
			hasNotNull := false
			for _, constraint := range col.Constraints {
				if _, ok := constraint.(schema.NotNullConstraint); ok {
					hasNotNull = true
					break
				}
			}
			if !hasNotNull {
				col.Constraints = append(col.Constraints, schema.NotNullConstraint{})
			}
		}
		return col, nil
	})

	// check that all provided primary keys are being used
	for _, pk := range pks {
		col, ok := newCols.GetByName(pk)
		if !col.IsPartOfPK || !ok {
			return nil, ErrProvidedPkNotFound
		}
	}

	newCols, err = root.GenerateTagsForNewColColl(ctx, tableName, newCols)
	if err != nil {
		return nil, errhand.BuildDError("failed to generate new schema").AddCause(err).Build()
	}

	return schema.SchemaFromCols(newCols)
}
