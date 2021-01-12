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

package sqle

import (
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

// sqlTableEditor is a wrapper for *doltdb.SessionedTableEditor that complies with the SQL interface.
//
// The sqlTableEditor has two levels of batching: one supported at the SQL engine layer where a single UPDATE, DELETE or
// INSERT statement will touch many rows, and we want to avoid unnecessary intermediate writes; and one at the dolt
// layer as a "batch mode" in DoltDatabase. In the latter mode, it's possible to mix inserts, updates and deletes in any
// order. In general, this is unsafe and will produce incorrect results in many cases. The editor makes reasonable
// attempts to produce correct results when interleaving insert and delete statements, but this is almost entirely to
// support REPLACE statements, which are implemented as a DELETE followed by an INSERT. In general, not flushing the
// editor after every SQL statement is incorrect and will return incorrect results. The single reliable exception is an
// unbroken chain of INSERT statements, where we have taken pains to batch writes to speed things up.
type sqlTableEditor struct {
	t           *WritableDoltTable
	tableEditor editor.TableEditor
	sess        *editor.TableEditSession
}

var _ sql.RowReplacer = (*sqlTableEditor)(nil)
var _ sql.RowUpdater = (*sqlTableEditor)(nil)
var _ sql.RowInserter = (*sqlTableEditor)(nil)
var _ sql.RowDeleter = (*sqlTableEditor)(nil)

func newSqlTableEditor(ctx *sql.Context, t *WritableDoltTable) (*sqlTableEditor, error) {
	sess := t.db.TableEditSession(ctx)
	tableEditor, err := sess.GetTableEditor(ctx, t.name, t.sch)
	if err != nil {
		return nil, err
	}
	return &sqlTableEditor{
		t:           t,
		tableEditor: tableEditor,
		sess:        sess,
	}, nil
}

func (te *sqlTableEditor) Insert(ctx *sql.Context, sqlRow sql.Row) error {
	dRow, err := sqlutil.SqlRowToDoltRow(ctx, te.t.table.ValueReadWriter(), sqlRow, te.t.sch)
	if err != nil {
		return err
	}

	return te.tableEditor.InsertRow(ctx, dRow)
}

func (te *sqlTableEditor) Delete(ctx *sql.Context, sqlRow sql.Row) error {
	dRow, err := sqlutil.SqlRowToDoltRow(ctx, te.t.table.ValueReadWriter(), sqlRow, te.t.sch)
	if err != nil {
		return err
	}

	return te.tableEditor.DeleteRow(ctx, dRow)
}

func (te *sqlTableEditor) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) error {
	dOldRow, err := sqlutil.SqlRowToDoltRow(ctx, te.t.table.ValueReadWriter(), oldRow, te.t.sch)
	if err != nil {
		return err
	}
	dNewRow, err := sqlutil.SqlRowToDoltRow(ctx, te.t.table.ValueReadWriter(), newRow, te.t.sch)
	if err != nil {
		return err
	}

	return te.tableEditor.UpdateRow(ctx, dOldRow, dNewRow)
}

func (te *sqlTableEditor) GetAutoIncrementValue() (interface{}, error) {
	val := te.tableEditor.GetAutoIncrementValue()
	return te.t.DoltTable.autoIncCol.TypeInfo.ConvertNomsValueToValue(val)
}

func (te *sqlTableEditor) SetAutoIncrementValue(ctx *sql.Context, val interface{}) error {
	nomsVal, err := te.t.DoltTable.autoIncCol.TypeInfo.ConvertValueToNomsValue(ctx, te.t.table.ValueReadWriter(), val)
	if err != nil {
		return err
	}
	if err = te.tableEditor.SetAutoIncrementValue(nomsVal); err != nil {
		return err
	}
	return te.flush(ctx)
}

// Close implements Closer
func (te *sqlTableEditor) Close(ctx *sql.Context) error {
	// If we're running in batched mode, don't flush the edits until explicitly told to do so by the parent table.
	if te.t.db.batchMode == batched {
		return nil
	}
	return te.flush(ctx)
}

func (te *sqlTableEditor) flush(ctx *sql.Context) error {
	newRoot, err := te.sess.Flush(ctx)
	if err != nil {
		return err
	}

	newTable, ok, err := newRoot.GetTable(ctx, te.t.name)
	if err != nil {
		return errhand.BuildDError("failed to load updated table").AddCause(err).Build()
	}
	if !ok {
		return errhand.BuildDError("failed to find updated table").Build()
	}
	te.t.table = newTable
	return te.t.db.SetRoot(ctx, newRoot)
}
