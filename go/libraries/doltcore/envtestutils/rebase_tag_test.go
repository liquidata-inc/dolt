// Copyright 2020 Dolthub, Inc.
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

package envtestutils

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	dtu "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	tc "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/testcommands"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/rebase"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	dsqle "github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	IdTag = iota
	NameTag
	_
	AgeTag
)
const DripTag = 2360
const DripTagRebased = 2361

type RebaseTagTest struct {
	// The name of this test. Names should be unique and descriptive.
	Name string
	// The modifying queries to run
	Commands []tc.Command
	// The pairs {old, new} of tags that need to be exchanged
	OldTag uint64
	NewTag uint64
	// The select query to run to verify the results
	SelectResultQuery string
	// The schema of the result of the query, nil if an error is expected
	ExpectedSchema schema.Schema
	// The rows the select query should return, nil if an error is expected
	ExpectedRows []row.Row
	// An expected error string
	ExpectedErrStr string
}

var createPeopleTable = `
	create table people (
		id int,
		name varchar(20) not null,
		age int,
		primary key (id)
	);`

func columnCollection(cols ...schema.Column) *schema.ColCollection {
	pcc, err := schema.NewColCollection(cols...)
	if err != nil {
		panic(err)
	}
	return pcc
}

func newRow(vals row.TaggedValues, cc *schema.ColCollection) row.Row {
	r, err := row.New(types.Format_7_18, schema.MustSchemaFromCols(cc), vals)
	if err != nil {
		panic(err)
	}
	return r
}

func newColTypeInfo(name string, tag uint64, typeInfo typeinfo.TypeInfo, partOfPK bool, constraints ...schema.ColConstraint) schema.Column {
	c, err := schema.NewColumnWithTypeInfo(name, tag, typeInfo, partOfPK, "", false, "", constraints...)
	if err != nil {
		panic("could not create column")
	}
	return c
}

func varchar(length int64) typeinfo.TypeInfo {
	ti, _ := typeinfo.FromSqlType(sql.MustCreateStringWithDefaults(sqltypes.VarChar, length))
	return ti
}

var people = columnCollection(
	newColTypeInfo("id", IdTag, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
	newColTypeInfo("name", NameTag, varchar(20), false, schema.NotNullConstraint{}),
	newColTypeInfo("age", AgeTag, typeinfo.Int32Type, false),
)

var peopleWithDrip = columnCollection(
	newColTypeInfo("id", IdTag, typeinfo.Int32Type, true, schema.NotNullConstraint{}),
	newColTypeInfo("name", NameTag, varchar(20), false, schema.NotNullConstraint{}),
	newColTypeInfo("age", AgeTag, typeinfo.Int32Type, false),
	newColTypeInfo("drip", DripTagRebased, typeinfo.Float64Type, false),
)

var peopleRows = []row.Row{
	newRow(row.TaggedValues{IdTag: types.Int(6), NameTag: types.String("Homer Simpson"), AgeTag: types.Int(44)}, people),
	newRow(row.TaggedValues{IdTag: types.Int(7), NameTag: types.String("Maggie Simpson"), AgeTag: types.Int(1)}, people),
	newRow(row.TaggedValues{IdTag: types.Int(8), NameTag: types.String("Milhouse Van Houten"), AgeTag: types.Int(8)}, people),
	newRow(row.TaggedValues{IdTag: types.Int(9), NameTag: types.String("Jacqueline Bouvier"), AgeTag: types.Int(80)}, people),
	newRow(row.TaggedValues{IdTag: types.Int(10), NameTag: types.String("Patty Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(8.5)}, peopleWithDrip),
	newRow(row.TaggedValues{IdTag: types.Int(11), NameTag: types.String("Selma Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(8.5)}, peopleWithDrip),
}

var RebaseTagTests = []RebaseTagTest{
	{
		Name: "rebase non-existent tag",
		Commands: []tc.Command{
			tc.Query{Query: createPeopleTable},
			tc.CommitAll{Message: "made changes"},
		},
		OldTag:            DripTag,
		NewTag:            DripTagRebased,
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schema.MustSchemaFromCols(people),
		ExpectedRows:      []row.Row{},
	},
	{
		Name: "rebase entire history",
		Commands: []tc.Command{
			tc.Query{Query: createPeopleTable},
			tc.Query{Query: `alter table people add drip double;`},
			tc.Query{Query: `insert into people (id, name, age, drip) values
				(10, "Patty Bouvier", 40, 8.5),
				(11, "Selma Bouvier", 40, 8.5);`},
			tc.CommitAll{Message: "made changes"},
		},
		OldTag:            DripTag,
		NewTag:            DripTagRebased,
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schema.MustSchemaFromCols(peopleWithDrip),
		ExpectedRows: []row.Row{
			newRow(row.TaggedValues{IdTag: types.Int(10), NameTag: types.String("Patty Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(8.5)}, peopleWithDrip),
			newRow(row.TaggedValues{IdTag: types.Int(11), NameTag: types.String("Selma Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(8.5)}, peopleWithDrip),
		},
	},
	{
		Name: "create new column, rebase column's tag",
		Commands: []tc.Command{
			tc.Query{Query: createPeopleTable},
			tc.CommitAll{Message: "made changes"},
			tc.Query{Query: `alter table people add drip double;`},
			tc.CommitAll{Message: "made changes"},
		},
		OldTag:            DripTag,
		NewTag:            DripTagRebased,
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schema.MustSchemaFromCols(peopleWithDrip),
		ExpectedRows:      []row.Row{},
	},
	{
		Name: "create new column, insert value to column, rebase column's tag",
		Commands: []tc.Command{
			tc.Query{Query: createPeopleTable},
			tc.Query{Query: `insert into people (id, name, age) values (9, "Jacqueline Bouvier", 80);`},
			tc.CommitAll{Message: "made changes"},
			tc.Query{Query: `alter table people add drip double;`},
			tc.Query{Query: `insert into people (id, name, age, drip) values (11, "Selma Bouvier", 40, 8.5);`},
			tc.CommitAll{Message: "made changes"},
		},
		OldTag:            DripTag,
		NewTag:            DripTagRebased,
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schema.MustSchemaFromCols(peopleWithDrip),
		ExpectedRows: []row.Row{
			newRow(row.TaggedValues{IdTag: types.Int(9), NameTag: types.String("Jacqueline Bouvier"), AgeTag: types.Int(80)}, people),
			newRow(row.TaggedValues{IdTag: types.Int(11), NameTag: types.String("Selma Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(8.5)}, peopleWithDrip),
		},
	},
	{
		Name: "create new column, update column value in existing row, rebase column's tag",
		Commands: []tc.Command{
			tc.Query{Query: createPeopleTable},
			tc.Query{Query: `insert into people (id, name, age) values (9, "Jacqueline Bouvier", 80);`},
			tc.CommitAll{Message: "made changes"},
			tc.Query{Query: `alter table people add drip double;`},
			tc.Query{Query: `update people set drip=9.9 where id=9;`},
			tc.CommitAll{Message: "made changes"},
		},
		OldTag:            DripTag,
		NewTag:            DripTagRebased,
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schema.MustSchemaFromCols(peopleWithDrip),
		ExpectedRows: []row.Row{
			newRow(row.TaggedValues{IdTag: types.Int(9), NameTag: types.String("Jacqueline Bouvier"), AgeTag: types.Int(80), DripTagRebased: types.Float(9.9)}, peopleWithDrip),
		},
	},
	{
		Name: "create new column, insert value to column, update column value in inserted row, rebase column's tag",
		Commands: []tc.Command{
			tc.Query{Query: createPeopleTable},
			tc.Query{Query: `insert into people (id, name, age) values (9, "Jacqueline Bouvier", 80);`},
			tc.CommitAll{Message: "made changes"},
			tc.Query{Query: `alter table people add drip double;`},
			tc.Query{Query: `insert into people (id, name, age, drip) values (11, "Selma Bouvier", 40, 8.5);`},
			tc.CommitAll{Message: "made changes"},
			tc.Query{Query: `update people set drip=9.9 where id=11;`},
			tc.CommitAll{Message: "made changes"},
		},
		OldTag:            DripTag,
		NewTag:            DripTagRebased,
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schema.MustSchemaFromCols(peopleWithDrip),
		ExpectedRows: []row.Row{
			newRow(row.TaggedValues{IdTag: types.Int(9), NameTag: types.String("Jacqueline Bouvier"), AgeTag: types.Int(80)}, people),
			newRow(row.TaggedValues{IdTag: types.Int(11), NameTag: types.String("Selma Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(9.9)}, peopleWithDrip),
		},
	},
	{
		Name: "create new column, insert value to column, update column value in inserted row, rebase column's tag",
		Commands: []tc.Command{
			tc.Query{Query: createPeopleTable},
			tc.Query{Query: `insert into people (id, name, age) values (9, "Jacqueline Bouvier", 80);`},
			tc.CommitAll{Message: "made changes"},
			tc.Query{Query: `alter table people add drip double;`},
			tc.Query{Query: `insert into people (id, name, age, drip) values (11, "Selma Bouvier", 40, 8.5);`},
			tc.CommitAll{Message: "made changes"},
			tc.Query{Query: `update people set drip=9.9 where id=11;`},
			tc.Query{Query: `update people set drip=1.1 where id=9;`},
			tc.CommitAll{Message: "made changes"},
		},
		OldTag:            DripTag,
		NewTag:            DripTagRebased,
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schema.MustSchemaFromCols(peopleWithDrip),
		ExpectedRows: []row.Row{
			newRow(row.TaggedValues{IdTag: types.Int(9), NameTag: types.String("Jacqueline Bouvier"), AgeTag: types.Int(80), DripTagRebased: types.Float(1.1)}, peopleWithDrip),
			newRow(row.TaggedValues{IdTag: types.Int(11), NameTag: types.String("Selma Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(9.9)}, peopleWithDrip),
		},
	},
	{
		Name: "create new column, modify rows without value for column, rebase column's tag",
		Commands: []tc.Command{
			tc.Query{Query: createPeopleTable},
			tc.Query{Query: `insert into people (id, name, age) values
				(7, "Maggie Simpson", 1),
				(8, "Milhouse Van Houten", 8);`},
			tc.CommitAll{Message: "made changes"},
			tc.Query{Query: `alter table people add drip double;`},
			tc.CommitAll{Message: "made changes"},
			tc.Query{Query: `update people set age=2 where id=7;`},
			tc.Query{Query: `delete from people where id=8;`},
			tc.Query{Query: `insert into people (id, name, age) values (9, "Jacqueline Bouvier", 80);`},
			tc.CommitAll{Message: "made changes"},
		},
		OldTag:            DripTag,
		NewTag:            DripTagRebased,
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schema.MustSchemaFromCols(peopleWithDrip),
		ExpectedRows: []row.Row{
			newRow(row.TaggedValues{IdTag: types.Int(7), NameTag: types.String("Maggie Simpson"), AgeTag: types.Int(2)}, people),
			newRow(row.TaggedValues{IdTag: types.Int(9), NameTag: types.String("Jacqueline Bouvier"), AgeTag: types.Int(80)}, people),
		},
	},
	// https://github.com/dolthub/dolt/issues/773
	/*{
		Name: "create new column on master, insert to table on other branch, merge",
		Commands: []tc.Command{
			tc.Query{Query: createPeopleTable},
			tc.CommitAll{Message: "made changes"},
			tc.Branch{BranchName: "newBranch"},
			tc.Query{Query: `alter table people add drip double;`},
			tc.Query{Query: `insert into people (id, name, age, drip) values (11, "Selma Bouvier", 40, 8.5);`},
			tc.CommitAll{Message: "made changes"},
			tc.Checkout{BranchName: "newBranch"},
			tc.Query{Query: `insert into people (id, name, age) values (9, "Jacqueline Bouvier", 80);`},
			tc.CommitAll{Message: "made changes"},
			tc.Checkout{BranchName: "master"},
			tc.Merge{BranchName: "newBranch"},
			tc.CommitAll{Message: "made changes"},
		},
		OldTag:            DripTag,
		NewTag:            DripTagRebased,
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schschema.MustSchemaFromCols(peopleWithDrip),
		ExpectedRows: []row.Row{
			newRow(row.TaggedValues{IdTag: types.Int(9), NameTag: types.String("Jacqueline Bouvier"), AgeTag: types.Int(80)}, people),
			newRow(row.TaggedValues{IdTag: types.Int(11), NameTag: types.String("Selma Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(8.5)}, peopleWithDrip),
		},
	},
	{
		Name: "create new column on master; insert, update, delete on both branches; merge",
		Commands: []tc.Command{
			tc.Query{Query: createPeopleTable},
			tc.Query{Query: `insert into people (id, name, age) values
				(6, "Homer Simpson", 44),
				(7, "Maggie Simpson", 1),
				(8, "Milhouse Van Houten", 8),
				(9, "Jacqueline Bouvier", 80);`},
			tc.CommitAll{Message: "made changes"},
			tc.Branch{BranchName: "newBranch"},
			tc.Query{Query: `alter table people add drip double;`},
			tc.Query{Query: `delete from people where id=6;`},
			tc.Query{Query: `update people set drip=99.9 where id=7;`},
			tc.Query{Query: `insert into people (id, name, age, drip) values (11, "Selma Bouvier", 40, 8.5);`},
			tc.CommitAll{Message: "made changes"},
			tc.Checkout{BranchName: "newBranch"},
			tc.Query{Query: `insert into people (id, name, age) values (10, "Patty Bouvier", 40);`},
			tc.Query{Query: `delete from people where id=8;`},
			tc.Query{Query: `update people set age=40 where id=9;`},
			tc.CommitAll{Message: "made changes"},
			tc.Checkout{BranchName: "master"},
			tc.Merge{BranchName: "newBranch"},
			tc.CommitAll{Message: "made changes"},
		},
		OldTag:            DripTag,
		NewTag:            DripTagRebased,
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schema.MustSchemaFromCols(peopleWithDrip),
		ExpectedRows: []row.Row{
			newRow(row.TaggedValues{IdTag: types.Int(7), NameTag: types.String("Maggie Simpson"), AgeTag: types.Int(1), DripTagRebased: types.Float(99.9)}, peopleWithDrip),
			newRow(row.TaggedValues{IdTag: types.Int(9), NameTag: types.String("Jacqueline Bouvier"), AgeTag: types.Int(40)}, people),
			newRow(row.TaggedValues{IdTag: types.Int(10), NameTag: types.String("Patty Bouvier"), AgeTag: types.Int(40)}, people),
			newRow(row.TaggedValues{IdTag: types.Int(11), NameTag: types.String("Selma Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(8.5)}, peopleWithDrip),
		},
	},
	{
		Name: "create new column on other branch, merge into master",
		Commands: []tc.Command{
			tc.Query{Query: createPeopleTable},
			tc.CommitAll{Message: "made changes"},
			tc.Branch{BranchName: "newBranch"},
			tc.Checkout{BranchName: "newBranch"},
			tc.Query{Query: `alter table people add drip double;`},
			tc.Query{Query: `insert into people (id, name, age, drip) values (11, "Selma Bouvier", 40, 8.5);`},
			tc.CommitAll{Message: "made changes"},
			tc.Checkout{BranchName: "master"},
			tc.Query{Query: `insert into people (id, name, age) values (9, "Jacqueline Bouvier", 80);`},
			tc.CommitAll{Message: "made changes"},
			tc.Merge{BranchName: "newBranch"},
			tc.CommitAll{Message: "made changes"},
		},
		OldTag:            DripTag,
		NewTag:            DripTagRebased,
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schema.MustSchemaFromCols(peopleWithDrip),
		ExpectedRows: []row.Row{
			newRow(row.TaggedValues{IdTag: types.Int(9), NameTag: types.String("Jacqueline Bouvier"), AgeTag: types.Int(80)}, people),
			newRow(row.TaggedValues{IdTag: types.Int(11), NameTag: types.String("Selma Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(8.5)}, peopleWithDrip),
		},
	},*/
	{
		Name: "create new column, use on multiple branches, merge",
		Commands: []tc.Command{
			tc.Query{Query: createPeopleTable},
			tc.CommitAll{Message: "made changes"},
			tc.Query{Query: `alter table people add drip double;`},
			tc.Query{Query: `insert into people (id, name, age, drip) values (9, "Jacqueline Bouvier", 80, 8.5);`},
			tc.CommitAll{Message: "made changes"},
			tc.Branch{BranchName: "newBranch"},
			tc.Checkout{BranchName: "newBranch"},
			tc.Query{Query: `insert into people (id, name, age, drip) values (11, "Selma Bouvier", 40, 8.5);`},
			tc.CommitAll{Message: "made changes"},
			tc.Checkout{BranchName: "master"},
			tc.Query{Query: `insert into people (id, name, age, drip) values (10, "Patty Bouvier", 40, 8.5);`},
			tc.CommitAll{Message: "made changes"},
			tc.Merge{BranchName: "newBranch"},
			tc.CommitAll{Message: "made changes"},
		},
		OldTag:            DripTag,
		NewTag:            DripTagRebased,
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schema.MustSchemaFromCols(peopleWithDrip),
		ExpectedRows: []row.Row{
			newRow(row.TaggedValues{IdTag: types.Int(9), NameTag: types.String("Jacqueline Bouvier"), AgeTag: types.Int(80), DripTagRebased: types.Float(8.5)}, peopleWithDrip),
			newRow(row.TaggedValues{IdTag: types.Int(10), NameTag: types.String("Patty Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(8.5)}, peopleWithDrip),
			newRow(row.TaggedValues{IdTag: types.Int(11), NameTag: types.String("Selma Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(8.5)}, peopleWithDrip),
		},
	},
	{
		Name: "rebase tag that exists in history but not at the head commit",
		Commands: []tc.Command{
			tc.Query{Query: createPeopleTable},
			tc.Query{Query: `insert into people (id, name, age) values (7, "Maggie Simpson", 1);`},
			tc.CommitAll{Message: "made changes"},
			tc.Query{Query: `alter table people add drip double;`},
			tc.CommitAll{Message: "made changes"},
			tc.Query{Query: `insert into people (id, name, age) values (9, "Jacqueline Bouvier", 80);`},
			tc.CommitAll{Message: "made changes"},
			tc.Query{Query: `delete from people where id=9;`},
			tc.CommitAll{Message: "made changes"},
			tc.Query{Query: `alter table people drop column drip;`},
			tc.CommitAll{Message: "made changes"},
		},
		OldTag:            DripTag,
		NewTag:            DripTagRebased,
		SelectResultQuery: "select * from people;",
		ExpectedSchema:    schema.MustSchemaFromCols(people),
		ExpectedRows: []row.Row{
			newRow(row.TaggedValues{IdTag: types.Int(7), NameTag: types.String("Maggie Simpson"), AgeTag: types.Int(1)}, people),
		},
	},
}

func TestRebaseTag(t *testing.T) {
	for _, test := range RebaseTagTests {
		t.Run(test.Name, func(t *testing.T) {
			testRebaseTag(t, test)
		})
	}
	t.Run("ensure that rebased history does not overlap with previous history", testRebaseTagHistory)
}

func testRebaseTag(t *testing.T, test RebaseTagTest) {
	validateTest(t, test)

	dEnv := dtu.CreateTestEnv()
	for idx, cmd := range test.Commands {
		fmt.Println(fmt.Sprintf("%d: %s: %s", idx, cmd.CommandString(), cmd))
		setupErr := cmd.Exec(t, dEnv)
		require.NoError(t, setupErr)
	}

	bs, _ := dEnv.DoltDB.GetBranches(context.Background()) // master
	rebasedCommit, err := rebase.TagRebaseForRef(context.Background(), bs[0], dEnv.DoltDB, rebase.TagMapping{"people": map[uint64]uint64{test.OldTag: test.NewTag}})

	if test.ExpectedErrStr != "" {
		require.NotNil(t, err)
		require.Contains(t, err.Error(), test.ExpectedErrStr)
	} else {
		require.NoError(t, err)
		require.NotNil(t, rebasedCommit)

		mcs, _ := doltdb.NewCommitSpec("master")
		masterCm, _ := dEnv.DoltDB.Resolve(context.Background(), mcs, nil)
		rch, _ := rebasedCommit.HashOf()
		mch, _ := masterCm.HashOf()
		require.Equal(t, rch, mch)

		rebasedRoot, _ := rebasedCommit.GetRootValue()
		checkSchema(t, rebasedRoot, "people", test.ExpectedSchema)
		checkRows(t, dEnv, rebasedRoot, "people", test.ExpectedSchema, test.SelectResultQuery, test.ExpectedRows)
	}
}

func testRebaseTagHistory(t *testing.T) {
	cmds := []tc.Command{
		tc.Query{Query: createPeopleTable},
		tc.Query{Query: `insert into people (id, name, age) values 
			(7, "Maggie Simpson", 1);`},
		tc.CommitAll{Message: "made changes"}, // common ancestor of (newMaster, oldMaster) and (newMaster, other)

		tc.Query{Query: `alter table people add drip double;`},
		tc.CommitAll{Message: "made changes"}, // common ancestor of (oldMaster, other)

		tc.Branch{BranchName: "other"},
		tc.Query{Query: `insert into people (id, name, age, drip) values (10, "Patty Bouvier", 40, 8.5);`},
		tc.CommitAll{Message: "made changes"},
	}

	dEnv := dtu.CreateTestEnv()
	for _, cmd := range cmds {
		err := cmd.Exec(t, dEnv)
		require.NoError(t, err)
	}

	mcs, _ := doltdb.NewCommitSpec("master")
	oldMasterCm, _ := dEnv.DoltDB.Resolve(context.Background(), mcs, nil)
	ocs, _ := doltdb.NewCommitSpec("other")
	otherCm, _ := dEnv.DoltDB.Resolve(context.Background(), ocs, nil)

	bs, _ := dEnv.DoltDB.GetBranches(context.Background()) // master
	newMasterCm, err := rebase.TagRebaseForRef(context.Background(), bs[0], dEnv.DoltDB, rebase.TagMapping{"people": map[uint64]uint64{DripTag: DripTagRebased}})
	require.NoError(t, err)

	expectedSch := schema.MustSchemaFromCols(peopleWithDrip)
	rebasedRoot, _ := newMasterCm.GetRootValue()
	checkSchema(t, rebasedRoot, "people", expectedSch)
	checkRows(t, dEnv, rebasedRoot, "people", expectedSch, "select * from people;", []row.Row{
		newRow(row.TaggedValues{IdTag: types.Int(7), NameTag: types.String("Maggie Simpson"), AgeTag: types.Int(1)}, people),
		newRow(row.TaggedValues{IdTag: types.Int(10), NameTag: types.String("Patty Bouvier"), AgeTag: types.Int(40), DripTagRebased: types.Float(8.5)}, peopleWithDrip),
	})

	// assert that histories have been forked
	anc1, err := doltdb.GetCommitAncestor(context.Background(), oldMasterCm, otherCm)
	require.NoError(t, err)
	ancHash1, _ := anc1.HashOf()

	anc2, err := doltdb.GetCommitAncestor(context.Background(), newMasterCm, oldMasterCm)
	require.NoError(t, err)
	ancHash2, _ := anc2.HashOf()

	anc3, err := doltdb.GetCommitAncestor(context.Background(), newMasterCm, otherCm)
	require.NoError(t, err)
	ancHash3, _ := anc3.HashOf()

	require.NotEqual(t, ancHash1, ancHash2)
	require.NotEqual(t, ancHash1, ancHash3)
	require.Equal(t, ancHash2, ancHash3)
}

func validateTest(t *testing.T, test RebaseTagTest) {
	require.NotNil(t, test.Name)
	require.NotNil(t, test.Commands)
	require.NotNil(t, test.OldTag)
	require.NotNil(t, test.NewTag)
	require.NotNil(t, test.SelectResultQuery)
	if test.ExpectedErrStr == "" {
		require.NotNil(t, test.ExpectedSchema)
		require.NotNil(t, test.ExpectedRows)
	}
}

func checkSchema(t *testing.T, r *doltdb.RootValue, tableName string, expectedSch schema.Schema) {
	tbl, _, err := r.GetTable(context.Background(), tableName)
	require.NoError(t, err)
	sch, err := tbl.GetSchema(context.Background())
	require.NoError(t, err)
	require.Equal(t, expectedSch.GetAllCols().Size(), sch.GetAllCols().Size())
	cols := sch.GetAllCols().GetColumns()
	for i, expectedCol := range expectedSch.GetAllCols().GetColumns() {
		col := cols[i]
		col.Tag = expectedCol.Tag
		assert.Equal(t, expectedCol, col)
	}
}

func checkRows(t *testing.T, dEnv *env.DoltEnv, root *doltdb.RootValue, tableName string, sch schema.Schema, selectQuery string, expectedRows []row.Row) {
	sqlDb := dsqle.NewDatabase("dolt", dEnv.DbData())
	engine, sqlCtx, err := dsqle.NewTestEngine(context.Background(), sqlDb, root)
	require.NoError(t, err)

	s, rowIter, err := engine.Query(sqlCtx, selectQuery)
	require.NoError(t, err)
	_, err = sqlutil.ToDoltSchema(context.Background(), root, tableName, s)
	require.NoError(t, err)

	actualRows := []row.Row{}
	for {
		r, err := rowIter.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		rr, err := sqlutil.SqlRowToDoltRow(context.Background(), root.VRW(), r, sch)
		require.NoError(t, err)
		actualRows = append(actualRows, rr)
	}

	require.Equal(t, len(actualRows), len(expectedRows))

	for idx := 0; idx < len(expectedRows); idx++ {
		require.True(t, idx < len(expectedRows))
		require.Equal(t, expectedRows[idx], actualRows[idx])
		idx++
	}
}
