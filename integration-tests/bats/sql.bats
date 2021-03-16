#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt sql <<SQL
CREATE TABLE one_pk (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk)
);
CREATE TABLE two_pk (
  pk1 BIGINT NOT NULL,
  pk2 BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk1,pk2)
);
CREATE TABLE has_datetimes (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  date_created DATETIME COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
INSERT INTO one_pk (pk,c1,c2,c3,c4,c5) VALUES (0,0,0,0,0,0),(1,10,10,10,10,10),(2,20,20,20,20,20),(3,30,30,30,30,30);
INSERT INTO two_pk (pk1,pk2,c1,c2,c3,c4,c5) VALUES (0,0,0,0,0,0,0),(0,1,10,10,10,10,10),(1,0,20,20,20,20,20),(1,1,30,30,30,30,30);
INSERT INTO has_datetimes (pk, date_created) VALUES (0, '2020-02-17 00:00:00');
SQL
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "sql: select from multiple tables" {
    run dolt sql -q "select pk,pk1,pk2 from one_pk,two_pk"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 20 ]
    run dolt sql -q "select pk,pk1,pk2 from one_pk,two_pk where one_pk.c1=two_pk.c1"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 8 ]
    run dolt sql -q "select pk,pk1,pk2,one_pk.c1 as foo,two_pk.c1 as bar from one_pk,two_pk where one_pk.c1=two_pk.c1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ foo ]] || false
    [[ "$output" =~ bar ]] || false
    [ "${#lines[@]}" -eq 8 ]
}

@test "sql: AS OF queries" {
    dolt add .
    dolt commit -m "Initial master commit" --date "2020-03-01T12:00:00Z"

    master_commit=`dolt log | head -n1 | cut -d' ' -f2`
    dolt sql -q "update one_pk set c1 = c1 + 1"
    dolt sql -q "drop table two_pk"
    dolt checkout -b new_branch
    dolt add .
    dolt commit -m "Updated a table, dropped a table" --date "2020-03-01T13:00:00Z"
    new_commit=`dolt log | head -n1 | cut -d' ' -f2`
    
    run dolt sql -r csv -q "select pk,c1 from one_pk order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,1" ]] || false
    [[ "$output" =~ "1,11" ]] || false
    [[ "$output" =~ "2,21" ]] || false
    [[ "$output" =~ "3,31" ]] || false
    
    run dolt sql -r csv -q "select pk,c1 from one_pk as of 'master' order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "1,10" ]] || false
    [[ "$output" =~ "2,20" ]] || false
    [[ "$output" =~ "3,30" ]] || false

    run dolt sql -r csv -q "select pk,c1 from one_pk as of '$master_commit' order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "1,10" ]] || false
    [[ "$output" =~ "2,20" ]] || false
    [[ "$output" =~ "3,30" ]] || false
    
    run dolt sql -r csv -q "select count(*) from two_pk as of 'master'"
    [ $status -eq 0 ]
    [[ "$output" =~ "4" ]] || false

    run dolt sql -r csv -q "select count(*) from two_pk as of '$master_commit'"
    [ $status -eq 0 ]
    [[ "$output" =~ "4" ]] || false

    run dolt sql -r csv -q "select pk,c1 from one_pk as of 'HEAD~' order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "1,10" ]] || false
    [[ "$output" =~ "2,20" ]] || false
    [[ "$output" =~ "3,30" ]] || false

    run dolt sql -r csv -q "select pk,c1 from one_pk as of 'new_branch^' order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "1,10" ]] || false
    [[ "$output" =~ "2,20" ]] || false
    [[ "$output" =~ "3,30" ]] || false
    
    dolt checkout master
    run dolt sql -r csv -q "select pk,c1 from one_pk as of 'new_branch' order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,1" ]] || false
    [[ "$output" =~ "1,11" ]] || false
    [[ "$output" =~ "2,21" ]] || false
    [[ "$output" =~ "3,31" ]] || false

    run dolt sql -r csv -q "select pk,c1 from one_pk as of '$new_commit' order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,1" ]] || false
    [[ "$output" =~ "1,11" ]] || false
    [[ "$output" =~ "2,21" ]] || false
    [[ "$output" =~ "3,31" ]] || false

    dolt checkout new_branch
    run dolt sql -r csv -q "select pk,c1 from one_pk as of CONVERT('2020-03-01 12:00:00', DATETIME) order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "1,10" ]] || false
    [[ "$output" =~ "2,20" ]] || false
    [[ "$output" =~ "3,30" ]] || false

    run dolt sql -r csv -q "select pk,c1 from one_pk as of CONVERT('2020-03-01 12:15:00', DATETIME) order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "1,10" ]] || false
    [[ "$output" =~ "2,20" ]] || false
    [[ "$output" =~ "3,30" ]] || false

    run dolt sql -r csv -q "select pk,c1 from one_pk as of CONVERT('2020-03-01 13:00:00', DATETIME) order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,1" ]] || false
    [[ "$output" =~ "1,11" ]] || false
    [[ "$output" =~ "2,21" ]] || false
    [[ "$output" =~ "3,31" ]] || false

    run dolt sql -r csv -q "select pk,c1 from one_pk as of CONVERT('2020-03-01 13:15:00', DATETIME) order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,1" ]] || false
    [[ "$output" =~ "1,11" ]] || false
    [[ "$output" =~ "2,21" ]] || false
    [[ "$output" =~ "3,31" ]] || false

    run dolt sql -r csv -q "select pk,c1 from one_pk as of CONVERT('2020-03-01 11:59:59', DATETIME) order by c1"
    [ $status -eq 1 ]
    [[ "$output" =~ "not found" ]] || false
}

@test "sql: output formats" {
    dolt sql <<SQL
    CREATE TABLE test (
    a int primary key,
    b float,
    c varchar(80),
    d datetime
);
SQL
    dolt sql <<SQL
    insert into test values (1, 1.5, "1", "2020-01-01");
    insert into test values (2, 2.5, "2", "2020-02-02");
    insert into test values (3, NULL, "3", "2020-03-03");
    insert into test values (4, 4.5, NULL, "2020-04-04");
    insert into test values (5, 5.5, "5", NULL);
SQL

    run dolt sql -r csv -q "select * from test order by a"
    [ $status -eq 0 ]
    [[ "$output" =~ "a,b,c,d" ]] || false
    [[ "$output" =~ '1,1.5,1,2020-01-01 00:00:00 +0000 UTC' ]] || false
    [[ "$output" =~ '2,2.5,2,2020-02-02 00:00:00 +0000 UTC' ]] || false
    [[ "$output" =~ '3,,3,2020-03-03 00:00:00 +0000 UTC' ]] || false
    [[ "$output" =~ '4,4.5,,2020-04-04 00:00:00 +0000 UTC' ]] || false
    [[ "$output" =~ '5,5.5,5,' ]] || false
    [ "${#lines[@]}" -eq 6 ]

    run dolt sql -r json -q "select * from test order by a"
    [ $status -eq 0 ]
    echo $output
    [ "$output" == '{"rows": [{"a":1,"b":1.5,"c":"1","d":"2020-01-01 00:00:00 +0000 UTC"},{"a":2,"b":2.5,"c":"2","d":"2020-02-02 00:00:00 +0000 UTC"},{"a":3,"c":"3","d":"2020-03-03 00:00:00 +0000 UTC"},{"a":4,"b":4.5,"d":"2020-04-04 00:00:00 +0000 UTC"},{"a":5,"b":5.5,"c":"5"}]}' ]
}

@test "sql: output for escaped longtext exports properly" {
 dolt sql <<SQL
    CREATE TABLE test (
    a int primary key,
    v LONGTEXT
);
SQL
dolt sql <<SQL
    insert into test values (1, "{""key"": ""value""}");
    insert into test values (2, """Hello""");
SQL

    run dolt sql -r json -q "select * from test order by a"
    [ $status -eq 0 ]
    [ "$output" == '{"rows": [{"a":1,"v":"{\"key\": \"value\"}"},{"a":2,"v":"\"Hello\""}]}' ]

    run dolt sql -r csv -q "select * from test order by a"
    [ $status -eq 0 ]
    [[ "$output" =~ "a,v" ]] || false
    [[ "$output" =~ '1,"{""key"": ""value""}"' ]] || false
    [[ "$output" =~ '2,"""Hello"""' ]] || false
}

@test "sql: ambiguous column name" {
    run dolt sql -q "select pk,pk1,pk2 from one_pk,two_pk where c1=0"
    [ "$status" -eq 1 ]
    [ "$output" = "ambiguous column name \"c1\", it's present in all these tables: one_pk, two_pk" ]
}

@test "sql: select with and and or clauses" {
    run dolt sql -q "select pk,pk1,pk2 from one_pk,two_pk where pk=0 and pk1=0 or pk2=1"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 13 ]
}

@test "sql: select the same column twice using column aliases" {
    run dolt sql -q "select pk,c1 as foo,c1 as bar from one_pk"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "NULL" ]] || false
    [[ "$output" =~ "foo" ]] || false
    [[ "$output" =~ "bar" ]] || false
}

@test "sql: select same column twice using table aliases" {
    run dolt sql -q "select foo.pk,foo.c1,bar.c1 from one_pk as foo, one_pk as bar"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "NULL" ]] || false
    [[ "$output" =~ "c1" ]] || false
}

@test "sql: select ambiguous column using table aliases" {
    run dolt sql -q "select pk,foo.c1,bar.c1 from one_pk as foo, one_pk as bar"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "ambiguous" ]] || false
}

@test "sql: basic inner join" {
    run dolt sql -q "select pk,pk1,pk2 from one_pk join two_pk on one_pk.c1=two_pk.c1"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 8 ]
    first_join_output=$output
    run dolt sql -q "select pk,pk1,pk2 from two_pk join one_pk on one_pk.c1=two_pk.c1"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 8 ]
    [ "$output" = "$first_join_output" ]
    run dolt sql -q "select pk,pk1,pk2 from one_pk join two_pk on one_pk.c1=two_pk.c1 where pk=1"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    run dolt sql -q "select pk,pk1,pk2,one_pk.c1 as foo,two_pk.c1 as bar from one_pk join two_pk on one_pk.c1=two_pk.c1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ foo ]] || false
    [[ "$output" =~ bar ]] || false
    [ "${#lines[@]}" -eq 8 ]
    run dolt sql -q "select pk,pk1,pk2,one_pk.c1 as foo,two_pk.c1 as bar from one_pk join two_pk on one_pk.c1=two_pk.c1  where one_pk.c1=10"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "10" ]] || false
}

@test "sql: select two tables and join to one" {
    run dolt sql -q "select op.pk,pk1,pk2 from one_pk,two_pk join one_pk as op on op.pk=pk1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 20 ]
}

@test "sql: non unique table alias" {
    run dolt sql -q "select pk from one_pk,one_pk"
    skip "This should be an error. MySQL gives: Not unique table/alias: 'one_pk'"
    [ $status -eq 1 ]
}

@test "sql: is null and is not null statements" {
    dolt sql -q "insert into one_pk (pk,c1,c2) values (11,0,0)"
    run dolt sql -q "select pk from one_pk where c3 is null"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "11" ]] || false
    run dolt sql -q "select pk from one_pk where c3 is not null"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 8 ]
    [[ ! "$output" =~ "11" ]] || false
}

@test "sql: addition and subtraction" {
    dolt sql -q "insert into one_pk (pk,c1,c2,c3,c4,c5) values (11,0,5,10,15,20)"
    run dolt sql -q "select pk from one_pk where c2-c1>=5"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "11" ]] || false
    run dolt sql -q "select pk from one_pk where c3-c2-c1>=5"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "11" ]] || false
    run dolt sql -q "select pk from one_pk where c2+c1<=5"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "$output" =~ "0" ]] || false
    [[ "$output" =~ "11" ]] || false
}

@test "sql: order by and limit" {
    run dolt sql -q "select * from one_pk order by pk limit 1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ " 0 " ]] || false
    [[ ! "$output" =~ " 10 " ]] || false
    run dolt sql -q "select * from one_pk order by pk limit 0,1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ " 0 " ]] || false
    [[ ! "$output" =~ " 10 " ]] || false
    run dolt sql -q "select * from one_pk order by pk limit 1,1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ " 10 " ]] || false
    [[ ! "$output" =~ " 0 " ]] || false
    run dolt sql -q "select * from one_pk order by pk limit 1,0"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
    [[ ! "$output" =~ " 0 " ]] || false
    run dolt sql -q "select * from one_pk order by pk desc limit 1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "30" ]] || false
    [[ ! "$output" =~ "10" ]] || false
    run dolt sql -q "select * from two_pk order by pk1, pk2 desc limit 1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "10" ]] || false
    run dolt sql -q "select pk,c2 from one_pk order by c1 limit 1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "0" ]] || false
    [[ ! "$output" =~ "10" ]] || false
    run dolt sql -q "select * from one_pk,two_pk order by pk1,pk2,pk limit 1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "0" ]] || false
    [[ ! "$output" =~ "10" ]] || false
    dolt sql -q "select * from one_pk join two_pk order by pk1,pk2,pk limit 1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "0" ]] || false
    [[ ! "$output" =~ "10" ]] || false
    run dolt sql -q "select * from one_pk order by limit 1"
    [ $status -eq 1 ]
    run dolt sql -q "select * from one_pk order by bad limit 1"
    [ $status -eq 1 ]
    [[ "$output" =~ "column \"bad\" could not be found" ]] || false
    run dolt sql -q "select * from one_pk order pk by limit"
    [ $status -eq 1 ]
}

@test "sql: limit less than zero" {
    run dolt sql -q "select * from one_pk order by pk limit -1"
    [ $status -eq 1 ]
    [[ "$output" =~ "unsupported syntax: LIMIT must be >= 0" ]] || false
    run dolt sql -q "select * from one_pk order by pk limit -2"
    [ $status -eq 1 ]
    [[ "$output" =~ "unsupported syntax: LIMIT must be >= 0" ]] || false
    run dolt sql -q "select * from one_pk order by pk limit -1,1"
    [ $status -eq 1 ]
    [[ "$output" =~ "unsupported syntax: OFFSET must be >= 0" ]] || false
}

@test "sql: addition on both left and right sides of comparison operator" {
    dolt sql -q "insert into one_pk (pk,c1,c2,c3,c4,c5) values (11,5,5,10,15,20)"
    run dolt sql -q "select pk from one_pk where c2+c1<=5+5"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "$output" =~ 0 ]] || false
    [[ "$output" =~ 11 ]] || false
}

@test "sql: select with in list" {
    run dolt sql -q "select pk from one_pk where c1 in (10,20)"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    run dolt sql -q "select pk from one_pk where c1 in (11,21)"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
    run dolt sql -q "select pk from one_pk where c1 not in (10,20)"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "$output" =~ "0" ]] || false
    [[ "$output" =~ "3" ]] || false
    run dolt sql -q "select pk from one_pk where c1 not in (10,20) and c1 in (30)"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "3" ]] || false
}

@test "sql: parser does not support empty list" {
    run dolt sql -q "select pk from one_pk where c1 not in ()"
    [ $status -eq 1 ]
    [[ "$output" =~ "Error parsing SQL" ]] || false
}

@test "sql: addition in join statement" {
    run dolt sql -q "select * from one_pk join two_pk on pk1-pk>0 and pk2<1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "20" ]] || false
}

@test "sql: leave off table name in select" {
    dolt sql -q "insert into one_pk (pk,c1,c2) values (11,0,0)"
    run dolt sql -q "select pk where c3 is null"
    [ $status -eq 1 ]
    [[ "$output" =~ "column \"c3\" could not be found in any table in scope" ]] || false
}

@test "sql: show tables" {
    run dolt sql -q "show tables"
    [ $status -eq 0 ]
    echo ${#lines[@]}
    [ "${#lines[@]}" -eq 7 ]
    [[ "$output" =~ "one_pk" ]] || false
    [[ "$output" =~ "two_pk" ]] || false
    [[ "$output" =~ "has_datetimes" ]] || false
}

@test "sql: show tables AS OF" {
    dolt add .; dolt commit -m 'commit tables'
    dolt sql <<SQL
CREATE TABLE table_a(x int primary key);
CREATE TABLE table_b(x int primary key);
SQL
    dolt add .; dolt commit -m 'commit tables'
    
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "$output" =~ table_a ]] || false

    run dolt sql -q "show tables AS OF 'HEAD~'" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
    [[ ! "$output" =~ table_a ]] || false    
}

@test "sql: describe" {
    run dolt sql -q "describe one_pk"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 10 ]
    [[ "$output" =~ "pk" ]] || false
    [[ "$output" =~ "c5" ]] || false
}

@test "sql: decribe bad table name" {
    run dolt sql -q "describe poop"
    [ $status -eq 1 ]
    [[ "$output" =~ "table not found: poop" ]] || false
}

@test "sql: alter table to add and delete a column" {
    run dolt sql -q "alter table one_pk add (c6 int)"
    [ $status -eq 0 ]
    run dolt sql -q "describe one_pk"
    [ $status -eq 0 ]
    [[ "$output" =~ "c6" ]] || false
    run dolt schema show one_pk
    [[ "$output" =~ "c6" ]] || false
    run dolt sql -q "alter table one_pk drop column c6"
    [ $status -eq 0 ]
    run dolt sql -q "describe one_pk"
    [ $status -eq 0 ]
    [[ ! "$output" =~ "c6" ]] || false
    run dolt schema show one_pk
    [[ ! "$output" =~ "c6" ]] || false
}

@test "sql: alter table to rename a column" {
    dolt sql -q "alter table one_pk add (c6 int)"
    run dolt sql -q "alter table one_pk rename column c6 to c7"
    [ $status -eq 0 ]
    run dolt sql -q "describe one_pk"
    [ $status -eq 0 ]
    [[ "$output" =~ "c7" ]] || false
    [[ ! "$output" =~ "c6" ]] || false
}

@test "sql: alter table change column to rename a column" {
    dolt sql -q "alter table one_pk add (c6 int)"
    dolt sql -q "alter table one_pk change column c6 c7 int"
    run dolt sql -q "describe one_pk"
    [ $status -eq 0 ]
    [[ "$output" =~ "c7" ]] || false
    [[ ! "$output" =~ "c6" ]] || false
}

@test "sql: alter table without parentheses" {
    run dolt sql -q "alter table one_pk add c6 int"
    [ $status -eq 0 ]
    run dolt sql -q "describe one_pk"
    [ $status -eq 0 ]
    [[ "$output" =~ "c6" ]] || false
}

@test "sql: alter table modify column with no actual change" {
    # this specifically tests a previous bug where we would get a name collision and fail
    dolt sql -q "alter table one_pk modify column c5 bigint"
    run dolt schema show one_pk
    [ $status -eq 0 ]
    [[ "$output" =~ '`pk` bigint NOT NULL' ]] || false
    [[ "$output" =~ '`c1` bigint' ]] || false
    [[ "$output" =~ '`c2` bigint' ]] || false
    [[ "$output" =~ '`c3` bigint' ]] || false
    [[ "$output" =~ '`c4` bigint' ]] || false
    [[ "$output" =~ '`c5` bigint' ]] || false
    [[ "$output" =~ 'PRIMARY KEY (`pk`)' ]] || false
}

@test "sql: alter table change column with no actual change" {
    # this specifically tests a previous bug where we would get a name collision and fail
    dolt sql -q "alter table one_pk change column c5 c5 bigint"
    run dolt schema show one_pk
    [ $status -eq 0 ]
    [[ "$output" =~ '`pk` bigint NOT NULL' ]] || false
    [[ "$output" =~ '`c1` bigint' ]] || false
    [[ "$output" =~ '`c2` bigint' ]] || false
    [[ "$output" =~ '`c3` bigint' ]] || false
    [[ "$output" =~ '`c4` bigint' ]] || false
    [[ "$output" =~ '`c5` bigint' ]] || false
    [[ "$output" =~ 'PRIMARY KEY (`pk`)' ]] || false
}

@test "sql: alter table modify column type success" {
    dolt sql <<SQL
CREATE TABLE t1(pk BIGINT PRIMARY KEY, v1 INT, INDEX(v1));
CREATE TABLE t2(pk BIGINT PRIMARY KEY, v1 VARCHAR(20), INDEX(v1));
CREATE TABLE t3(pk BIGINT PRIMARY KEY, v1 DATETIME, INDEX(v1));
INSERT INTO t1 VALUES (0,-1),(1,1);
INSERT INTO t2 VALUES (0,'hi'),(1,'bye');
INSERT INTO t3 VALUES (0,'1999-11-02 17:39:38'),(1,'2021-01-08 02:59:27');
ALTER TABLE t1 MODIFY COLUMN v1 BIGINT;
ALTER TABLE t2 MODIFY COLUMN v1 VARCHAR(2000);
ALTER TABLE t3 MODIFY COLUMN v1 TIMESTAMP;
SQL
    run dolt sql -q "SELECT * FROM t1 ORDER BY pk" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "0,-1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM t2 ORDER BY pk" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "0,hi" ]] || false
    [[ "$output" =~ "1,bye" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM t3 ORDER BY pk" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "0,1999-11-02 17:39:38" ]] || false
    [[ "$output" =~ "1,2021-01-08 02:59:27" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "sql: alter table modify column type failure" {
    dolt sql <<SQL
CREATE TABLE t1(pk BIGINT PRIMARY KEY, v1 INT, INDEX(v1));
CREATE TABLE t2(pk BIGINT PRIMARY KEY, v1 VARCHAR(20), INDEX(v1));
CREATE TABLE t3(pk BIGINT PRIMARY KEY, v1 DATETIME, INDEX(v1));
INSERT INTO t1 VALUES (0,-1),(1,1);
INSERT INTO t2 VALUES (0,'hi'),(1,'bye');
INSERT INTO t3 VALUES (0,'1999-11-02 17:39:38'),(1,'3021-01-08 02:59:27');
SQL
    run dolt sql -q "ALTER TABLE t1 MODIFY COLUMN v1 INT UNSIGNED"
    [ "$status" -eq "1" ]
    run dolt sql -q "ALTER TABLE t2 MODIFY COLUMN v1 VARCHAR(2)"
    [ "$status" -eq "1" ]
    run dolt sql -q "ALTER TABLE t3 MODIFY COLUMN v1 TIMESTAMP"
    [ "$status" -eq "1" ]
}

@test "sql: alter table modify column type no data change" {
    # there was a bug on NULLs where it would register a change
    dolt sql <<SQL
CREATE TABLE t1(pk BIGINT PRIMARY KEY, v1 VARCHAR(64), INDEX(v1));
INSERT INTO t1 VALUES (0,NULL),(1,NULL);
SQL
    dolt add -A
    dolt commit -m "commit"
    dolt sql -q "ALTER TABLE t1 MODIFY COLUMN v1 VARCHAR(100) NULL"
    run dolt diff -d
    [ "$status" -eq "0" ]
    [[ ! "$output" =~ "|  <  |" ]] || false
    [[ ! "$output" =~ "|  >  |" ]] || false
}

@test "sql: drop table" {
    dolt sql -q "drop table one_pk"
    run dolt ls
    [[ ! "$output" =~ "one_pk" ]] || false
    run dolt sql -q "drop table poop"
    [ $status -eq 1 ]
    [ "$output" = "table not found: poop" ]
}

@test "sql: explain simple select query" {
    run dolt sql -q "explain select * from one_pk"
    [ $status -eq 0 ]
    [[ "$output" =~ "plan" ]] || false
    [[ "$output" =~ "one_pk" ]] || false
}

@test "sql: explain simple query with where clause" {
    run dolt sql -q "explain select * from one_pk where pk=0"
    [ $status -eq 0 ]
    [[ "$output" =~ "Filter" ]] || false
}

@test "sql: explain simple join" {
    run dolt sql -q "explain select op.pk,pk1,pk2 from one_pk,two_pk join one_pk as op on op.pk=pk1"
    [ $status -eq 0 ]
    [[ "$output" =~ "IndexedJoin" ]] || false
}

@test "sql: replace count" {
    skip "right now we always count a replace as a delete and insert when we shouldn't"
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v BIGINT);"
    run dolt sql -q "REPLACE INTO test VALUES (1, 1);"
    [ $status -eq 0 ]
    [[ "${lines[3]}" =~ " 1 " ]] || false
    run dolt sql -q "REPLACE INTO test VALUES (1, 2);"
    [ $status -eq 0 ]
    [[ "${lines[3]}" =~ " 2 " ]] || false
}

@test "sql: unix_timestamp function" {
    run dolt sql -q "SELECT UNIX_TIMESTAMP(NOW()) FROM dual;"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
}

@test "sql: select union all" {
    run dolt sql -r csv -q "SELECT 2+2 FROM dual UNION ALL SELECT 2+2 FROM dual UNION ALL SELECT 2+3 FROM dual;"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
}

@test "sql: select union" {
    run dolt sql -r csv -q "SELECT 2+2 FROM dual UNION SELECT 2+2 FROM dual UNION SELECT 2+3 FROM dual;"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    run dolt sql -r csv -q "SELECT 2+2 FROM dual UNION DISTINCT SELECT 2+2 FROM dual UNION SELECT 2+3 FROM dual;"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    run dolt sql -r csv -q "(SELECT 2+2 FROM dual UNION DISTINCT SELECT 2+2 FROM dual) UNION SELECT 2+3 FROM dual;"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    run dolt sql -r csv -q "SELECT 2+2 FROM dual UNION DISTINCT (SELECT 2+2 FROM dual UNION SELECT 2+3 FROM dual);"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
}

@test "sql: greatest/least with a timestamp" {
    run dolt sql -q "SELECT GREATEST(NOW(), DATE_ADD(NOW(), INTERVAL 2 DAY)) FROM dual;"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    run dolt sql -q "SELECT LEAST(NOW(), DATE_ADD(NOW(), INTERVAL 2 DAY)) FROM dual;"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
}

@test "sql: greatest with converted null" {
    run dolt sql -q "SELECT GREATEST(CAST(NOW() AS CHAR), CAST(NULL AS CHAR)) FROM dual;"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "${lines[3]}" =~ " NULL " ]] || false
}

@test "sql: date_format function" {
    skip "date_format() not supported" 
    dolt sql -q "select date_format(date_created, '%Y-%m-%d') from has_datetimes"
}

@test "sql: DATE_ADD and DATE_SUB in where clause" {
    run dolt sql -q "select * from has_datetimes where date_created > DATE_SUB('2020-02-18 00:00:00', INTERVAL 2 DAY)"
    [ $status -eq 0 ]
    [[ "$output" =~ "17 " ]] || false
    run dolt sql -q "select * from has_datetimes where date_created > DATE_ADD('2020-02-14 00:00:00', INTERVAL 2 DAY)"
    [ $status -eq 0 ]
    [[ "$output" =~ "17 " ]] || false
}

@test "sql: update a datetime column" {
    dolt sql -q "insert into has_datetimes (pk) values (1)"
    run dolt sql -q "update has_datetimes set date_created='2020-02-11 00:00:00' where pk=1"
    [ $status -eq 0 ]
    [[ ! "$output" =~ "Expected GetField expression" ]] || false
}

@test "sql: group by statements" {
    dolt sql -q "insert into one_pk (pk,c1,c2,c3,c4,c5) values (4,0,0,0,0,0),(5,0,0,0,0,0)"
    run dolt sql -q "select max(pk) from one_pk group by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ " 5 " ]] || false
    [[ ! "$output" =~ " 4 " ]] || false
    run dolt sql -q "select max(pk), min(c2) from one_pk group by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ " 5 " ]] || false
    [[ "$output" =~ " 0 " ]] || false
    [[ ! "$output" =~ " 4 " ]] || false
    run dolt sql -q "select max(pk),c2 from one_pk group by c1"
    [ $status -eq 1 ]
    [[ "$output" =~ "doesn't appear in the grouping columns" ]] || false
    skip "group by behavior is inconsistent"
    run dolt sql -q "select max(pk),c2 from one_pk group by c1 limit 1"
    [ $status -eq 1 ]
    [[ "$output" =~ "doesn't appear in the grouping columns" ]] || false
    run dolt sql -q "select max(pk) as broke_group_by, c2 from one_pk group by c1"
    [ $status -eq 1 ]
    [[ "$output" =~ "doesn't appear in the grouping columns" ]] || false
}

@test "sql: substr() and cast() functions" {
    run dolt sql -q "select substr(cast(date_created as char), 1, 4) from has_datetimes"
    [ $status -eq 0 ]
    [[ "$output" =~ " 2020 " ]] || false
    [[ ! "$output" =~ "17" ]] || false
}

@test "sql: divide by zero does not panic" {
    run dolt sql -q "select 1/0 from dual"
    [ $status -eq 0 ]
    echo $output
    [[ "$output" =~ "NULL" ]] || false
    [[ ! "$output" =~ "panic: " ]] || false
    run dolt sql -q "select 1.0/0.0 from dual"
    [ $status -eq 0 ]
    [[ "$output" =~ "NULL" ]] || false
    [[ ! "$output" =~ "panic: " ]] || false
    run dolt sql -q "select 1 div 0 from dual"
    [ $status -eq 0 ]
    [[ "$output" =~ "NULL" ]] || false
    [[ ! "$output" =~ "panic: " ]] || false
}

@test "sql: delete all rows in table" {
    run dolt sql <<SQL
DELETE FROM one_pk;
SELECT count(*) FROM one_pk;
SQL
    [ $status -eq 0 ]
    [[ "$output" =~ "0" ]] || false
}

@test "sql: shell works after failing query" {
    skiponwindows "Need to install expect and make this script work on windows."
    $BATS_TEST_DIRNAME/sql-works-after-failing-query.expect
}

@test "sql: shell delimiter" {
    skiponwindows "Need to install expect and make this script work on windows."
    mkdir doltsql
    cd doltsql
    dolt init

    run $BATS_TEST_DIRNAME/sql-delimiter.expect
    [ "$status" -eq "0" ]
    [[ ! "$output" =~ "Error" ]] || false
    [[ ! "$output" =~ "error" ]] || false

    run dolt sql -q "SELECT * FROM test ORDER BY 1" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false

    run dolt sql -q "SHOW TRIGGERS"
    [ "$status" -eq "0" ]
    [[ "$output" =~ "SET NEW.v1 = NEW.v1 * 11" ]] || false

    cd ..
    rm -rf doltsql
}

@test "sql: batch delimiter" {
    dolt sql <<SQL
DELIMITER // ;
CREATE TABLE test (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT
)//
INSERT INTO test VALUES (1, 1, 1) //
DELIMITER $ //
INSERT INTO test VALUES (2, 2, 2)$ $
CREATE PROCEDURE p1(x BIGINT)
BEGIN
  IF x < 10 THEN
    SET x = 10;
  END IF;
  SELECT pk+x, v1+x, v2+x FROM test ORDER BY 1;
END$
DELIMITER ;   $
INSERT INTO test VALUES (3, 3, 3);
DELIMITER ********** ;
INSERT INTO test VALUES (4, 4, 4)**********
INSERT INTO test VALUES (5, 5, 5)
SQL
    run dolt sql -q "CALL p1(3)" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "test.pk + x,test.v1 + x,test.v2 + x" ]] || false
    [[ "$output" =~ "11,11,11" ]] || false
    [[ "$output" =~ "12,12,12" ]] || false
    [[ "$output" =~ "13,13,13" ]] || false
    [[ "$output" =~ "14,14,14" ]] || false
    [[ "$output" =~ "15,15,15" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false

    run dolt sql -q "CALL p1(20)" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "test.pk + x,test.v1 + x,test.v2 + x" ]] || false
    [[ "$output" =~ "21,21,21" ]] || false
    [[ "$output" =~ "22,22,22" ]] || false
    [[ "$output" =~ "23,23,23" ]] || false
    [[ "$output" =~ "24,24,24" ]] || false
    [[ "$output" =~ "25,25,25" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
}

@test "sql: insert on duplicate key inserts data by column" {
    run dolt sql -q "CREATE TABLE test (col_a varchar(2) not null, col_b varchar(2), col_c varchar(2), primary key(col_a));"
    [ $status -eq 0 ]
    run dolt sql -q "INSERT INTO test (col_a,col_b) VALUES('a', 'b');"
    [ $status -eq 0 ]
    skip "on duplicate key not supported"
    run dolt sql -q "INSERT INTO test (col_a,col_b,col_c) VALUES ('a','','b') ON DUPLICATE KEY UPDATE col_a = col_a, col_b = col_b, col_c = VALUES(col_c);"
    [ $status -eq 0 ]
    [[ ! "$output" =~ 'unsupported feature' ]] || false
}

@test "sql: at commit" {
  dolt add .
  dolt commit -m "seed initial values"
  dolt checkout -b one
  dolt sql -q "UPDATE one_pk SET c1 = 100 WHERE pk = 0"
  dolt add .
  dolt commit -m "100"
  dolt checkout -b two
  dolt sql -q "UPDATE one_pk SET c1 = 200 WHERE pk = 0"
  dolt add .
  dolt commit -m "200"

  EXPECTED=$( echo -e "c1\n200" )
  run dolt sql -r csv -q "SELECT c1 FROM one_pk WHERE pk=0"
  [ $status -eq 0 ]
  [[ "$output" = "$EXPECTED" ]] || false
  run dolt sql -r csv -q "SELECT c1 FROM one_pk WHERE pk=0" HEAD
  [ $status -eq 0 ]
  [[ "$output" = "$EXPECTED" ]] || false
  run dolt sql -r csv -q "SELECT c1 FROM one_pk WHERE pk=0" two
  [ $status -eq 0 ]
  [[ "$output" = "$EXPECTED" ]] || false

  EXPECTED=$( echo -e "c1\n100" )
  run dolt sql -r csv -q "SELECT c1 FROM one_pk WHERE pk=0" HEAD~
  [ $status -eq 0 ]
  [[ "$output" = "$EXPECTED" ]] || false
  run dolt sql -r csv -q "SELECT c1 FROM one_pk WHERE pk=0" one
  [ $status -eq 0 ]
  [[ "$output" = "$EXPECTED" ]] || false

  EXPECTED=$( echo -e "c1\n0" )
  run dolt sql -r csv -q "SELECT c1 FROM one_pk WHERE pk=0" HEAD~2
  [ $status -eq 0 ]
  [[ "$output" = "$EXPECTED" ]] || false
  run dolt sql -r csv -q "SELECT c1 FROM one_pk WHERE pk=0" master
  [ $status -eq 0 ]
  [[ "$output" = "$EXPECTED" ]] || false

  #writes should fail if commit is specified
  run dolt sql -q "UPDATE one_pk SET c1 = 200 WHERE pk = 0" HEAD~
  [ $status -ne 0 ]
}

@test "sql: select with json output supports datetime" {
    run dolt sql -r json -q "select * from has_datetimes"
    [ $status -eq 0 ]
    [[ "$output" =~ "2020-02-17 00:00:00" ]] || false
}

@test "sql: dolt_version() func" {
    SQL=$(dolt sql -q 'select dolt_version() from dual;' -r csv | tail -n 1)
    CLI=$(dolt version | cut -f 3 -d ' ')
    [ "$SQL" == "$CLI" ]
}

@test "sql: stored procedures creation check" {
    dolt sql -q "
CREATE PROCEDURE p1(s VARCHAR(200), N DOUBLE, m DOUBLE)
BEGIN
  SET s = '';
  IF n = m THEN SET s = 'equals';
  ELSE
    IF n > m THEN SET s = 'greater';
    ELSE SET s = 'less';
    END IF;
    SET s = CONCAT('is ', s, ' than');
  END IF;
  SET s = CONCAT(n, ' ', s, ' ', m, '.');
  SELECT s;
END;"
    run dolt sql -q "CALL p1('', 1, 1)" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "1 equals 1." ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "CALL p1('', 2, 1)" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "2 is greater than 1." ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "CALL p1('', 1, 2)" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "1 is less than 2." ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_procedures" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "name,create_stmt,created_at,modified_at" ]] || false
    # Just the beginning portion is good enough, we don't need to test the timestamps as they change
    [[ "$output" =~ 'p1,"CREATE PROCEDURE p1(s VARCHAR(200), N DOUBLE, m DOUBLE)' ]] || false
    [[ "${#lines[@]}" = "14" ]] || false
}

@test "sql: stored procedures show and delete" {
    dolt sql <<SQL
CREATE PROCEDURE p1() SELECT 5*5;
CREATE PROCEDURE p2() SELECT 6*6;
SQL
    # We're excluding timestamps in these statements
    # Initial look
    run dolt sql -q "SELECT * FROM dolt_procedures" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "name,create_stmt,created_at,modified_at" ]] || false
    [[ "$output" =~ 'p1,CREATE PROCEDURE p1() SELECT 5*5' ]] || false
    [[ "$output" =~ 'p2,CREATE PROCEDURE p2() SELECT 6*6' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SHOW PROCEDURE STATUS" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "Db,Name,Type,Definer,Modified,Created,Security_type,Comment,character_set_client,collation_connection,Database Collation" ]] || false
    [[ "$output" =~ ',p1,PROCEDURE,' ]] || false
    [[ "$output" =~ ',p2,PROCEDURE,' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    # Drop p2
    dolt sql -q "DROP PROCEDURE p2"
    run dolt sql -q "SELECT * FROM dolt_procedures" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "name,create_stmt,created_at,modified_at" ]] || false
    [[ "$output" =~ 'p1,CREATE PROCEDURE p1() SELECT 5*5' ]] || false
    [[ ! "$output" =~ 'p2,CREATE PROCEDURE p2() SELECT 6*6' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SHOW PROCEDURE STATUS" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "Db,Name,Type,Definer,Modified,Created,Security_type,Comment,character_set_client,collation_connection,Database Collation" ]] || false
    [[ "$output" =~ ',p1,PROCEDURE,' ]] || false
    [[ ! "$output" =~ ',p2,PROCEDURE,' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    # Drop p2 again and error
    run dolt sql -q "DROP PROCEDURE p2"
    [ "$status" -eq "1" ]
    [[ "$output" =~ '"p2" does not exist' ]] || false
    # Drop p1 using if exists
    dolt sql -q "DROP PROCEDURE IF EXISTS p1"
    run dolt sql -q "SELECT * FROM dolt_procedures" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "name,create_stmt,created_at,modified_at" ]] || false
    [[ ! "$output" =~ 'p1,CREATE PROCEDURE p1() SELECT 5*5' ]] || false
    [[ ! "$output" =~ 'p2,CREATE PROCEDURE p2() SELECT 6*6' ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SHOW PROCEDURE STATUS" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "Db,Name,Type,Definer,Modified,Created,Security_type,Comment,character_set_client,collation_connection,Database Collation" ]] || false
    [[ ! "$output" =~ ',p1,PROCEDURE,' ]] || false
    [[ ! "$output" =~ ',p2,PROCEDURE,' ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
}

@test "sql: active_branch() func" {
    run dolt sql -q 'select active_branch()' -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "ACTIVE_BRANCH()" ]] || false
    [[ "$output" =~ "master" ]] || false
}

@test "sql: active_branch() func on feature branch" {
    run dolt branch tmp_br
    run dolt checkout tmp_br
    run dolt sql -q 'select active_branch()' -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "ACTIVE_BRANCH()" ]] || false
    [[ "$output" =~ "tmp_br" ]] || false
}
