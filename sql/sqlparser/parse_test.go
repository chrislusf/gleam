// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGen(t *testing.T) {
	_, err := Parse("select :a from a where a in (:b)")
	if err != nil {
		t.Error(err)
	}
}

func TestParse(t *testing.T) {
	sql := "select a from (select * from table1 where table1.a = 'tom') as t1, table2, table3 as t3, table4 left join table5 where t1.k = '1'"
	_, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
}

func TestParseInsert(t *testing.T) {
	sql := "INSERT INTO t3 VALUES (8, 10, 'baz')"
	_, err := Parse(sql)
	assert.Nil(t, err)
}

func TestCreatTable1(t *testing.T) {
	sql := `create table t1 (
	ID int primary key,
	LastName varchar(255),
	FirstName varchar(255)
)`
	tree, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	s := String(tree)

	assert.Equal(t, sql, s)
}

func TestCreatTable2(t *testing.T) {
	sql := `create table t1 (
	ID int primary key not null auto_increment,
	LastName varchar(255),
	FirstName varchar(255)
)`
	tree, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	s := String(tree)

	assert.Equal(t, sql, s)
}

func TestCreatTable3(t *testing.T) {
	sql := `create table t1 (
	ID int unique key not null auto_increment,
	LastName varchar(255),
	FirstName varchar(255)
)`
	tree, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	s := String(tree)

	assert.Equal(t, sql, s)
}

func TestCreatTable4(t *testing.T) {
	for_precision := []string{"real", "double", "float", "decimal", "numeric"}
	for _, p := range for_precision {
		precision := "(32, 8)"
		for i := 0; i < 2; i++ {
			data_type := p
			if i == 0 {
				data_type += precision
			}
			sql := fmt.Sprintf(`create table t1 (
	ID int unique key not null auto_increment,
	LastName varchar(255),
	FirstName varchar(255),
	Balance %s%s
)`, p, precision)
			tree, err := Parse(sql)
			assert.Nil(t, err)
			s := String(tree)

			assert.Equal(t, sql, s)
		}
	}

	for_length := []string{"bit", "tinyint", "smallint", "mediumint", "int", "integer", "bigint", "decimal", "numeric"}
	for _, p := range for_length {
		length := "(32)"
		for i := 0; i < 2; i++ {
			data_type := p
			if i == 0 {
				data_type += length
			}
			sql := fmt.Sprintf(`create table t1 (
	ID int unique key not null auto_increment,
	LastName varchar(255),
	FirstName varchar(255),
	Balance %s%s
)`, p, length)
			tree, err := Parse(sql)
			assert.Nil(t, err)
			s := String(tree)

			assert.Equal(t, sql, s)
		}
	}
}

func TestCreatTable5(t *testing.T) {
	for_time := []string{"date", "time", "timestamp", "datetime", "year"}
	for _, time := range for_time {
		sql := fmt.Sprintf(`create table t1 (
	ID int unique key not null auto_increment,
	LastName varchar(255),
	FirstName varchar(255),
	LastUpdated %s
)`, time)
		tree, err := Parse(sql)
		assert.Nil(t, err, "fail to parse:\n%s", sql)
		s := String(tree)
		assert.Equal(t, sql, s)
	}
}

func BenchmarkParse1(b *testing.B) {
	sql := "select 'abcd', 20, 30.0, eid from a where 1=eid and name='3'"
	for i := 0; i < b.N; i++ {
		_, err := Parse(sql)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParse2(b *testing.B) {
	sql := "select aaaa, bbb, ccc, ddd, eeee, ffff, gggg, hhhh, iiii from tttt, ttt1, ttt3 where aaaa = bbbb and bbbb = cccc and dddd+1 = eeee group by fff, gggg having hhhh = iiii and iiii = jjjj order by kkkk, llll limit 3, 4"
	for i := 0; i < b.N; i++ {
		_, err := Parse(sql)
		if err != nil {
			b.Fatal(err)
		}
	}
}

type testCase struct {
	file   string
	lineno int
	input  string
	output string
}
