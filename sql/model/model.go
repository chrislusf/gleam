// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import (
	"strings"

	"github.com/chrislusf/gleam/sql/util/types"
)

// ColumnInfo provides meta data describing of a table column.
type ColumnInfo struct {
	Name            CIStr       `json:"name"`
	Offset          int         `json:"offset"`
	DefaultValue    interface{} `json:"default"`
	types.FieldType `json:"type"`
	Comment         string `json:"comment"`
}

// Clone clones ColumnInfo.
func (c *ColumnInfo) Clone() *ColumnInfo {
	nc := *c
	return &nc
}

// TableInfo provides meta data describing a DB table.
type TableInfo struct {
	Name    CIStr         `json:"name"`
	Columns []*ColumnInfo `json:"cols"` // Columns are listed in the order in which they appear in the schema.
	Indices []*IndexInfo  `json:"index_info"`
	Comment string        `json:"comment"`
}

// Clone clones TableInfo.
func (t *TableInfo) Clone() *TableInfo {
	nt := *t
	nt.Columns = make([]*ColumnInfo, len(t.Columns))
	nt.Indices = make([]*IndexInfo, len(t.Indices))

	for i := range t.Columns {
		nt.Columns[i] = t.Columns[i].Clone()
	}

	for i := range t.Indices {
		nt.Indices[i] = t.Indices[i].Clone()
	}

	return &nt
}

// IndexColumn provides index column info.
type IndexColumn struct {
	Name CIStr `json:"name"` // Index name
}

// Clone clones IndexColumn.
func (i *IndexColumn) Clone() *IndexColumn {
	ni := *i
	return &ni
}

// IndexType is the type of index
type IndexType int

// String implements Stringer interface.
func (t IndexType) String() string {
	switch t {
	case IndexTypeBtree:
		return "BTREE"
	case IndexTypeHash:
		return "HASH"
	}
	return ""
}

// IndexTypes
const (
	IndexTypeBtree IndexType = iota + 1
	IndexTypeHash
)

// IndexInfo provides meta data describing a DB index.
// It corresponds to the statement `CREATE INDEX Name ON Table (Column);`
// See https://dev.mysql.com/doc/refman/5.7/en/create-index.html
type IndexInfo struct {
	Name    CIStr          `json:"idx_name"`   // Index name.
	Table   CIStr          `json:"tbl_name"`   // Table name.
	Columns []*IndexColumn `json:"idx_cols"`   // Index columns.
	Unique  bool           `json:"is_unique"`  // Whether the index is unique.
	Primary bool           `json:"is_primary"` // Whether the index is primary key.
	Comment string         `json:"comment"`    // Comment
	Tp      IndexType      `json:"index_type"` // Index type: Btree or Hash
}

// Clone clones IndexInfo.
func (index *IndexInfo) Clone() *IndexInfo {
	ni := *index
	ni.Columns = make([]*IndexColumn, len(index.Columns))
	for i := range index.Columns {
		ni.Columns[i] = index.Columns[i].Clone()
	}
	return &ni
}

// DBInfo provides meta data describing a DB.
type DBInfo struct {
	Name   CIStr        `json:"db_name"` // DB name.
	Tables []*TableInfo `json:"-"`       // Tables in the DB.
}

// Clone clones DBInfo.
func (db *DBInfo) Clone() *DBInfo {
	newInfo := *db
	newInfo.Tables = make([]*TableInfo, len(db.Tables))
	for i := range db.Tables {
		newInfo.Tables[i] = db.Tables[i].Clone()
	}
	return &newInfo
}

// CIStr is case insensitive string.
type CIStr struct {
	O string `json:"O"` // Original string.
	L string `json:"L"` // Lower case string.
}

// String implements fmt.Stringer interface.
func (cis CIStr) String() string {
	return cis.O
}

// NewCIStr creates a new CIStr.
func NewCIStr(s string) (cs CIStr) {
	cs.O = s
	cs.L = strings.ToLower(s)
	return
}
