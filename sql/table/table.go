// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package table

import (
	"github.com/chrislusf/gleam/sql/model"
	"github.com/chrislusf/gleam/sql/terror"
)

var (
	// errNoDefaultValue is used when insert a row, the column value is not given, and the column has not null flag
	// and it doesn't have a default value.
	errNoDefaultValue  = terror.ClassTable.New(codeNoDefaultValue, "field doesn't have a default value")
	errColumnCantNull  = terror.ClassTable.New(codeColumnCantNull, "column can not be null")
	errUnknownColumn   = terror.ClassTable.New(codeUnknownColumn, "unknown column")
	errDuplicateColumn = terror.ClassTable.New(codeDuplicateColumn, "duplicate column")

	errGetDefaultFailed = terror.ClassTable.New(codeGetDefaultFailed, "get default value fail")

	// ErrIndexOutBound returns for index column offset out of bound.
	ErrIndexOutBound = terror.ClassTable.New(codeIndexOutBound, "index column offset out of bound")
	// ErrUnsupportedOp returns for unsupported operation.
	ErrUnsupportedOp = terror.ClassTable.New(codeUnsupportedOp, "operation not supported")
	// ErrRowNotFound returns for row not found.
	ErrRowNotFound = terror.ClassTable.New(codeRowNotFound, "can not find the row")
	// ErrTableStateCantNone returns for table none state.
	ErrTableStateCantNone = terror.ClassTable.New(codeTableStateCantNone, "table can not be in none state")
	// ErrColumnStateCantNone returns for column none state.
	ErrColumnStateCantNone = terror.ClassTable.New(codeColumnStateCantNone, "column can not be in none state")
	// ErrColumnStateNonPublic returns for column non-public state.
	ErrColumnStateNonPublic = terror.ClassTable.New(codeColumnStateNonPublic, "can not use non-public column")
	// ErrIndexStateCantNone returns for index none state.
	ErrIndexStateCantNone = terror.ClassTable.New(codeIndexStateCantNone, "index can not be in none state")
	// ErrInvalidRecordKey returns for invalid record key.
	ErrInvalidRecordKey = terror.ClassTable.New(codeInvalidRecordKey, "invalid record key")
)

// Table is used to retrieve and modify rows in table.
type Table interface {

	// Meta returns TableInfo.
	Meta() *model.TableInfo
}

// MockTableFromMeta only serves for test.
var MockTableFromMeta func(tableInfo *model.TableInfo) Table

// Table error codes.
const (
	codeGetDefaultFailed     = 1
	codeIndexOutBound        = 2
	codeUnsupportedOp        = 3
	codeRowNotFound          = 4
	codeTableStateCantNone   = 5
	codeColumnStateCantNone  = 6
	codeColumnStateNonPublic = 7
	codeIndexStateCantNone   = 8
	codeInvalidRecordKey     = 9

	codeColumnCantNull  = 1048
	codeUnknownColumn   = 1054
	codeDuplicateColumn = 1110
	codeNoDefaultValue  = 1364
)
