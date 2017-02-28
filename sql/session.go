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

package sql

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/chrislusf/gleam/sql/ast"
	"github.com/chrislusf/gleam/sql/context"
	"github.com/chrislusf/gleam/sql/infoschema"
	"github.com/chrislusf/gleam/sql/mysql"
	"github.com/chrislusf/gleam/sql/parser"
	"github.com/chrislusf/gleam/sql/plan"
	"github.com/chrislusf/gleam/sql/resolver"
	"github.com/chrislusf/gleam/sql/sessionctx/variable"
	"github.com/chrislusf/gleam/sql/util/types"
	"github.com/juju/errors"
)

// Session context
type Session interface {
	context.Context
	Status() uint16 // Flag of current status, such as autocommit.
	String() string // For debug
	Close() error
}

var (
	_         Session = (*session)(nil)
	sessionMu sync.Mutex
)

type stmtRecord struct {
	stmtID uint32
	st     ast.Statement
	params []interface{}
}

type stmtHistory struct {
	history []*stmtRecord
}

func (h *stmtHistory) add(stmtID uint32, st ast.Statement, params ...interface{}) {
	s := &stmtRecord{
		stmtID: stmtID,
		st:     st,
		params: append(([]interface{})(nil), params...),
	}
	h.history = append(h.history, s)
}

type session struct {
	values      map[fmt.Stringer]interface{}
	parser      *parser.Parser
	sessionVars *variable.SessionVars
}

func (s *session) Status() uint16 {
	return s.sessionVars.Status
}

func (s *session) String() string {
	// TODO: how to print binded context in values appropriately?
	sessVars := s.sessionVars
	data := map[string]interface{}{
		"user":       sessVars.User,
		"currDBName": sessVars.CurrentDB,
		"stauts":     sessVars.Status,
		"strictMode": sessVars.StrictSQLMode,
	}
	b, _ := json.MarshalIndent(data, "", "  ")
	return string(b)
}

func (s *session) ParseSQL(sql, charset, collation string) ([]ast.StmtNode, error) {
	return s.parser.Parse(sql, charset, collation)
}

// checkArgs makes sure all the arguments' types are known and can be handled.
// integer types are converted to int64 and uint64, time.Time is converted to types.Time.
// time.Duration is converted to types.Duration, other known types are leaved as it is.
func checkArgs(args ...interface{}) error {
	for i, v := range args {
		switch x := v.(type) {
		case bool:
			if x {
				args[i] = int64(1)
			} else {
				args[i] = int64(0)
			}
		case int8:
			args[i] = int64(x)
		case int16:
			args[i] = int64(x)
		case int32:
			args[i] = int64(x)
		case int:
			args[i] = int64(x)
		case uint8:
			args[i] = uint64(x)
		case uint16:
			args[i] = uint64(x)
		case uint32:
			args[i] = uint64(x)
		case uint:
			args[i] = uint64(x)
		case int64:
		case uint64:
		case float32:
		case float64:
		case string:
		case []byte:
		case time.Duration:
			args[i] = types.Duration{Duration: x}
		case time.Time:
			args[i] = types.Time{Time: types.FromGoTime(x), Type: mysql.TypeDatetime}
		case nil:
		default:
			return errors.Errorf("cannot use arg[%d] (type %T):unsupported type", i, v)
		}
	}
	return nil
}

func (s *session) SetValue(key fmt.Stringer, value interface{}) {
	s.values[key] = value
}

func (s *session) Value(key fmt.Stringer) interface{} {
	value := s.values[key]
	return value
}

func (s *session) ClearValue(key fmt.Stringer) {
	delete(s.values, key)
}

// Close function does some clean work when session end.
func (s *session) Close() error {
	return nil
}

// GetSessionVars implements the context.Context interface.
func (s *session) GetSessionVars() *variable.SessionVars {
	return s.sessionVars
}

// Some vars name for debug.
const (
	retryEmptyHistoryList = "RetryEmptyHistoryList"
)

// CreateSession creates a new session environment.
func CreateSession(info infoschema.InfoSchema) (Session, error) {
	s, err := createSession(info)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return s, nil
}

func createSession(info infoschema.InfoSchema) (*session, error) {
	s := &session{
		values:      make(map[fmt.Stringer]interface{}),
		parser:      parser.New(),
		sessionVars: variable.NewSessionVars(),
	}
	s.sessionVars.TxnCtx.InfoSchema = info

	return s, nil
}

// Compile is safe for concurrent use by multiple goroutines.
func Compile(ctx context.Context, rawStmt ast.StmtNode) (plan.Plan, error) {
	info := ctx.GetSessionVars().TxnCtx.InfoSchema.(infoschema.InfoSchema)

	node := rawStmt
	err := resolver.ResolveName(node, info, ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	p, err := plan.Optimize(ctx, node, info)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return p, nil
}

// runStmt executes the ast.Statement and commit or rollback the current transaction.
func runStmt(ctx context.Context, s ast.Statement) (ast.RecordSet, error) {
	var err error
	var rs ast.RecordSet
	rs, err = s.Exec(ctx)
	return rs, errors.Trace(err)
}
