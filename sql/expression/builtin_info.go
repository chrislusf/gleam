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

package expression

import (
	"github.com/chrislusf/gleam/sql/context"
	"github.com/chrislusf/gleam/sql/mysql"
	"github.com/chrislusf/gleam/sql/util/types"
	"github.com/juju/errors"
)

var (
	_ functionClass = &databaseFunctionClass{}
	_ functionClass = &foundRowsFunctionClass{}
	_ functionClass = &currentUserFunctionClass{}
	_ functionClass = &userFunctionClass{}
	_ functionClass = &versionFunctionClass{}
)

var (
	_ builtinFunc = &builtinDatabaseSig{}
	_ builtinFunc = &builtinFoundRowsSig{}
	_ builtinFunc = &builtinCurrentUserSig{}
	_ builtinFunc = &builtinUserSig{}
	_ builtinFunc = &builtinVersionSig{}
)

type databaseFunctionClass struct {
	baseFunctionClass
}

func (c *databaseFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := errors.Trace(c.verifyArgs(args)); err != nil {
		return nil, errors.Trace(err)
	}
	bt := &builtinDatabaseSig{newBaseBuiltinFunc(args, ctx)}
	bt.deterministic = false
	return bt, nil
}

type builtinDatabaseSig struct {
	baseBuiltinFunc
}

func (b *builtinDatabaseSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinDatabase(args, b.ctx)
}

// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html
func builtinDatabase(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	currentDB := ctx.GetSessionVars().CurrentDB
	if currentDB == "" {
		return d, nil
	}
	d.SetString(currentDB)
	return d, nil
}

type foundRowsFunctionClass struct {
	baseFunctionClass
}

func (c *foundRowsFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := errors.Trace(c.verifyArgs(args)); err != nil {
		return nil, errors.Trace(err)
	}
	bt := &builtinFoundRowsSig{newBaseBuiltinFunc(args, ctx)}
	bt.deterministic = false
	return bt, nil
}

type builtinFoundRowsSig struct {
	baseBuiltinFunc
}

func (b *builtinFoundRowsSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinFoundRows(args, b.ctx)
}

func builtinFoundRows(arg []types.Datum, ctx context.Context) (d types.Datum, err error) {
	data := ctx.GetSessionVars()
	if data == nil {
		return d, errors.Errorf("Missing session variable when evalue builtin")
	}

	d.SetUint64(data.StmtCtx.FoundRows())
	return d, nil
}

type currentUserFunctionClass struct {
	baseFunctionClass
}

func (c *currentUserFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := errors.Trace(c.verifyArgs(args)); err != nil {
		return nil, errors.Trace(err)
	}
	bt := &builtinCurrentUserSig{newBaseBuiltinFunc(args, ctx)}
	bt.deterministic = false
	return bt, nil
}

type builtinCurrentUserSig struct {
	baseBuiltinFunc
}

func (b *builtinCurrentUserSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinCurrentUser(args, b.ctx)
}

// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_current-user
// TODO: The value of CURRENT_USER() can differ from the value of USER(). We will finish this after we support grant tables.
func builtinCurrentUser(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	data := ctx.GetSessionVars()
	if data == nil {
		return d, errors.Errorf("Missing session variable when evalue builtin")
	}

	d.SetString(data.User)
	return d, nil
}

type userFunctionClass struct {
	baseFunctionClass
}

func (c *userFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := errors.Trace(c.verifyArgs(args)); err != nil {
		return nil, errors.Trace(err)
	}
	bt := &builtinUserSig{newBaseBuiltinFunc(args, ctx)}
	bt.deterministic = false
	return bt, nil
}

type builtinUserSig struct {
	baseBuiltinFunc
}

func (b *builtinUserSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinUser(args, b.ctx)
}

func builtinUser(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	data := ctx.GetSessionVars()
	if data == nil {
		return d, errors.Errorf("Missing session variable when evalue builtin")
	}

	d.SetString(data.User)
	return d, nil
}

type versionFunctionClass struct {
	baseFunctionClass
}

func (c *versionFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := errors.Trace(c.verifyArgs(args)); err != nil {
		return nil, errors.Trace(err)
	}
	bt := &builtinVersionSig{newBaseBuiltinFunc(args, ctx)}
	bt.deterministic = false
	return bt, nil
}

type builtinVersionSig struct {
	baseBuiltinFunc
}

func (b *builtinVersionSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinVersion(args, b.ctx)
}

func builtinVersion(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	d.SetString(mysql.ServerVersion)
	return d, nil
}
