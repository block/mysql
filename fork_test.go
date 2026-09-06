// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"database/sql"
	rtdebug "runtime/debug" // aliased: const.go declares a package-level `debug`
	"slices"
	"testing"
)

// forkModulePath and forkDriverName are the two values that make this a fork
// rather than a vendored copy. See README.md.
const (
	forkModulePath = "github.com/block/mysql"
	forkDriverName = "block-mysql"
)

// TestForkIdentity pins the module path and the registered driver name.
//
// Neither is observable from any other test: no file in the package imports its
// own module path, and every test opens `driverNameTest`, which defaults to
// whatever `driverName` holds — so reverting either value builds clean and
// passes the suite. That matters because these are exactly the two lines the
// documented `git merge upstream/master` workflow puts a conflict on every
// time, and reverting either is silent: the module path reintroduces the
// substitutability problem the fork exists to prevent (`replace` is not
// inherited across module boundaries), and the driver name reintroduces the
// duplicate-`sql.Register` panic for consumers who link both drivers.
func TestForkIdentity(t *testing.T) {
	// Main.Path reads the real go.mod of the module under test, not a copy of
	// the string kept somewhere in the package.
	bi, ok := rtdebug.ReadBuildInfo()
	if !ok {
		t.Fatal("runtime/debug.ReadBuildInfo() failed; cannot verify the module path")
	}
	if bi.Main.Path != forkModulePath {
		t.Errorf("module path is %q, want %q — a fork of go-sql-driver/mysql needs its own module path, "+
			"or downstream modules silently link upstream instead (a `replace` is main-module-only)",
			bi.Main.Path, forkModulePath)
	}

	if !slices.Contains(sql.Drivers(), forkDriverName) {
		t.Errorf("registered drivers are %q, want one named %q — under upstream's name, any binary that also "+
			"links go-sql-driver panics in init on the duplicate sql.Register",
			sql.Drivers(), forkDriverName)
	}
}
