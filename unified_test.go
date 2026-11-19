// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
)

// unifiedExec runs query through QueryResultContext on a raw connection and
// returns either the fully-read resultset (columns + rows, cells copied) or
// the OK-response counters.
type unifiedResponse struct {
	columns      []string
	scanTypes    []reflect.Type
	rows         [][][]byte // nil cell = NULL
	isResultset  bool
	rowsAffected int64
	lastInsertID int64
}

func unifiedExec(ctx context.Context, dbt *DBTest, query string, args []driver.NamedValue) (*unifiedResponse, error) {
	conn, err := dbt.db.Conn(ctx)
	if err != nil {
		dbt.Fatalf("getting conn: %s", err)
	}
	defer conn.Close()
	return unifiedExecOn(ctx, dbt, conn, nil, query, args)
}

// unifiedExecOn is unifiedExec on conn, with the prelude statements run first
// through QueryResultContext.
func unifiedExecOn(ctx context.Context, dbt *DBTest, conn *sql.Conn, statements []string, query string, args []driver.NamedValue) (*unifiedResponse, error) {
	var prelude *Prelude
	if statements != nil {
		var err error
		if prelude, err = NewPrelude(statements...); err != nil {
			dbt.Fatalf("NewPrelude(%q): %s", statements, err)
		}
	}
	resp := &unifiedResponse{}
	rawErr := conn.Raw(func(dc any) error {
		mc, ok := dc.(*mysqlConn)
		if !ok {
			dbt.Fatalf("driver conn is %T, not *mysqlConn", dc)
		}
		var rows driver.Rows
		var result driver.Result
		var err error
		rows, result, err = mc.QueryResultContext(ctx, prelude, query, args)
		if err != nil {
			return err
		}
		if rows == nil {
			resp.rowsAffected, _ = result.RowsAffected()
			resp.lastInsertID, _ = result.LastInsertId()
			return nil
		}
		defer rows.Close()
		resp.isResultset = true
		resp.columns = rows.Columns()
		if st, ok := rows.(driver.RowsColumnTypeScanType); ok {
			resp.scanTypes = make([]reflect.Type, len(resp.columns))
			for i := range resp.scanTypes {
				resp.scanTypes[i] = st.ColumnTypeScanType(i)
			}
		}
		dest := make([]driver.Value, len(resp.columns))
		for {
			if err := rows.Next(dest); err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}
			row := make([][]byte, len(dest))
			for i, v := range dest {
				if v == nil {
					continue
				}
				b, ok := v.([]byte)
				if !ok {
					dbt.Fatalf("unified cell %d is %T, want []byte (wire pass-through)", i, v)
				}
				row[i] = bytes.Clone(b)
			}
			resp.rows = append(resp.rows, row)
		}
	})
	if rawErr != nil {
		return nil, rawErr
	}
	return resp, nil
}

func TestQueryResultContext(t *testing.T) {
	runTestsParallel(t, dsn, func(dbt *DBTest, tbl string) {
		ctx := context.Background()
		dbt.mustExec("CREATE TABLE " + tbl + " (id INT PRIMARY KEY AUTO_INCREMENT, dt DATETIME, note VARCHAR(16))")

		// OK response: rows must be nil, counters populated.
		resp, err := unifiedExec(ctx, dbt, "INSERT INTO "+tbl+" (dt, note) VALUES ('2026-08-01 12:00:00', 'a'), (NULL, 'b')", nil)
		if err != nil {
			dbt.Fatalf("insert: %s", err)
		}
		if resp.isResultset {
			dbt.Fatal("INSERT returned a resultset")
		}
		if resp.rowsAffected != 2 || resp.lastInsertID != 1 {
			dbt.Fatalf("INSERT counters: affected=%d insertID=%d", resp.rowsAffected, resp.lastInsertID)
		}

		// Resultset: cells arrive as raw wire text, NULL as nil.
		resp, err = unifiedExec(ctx, dbt, "SELECT id, dt, note FROM "+tbl+" ORDER BY id", nil)
		if err != nil {
			dbt.Fatalf("select: %s", err)
		}
		if !resp.isResultset || len(resp.columns) != 3 || len(resp.rows) != 2 {
			dbt.Fatalf("SELECT shape: resultset=%v cols=%v rows=%d", resp.isResultset, resp.columns, len(resp.rows))
		}
		if got := string(resp.rows[0][0]); got != "1" {
			dbt.Errorf("id wire text: %q", got)
		}
		if got := string(resp.rows[0][1]); got != "2026-08-01 12:00:00" {
			dbt.Errorf("datetime wire text: %q", got)
		}
		if resp.rows[1][1] != nil {
			dbt.Errorf("NULL cell: %q", resp.rows[1][1])
		}

		// ColumnTypeScanType must describe what Next actually delivers on
		// this path — []byte for every column, whatever its MySQL type —
		// rather than the Go type the parsing path would have produced.
		for i, st := range resp.scanTypes {
			if st != scanTypeBytes {
				dbt.Errorf("scan type for column %q: %v, want []uint8", resp.columns[i], st)
			}
		}

		// Empty resultset is still a resultset, not an OK response.
		resp, err = unifiedExec(ctx, dbt, "SELECT id FROM "+tbl+" WHERE 1 = 0", nil)
		if err != nil {
			dbt.Fatalf("empty select: %s", err)
		}
		if !resp.isResultset || len(resp.rows) != 0 {
			dbt.Fatalf("empty SELECT shape: resultset=%v rows=%d", resp.isResultset, len(resp.rows))
		}

		// A statement callers routinely misclassify: CHECK TABLE answers with
		// a resultset even though it reads like an admin/modify statement.
		resp, err = unifiedExec(ctx, dbt, "CHECK TABLE "+tbl, nil)
		if err != nil {
			dbt.Fatalf("check table: %s", err)
		}
		if !resp.isResultset || len(resp.rows) != 1 {
			dbt.Fatalf("CHECK TABLE shape: resultset=%v rows=%d", resp.isResultset, len(resp.rows))
		}
		if got := string(resp.rows[0][3]); got != "OK" {
			dbt.Errorf("CHECK TABLE msg: %q", got)
		}

		// Args require InterpolateParams (pre-write driver.ErrSkip without
		// it), exactly like QueryContext. The harness runs this test under
		// both DSN variants, so accept either outcome — but each must be
		// exact.
		args := []driver.NamedValue{{Ordinal: 1, Value: int64(1)}}
		resp, err = unifiedExec(ctx, dbt, "SELECT note FROM "+tbl+" WHERE id = ?", args)
		if err != nil {
			if !errors.Is(err, driver.ErrSkip) {
				dbt.Fatalf("args select: %s", err)
			}
		} else if len(resp.rows) != 1 || string(resp.rows[0][0]) != "a" {
			dbt.Fatalf("args SELECT rows: %v", resp.rows)
		}

		// Named parameters are unsupported, exactly as on QueryContext. This
		// is checked before the InterpolateParams gate, so the outcome is the
		// same under both DSN variants and must not be a silent positional
		// bind.
		named := []driver.NamedValue{{Name: "id", Ordinal: 1, Value: int64(1)}}
		if _, err = unifiedExec(ctx, dbt, "SELECT note FROM "+tbl+" WHERE id = ?", named); err == nil {
			dbt.Fatal("named parameter accepted")
		} else if !strings.Contains(err.Error(), "Named Parameters") {
			dbt.Fatalf("named parameter error: %s", err)
		}

		// Errors surface normally and leave the connection reusable.
		if _, err = unifiedExec(ctx, dbt, "SELECT syntax error from", nil); err == nil {
			dbt.Fatal("syntax error did not surface")
		}
		var myErr *MySQLError
		if _, err = unifiedExec(ctx, dbt, "SELECT * FROM does_not_exist_"+tbl, nil); !errors.As(err, &myErr) {
			dbt.Fatalf("missing table error: %v", err)
		}
		if resp, err = unifiedExec(ctx, dbt, "SELECT COUNT(*) FROM "+tbl, nil); err != nil || string(resp.rows[0][0]) != "2" {
			dbt.Fatalf("conn not reusable after errors: %v %v", resp, err)
		}
	})
}

func TestQueryResultContextPrelude(t *testing.T) {
	// Run with and without InterpolateParams, so the args case is exercised
	// both ways.
	for name, params := range map[string]string{"multiStatements": "&multiStatements=true", "multiStatementsInterpolateParams": "&multiStatements=true&interpolateParams=true"} {
		t.Run(name, func(t *testing.T) { testQueryResultContextPrelude(t, dsn+params) })
	}
}

func testQueryResultContextPrelude(t *testing.T, dsn string) {
	runTestsParallel(t, dsn, func(dbt *DBTest, tbl string) {
		ctx := context.Background()
		dbt.mustExec("CREATE TABLE " + tbl + " (id INT PRIMARY KEY AUTO_INCREMENT, note VARCHAR(16))")
		conn, err := dbt.db.Conn(ctx)
		if err != nil {
			dbt.Fatalf("getting conn: %s", err)
		}
		defer conn.Close()
		count := func() string {
			resp, err := unifiedExecOn(ctx, dbt, conn, nil, "SELECT COUNT(*) FROM "+tbl, nil)
			if err != nil {
				dbt.Fatalf("count: %s", err)
			}
			return string(resp.rows[0][0])
		}

		// The query runs inside the transaction the prelude began, and the
		// counters are the query's, not BEGIN's.
		resp, err := unifiedExecOn(ctx, dbt, conn, []string{"BEGIN"}, "INSERT INTO "+tbl+" (note) VALUES ('a')", nil)
		if err != nil {
			dbt.Fatalf("BEGIN + INSERT: %s", err)
		}
		if resp.isResultset || resp.rowsAffected != 1 || resp.lastInsertID != 1 {
			dbt.Fatalf("BEGIN + INSERT response: resultset=%v affected=%d insertID=%d", resp.isResultset, resp.rowsAffected, resp.lastInsertID)
		}
		if _, err := conn.ExecContext(ctx, "ROLLBACK"); err != nil {
			dbt.Fatalf("rollback: %s", err)
		}
		if got := count(); got != "0" {
			dbt.Fatalf("rows after ROLLBACK: %s, want 0 (the INSERT ran outside the transaction)", got)
		}

		// A prelude statement's resultset is discarded; the query's is
		// returned. Args are interpolated into the query only, so a '?' in
		// the prelude is left alone.
		if _, err := conn.ExecContext(ctx, "INSERT INTO "+tbl+" (note) VALUES ('b')"); err != nil {
			dbt.Fatalf("insert: %s", err)
		}
		args := []driver.NamedValue{{Ordinal: 1, Value: "b"}}
		resp, err = unifiedExecOn(ctx, dbt, conn, []string{"SELECT 1", "SET @prelude = '?'"}, "SELECT note, @prelude FROM "+tbl+" WHERE note = ?", args)
		if err != nil {
			if !errors.Is(err, driver.ErrSkip) {
				dbt.Fatalf("prelude resultset + args: %s", err)
			}
		} else if !resp.isResultset || len(resp.rows) != 1 || string(resp.rows[0][0]) != "b" || string(resp.rows[0][1]) != "?" {
			dbt.Fatalf("prelude resultset + args rows: %q", resp.rows)
		}

		// A failed prelude statement stops the command: the query does not
		// run, the error says so, and the connection stays usable.
		_, err = unifiedExecOn(ctx, dbt, conn, []string{"SELECT 1", "SELECT * FROM does_not_exist_" + tbl}, "INSERT INTO "+tbl+" (note) VALUES ('c')", nil)
		var preludeErr *PreludeError
		var myErr *MySQLError
		if !errors.As(err, &preludeErr) || preludeErr.Index != 1 || !errors.As(err, &myErr) || myErr.Number != 1146 {
			dbt.Fatalf("failed prelude error: %v", err)
		}
		if got := count(); got != "1" {
			dbt.Fatalf("rows after a failed prelude: %s, want 1 (the query ran)", got)
		}

		// A failure of the query itself is the query's error, not a
		// PreludeError, and the transaction the prelude began stays open.
		_, err = unifiedExecOn(ctx, dbt, conn, []string{"BEGIN"}, "SELECT * FROM does_not_exist_"+tbl, nil)
		if errors.As(err, &preludeErr) || !errors.As(err, &myErr) || myErr.Number != 1146 {
			dbt.Fatalf("query error: %v", err)
		}
		if _, err := conn.ExecContext(ctx, "INSERT INTO "+tbl+" (note) VALUES ('d')"); err != nil {
			dbt.Fatalf("insert: %s", err)
		}
		if _, err := conn.ExecContext(ctx, "ROLLBACK"); err != nil {
			dbt.Fatalf("rollback: %s", err)
		}
		if got := count(); got != "1" {
			dbt.Fatalf("rows after ROLLBACK: %s, want 1 (the failed query ended the transaction)", got)
		}

		// A trailing line comment in a prelude entry ends at its line: it
		// cannot swallow the statements after it.
		resp, err = unifiedExecOn(ctx, dbt, conn, []string{"BEGIN -- open the transaction", "SET @commented = 7 # set it"}, "SELECT @commented", nil)
		if err != nil || !resp.isResultset || string(resp.rows[0][0]) != "7" {
			dbt.Fatalf("prelude with trailing comments: %v %v", resp, err)
		}
		if _, err := conn.ExecContext(ctx, "ROLLBACK"); err != nil {
			dbt.Fatalf("rollback: %s", err)
		}

		// Args are escaped for backslash escapes, which a prelude that sets
		// sql_mode leaves on.
		tricky := []driver.NamedValue{{Ordinal: 1, Value: `it's a \ back\'slash`}}
		resp, err = unifiedExecOn(ctx, dbt, conn, []string{"SET SESSION sql_mode = 'TRADITIONAL'"}, "SELECT ?", tricky)
		if _, resetErr := conn.ExecContext(ctx, "SET SESSION sql_mode = DEFAULT"); resetErr != nil {
			dbt.Fatalf("reset sql_mode: %s", resetErr)
		}
		if err != nil {
			if !errors.Is(err, driver.ErrSkip) {
				dbt.Fatalf("arg after sql_mode SET: %s", err)
			}
		} else if string(resp.rows[0][0]) != tricky[0].Value {
			dbt.Fatalf("arg after sql_mode SET: %q, want %q", resp.rows[0][0], tricky[0].Value)
		}

		// On a connection in NO_BACKSLASH_ESCAPES mode, where a prelude
		// could turn the mode off before the query is read, a prelude with
		// args is declined before anything is written. Without args it runs.
		if _, err := conn.ExecContext(ctx, "SET SESSION sql_mode = 'NO_BACKSLASH_ESCAPES'"); err != nil {
			dbt.Fatalf("set sql_mode: %s", err)
		}
		_, skipErr := unifiedExecOn(ctx, dbt, conn, []string{"SET SESSION sql_mode = 'TRADITIONAL'"}, "SELECT ?", tricky)
		resp, err = unifiedExecOn(ctx, dbt, conn, []string{"SELECT 1"}, "SELECT @@session.sql_mode", nil)
		if _, resetErr := conn.ExecContext(ctx, "SET SESSION sql_mode = DEFAULT"); resetErr != nil {
			dbt.Fatalf("reset sql_mode: %s", resetErr)
		}
		if !errors.Is(skipErr, driver.ErrSkip) {
			dbt.Fatalf("args after a prelude in NO_BACKSLASH_ESCAPES mode: %v, want driver.ErrSkip", skipErr)
		}
		if err != nil || string(resp.rows[0][0]) != "NO_BACKSLASH_ESCAPES" {
			dbt.Fatalf("no-args prelude in NO_BACKSLASH_ESCAPES mode: %v %v", resp, err)
		}

		// The warning count describes the query alone, even when the query
		// fails after a prelude statement that warned.
		if err := conn.Raw(func(dc any) error {
			mc := dc.(*mysqlConn)
			prelude, err := NewPrelude("SELECT CAST('x' AS SIGNED)")
			if err != nil {
				dbt.Fatalf("NewPrelude: %s", err)
			}
			_, _, err = mc.QueryResultContext(ctx, prelude, "SELECT * FROM does_not_exist_"+tbl, nil)
			var myErr *MySQLError
			if !errors.As(err, &myErr) || myErr.Number != 1146 {
				dbt.Fatalf("query error after warning prelude: %v", err)
			}
			if w := mc.Warnings(); w != 0 {
				dbt.Fatalf("warnings after a failed query: %d, want 0 (the prelude's)", w)
			}
			return nil
		}); err != nil {
			dbt.Fatalf("raw: %s", err)
		}

		// An empty prelude is QueryResultContext.
		resp, err = unifiedExecOn(ctx, dbt, conn, []string{}, "SELECT COUNT(*) FROM "+tbl, nil)
		if err != nil || string(resp.rows[0][0]) != "1" {
			dbt.Fatalf("empty prelude: %v %v", resp, err)
		}
	})
}

func TestQueryResultContextPreludeNeedsMultiStatements(t *testing.T) {
	runTestsParallel(t, dsn, func(dbt *DBTest, tbl string) {
		ctx := context.Background()
		dbt.mustExec("CREATE TABLE " + tbl + " (id INT PRIMARY KEY AUTO_INCREMENT)")
		conn, err := dbt.db.Conn(ctx)
		if err != nil {
			dbt.Fatalf("getting conn: %s", err)
		}
		defer conn.Close()

		// Without MultiStatements nothing is written: the caller falls back.
		if _, err := unifiedExecOn(ctx, dbt, conn, []string{"BEGIN"}, "INSERT INTO "+tbl+" () VALUES ()", nil); !errors.Is(err, driver.ErrSkip) {
			dbt.Fatalf("without MultiStatements: %v, want driver.ErrSkip", err)
		}
		resp, err := unifiedExecOn(ctx, dbt, conn, nil, "SELECT COUNT(*) FROM "+tbl, nil)
		if err != nil || string(resp.rows[0][0]) != "0" {
			dbt.Fatalf("after ErrSkip: %v %v", resp, err)
		}
	})
}

func TestNewPrelude(t *testing.T) {
	for _, tt := range []struct {
		statement string
		accepted  bool
	}{
		{statement: "BEGIN", accepted: true},
		{statement: "  begin work", accepted: true},
		{statement: "/* why */ BEGIN", accepted: true},
		{statement: "-- why\nBEGIN", accepted: true},
		{statement: "# why\nSTART TRANSACTION READ ONLY", accepted: true},
		{statement: "BEGIN -- trailing", accepted: true},
		{statement: "BEGIN --\x01 trailing", accepted: true},
		{statement: "SET @x = 1 --\x7f why\n, @y = 2", accepted: true},
		{statement: "SAVEPOINT s1", accepted: true},
		{statement: "RELEASE SAVEPOINT s1", accepted: true},
		{statement: "ROLLBACK TO SAVEPOINT s1", accepted: true},
		{statement: "COMMIT", accepted: true},
		{statement: "SELECT 1, 'a;b', `c;d`, \"e;f\"", accepted: true},
		{statement: "SET @x = 'it''s', @y := 2", accepted: true},
		{statement: "SET innodb_lock_wait_timeout = 30, time_zone = '+00:00'", accepted: true},
		{statement: "SET TRANSACTION ISOLATION LEVEL READ COMMITTED", accepted: true},
		{statement: "SET @x = CONCAT('a', 'b'), max_execution_time = 10", accepted: true},
		{statement: "SET sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_DATE'", accepted: true},
		{statement: "SET innodb_lock_wait_timeout = 30, sql_mode = 'TRADITIONAL'", accepted: true},
		{statement: "SET @@session.sql_mode = ''", accepted: true},
		{statement: "SET SESSION sql_mode = \"ANSI\"", accepted: true},
		{statement: "set local `SQL_MODE` = TRADITIONAL", accepted: true},
		{statement: "SET @sql_mode = 'NO_BACKSLASH_ESCAPES'", accepted: true},
		// A byte at or above 0x80 is fine inside a string literal: its
		// quotes are below 0x40, so they are never half of a character.
		{statement: "SELECT 'caf\xc3\xa9', \"\xe2\x82\xac\"", accepted: true},
		{statement: "SELECT 'a\x81''b\x81', 1", accepted: true},
		// A doubled quote is one quote character inside the string, so this
		// is one sql_mode value, not two adjacent literals. The server
		// rejects it as a mode name; it is not NO_BACKSLASH_ESCAPES.
		{statement: "SET sql_mode = 'ANSI''ANSI'", accepted: true},
		{statement: "SELECT 'a'';SELECT 2'", accepted: true},

		// Not one result each.
		{statement: "CALL p()"},
		{statement: "INSERT INTO t VALUES (1)"},
		{statement: "START REPLICA"},
		{statement: "BEGIN; DELETE FROM t"},
		{statement: "BEGIN;"},
		{statement: "/*!40101 SET @x = 1 */"},
		{statement: "SET @x = 1 /*!; DELETE FROM t */"},
		{statement: "/*M!100100 SET @x = 1 */"},
		{statement: "SET @x = 1 /*M! , sql_mode='NO_BACKSLASH_ESCAPES' */"},
		{statement: "--BEGIN"},
		// The server reads -- plus a control byte as a comment, so the
		// quote after it is not one, and the next line sets the mode.
		{statement: "SET @x = 1 --\x01 '\n, sql_mode = 'NO_BACKSLASH_ESCAPES', @y = 1 --\x01 '"},
		{statement: "SET @x = 1 --\x7f '\n, sql_mode = 'NO_BACKSLASH_ESCAPES', @y = 1 --\x7f '"},
		// Whether a non-ASCII byte after -- is a separator depends on the character set.
		{statement: "SET @x = 1 --\xa0 '\n, sql_mode = 'NO_BACKSLASH_ESCAPES', @y = 1 --\xa0 '"},
		// In gbk, big5, sjis, cp932 and gb18030, 0x81 0x60 is one
		// character, so a server reading in one of them sees no backtick
		// here, and the first ; ends the statement.
		{statement: "SELECT 1 AS \x81` ;DROP TABLE t; -- `"},
		// The same for the backtick that closes an identifier.
		{statement: "SELECT `\x81``"},
		{statement: "SELECT `caf\xc3\xa9`"},
		{statement: "SELECT caf\xc3\xa9"},
		{statement: "SELECT 1 /* caf\xc3\xa9 */"},
		{statement: "SELECT 1 -- caf\xc3\xa9"},
		{statement: "SELECT 1 # caf\xc3\xa9"},
		{statement: "SELECT @caf\xc3\xa9"},
		{statement: ""},
		{statement: "/* unterminated BEGIN"},
		{statement: "BEGIN /* unterminated"},
		{statement: "RELEASE s1"},
		{statement: "RELEASE"},
		{statement: "SELECT 'a'';SELECT 2"},
		{statement: "SET @ = 1"},
		{statement: "SET GLOBAL = 1"},
		{statement: "SELECT 'unterminated"},
		// Changes how the query's args are read.
		{statement: "SET sql_mode = 'NO_BACKSLASH_ESCAPES'"},
		{statement: "SET sql_mode = 'ANSI_QUOTES, no_backslash_escapes'"},
		{statement: "SET sql_mode = DEFAULT"},
		{statement: "SET sql_mode = @saved"},
		{statement: "SET sql_mode = 2097152"},
		{statement: "SET sql_mode = CONCAT(@@sql_mode, ',NO_BACKSLASH_ESCAPES')"},
		{statement: "SET @@session.`sql_mode` = ''"},
		{statement: "SET GLOBAL sql_mode = ''"},
		{statement: "SET @@global.time_zone = '+00:00'"},
		{statement: "SET NAMES gbk"},
		{statement: "SET CHARACTER SET sjis"},
		{statement: "SET character_set_client = big5"},
		{statement: "SET @x = 'a\\'"},
		{statement: "SET ROLE r"},
	} {
		_, err := NewPrelude("BEGIN", tt.statement)
		if accepted := err == nil; accepted != tt.accepted {
			t.Errorf("NewPrelude(%q): %v, want accepted=%v", tt.statement, err, tt.accepted)
		}
	}

	p, err := NewPrelude("BEGIN", "SET @x = 1 # why")
	if err != nil || p.Len() != 2 || p.text != "BEGIN\n;SET @x = 1 # why\n;" {
		t.Errorf("NewPrelude text: %v %+v", err, p)
	}
	if n := (*Prelude)(nil).Len(); n != 0 {
		t.Errorf("nil Prelude Len = %d", n)
	}
}

// fakeServer builds a server's response stream with consecutive sequence
// numbers, for a mockConn to replay.
type fakeServer struct {
	seq byte
	b   []byte
}

func (s *fakeServer) packet(payload []byte) {
	n := len(payload)
	s.b = append(s.b, byte(n), byte(n>>8), byte(n>>16), s.seq)
	s.b = append(s.b, payload...)
	s.seq++
}

func (s *fakeServer) ok(affected, insertID byte, status, warnings uint16) {
	s.packet([]byte{iOK, affected, insertID, byte(status), byte(status >> 8), byte(warnings), byte(warnings >> 8)})
}

func (s *fakeServer) err(number uint16, message string) {
	s.packet(append([]byte{iERR, byte(number), byte(number >> 8), '#', 'H', 'Y', '0', '0', '0'}, message...))
}

// preludeFakeConn returns a connection with MultiStatements on, that replays
// the response. The mock fails a read after a few instead of waiting for a
// server's answer that never comes, so a call that reads more than the
// response holds returns an error.
func preludeFakeConn(multiStatements, interpolateParams bool, response []byte) (*mockConn, *mysqlConn) {
	conn, mc := newRWMockConn(0)
	mc.cfg.MultiStatements = multiStatements
	mc.cfg.InterpolateParams = interpolateParams
	conn.queuedReplies = [][]byte{response}
	conn.maxReads = 8
	return conn, mc
}

func mustNewPrelude(t *testing.T, statements ...string) *Prelude {
	t.Helper()
	p, err := NewPrelude(statements...)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// The counters, AllRowsAffected included, describe the query alone.
func TestQueryResultContextPreludeCountersAreQueryOnly(t *testing.T) {
	const more = uint16(statusMoreResultsExists)
	s := &fakeServer{seq: 1}
	s.ok(7, 0, more, 0) // prelude statement
	s.ok(1, 5, more, 0) // query statement 1
	s.ok(2, 6, 0, 0)    // query statement 2
	_, mc := preludeFakeConn(true, false, s.b)
	_, res, err := mc.QueryResultContext(context.Background(), mustNewPrelude(t, "SELECT 1"), "INSERT a; INSERT b", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := res.(*mysqlResult).AllRowsAffected(); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("AllRowsAffected = %v, want [1 2]", got)
	}
}

// A ? outside quotes in a prelude statement is not a placeholder: the args
// are interpolated into the query before the prelude is put in front.
func TestQueryResultContextPreludeUnquotedQuestionMark(t *testing.T) {
	const more = uint16(statusMoreResultsExists)
	s := &fakeServer{seq: 1}
	s.ok(0, 0, more, 0)
	s.ok(1, 0, 0, 0)
	conn, mc := preludeFakeConn(true, true, s.b)
	args := []driver.NamedValue{{Ordinal: 1, Value: "v"}}
	if _, _, err := mc.QueryResultContext(context.Background(), mustNewPrelude(t, "SELECT ?"), "UPDATE t SET a = ?", args); err != nil {
		t.Fatal(err)
	}
	if got := string(conn.written[5:]); got != "SELECT ?\n;UPDATE t SET a = 'v'" {
		t.Fatalf("sent %q", got)
	}
}

// A response that ends before every prelude statement has answered is an
// error, not a read of the query's response that is not there.
func TestQueryResultContextPreludeIncompleteResponse(t *testing.T) {
	s := &fakeServer{seq: 1}
	s.ok(0, 0, 0, 0) // no more results
	_, mc := preludeFakeConn(true, false, s.b)
	_, _, err := mc.QueryResultContext(context.Background(), mustNewPrelude(t, "BEGIN"), "SELECT 1", nil)
	if !errors.Is(err, errPreludeIncomplete) {
		t.Fatalf("err = %v, want errPreludeIncomplete", err)
	}
}

// A command that fits alone but not behind the prelude is declined before
// anything is written. A query too large on its own is an error, as it is
// without a prelude.
func TestQueryResultContextPreludeTooLarge(t *testing.T) {
	prelude := mustNewPrelude(t, "BEGIN") // 7 bytes in the command
	for _, tt := range []struct {
		name    string
		queryAt int // the query's length
		want    error
		written bool
	}{
		{"fits exactly", 56, nil, true}, // 1 + 7 + 56 = 64
		{"too large with the prelude", 57, driver.ErrSkip, false},
		{"fits alone", 63, driver.ErrSkip, false}, // 1 + 63 = 64
		{"too large alone", 64, ErrPktTooLarge, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := &fakeServer{seq: 1}
			s.ok(0, 0, uint16(statusMoreResultsExists), 0)
			s.ok(1, 0, 0, 0)
			conn, mc := preludeFakeConn(true, false, s.b)
			mc.maxAllowedPacket = 64
			query := "SELECT " + strings.Repeat("1", tt.queryAt-len("SELECT "))
			_, _, err := mc.QueryResultContext(context.Background(), prelude, query, nil)
			if !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
			if written := len(conn.written) != 0; written != tt.written {
				t.Fatalf("written = %q", conn.written)
			}
		})
	}
}

// With nothing after the prelude's last separator the server would answer
// without a result for the query, after the prelude had run.
func TestQueryResultContextPreludeEmptyQuery(t *testing.T) {
	for _, query := range []string{"", " \t\r\n\f\v"} {
		s := &fakeServer{seq: 1}
		s.ok(0, 0, 0, 0)
		conn, mc := preludeFakeConn(true, false, s.b)
		_, _, err := mc.QueryResultContext(context.Background(), mustNewPrelude(t, "BEGIN"), query, nil)
		if !errors.Is(err, driver.ErrSkip) || len(conn.written) != 0 {
			t.Fatalf("query %q: err = %v, written = %q; want ErrSkip and nothing written", query, err, conn.written)
		}
	}
}

// The query did not run after a PreludeError, so it has no warnings; the
// count an earlier prelude statement left must not be reported.
func TestQueryResultContextPreludeErrorHasNoWarnings(t *testing.T) {
	s := &fakeServer{seq: 1}
	s.ok(0, 0, uint16(statusMoreResultsExists), 3) // prelude statement 0, 3 warnings
	s.err(1146, "no such table")                   // prelude statement 1
	_, mc := preludeFakeConn(true, false, s.b)
	_, _, err := mc.QueryResultContext(context.Background(), mustNewPrelude(t, "SELECT 1", "SELECT 2"), "SELECT 3", nil)
	var preludeErr *PreludeError
	if !errors.As(err, &preludeErr) || preludeErr.Index != 1 {
		t.Fatalf("err = %v, want a PreludeError at index 1", err)
	}
	if w := mc.Warnings(); w != 0 {
		t.Fatalf("Warnings = %d, want 0", w)
	}
}
