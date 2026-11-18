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

// unifiedExecOn is unifiedExec on conn, with prelude run first through
// QueryResultAfterContext when it is non-nil.
func unifiedExecOn(ctx context.Context, dbt *DBTest, conn *sql.Conn, prelude []string, query string, args []driver.NamedValue) (*unifiedResponse, error) {
	resp := &unifiedResponse{}
	rawErr := conn.Raw(func(dc any) error {
		mc, ok := dc.(*mysqlConn)
		if !ok {
			dbt.Fatalf("driver conn is %T, not *mysqlConn", dc)
		}
		var rows driver.Rows
		var result driver.Result
		var err error
		if prelude == nil {
			rows, result, err = mc.QueryResultContext(ctx, query, args)
		} else {
			rows, result, err = mc.QueryResultAfterContext(ctx, prelude, query, args)
		}
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

func TestQueryResultAfterContext(t *testing.T) {
	// Run with and without InterpolateParams, so the args case is exercised
	// both ways.
	for name, params := range map[string]string{"multiStatements": "&multiStatements=true", "multiStatementsInterpolateParams": "&multiStatements=true&interpolateParams=true"} {
		t.Run(name, func(t *testing.T) { testQueryResultAfterContext(t, dsn+params) })
	}
}

func testQueryResultAfterContext(t *testing.T, dsn string) {
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

		// An empty prelude is QueryResultContext.
		resp, err = unifiedExecOn(ctx, dbt, conn, []string{}, "SELECT COUNT(*) FROM "+tbl, nil)
		if err != nil || string(resp.rows[0][0]) != "1" {
			dbt.Fatalf("empty prelude: %v %v", resp, err)
		}
	})
}

func TestQueryResultAfterContextNeedsMultiStatements(t *testing.T) {
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
