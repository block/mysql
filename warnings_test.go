// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"testing"
)

func TestReadWarnings(t *testing.T) {
	if got := readWarnings([]byte{0x07, 0x00}); got != 7 {
		t.Errorf("little-endian decode: got %d, want 7", got)
	}
	if got := readWarnings([]byte{0x00, 0x01}); got != 256 {
		t.Errorf("high byte: got %d, want 256", got)
	}
	// A packet too short to hold the field must report zero rather than
	// panicking on the slice bound.
	if got := readWarnings([]byte{0x07}); got != 0 {
		t.Errorf("short buffer: got %d, want 0", got)
	}
	if got := readWarnings(nil); got != 0 {
		t.Errorf("empty buffer: got %d, want 0", got)
	}
}

func TestHandleOkPacketWarnings(t *testing.T) {
	// OK packet: header, affected_rows, last_insert_id, status, warnings.
	data := []byte{iOK, 0x03, 0x07, 0x02, 0x00, 0x05, 0x00}

	mc := new(mysqlConn)
	if err := mc.clearResult().handleOkPacket(data); err != nil {
		t.Fatalf("handleOkPacket: %s", err)
	}
	if got := mc.Warnings(); got != 5 {
		t.Errorf("Warnings: got %d, want 5", got)
	}
	if got := mc.status; got != 0x0002 {
		t.Errorf("status: got %#04x, want 0x0002", got)
	}

	// The next statement starts by clearing the previous one's count, so a
	// statement that warns cannot make a later quiet statement look noisy.
	mc.clearResult()
	if got := mc.Warnings(); got != 0 {
		t.Errorf("Warnings after clearResult: got %d, want 0", got)
	}
}

func TestHandleOkPacketWarningsWithMoreResults(t *testing.T) {
	// statusMoreResultsExists set: the count still belongs to the statement
	// that just finished, so it must be recorded before the early return.
	data := []byte{iOK, 0x00, 0x00, byte(statusMoreResultsExists), 0x00, 0x02, 0x00}

	mc := new(mysqlConn)
	if err := mc.clearResult().handleOkPacket(data); err != nil {
		t.Fatalf("handleOkPacket: %s", err)
	}
	if got := mc.Warnings(); got != 2 {
		t.Errorf("Warnings: got %d, want 2", got)
	}
}

func TestReadResultsetTerminatorWarnings(t *testing.T) {
	// Deprecated EOF packet: header, warnings, status. Note the field order is
	// the reverse of the OK packet's — the point of the shared reader.
	mc := new(mysqlConn)
	mc.readResultsetTerminator([]byte{iEOF, 0x04, 0x00, 0x02, 0x00})
	if got := mc.Warnings(); got != 4 {
		t.Errorf("EOF packet warnings: got %d, want 4", got)
	}
	if got := mc.status; got != 0x0002 {
		t.Errorf("EOF packet status: got %#04x, want 0x0002", got)
	}

	// OK packet with an 0xFE header, sent in place of EOF once
	// CLIENT_DEPRECATE_EOF is negotiated: status, then warnings.
	mc = new(mysqlConn)
	mc.capabilities |= clientDeprecateEOF
	mc.readResultsetTerminator([]byte{iEOF, 0x00, 0x00, 0x02, 0x00, 0x04, 0x00})
	if got := mc.Warnings(); got != 4 {
		t.Errorf("deprecate-EOF warnings: got %d, want 4", got)
	}
	if got := mc.status; got != 0x0002 {
		t.Errorf("deprecate-EOF status: got %#04x, want 0x0002", got)
	}
}

// execWarnings runs queries in order on one connection and reports the warning
// count left behind by the last of them, reading it the way an external caller
// must: through Raw, after the response is complete. The error from the last
// query is returned rather than fatal, so a failing statement can be probed.
func execWarnings(ctx context.Context, dbt *DBTest, queries ...string) (uint16, error) {
	dbt.Helper()
	conn, err := dbt.db.Conn(ctx)
	if err != nil {
		dbt.Fatalf("getting conn: %s", err)
	}
	defer conn.Close()

	var (
		warnings uint16
		queryErr error
	)
	if err := conn.Raw(func(dc any) error {
		mc := dc.(*mysqlConn)
		for _, query := range queries {
			rows, _, err := mc.QueryResultContext(ctx, query, nil)
			if err != nil {
				queryErr = err
				break
			}
			if rows != nil {
				// The terminating packet that carries the count has not been
				// read until the rows are drained.
				if err := drainRows(rows); err != nil {
					return err
				}
			}
		}
		warnings = mc.Warnings()
		return nil
	}); err != nil {
		dbt.Fatalf("%v: %s", queries, err)
	}
	return warnings, queryErr
}

// connWarnings is execWarnings for a single query expected to succeed.
func connWarnings(ctx context.Context, dbt *DBTest, query string) uint16 {
	dbt.Helper()
	warnings, err := execWarnings(ctx, dbt, query)
	if err != nil {
		dbt.Fatalf("%s: %s", query, err)
	}
	return warnings
}

func drainRows(rows driver.Rows) error {
	defer rows.Close()
	dest := make([]driver.Value, len(rows.Columns()))
	for {
		err := rows.Next(dest)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func TestWarnings(t *testing.T) {
	runTestsParallel(t, dsn, func(dbt *DBTest, tbl string) {
		ctx := context.Background()
		dbt.mustExec("CREATE TABLE " + tbl + " (id INT PRIMARY KEY, note VARCHAR(4))")

		if got := connWarnings(ctx, dbt, "INSERT INTO "+tbl+" VALUES (1, 'ok')"); got != 0 {
			dbt.Errorf("clean INSERT: got %d warnings, want 0", got)
		}

		// DROP TABLE IF EXISTS on a missing table is note 1051, reported in
		// the OK packet's warning count.
		if got := connWarnings(ctx, dbt, "DROP TABLE IF EXISTS "+tbl+"_absent"); got != 1 {
			dbt.Errorf("DROP IF EXISTS on a missing table: got %d warnings, want 1", got)
		}

		// A resultset carries its count in the terminating packet, not in a
		// leading OK packet, so this exercises the other reader.
		if got := connWarnings(ctx, dbt, "SELECT CAST('abc' AS SIGNED)"); got != 1 {
			dbt.Errorf("truncating CAST: got %d warnings, want 1", got)
		}
		if got := connWarnings(ctx, dbt, "SELECT * FROM "+tbl); got != 0 {
			dbt.Errorf("clean SELECT: got %d warnings, want 0", got)
		}

		// A failed statement reports zero even when the statement before it
		// warned: an error packet carries no count of its own, and the failing
		// statement's own start already cleared the previous one's.
		got, err := execWarnings(ctx, dbt, "SELECT CAST('abc' AS SIGNED)", "SELECT * FROM "+tbl+"_absent")
		if err == nil {
			dbt.Fatal("expected the second statement to fail")
		}
		if got != 0 {
			dbt.Errorf("after a failed statement: got %d warnings, want 0", got)
		}
	})
}
