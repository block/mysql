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
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

// TestRejectReadOnlyDSN covers what the rejectReadOnly parameter does now that
// the behaviour it used to control is unconditional. It is accepted in the
// direction that agrees with the driver and refused in the direction that does
// not, so a DSN carrying the upstream default is a loud failure rather than a
// silent disagreement.
func TestRejectReadOnlyDSN(t *testing.T) {
	for _, value := range []string{"true", "1", "TRUE"} {
		if _, err := ParseDSN("user:pass@tcp(127.0.0.1:3306)/db?rejectReadOnly=" + value); err != nil {
			t.Errorf("rejectReadOnly=%s: %v (a DSN written for upstream should still parse)", value, err)
		}
	}

	for _, value := range []string{"false", "0", "FALSE"} {
		_, err := ParseDSN("user:pass@tcp(127.0.0.1:3306)/db?rejectReadOnly=" + value)
		if err == nil {
			t.Errorf("rejectReadOnly=%s parsed without error; it states an expectation the driver will not meet", value)
			continue
		}
		if !strings.Contains(err.Error(), "cannot be disabled") {
			t.Errorf("rejectReadOnly=%s: error %q does not explain that the option is gone", value, err)
		}
	}

	if _, err := ParseDSN("user:pass@tcp(127.0.0.1:3306)/db?rejectReadOnly=yes"); err == nil {
		t.Error("rejectReadOnly=yes parsed without error; a non-boolean value is still a malformed DSN")
	}
}

// TestReadOnlyErrorsAreBadConn pins the rejection itself: the three read-only
// error numbers must yield driver.ErrBadConn — which is what makes
// database/sql discard the connection and retry — with no configuration
// involved. Upstream gates this on an option that defaults to off.
func TestReadOnlyErrorsAreBadConn(t *testing.T) {
	// 1792: ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION
	// 1290: ER_OPTION_PREVENTS_STATEMENT (returned by Aurora during failover)
	// 1836: ER_READ_ONLY_MODE
	for _, errno := range []uint16{1792, 1290, 1836} {
		_, mc := newRWMockConn(0)
		err := mc.handleErrorPacket(errPacket(errno, "read-only"))
		if !errors.Is(err, driver.ErrBadConn) {
			t.Errorf("errno %d returned %v, want driver.ErrBadConn", errno, err)
		}
		if !mc.closed.Load() {
			t.Errorf("errno %d did not close the connection; database/sql would hand it back out", errno)
		}
	}

	// An unrelated error must still surface as itself. Widening the rejection
	// to errors that are not about read-only would turn a real failure into a
	// silent retry loop.
	_, mc := newRWMockConn(0)
	err := mc.handleErrorPacket(errPacket(1062, "Duplicate entry"))
	var myErr *MySQLError
	if !errors.As(err, &myErr) || myErr.Number != 1062 {
		t.Errorf("errno 1062 returned %v, want a *MySQLError with Number 1062", err)
	}
	if mc.closed.Load() {
		t.Error("errno 1062 closed the connection")
	}
}

// TestReadOnlyTxIsExempt covers the one case that must NOT be rejected: a
// transaction the caller opened with driver.TxOptions.ReadOnly. There the
// read-only error is the answer they asked for, and database/sql does not
// retry inside a transaction — rejecting would replace a usable *MySQLError
// with a dead transaction. (Upstream's TestContextBeginReadOnly is the
// end-to-end version of this and passes unmodified.)
func TestReadOnlyTxIsExempt(t *testing.T) {
	_, mc := newRWMockConn(0)
	mc.inReadOnlyTx = true

	err := mc.handleErrorPacket(errPacket(1792, "Cannot execute statement in a READ ONLY transaction"))
	var myErr *MySQLError
	if !errors.As(err, &myErr) || myErr.Number != 1792 {
		t.Errorf("inside an explicit read-only transaction, errno 1792 returned %v, want a *MySQLError", err)
	}
	if mc.closed.Load() {
		t.Error("the connection was closed; the caller's own read-only transaction is not a demoted writer")
	}

	// Once the transaction ends the exemption must end with it, or one
	// read-only transaction disarms the protection for the rest of the
	// connection's life.
	mc.inReadOnlyTx = false
	if err := mc.handleErrorPacket(errPacket(1792, "read-only")); !errors.Is(err, driver.ErrBadConn) {
		t.Errorf("after the transaction, errno 1792 returned %v, want driver.ErrBadConn", err)
	}
}

// TestReadOnlyTxLifecycle drives the exemption flag through begin, Commit,
// Rollback and ResetSession, which is the only place it can actually go wrong
// and the reason the field exists.
//
// TestReadOnlyTxIsExempt above proves handleErrorPacket *reads* the flag
// correctly, but it sets the field itself, so it cannot tell a flag that is
// cleared from one that nobody ever clears. A flag stuck true silently
// disables the read-only rejection for the rest of the connection's life in
// the pool — the whole feature off, with no symptom.
func TestReadOnlyTxLifecycle(t *testing.T) {
	// beginTx runs a scripted START TRANSACTION against a mock server and
	// returns the connection with the flag in whatever state begin left it.
	beginTx := func(t *testing.T, readOnly bool) (*mockConn, *mysqlConn, driver.Tx) {
		t.Helper()
		conn, mc := newRWMockConn(0)
		conn.queuedReplies = [][]byte{okPacket()}
		tx, err := mc.begin(readOnly)
		if err != nil {
			t.Fatalf("begin(%v): %v", readOnly, err)
		}
		return conn, mc, tx
	}

	// rejects reports what a read-only error does on this connection now.
	rejects := func(mc *mysqlConn) bool {
		return errors.Is(mc.handleErrorPacket(errPacket(1792, "read-only")), driver.ErrBadConn)
	}

	t.Run("a read-only transaction sets it", func(t *testing.T) {
		_, mc, _ := beginTx(t, true)
		if !mc.inReadOnlyTx {
			t.Error("begin(true) did not set inReadOnlyTx; the caller's own read-only transaction would be rejected")
		}
	})

	t.Run("a read-write transaction does not", func(t *testing.T) {
		// This is the direction that disarms the feature: if begin set the
		// flag unconditionally, any connection that had ever run a BeginTx
		// would stop rejecting read-only errors.
		_, mc, _ := beginTx(t, false)
		if mc.inReadOnlyTx {
			t.Error("begin(false) set inReadOnlyTx; an ordinary transaction would exempt the connection from read-only rejection")
		}
		if !rejects(mc) {
			t.Error("a read-only error inside a read-write transaction was not rejected")
		}
	})

	t.Run("Commit clears it", func(t *testing.T) {
		conn, mc, tx := beginTx(t, true)
		conn.queuedReplies = [][]byte{okPacket()}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		if mc.inReadOnlyTx {
			t.Fatal("Commit left inReadOnlyTx set")
		}
		if !rejects(mc) {
			t.Error("after Commit a read-only error was still exempt; one read-only transaction disarmed the connection for good")
		}
	})

	t.Run("Rollback clears it", func(t *testing.T) {
		conn, mc, tx := beginTx(t, true)
		conn.queuedReplies = [][]byte{okPacket()}
		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback: %v", err)
		}
		if mc.inReadOnlyTx {
			t.Fatal("Rollback left inReadOnlyTx set")
		}
		if !rejects(mc) {
			t.Error("after Rollback a read-only error was still exempt")
		}
	})

	t.Run("ResetSession clears it", func(t *testing.T) {
		// Belt and braces: every sql.Tx ends in Commit or Rollback, so this
		// should be unreachable — but it is the point where a pooled
		// connection's assumptions are re-established for a new borrower, and
		// the stuck direction of this flag is the unsafe one.
		_, mc, _ := beginTx(t, true)
		if err := mc.ResetSession(context.Background()); err != nil {
			t.Fatalf("ResetSession: %v", err)
		}
		if mc.inReadOnlyTx {
			t.Error("ResetSession left inReadOnlyTx set; the next borrower inherits the exemption")
		}
	})
}

// okPacket builds a minimal OK packet: header (length, sequence 1), then the
// 0x00 marker, zero affected rows, zero last-insert-id, autocommit status and
// no warnings. Sequence 1 is what the server answers a freshly written command
// with, which is what mc.exec expects back.
func okPacket() []byte {
	return []byte{7, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0}
}

// errPacket builds the ERR packet body handleErrorPacket expects: the 0xff
// marker, the error number, then a SQL state block and the message.
func errPacket(errno uint16, message string) []byte {
	data := []byte{iERR, 0, 0}
	binary.LittleEndian.PutUint16(data[1:3], errno)
	data = append(data, '#')
	data = append(data, []byte("HY000")...)
	return append(data, []byte(message)...)
}
