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
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// lockWaitQuery waits for the row lock openLockWaitTest's holder keeps. A lock
// wait is what an interrupted statement usually is, and unlike SELECT SLEEP()
// (which KILL QUERY ends with the result 1) it fails with error 1317.
const lockWaitQuery = "SELECT id FROM test_kill_query WHERE id = 1 FOR UPDATE"

// openLockWaitTest returns a pool on the test database, and a connection that
// holds a row lock lockWaitQuery waits for until the test ends.
func openLockWaitTest(t *testing.T) (*sql.DB, *sql.Conn) {
	t.Helper()
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}
	db, err := sql.Open(driverNameTest, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := t.Context()
	for _, stmt := range []string{
		"DROP TABLE IF EXISTS test_kill_query",
		"CREATE TABLE test_kill_query (id INT PRIMARY KEY)",
		"INSERT INTO test_kill_query VALUES (1)",
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	holder, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.ExecContext(ctx, "BEGIN"); err != nil {
		t.Fatal(err)
	}
	if _, err := holder.ExecContext(ctx, lockWaitQuery); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = holder.ExecContext(context.Background(), "ROLLBACK")
		holder.Close()
		_, _ = db.ExecContext(context.Background(), "DROP TABLE IF EXISTS test_kill_query")
	})
	return db, holder
}

func mustParseDSN(t *testing.T, dsn string) *Config {
	t.Helper()
	cfg, err := ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// countingConnector counts its dials and its Close calls.
type countingConnector struct {
	driver.Connector
	dials  atomic.Int32
	closes atomic.Int32
}

func (c *countingConnector) Connect(ctx context.Context) (driver.Conn, error) {
	c.dials.Add(1)
	return c.Connector.Connect(ctx)
}

func (c *countingConnector) Close() error {
	c.closes.Add(1)
	return nil
}

func newCountingConnector(t *testing.T) *countingConnector {
	t.Helper()
	connector, err := NewConnector(mustParseDSN(t, dsn))
	if err != nil {
		t.Fatal(err)
	}
	return &countingConnector{Connector: connector}
}

func TestReadKillTargetNamesTheConnection(t *testing.T) {
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}
	db, err := sql.Open(driverNameTest, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var want KillTarget
	if err := conn.QueryRowContext(t.Context(), "SELECT CONNECTION_ID(), @@server_uuid, @@hostname").
		Scan(&want.ThreadID, &want.ServerUUID, &want.Hostname); err != nil {
		t.Fatal(err)
	}
	var got KillTarget
	if err := conn.Raw(func(driverConn any) (err error) {
		got, err = ReadKillTarget(t.Context(), driverConn)
		return err
	}); err != nil {
		t.Fatalf("ReadKillTarget: %v", err)
	}
	if got != want {
		t.Errorf("ReadKillTarget: got %+v, want %+v", got, want)
	}
}

func TestReadKillTargetRefusesAConnectionThatCannotNameItsThread(t *testing.T) {
	for name, driverConn := range map[string]any{
		"another driver's connection": struct{}{},
		"a wrapping connection":       wrappedConn{Conn: &mysqlConn{connectionID: 5}},
		"no thread":                   &mysqlConn{},
	} {
		if _, err := ReadKillTarget(context.Background(), driverConn); !errors.Is(err, ErrKillQueryUnsupported) {
			t.Errorf("%s: got %v, want ErrKillQueryUnsupported", name, err)
		}
	}
}

// wrappedConn is a connection that wraps this driver's, as an
// instrumentation wrapper would.
type wrappedConn struct{ driver.Conn }

// wrappingConnector returns its connector's connections wrapped.
type wrappingConnector struct{ driver.Connector }

func (c wrappingConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return wrappedConn{Conn: conn}, nil
}

// TestInterrupterRefusesAWrappingConnector: the server check needs this
// driver's own connection, so an Interrupter whose connector wraps its
// connections sends nothing.
func TestInterrupterRefusesAWrappingConnector(t *testing.T) {
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}
	interrupter := NewInterrupter(wrappingConnector{newCountingConnector(t)})
	defer interrupter.Close()
	err := interrupter.KillQuery(t.Context(), testKillTarget)
	if !errors.Is(err, ErrKillQueryUnsupported) || !errors.Is(err, ErrKillQueryNotSent) {
		t.Fatalf("KillQuery: got %v, want ErrKillQueryUnsupported and ErrKillQueryNotSent", err)
	}
}

// TestInterrupterKillsAStatementWhileItsPoolIsFull verifies that KILL QUERY
// does not need a connection from the statement's pool, which is full when
// statements time out together.
func TestInterrupterKillsAStatementWhileItsPoolIsFull(t *testing.T) {
	db, _ := openLockWaitTest(t)
	db.SetMaxOpenConns(2) // the lock holder and the statement
	interrupter := NewInterrupter(newCountingConnector(t))
	defer interrupter.Close()

	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var target KillTarget
	if err := conn.Raw(func(driverConn any) (err error) {
		target, err = ReadKillTarget(t.Context(), driverConn)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	statementErr := make(chan error, 1)
	go func() {
		_, err := conn.ExecContext(context.Background(), lockWaitQuery)
		statementErr <- err
	}()
	waitForThreadState(t, interrupter, target.ThreadID)

	if err := interrupter.KillQuery(t.Context(), target); err != nil {
		t.Fatalf("KillQuery: %v", err)
	}
	select {
	case err := <-statementErr:
		var mysqlErr *MySQLError
		if !errors.As(err, &mysqlErr) || mysqlErr.Number != 1317 {
			t.Fatalf("statement: got %v, want error 1317", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the statement did not end after KILL QUERY")
	}
	var one int
	if err := conn.QueryRowContext(t.Context(), "SELECT 1").Scan(&one); err != nil {
		t.Errorf("KILL QUERY must leave the connection usable: %v", err)
	}
}

// waitForThreadState waits until threadID is running a statement, so a KILL
// QUERY sent next has something to end.
func waitForThreadState(t *testing.T, interrupter *Interrupter, threadID uint32) {
	t.Helper()
	db, err := interrupter.get()
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		if err := db.QueryRowContext(t.Context(),
			"SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE ID = ? AND INFO = ?", threadID, lockWaitQuery).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("thread %d never started the statement", threadID)
}

// TestInterrupterRefusesAnotherServer verifies that no KILL QUERY is sent on a
// connection that is not on the target's server: there the thread ID names
// an unrelated session. Each mismatching connection is discarded and a fresh
// one dialed, up to the attempt bound.
func TestInterrupterRefusesAnotherServer(t *testing.T) {
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}
	connector := newCountingConnector(t)
	interrupter := NewInterrupter(connector)
	defer interrupter.Close()

	db, err := sql.Open(driverNameTest, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var target KillTarget
	if err := conn.Raw(func(driverConn any) (err error) {
		target, err = ReadKillTarget(t.Context(), driverConn)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	target.ServerUUID = "another-server"

	err = interrupter.KillQuery(t.Context(), target)
	if !errors.Is(err, ErrKillQueryWrongServer) || !errors.Is(err, ErrKillQueryNotSent) {
		t.Fatalf("KillQuery: got %v, want ErrKillQueryWrongServer and ErrKillQueryNotSent", err)
	}
	if n := connector.dials.Load(); n != interrupterServerAttempts {
		t.Errorf("dialed %d interrupt connections, want %d", n, interrupterServerAttempts)
	}
	pool, err := interrupter.get()
	if err != nil {
		t.Fatal(err)
	}
	if n := pool.Stats().OpenConnections; n != 0 {
		t.Errorf("%d connections on the wrong server were kept", n)
	}
}

func TestInterrupterClose(t *testing.T) {
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}
	connector := newCountingConnector(t)
	interrupter := NewInterrupter(connector)
	// Open the pool, so Close has connections to close.
	if _, err := interrupter.get(); err != nil {
		t.Fatal(err)
	}
	if err := interrupter.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n := connector.closes.Load(); n != 0 {
		t.Errorf("Close closed the caller's connector %d times", n)
	}
	err := interrupter.KillQuery(t.Context(), testKillTarget)
	if !errors.Is(err, ErrInterrupterClosed) || !errors.Is(err, ErrKillQueryNotSent) {
		t.Errorf("KillQuery after Close: got %v, want ErrInterrupterClosed and ErrKillQueryNotSent", err)
	}
}
