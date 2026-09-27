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
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

func resetTestDB(t *testing.T, configure func(*Config)) *sql.DB {
	t.Helper()
	if !available {
		t.Skip("MySQL is not available")
	}
	cfg, err := ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if configure != nil {
		configure(cfg)
	}
	c, err := NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(c)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}
func resetExec(t *testing.T, db interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, q string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatal(q, err)
	}
}
func resetID(t *testing.T, db *sql.DB) int {
	t.Helper()
	var id int
	if err := db.QueryRow("SELECT CONNECTION_ID()").Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

// The observer checks locks before the cleaned connection is borrowed again.
// A ResetSession-only implementation would leave both locks held while idle.
func TestTransactionResetLocks(t *testing.T) {
	for _, finish := range []string{"commit", "rollback", "cancel"} {
		t.Run(finish, func(t *testing.T) {
			db := resetTestDB(t, nil)
			observer := resetTestDB(t, nil)
			name := "reset_locks_" + finish
			resetExec(t, observer, "DROP TABLE IF EXISTS "+name+", "+name+"_other")
			resetExec(t, observer, "CREATE TABLE "+name+" (id INT PRIMARY KEY)")
			resetExec(t, observer, "CREATE TABLE "+name+"_other (id INT PRIMARY KEY)")
			t.Cleanup(func() { db.Close(); resetExec(t, observer, "DROP TABLE IF EXISTS "+name+", "+name+"_other") })
			id := resetID(t, db)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			resetExec(t, tx, "SELECT GET_LOCK('"+name+"',0)")
			resetExec(t, tx, "LOCK TABLES "+name+" WRITE")
			switch finish {
			case "commit":
				err = tx.Commit()
			case "rollback":
				err = tx.Rollback()
			case "cancel":
				cancel()
				deadline := time.Now().Add(10 * time.Second)
				for db.Stats().InUse != 0 && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}
				if db.Stats().InUse != 0 {
					t.Fatal("automatic rollback did not return the connection")
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			var got int
			if err := observer.QueryRow("SELECT GET_LOCK(?,0)", name).Scan(&got); err != nil || got != 1 {
				t.Fatalf("idle connection retained advisory lock: %d %v", got, err)
			}
			resetExec(t, observer, "SELECT RELEASE_LOCK('"+name+"')")
			resetExec(t, observer, "SET lock_wait_timeout=1")
			resetExec(t, observer, "LOCK TABLES "+name+" WRITE")
			resetExec(t, observer, "UNLOCK TABLES")
			if after := resetID(t, db); after != id {
				t.Fatalf("healthy reset should reuse connection: %d -> %d", id, after)
			}
			// Access a table outside the leaked LOCK TABLES set, as in spirit#1276.
			resetExec(t, db, "UPDATE "+name+"_other SET id=id")
		})
	}
}

func TestTransactionResetBaseline(t *testing.T) {
	for _, config := range []struct {
		name, collation string
		charsets        []string
		compress        bool
		maxPacket       int
	}{
		{name: "default", maxPacket: defaultMaxAllowedPacket},
		{name: "latin1", collation: "latin1_swedish_ci", maxPacket: defaultMaxAllowedPacket},
		{name: "charset-fallback", charsets: []string{"none", "ascii"}, maxPacket: defaultMaxAllowedPacket},
		{name: "compressed", compress: true, maxPacket: defaultMaxAllowedPacket},
		{name: "discover-packet-limit", maxPacket: 0},
	} {
		t.Run(config.name, func(t *testing.T) {
			db := resetTestDB(t, func(c *Config) {
				c.Collation = config.collation
				c.charsets = config.charsets
				c.compress = config.compress
				c.MaxAllowedPacket = config.maxPacket
				c.Params = map[string]string{"sql_mode": "'NO_AUTO_VALUE_ON_ZERO'", "time_zone": "'+00:00'", "transaction_isolation": "'READ-COMMITTED'"}
			})
			query := "SELECT @@sql_mode,@@time_zone,@@transaction_isolation,@@character_set_client,@@character_set_connection,@@character_set_results,@@collation_connection,DATABASE()"
			read := func() []string {
				v := make([]string, 8)
				args := make([]any, 8)
				for i := range v {
					args[i] = &v[i]
				}
				if err := db.QueryRow(query).Scan(args...); err != nil {
					t.Fatal(err)
				}
				return v
			}
			before := fmt.Sprint(read())
			id := resetID(t, db)
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			resetExec(t, tx, "SET @reset_private=42")
			resetExec(t, tx, "CREATE TEMPORARY TABLE reset_private (id INT)")
			resetExec(t, tx, "SET sql_mode='',time_zone='+02:00',transaction_isolation='SERIALIZABLE'")
			resetExec(t, tx, "SET NAMES ascii")
			resetExec(t, tx, "USE information_schema")
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			if after := fmt.Sprint(read()); after != before {
				t.Fatalf("baseline changed: %s -> %s", before, after)
			}
			if resetID(t, db) != id {
				t.Fatal("reset unexpectedly reconnected")
			}
			var v any
			if err := db.QueryRow("SELECT @reset_private").Scan(&v); err != nil || v != nil {
				t.Fatalf("user variable survived: %v %v", v, err)
			}
			if _, err := db.Exec("SELECT * FROM reset_private"); err == nil {
				t.Fatal("temporary table survived reset")
			}
		})
	}
}

func TestTransactionResetReservedConn(t *testing.T) {
	db := resetTestDB(t, nil)
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	resetExec(t, conn, "SET @reserved=42")
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var v any
	if err := conn.QueryRowContext(ctx, "SELECT @reserved").Scan(&v); err != nil || v == nil {
		t.Fatalf("reserved session reset prematurely: %v %v", v, err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT @reserved").Scan(&v); err != nil || v != nil {
		t.Fatalf("reserved session not reset on return: %v %v", v, err)
	}
}

func TestTransactionResetPreparedStatements(t *testing.T) {
	db := resetTestDB(t, nil)
	stmt, err := db.Prepare("SELECT ? + 1")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	id := resetID(t, db)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if resetID(t, db) == id {
		t.Fatal("connection with cached statement was not discarded")
	}
	var got int
	if err := stmt.QueryRow(41).Scan(&got); err != nil || got != 42 {
		t.Fatalf("cached statement failed: %d %v", got, err)
	}
	// Transaction-owned statements are closed by database/sql before Validator.
	stmt.Close()
	id = resetID(t, db)
	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	owned, err := tx.Prepare("SELECT 42")
	if err != nil {
		t.Fatal(err)
	}
	if err := owned.QueryRow().Scan(&got); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if resetID(t, db) != id {
		t.Fatal("closed transaction statement caused needless discard")
	}
}

func TestTransactionResetNoDatabase(t *testing.T) {
	db := resetTestDB(t, func(c *Config) { c.DBName = "" })
	id := resetID(t, db)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	resetExec(t, tx, "USE information_schema")
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if resetID(t, db) == id {
		t.Fatal("cannot restore empty database without reconnecting")
	}
	var name sql.NullString
	if err := db.QueryRow("SELECT DATABASE()").Scan(&name); err != nil || name.Valid {
		t.Fatalf("database leaked: %v %v", name, err)
	}
}

func TestTransactionResetProtocol(t *testing.T) {
	for _, failure := range []string{"none", "reset", "charset", "database", "params"} {
		t.Run(failure, func(t *testing.T) {
			conn, mc := newRWMockConn(0)
			mc.cfg.DBName = "gotest"
			mc.cfg.Params = map[string]string{"time_zone": "'+00:00'"}
			conn.queuedReplies = [][]byte{okPacket()}
			tx, err := mc.BeginTx(context.Background(), driver.TxOptions{})
			if err != nil {
				t.Fatal(err)
			}
			conn.queuedReplies = [][]byte{okPacket()}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			replies := [][]byte{okPacket(), okPacket(), okPacket(), okPacket()}
			for i, label := range []string{"reset", "charset", "database", "params"} {
				if failure == label {
					replies[i] = resetErrorPacket(1234, "reset test failure")
				}
			}
			conn.queuedReplies = replies
			conn.written = nil
			valid := mc.IsValid()
			if valid != (failure == "none") {
				t.Fatalf("IsValid=%v for %s", valid, failure)
			}
			if len(conn.written) < 5 || conn.written[4] != comResetConnection {
				t.Fatal("did not send COM_RESET_CONNECTION first")
			}
			if failure != "none" && !mc.closed.Load() {
				t.Fatal("cleanup failure left connection open")
			}
			if failure == "none" {
				before := len(conn.written)
				if !mc.IsValid() || len(conn.written) != before {
					t.Fatal("clean pool return sent another reset")
				}
			}
		})
	}
}

func TestTransactionResetNoTransaction(t *testing.T) {
	conn, mc := newRWMockConn(0)
	if !mc.IsValid() || len(conn.written) != 0 {
		t.Fatal("ordinary pool return sent cleanup commands")
	}
}

func TestTransactionResetTimeout(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	_, mc := newRWMockConn(0)
	mc.netConn = client
	mc.rawConn = client
	mc.cfg.DBName = "gotest"
	mc.startWatcher()
	defer mc.cleanup()
	// Consume the request, but never return its OK packet.
	done := make(chan struct{})
	go func() { defer close(done); b := make([]byte, 5); io.ReadFull(server, b) }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := mc.resetTransactionSession(ctx); err == nil {
		t.Fatal("hung reset succeeded")
	}
	<-done
	if !mc.closed.Load() {
		t.Fatal("timeout did not close physical connection")
	}
}

// A validator failure is not a failed COMMIT and must not invite transaction retry.
type resetTestConnector struct{ conn driver.Conn }

func (c resetTestConnector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }
func (c resetTestConnector) Driver() driver.Driver                        { return &MySQLDriver{} }

func TestTransactionResetPreservesCommitResult(t *testing.T) {
	conn, mc := newRWMockConn(0)
	mc.cfg.DBName = "gotest"
	conn.queuedReplies = [][]byte{okPacket(), okPacket(), resetErrorPacket(1234, "cannot reset")}
	db := sql.OpenDB(resetTestConnector{mc})
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("successful commit reported failure: %v", err)
	}
	if db.Stats().OpenConnections != 0 {
		t.Fatal("failed cleanup did not discard committed connection")
	}
}

func TestTransactionResetDiscardFailedEnd(t *testing.T) {
	for _, commit := range []bool{false, true} {
		conn, mc := newRWMockConn(0)
		conn.queuedReplies = [][]byte{okPacket()}
		tx, err := mc.Begin()
		if err != nil {
			t.Fatal(err)
		}
		conn.queuedReplies = [][]byte{resetErrorPacket(1234, "end failed")}
		if commit {
			err = tx.Commit()
		} else {
			err = tx.Rollback()
		}
		var sqlErr *MySQLError
		if !errors.As(err, &sqlErr) || sqlErr.Number != 1234 {
			t.Fatalf("lost original error: %v", err)
		}
		if mc.IsValid() {
			t.Fatal("failed transaction end returned a reusable connection")
		}
	}
}

func BenchmarkTransactionReset(b *testing.B) {
	if !available {
		b.Skip("MySQL is not available")
	}
	for _, prepared := range []bool{false, true} {
		b.Run(fmt.Sprintf("prepared=%v", prepared), func(b *testing.B) {
			db, err := sql.Open(driverNameTest, dsn)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			var stmt *sql.Stmt
			if prepared {
				stmt, err = db.Prepare("SELECT 1")
				if err != nil {
					b.Fatal(err)
				}
				defer stmt.Close()
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tx, err := db.Begin()
				if err != nil {
					b.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					b.Fatal(err)
				}
				if stmt != nil {
					var n int
					if err := stmt.QueryRow().Scan(&n); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

func resetErrorPacket(code uint16, message string) []byte {
	body := errPacket(code, message)
	return append([]byte{byte(len(body)), 0, 0, 1}, body...)
}
