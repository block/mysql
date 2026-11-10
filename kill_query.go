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
	"strconv"
	"sync"
	"time"
)

const (
	// interrupterMaxOpen bounds the connections an Interrupter keeps. A KILL
	// QUERY takes microseconds on the server, so a few connections serve a
	// burst (many statements timing out at once) without a burst of new
	// connections to a server that is already struggling.
	interrupterMaxOpen = 4
	// interrupterMaxIdleTime is how long an interrupt connection may sit
	// idle. Interrupts are rare, so the pool normally holds nothing.
	interrupterMaxIdleTime = 30 * time.Second
	// interrupterServerAttempts bounds the interrupt connections KillQuery
	// tries before giving up on reaching the statement's server. Each one
	// that reports another server is discarded, so the next comes from a
	// fresh dial.
	interrupterServerAttempts = 3
)

var (
	// ErrKillQueryNotSent is wrapped by a KillQuery error when the KILL QUERY
	// was never sent, so it cannot reach the target's connection later. A
	// KillQuery error that does not wrap it may come after the KILL QUERY
	// went out.
	ErrKillQueryNotSent = errors.New("mysql: KILL QUERY was not sent")

	// ErrInterrupterClosed is KillQuery's answer once the Interrupter has
	// been closed.
	ErrInterrupterClosed = errors.New("mysql: interrupter closed")

	// ErrKillQueryWrongServer reports that no interrupt connection reached
	// the server the statement's connection is on, so no KILL QUERY was sent.
	ErrKillQueryWrongServer = errors.New("mysql: interrupt connections did not reach the statement's server")

	// ErrKillQueryUnsupported reports a connection that cannot be named for
	// KILL QUERY: it does not report its thread ID or its server.
	ErrKillQueryUnsupported = errors.New("mysql: connection cannot be interrupted with KILL QUERY")
)

// KillTarget names the server thread a statement runs on, for KILL QUERY:
// the thread ID the server gave the statement's connection, and which server
// gave it. A thread ID means something only on the server that issued it.
type KillTarget struct {
	// ThreadID is the connection's server thread ID (ConnectionID). Zero is
	// no thread: no MySQL connection has it.
	ThreadID uint32
	// ServerUUID and Hostname identify the server: its @@server_uuid and
	// @@hostname.
	ServerUUID string
	Hostname   string
}

// ReadKillTarget returns the KillTarget of driverConn, the driver connection
// (*sql.Conn).Raw hands its callback. Only this driver's own connections can
// be read: the error wraps ErrKillQueryUnsupported for any other, including
// one that wraps this driver's.
//
// Call it before starting the statement the target is for, never while a
// statement runs on the connection: the first call on a connection reads its
// @@server_uuid and @@hostname with a statement. Later calls are free.
func ReadKillTarget(ctx context.Context, driverConn any) (KillTarget, error) {
	mc, ok := driverConn.(*mysqlConn)
	if !ok {
		return KillTarget{}, fmt.Errorf("%w: %T is not a connection of this driver", ErrKillQueryUnsupported, driverConn)
	}
	threadID := mc.ConnectionID()
	if threadID == 0 {
		return KillTarget{}, fmt.Errorf("%w: the connection reports no thread ID", ErrKillQueryUnsupported)
	}
	uuid, hostname, err := mc.readServerIdentity(ctx)
	if err != nil {
		return KillTarget{}, err
	}
	return KillTarget{ThreadID: threadID, ServerUUID: uuid, Hostname: hostname}, nil
}

// QueryKiller sends KILL QUERY for a KillTarget. *Interrupter is the
// implementation; the interface lets ArmInterrupt be tested with others.
type QueryKiller interface {
	// KillQuery sends KILL QUERY target.ThreadID to target's server. It is
	// safe to call while the statement runs. An error that wraps
	// ErrKillQueryNotSent promises that nothing was sent; any other error
	// is taken to mean the KILL QUERY may still reach the server.
	KillQuery(ctx context.Context, target KillTarget) error
}

// Interrupter sends KILL QUERY on connections of its own, opened from its
// connector on first use and kept apart from any application pool.
//
// A statement's own connection is busy while it runs. When every connection
// of its pool is busy, which is when statements pile up and time out, a KILL
// QUERY that needed one of them would wait behind the statements it was meant
// to end. So an Interrupter keeps a few connections of its own (at most four,
// closed after 30 seconds idle).
//
// The interrupt connection is dialed to the same address as the statement's,
// but not necessarily the same server: an Aurora reader endpoint spreads new
// connections across replicas, and a writer endpoint moves on failover. A
// KILL QUERY sent to another server ends an unrelated session that happens to
// have the same thread ID there. So KillQuery sends KILL QUERY only on an
// interrupt connection whose @@server_uuid and @@hostname match the
// target's, dialing
// again (up to three times) when it does not. Behind a proxy that reports the
// variables of a server other than the one numbering its connections (a
// Vitess vtgate), the comparison proves nothing: do not interrupt statements
// through such a proxy.
//
// KILL QUERY names a thread, not a statement: it ends whatever the thread is
// running when the kill arrives. So until KillQuery has returned, the caller
// must start no other statement on the target's connection, nor return it to
// its pool. A KillQuery that failed may still have reached the server (its
// context can end mid-round trip), so the caller must then discard the
// connection. ArmInterrupt follows both rules.
//
// An Interrupter is safe for concurrent use. Close it when the connections it
// interrupts are closed.
type Interrupter struct {
	connector driver.Connector

	mu     sync.Mutex
	db     *sql.DB
	closed bool
}

// NewInterrupter returns an Interrupter that dials its connections with
// connector. Nothing is dialed until the first KillQuery.
//
// connector must return this driver's own connections: NewConnector's, or a
// wrapping connector whose Connect returns them unchanged. KillQuery refuses
// any other connection with ErrKillQueryUnsupported. Pass a connector that reaches the same servers with the same user
// as the statements to interrupt: a user may kill its own threads without
// any privilege. Pass one that does not shed or queue load either, since an
// interrupt relieves an overloaded server.
func NewInterrupter(connector driver.Connector) *Interrupter {
	return &Interrupter{connector: connector}
}

func (i *Interrupter) get() (*sql.DB, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed {
		return nil, ErrInterrupterClosed
	}
	if i.db == nil {
		// sql.OpenDB would close the connector with the pool; the
		// connector is the caller's, so it is hidden behind a type that
		// has no Close.
		i.db = sql.OpenDB(unclosableConnector{i.connector})
		i.db.SetMaxOpenConns(interrupterMaxOpen)
		i.db.SetMaxIdleConns(1)
		i.db.SetConnMaxIdleTime(interrupterMaxIdleTime)
	}
	return i.db, nil
}

// unclosableConnector hides its connector's Close from sql.DB.Close.
type unclosableConnector struct {
	connector driver.Connector
}

func (c unclosableConnector) Connect(ctx context.Context) (driver.Conn, error) {
	return c.connector.Connect(ctx)
}

func (c unclosableConnector) Driver() driver.Driver {
	return c.connector.Driver()
}

// KillQuery sends KILL QUERY target.ThreadID on an interrupt connection that
// is on target's server. ctx bounds it, including the dials; KillQuery has no
// timeout of its own.
//
// It returns nil once the server has acknowledged the KILL QUERY. An error
// that wraps ErrKillQueryNotSent means nothing was sent: the Interrupter is
// closed, no interrupt connection could be dialed or reached target's server
// (ErrKillQueryWrongServer), or ctx ended first. Any other error may come
// after the KILL QUERY was sent, so the server may still act on it.
func (i *Interrupter) KillQuery(ctx context.Context, target KillTarget) error {
	if target.ThreadID == 0 {
		return fmt.Errorf("%w: %w: no thread ID", ErrKillQueryNotSent, ErrKillQueryUnsupported)
	}
	db, err := i.get()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrKillQueryNotSent, err)
	}
	kill := "KILL QUERY " + strconv.FormatUint(uint64(target.ThreadID), 10)
	for range interrupterServerAttempts {
		conn, err := db.Conn(ctx)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrKillQueryNotSent, err)
		}
		sent := false
		// A callback error that wraps driver.ErrBadConn makes database/sql
		// close the driver connection, and conn with it.
		err = conn.Raw(func(driverConn any) error {
			mc, ok := driverConn.(*mysqlConn)
			if !ok {
				return fmt.Errorf("%w: interrupt connection %T is not a connection of this driver: %w", ErrKillQueryUnsupported, driverConn, driver.ErrBadConn)
			}
			uuid, hostname, err := mc.readServerIdentity(ctx)
			if err != nil {
				return fmt.Errorf("reading the interrupt connection's server identity: %w: %w", err, driver.ErrBadConn)
			}
			if uuid != target.ServerUUID || hostname != target.Hostname {
				// A fresh dial may reach the right server; this
				// connection never will.
				return fmt.Errorf("%w: %w", ErrKillQueryWrongServer, driver.ErrBadConn)
			}
			if err := ctx.Err(); err != nil {
				// The driver would refuse the statement before writing it.
				return err
			}
			sent = true
			_, err = mc.ExecContext(ctx, kill, nil)
			return err
		})
		if !errors.Is(err, driver.ErrBadConn) {
			_ = conn.Close()
		}
		switch {
		case err == nil:
			return nil
		case errors.Is(err, ErrKillQueryWrongServer):
			continue
		case sent && errors.Is(err, driver.ErrBadConn):
			// The driver reports ErrBadConn only when it wrote nothing,
			// typically on an idle connection the server has since
			// closed: a fresh dial may succeed.
			continue
		case sent:
			return err
		default:
			return fmt.Errorf("%w: %w", ErrKillQueryNotSent, err)
		}
	}
	return fmt.Errorf("%w: %w (%d attempts)", ErrKillQueryNotSent, ErrKillQueryWrongServer, interrupterServerAttempts)
}

// Close closes the Interrupter's connections. KillQuery fails with
// ErrInterrupterClosed afterwards. It does not close the connector.
func (i *Interrupter) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.closed = true
	if i.db == nil {
		return nil
	}
	return i.db.Close()
}
