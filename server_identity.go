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
	"fmt"
)

// serverIdentityQuery reads which server a connection is on. server_uuid
// alone tells MySQL servers apart; hostname is what tells Aurora instances
// apart, since they can share a server_uuid.
const serverIdentityQuery = "SELECT @@server_uuid, @@hostname"

// serverIdentity is readServerIdentity's cached answer.
type serverIdentity struct {
	uuid     string
	hostname string
}

// readServerIdentity reports which server this connection is on: its
// @@server_uuid and @@hostname. The first call reads them with one query on
// this connection; later calls return the cached values without a round trip,
// since a connection never moves to another server.
//
// It is what ReadKillTarget and Interrupter compare before a KILL QUERY sent
// from another connection. The second connection is dialed to the same
// address, but not necessarily the same server: an Aurora reader endpoint
// spreads new connections across replicas, and a writer endpoint moves on
// failover. A thread ID means something only on the server that issued it.
// Behind a proxy that reports variables of a server other than the one
// numbering its connections (a Vitess vtgate), the comparison proves nothing.
//
// Like every method of a connection, it must not run concurrently with other
// use of the connection. The first call is a statement, so it resets
// Warnings() to zero. A server without @@server_uuid (MariaDB) fails the
// first call, and every later one tries again.
func (mc *mysqlConn) readServerIdentity(ctx context.Context) (uuid, hostname string, err error) {
	if mc.serverIdentity != nil {
		return mc.serverIdentity.uuid, mc.serverIdentity.hostname, nil
	}
	rows, err := mc.QueryContext(ctx, serverIdentityQuery, nil)
	if err != nil {
		return "", "", err
	}
	values := make([]driver.Value, 2)
	identity := &serverIdentity{}
	// The row's []byte cells alias the connection's read buffer, and Close
	// reads the result's terminator into that buffer, so the values are
	// copied out of the row before the rows are closed.
	if err = rows.Next(values); err != nil {
		err = fmt.Errorf("reading the server identity: %w", err)
	} else if identity.uuid, err = identityString(values[0]); err != nil {
		err = fmt.Errorf("@@server_uuid: %w", err)
	} else if identity.hostname, err = identityString(values[1]); err != nil {
		err = fmt.Errorf("@@hostname: %w", err)
	}
	if closeErr := rows.Close(); err == nil && closeErr != nil {
		err = fmt.Errorf("reading the server identity: %w", closeErr)
	}
	if err != nil {
		return "", "", err
	}
	mc.serverIdentity = identity
	return identity.uuid, identity.hostname, nil
}

// identityString converts one text-protocol value of serverIdentityQuery to
// a string that owns its bytes.
func identityString(v driver.Value) (string, error) {
	switch v := v.(type) {
	case []byte:
		return string(v), nil
	case string:
		return v, nil
	case nil:
		return "", errors.New("is NULL")
	default:
		return "", fmt.Errorf("unexpected type %T", v)
	}
}
