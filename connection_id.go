// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

// ConnectionID reports the server's thread ID for this connection, as sent in
// the initial handshake packet. It is the value CONNECTION_ID() returns on the
// connection and the ID that KILL and KILL QUERY take.
//
// It exists so a caller can interrupt a statement without closing the
// connection. When a context ends mid-statement the driver closes the socket,
// but the server does not notice a closed socket while it executes: the
// statement keeps running, holding its locks, until it finishes on its own. A
// KILL QUERY sent on another connection ends it with error 1317 and leaves
// this connection usable. Without the handshake value, learning the ID costs a
// SELECT CONNECTION_ID() round trip.
//
// The handshake field is 4 bytes wide. MySQL thread IDs fit; a server whose
// thread IDs exceed 32 bits reports a truncated value here.
//
// Reach it through (*sql.Conn).Raw and an interface assertion:
//
//	err := conn.Raw(func(dc any) error {
//		if c, ok := dc.(interface{ ConnectionID() uint32 }); ok {
//			id = c.ConnectionID()
//		}
//		return nil
//	})
//
// It is safe to call from any goroutine: the value is written once, during
// the handshake, before the connection is returned to database/sql.
func (mc *mysqlConn) ConnectionID() uint32 {
	return mc.connectionID
}
