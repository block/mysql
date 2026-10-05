// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

// ConnectionID reports the thread ID that the server answering the handshake
// gave this connection, as sent in the initial handshake packet. Connected
// directly to MySQL, it is the value CONNECTION_ID() returns on the
// connection and the ID that KILL and KILL QUERY take.
//
// It exists so a caller can interrupt a statement without closing the
// connection. When the context of a running statement ends, the driver closes
// the connection: it is marked bad, its socket is closed, and it cannot be
// used again. The server does not notice a closed socket while it executes,
// so the statement keeps running, holding its locks, until it finishes on its
// own. A caller that wants the statement to end and the connection to stay
// usable must therefore run the statement on a context that does not end, and
// interrupt it out of band: KILL QUERY sent on another connection ends it and
// leaves this connection, and a transaction open on it, usable. A statement
// waiting on a lock, or reading or writing rows, fails with error 1317; an
// interrupted SELECT SLEEP(n) returns 1 with no error. Without the handshake
// value, learning the ID costs a SELECT CONNECTION_ID() round trip.
//
// KILL QUERY names a thread, not a statement: it interrupts whatever that
// thread is running when the kill arrives. A kill that arrives while the
// connection is idle is discarded. So until the KILL QUERY has been sent, or
// the decision to send it abandoned, the caller must keep the *sql.Conn and
// start no other statement on it. Otherwise a kill meant for a statement that
// has just finished interrupts the caller's next statement, or, once the
// connection is back in the pool, another caller's.
//
// The ID is only as good as the process that issued it. Behind a proxy that
// answers the handshake itself, such as a connection pooler or a sharding
// gateway, it is the proxy's ID for the client connection. It need not match
// the backend's CONNECTION_ID(), or any thread a KILL sent to the backend
// would find. The handshake field is also 4 bytes wide, so a server whose
// thread IDs exceed 32 bits reports only their low 32 bits here. MySQL's
// thread IDs are 32 bits wide, so against MySQL itself the value is exact.
// In either case the value may name a different connection, and passing it to
// KILL would interrupt or end someone else's session. Unless the connection
// is known to go directly to a server with 32-bit thread IDs, compare the
// value with SELECT CONNECTION_ID() before using it with KILL.
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
