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
// connection. When the context of a running statement ends, the driver closes
// the connection: it is marked bad, its socket is closed, and it cannot be
// used again. The server does not notice a closed socket while it executes,
// so the statement keeps running, holding its locks, until it finishes on its
// own. A caller that wants the statement to end and the connection to stay
// usable must therefore run the statement on a context that does not end, and
// interrupt it out of band: KILL QUERY sent on another connection ends it
// with error 1317 and leaves this connection, and a transaction open on it,
// usable. Without the handshake value, learning the ID costs a
// SELECT CONNECTION_ID() round trip.
//
// The handshake field is 4 bytes wide, so a server whose thread IDs exceed 32
// bits reports only their low 32 bits here. MySQL's thread IDs are 32 bits
// wide, so the value is exact against MySQL. Against a server that can issue
// wider IDs, the truncated value may name a different connection: it must not
// be passed to KILL, which would then interrupt or end someone else's
// session. Compare it with SELECT CONNECTION_ID() first if the server is not
// known to use 32-bit IDs.
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
