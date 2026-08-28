// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

// Warnings reports the warning count the server sent in the packet that
// terminated the last statement executed on this connection — the OK packet
// for a statement without a resultset, the EOF (or 0xFE-headered OK) packet
// that ends the rows otherwise. It is the same number MySQL's own client
// prints as "N warnings" and exposes as @@warning_count.
//
// The count is what makes reading warnings affordable. MySQL keeps the
// diagnostics themselves in per-connection state that only SHOW WARNINGS can
// read, so a caller that wants them has to spend a round trip; the count says
// whether there is anything to spend it on. Clients that surface warnings —
// Connector/J among them — use it exactly that way.
//
// It is valid from the moment the statement's response is complete until the
// next statement starts on this connection, which resets it to zero. For a
// resultset that means after the rows have been fully read (or Close called):
// the terminating packet carrying the count has not arrived before then.
//
// An error packet carries no count of its own, so a statement that fails
// reports zero — the value the statement's own start left behind. Inside a
// multi-statement, where each statement gets its own OK packet, a failure
// instead leaves the last successful statement's count in place.
//
// Reach it through (*sql.Conn).Raw and an interface assertion:
//
//	err := conn.Raw(func(dc any) error {
//		if wc, ok := dc.(interface{ Warnings() uint16 }); ok && wc.Warnings() > 0 {
//			// SHOW WARNINGS on this same connection
//		}
//		return nil
//	})
//
// Note that database/sql may hand the connection to another caller as soon as
// it is released, so the count must be read before that happens.
func (mc *mysqlConn) Warnings() uint16 {
	return mc.warnings
}
