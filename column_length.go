// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

// ColumnLength reports the length of column index exactly as the server sent
// it in the column definition packet: for a DECIMAL, the precision plus one
// byte for the sign when the column is signed, plus one for the point when it
// has a scale; for a string, its maximum length in bytes of the connection's
// result character set (a VARCHAR(8) is 32 under utf8mb4); for an integer,
// its display width.
//
// It exists for callers that forward column definitions to their own MySQL
// clients, such as a proxy, and must send the server's length rather than one
// rebuilt from ColumnTypePrecisionScale. That rebuild needs the signedness of
// a DECIMAL, which no database/sql interface reports, so it cannot be exact.
//
// It is deliberately not database/sql's ColumnTypeLength: implementing that
// would change ColumnType.Length for every caller of this driver, which
// upstream keeps disabled. Reach it with an interface assertion on the
// driver.Rows a driver-level call returns, for example QueryResultContext:
//
//	if l, ok := rows.(interface{ ColumnLength(int) uint32 }); ok {
//		length = l.ColumnLength(i)
//	}
func (rows *mysqlRows) ColumnLength(index int) uint32 {
	return rows.rs.columns[index].length
}
