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
)

// QueryResultContext executes query, after the statements in prelude, and
// returns query's response in the form the MySQL protocol describes it:
// exactly one of rows or result is non-nil on success. A statement that
// produces a resultset (even an empty one) yields rows; a statement answered
// with an OK packet (INSERT, UPDATE, DDL, SET, ...) yields result, carrying
// the affected-row count and last insert id. prelude may be nil.
//
// database/sql callers must choose between QueryContext and ExecContext
// before executing. A caller that receives arbitrary SQL (a proxy, a REPL)
// therefore has to classify statements up front, and a misclassification
// either discards a resultset (Exec) or loses the OK metadata (Query). This
// method lets such callers branch on what the server actually sent instead.
// It is intended to be reached through (*sql.Conn).Raw and a structural
// interface assertion:
//
//	err := conn.Raw(func(dc any) error {
//		uq := dc.(interface {
//			QueryResultContext(context.Context, *mysql.Prelude, string, []driver.NamedValue) (driver.Rows, driver.Result, error)
//		})
//		rows, result, err := uq.QueryResultContext(ctx, nil, query, args)
//		...
//	})
//
// Named parameters are rejected, as they are by QueryContext and ExecContext.
// Because Raw also bypasses database/sql's argument conversion, args are
// normalized here with CheckNamedValue, exactly as database/sql would do
// before QueryContext. As with QueryContext, non-empty args require
// InterpolateParams; otherwise driver.ErrSkip is returned before anything is
// written. driver.ErrBadConn is returned only when no command reached the
// server, so the caller may safely retry on it. When rows is non-nil the
// connection is busy until rows.Close.
//
// Unlike QueryContext, rows delivers every non-NULL cell as its MySQL wire
// text: a []byte aliasing the connection's read buffer, valid only until the
// next Next or Close call. Nothing is parsed into Go types (parseTime does
// not apply), so values forward verbatim — zero dates, float representations,
// and fractional-second padding survive untouched.
//
// # Prelude
//
// A non-empty prelude, built with NewPrelude, is sent ahead of query in the
// same round trip, as one multi-statement COM_QUERY. The prelude's responses
// are read and discarded, and query's response is returned as described
// above. The result's counters (RowsAffected, LastInsertId, AllRowsAffected)
// and the warning count describe query alone.
//
// A caller that must run a statement before another on the same connection —
// BEGIN before a transaction's first statement, or session settings before a
// statement that depends on them — would otherwise wait out one round trip
// per statement. That costs little against a local server and a full network
// round trip each against a remote one.
//
// The server runs no statement after one that fails, so when the server
// rejects a prelude statement query is not executed, and the error is a
// *PreludeError wrapping the server's *MySQLError. Any other error is query's
// own, or a read or write failure after which what the server executed is
// unknown: it may have run every statement, query included.
//
// Args apply to query alone, and are escaped for backslash escapes, which
// NewPrelude has made sure the prelude leaves on. On a connection in
// NO_BACKSLASH_ESCAPES mode a prelude with args is declined with
// driver.ErrSkip, since a prelude that sets sql_mode would turn the mode off
// before query is read.
//
// Sending several statements in one command needs the MultiStatements
// option; without it a non-empty prelude returns driver.ErrSkip. Every
// declined call returns before anything is written, so the caller can fall
// back to running the statements one at a time.
func (mc *mysqlConn) QueryResultContext(ctx context.Context, prelude *Prelude, query string, args []driver.NamedValue) (driver.Rows, driver.Result, error) {
	if mc.closed.Load() {
		return nil, nil, driver.ErrBadConn
	}

	// Reject named parameters exactly as QueryContext and ExecContext do.
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, nil, err
	}

	// Raw bypasses database/sql's argument conversion, which on the normal
	// path runs CheckNamedValue before the driver sees the args. Apply it
	// here so interpolateParams gets the same values either way.
	for i := range dargs {
		nv := driver.NamedValue{Ordinal: i + 1, Value: dargs[i]}
		if err := mc.CheckNamedValue(&nv); err != nil {
			return nil, nil, err
		}
		dargs[i] = nv.Value
	}

	if err := mc.watchCancel(ctx); err != nil {
		return nil, nil, err
	}

	rows, result, err := mc.queryResult(prelude, query, dargs)
	if err != nil || rows == nil {
		// Error, or a complete OK response: the connection is idle again.
		mc.finish()
		return nil, result, err
	}
	// Resultset: the context watcher stays armed until the caller finishes
	// reading, mirroring QueryContext.
	rows.finish = mc.finish
	return rows, nil, nil
}

// queryResult is the transport half of QueryResultContext. It mirrors
// mysqlConn.query, except that an OK response is surfaced as a driver.Result
// (the way Exec reports it) instead of being hidden inside an empty, done
// resultset, and that prelude statements, when there are any, are sent ahead
// of query in the same command and their responses discarded.
func (mc *mysqlConn) queryResult(prelude *Prelude, query string, args []driver.Value) (*textRows, driver.Result, error) {
	handleOk := mc.clearResult()

	count := prelude.Len()
	if count != 0 && !mc.cfg.MultiStatements {
		return nil, nil, driver.ErrSkip
	}
	if len(args) != 0 {
		if !mc.cfg.InterpolateParams {
			return nil, nil, driver.ErrSkip
		}
		// The args are escaped for the mode the connection is in now, and
		// the prelude must leave it: NewPrelude refuses
		// NO_BACKSLASH_ESCAPES, so only a connection already in that mode
		// could see it change, and there a prelude is declined.
		if count != 0 && mc.status&statusNoBackslashEscapes != 0 {
			return nil, nil, driver.ErrSkip
		}
		// try client-side prepare to reduce roundtrip
		prepared, err := mc.interpolateParams(query, args)
		if err != nil {
			return nil, nil, err
		}
		query = prepared
	}
	if count != 0 {
		// Interpolated first, so a '?' in a prelude statement is never
		// taken for a placeholder.
		query = prelude.text + query
	}

	// Send command
	if err := mc.writeCommandPacketStr(comQuery, query); err != nil {
		return nil, nil, mc.markBadConn(err)
	}

	// Each prelude statement answers in turn, and the server marks every
	// answer but the last with statusMoreResultsExists. An error ends the
	// response: nothing after the failed statement runs.
	for i := range count {
		resLen, _, err := handleOk.readResultSetHeaderPacket()
		if err != nil {
			return nil, nil, asPreludeError(i, err)
		}
		if resLen > 0 {
			if err := mc.skipColumns(resLen); err != nil {
				return nil, nil, asPreludeError(i, err)
			}
			// A statement can fail after its resultset has begun; the
			// server's error ends the response there too.
			if err := mc.skipRows(); err != nil {
				return nil, nil, asPreludeError(i, err)
			}
		}
		if mc.status&statusMoreResultsExists == 0 {
			return nil, nil, errPreludeIncomplete
		}
	}
	// Query's counters and warning count alone, not the prelude's: the
	// warning count is otherwise kept until a packet replaces it, and an
	// error packet from query would not.
	handleOk = mc.clearResult()
	if count != 0 {
		mc.warnings = 0
	}

	// Read Result
	resLen, _, err := handleOk.readResultSetHeaderPacket()
	if err != nil {
		return nil, nil, err
	}

	if resLen == 0 {
		// OK packet: no resultset follows. Drain any trailing results of a
		// multi-statement exactly like exec, then surface the accumulated
		// result the same way Exec does.
		if err := handleOk.discardResults(); err != nil {
			return nil, nil, err
		}
		copied := mc.result
		return nil, &copied, nil
	}

	// Columns
	rows := new(textRows)
	rows.mc = mc
	rows.raw = true
	rows.rs.columns, err = mc.readColumns(resLen, nil)
	if err != nil {
		return nil, nil, err
	}
	return rows, nil, nil
}
