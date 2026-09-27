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
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	comResetConnection      = 0x1f
	transactionResetTimeout = 5 * time.Second
)

// validateTransactionSession runs in driver.Validator, before database/sql
// returns the connection to the pool. ResetSession runs on checkout, which is
// too late: an idle session can still hold LOCK TABLES or GET_LOCK locks.
// A transaction on a reserved sql.Conn does not call Validator until Conn.Close.
// Cleanup failure must not change the result of an already successful COMMIT.
func (mc *mysqlConn) validateTransactionSession() bool {
	if !mc.needsTransactionReset {
		return true
	}
	// Reset destroys prepared statement IDs still cached by database/sql. Let
	// database/sql replace this connection and reprepare its surviving statements.
	// COM_RESET_CONNECTION also preserves the selected database; without a
	// configured database we cannot restore "no database selected" after USE.
	if mc.openStatements != 0 || mc.cfg.DBName == "" {
		mc.cleanup()
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), transactionResetTimeout)
	defer cancel()
	if err := mc.resetTransactionSession(ctx); err != nil {
		mc.log("discarding connection after transaction session reset: ", err)
		mc.cleanup()
		return false
	}
	mc.needsTransactionReset = false
	return !mc.closed.Load()
}

func (mc *mysqlConn) resetTransactionSession(ctx context.Context) error {
	// The watcher bounds the entire sequence, even with no configured socket
	// timeouts. It is independent of the canceled BeginTx context.
	if err := mc.watchCancel(ctx); err != nil {
		return err
	}
	defer mc.finish()
	handleOK := mc.clearResult()
	if err := mc.writeCommandPacket(comResetConnection); err != nil {
		return err
	}
	if err := handleOK.readResultOK(); err != nil {
		return err
	}
	mc.inReadOnlyTx = false
	if err := mc.configurePacketSize(); err != nil {
		return err
	}
	// Restore the handshake's character set baseline before applying the same
	// initialization as a fresh connection. Match the handshake fallback for
	// collations not representable by its one-byte collation field.
	collation := "utf8mb4_general_ci"
	if _, ok := collations[mc.cfg.Collation]; ok {
		collation = mc.cfg.Collation
	}
	charset, _, _ := strings.Cut(collation, "_")
	if err := mc.exec(fmt.Sprintf("SET character_set_client = '%s', character_set_results = '%s', collation_connection = '%s'", charset, charset, collation)); err != nil {
		return err
	}
	if err := mc.writeCommandPacketStr(comInitDB, mc.cfg.DBName); err != nil {
		return err
	}
	if err := mc.clearResult().readResultOK(); err != nil {
		return err
	}
	if err := mc.initializeSession(); err != nil {
		return err
	}
	mc.clearResult()
	mc.warnings = 0
	return nil
}

// initializeSession is shared by initial connection setup and pool-return
// cleanup. Use the effective per-connection config, not a new BeforeConnect call.
func (mc *mysqlConn) initializeSession() error {
	if len(mc.cfg.charsets) > 0 {
		var err error
		for _, cs := range mc.cfg.charsets {
			if mc.cfg.Collation != "" {
				err = mc.exec("SET NAMES " + cs + " COLLATE " + mc.cfg.Collation)
			} else {
				err = mc.exec("SET NAMES " + cs)
			}
			if err == nil {
				break
			}
		}
		if err != nil {
			return err
		}
	}
	return mc.handleParams()
}

// COM_RESET_CONNECTION adopts current global values, including the server's
// packet limit. Refresh the cache when maxAllowedPacket=0 requests discovery.
func (mc *mysqlConn) configurePacketSize() error {
	if mc.cfg.MaxAllowedPacket > 0 {
		mc.maxAllowedPacket = mc.cfg.MaxAllowedPacket
	} else {
		maxap, err := mc.getSystemVar("max_allowed_packet")
		if err != nil {
			return err
		}
		n, err := strconv.Atoi(maxap)
		if err != nil {
			return fmt.Errorf("invalid max_allowed_packet value (%q): %w", maxap, err)
		}
		mc.maxAllowedPacket = n - 1
	}
	mc.maxWriteSize = maxPacketSize - 1
	if mc.maxAllowedPacket < maxPacketSize {
		mc.maxWriteSize = mc.maxAllowedPacket
	}
	return nil
}
