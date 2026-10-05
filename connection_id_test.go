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
	"testing"
)

func TestReadHandshakePacketConnectionID(t *testing.T) {
	conn := new(mockConn)
	mc := &mysqlConn{
		netConn:  conn,
		buf:      newBuffer(),
		cfg:      new(Config),
		sequence: 42,
		closech:  make(chan struct{}),
	}

	// The handshake from TestRegression801 with the connection id changed to
	// 0x04030201, so every byte position is distinguishable.
	conn.data = []byte{72, 0, 0, 42, 10, 53, 46, 53, 46, 56, 0, 1, 2, 3, 4,
		60, 70, 63, 58, 68, 104, 34, 97, 0, 223, 247, 33, 2, 0, 15, 128, 21, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 98, 120, 114, 47, 85, 75, 109, 99, 51, 77,
		50, 64, 0, 109, 121, 115, 113, 108, 95, 110, 97, 116, 105, 118, 101, 95,
		112, 97, 115, 115, 119, 111, 114, 100}
	conn.maxReads = 1

	if _, _, _, _, err := mc.readHandshakePacket(); err != nil {
		t.Fatalf("readHandshakePacket: %v", err)
	}
	if got, want := mc.ConnectionID(), uint32(0x04030201); got != want {
		t.Errorf("ConnectionID: got %#x, want %#x", got, want)
	}
}

func TestConnectionIDMatchesServer(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		ctx := context.Background()
		conn, err := dbt.db.Conn(ctx)
		if err != nil {
			dbt.Fatalf("Conn: %v", err)
		}
		defer conn.Close()

		var fromServer uint64
		if err := conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&fromServer); err != nil {
			dbt.Fatalf("SELECT CONNECTION_ID(): %v", err)
		}
		var fromHandshake uint32
		if err := conn.Raw(func(dc any) error {
			fromHandshake = dc.(interface{ ConnectionID() uint32 }).ConnectionID()
			return nil
		}); err != nil {
			dbt.Fatalf("Raw: %v", err)
		}
		if uint64(fromHandshake) != fromServer {
			dbt.Errorf("ConnectionID: handshake reported %d, CONNECTION_ID() returned %d", fromHandshake, fromServer)
		}
	})
}
