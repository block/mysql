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
	"testing"
)

type serverIdentifier interface {
	ServerIdentity(context.Context) (string, string, error)
}

func TestServerIdentityMatchesServerAndIsCached(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		ctx := context.Background()
		conn, err := dbt.db.Conn(ctx)
		if err != nil {
			dbt.Fatalf("Conn: %v", err)
		}
		defer conn.Close()

		var wantUUID, wantHostname string
		if err := conn.QueryRowContext(ctx, serverIdentityQuery).Scan(&wantUUID, &wantHostname); err != nil {
			dbt.Fatalf("%s: %v", serverIdentityQuery, err)
		}
		if err := conn.Raw(func(dc any) error {
			uuid, hostname, err := dc.(serverIdentifier).ServerIdentity(ctx)
			if err != nil {
				dbt.Fatalf("ServerIdentity: %v", err)
			}
			if uuid != wantUUID || hostname != wantHostname {
				dbt.Errorf("ServerIdentity: got (%q, %q), want (%q, %q)", uuid, hostname, wantUUID, wantHostname)
			}

			// The second call must not touch the connection: with a
			// cancelled context, a query would fail before it is sent.
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			uuid, hostname, err = dc.(serverIdentifier).ServerIdentity(cancelled)
			if err != nil {
				dbt.Fatalf("cached ServerIdentity: %v", err)
			}
			if uuid != wantUUID || hostname != wantHostname {
				dbt.Errorf("cached ServerIdentity: got (%q, %q), want (%q, %q)", uuid, hostname, wantUUID, wantHostname)
			}
			return nil
		}); err != nil {
			dbt.Fatalf("Raw: %v", err)
		}
	})
}

func TestServerIdentityFailureIsNotCached(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		ctx := context.Background()
		conn, err := dbt.db.Conn(ctx)
		if err != nil {
			dbt.Fatalf("Conn: %v", err)
		}
		defer conn.Close()

		if err := conn.Raw(func(dc any) error {
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if _, _, err := dc.(serverIdentifier).ServerIdentity(cancelled); err == nil {
				dbt.Fatal("ServerIdentity with a cancelled context: got nil error")
			}
			if _, _, err := dc.(serverIdentifier).ServerIdentity(ctx); err != nil {
				dbt.Fatalf("ServerIdentity after a failed call: %v", err)
			}
			return nil
		}); err != nil && err != driver.ErrBadConn {
			dbt.Fatalf("Raw: %v", err)
		}
	})
}

func TestIdentityString(t *testing.T) {
	for _, tc := range []struct {
		in      driver.Value
		want    string
		wantErr bool
	}{
		{in: []byte("abc"), want: "abc"},
		{in: "abc", want: "abc"},
		{in: nil, wantErr: true},
		{in: int64(1), wantErr: true},
	} {
		got, err := identityString(tc.in)
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Errorf("identityString(%#v) = (%q, %v), want (%q, error=%v)", tc.in, got, err, tc.want, tc.wantErr)
		}
	}
}
