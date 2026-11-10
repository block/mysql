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
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

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
			uuid, hostname, err := dc.(*mysqlConn).readServerIdentity(ctx)
			if err != nil {
				dbt.Fatalf("readServerIdentity: %v", err)
			}
			if uuid != wantUUID || hostname != wantHostname {
				dbt.Errorf("readServerIdentity: got (%q, %q), want (%q, %q)", uuid, hostname, wantUUID, wantHostname)
			}

			// The second call must not touch the connection: with a
			// cancelled context, a query would fail before it is sent.
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			uuid, hostname, err = dc.(*mysqlConn).readServerIdentity(cancelled)
			if err != nil {
				dbt.Fatalf("cached readServerIdentity: %v", err)
			}
			if uuid != wantUUID || hostname != wantHostname {
				dbt.Errorf("cached readServerIdentity: got (%q, %q), want (%q, %q)", uuid, hostname, wantUUID, wantHostname)
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
			if _, _, err := dc.(*mysqlConn).readServerIdentity(cancelled); err == nil {
				dbt.Fatal("readServerIdentity with a cancelled context: got nil error")
			}
			if _, _, err := dc.(*mysqlConn).readServerIdentity(ctx); err != nil {
				dbt.Fatalf("readServerIdentity after a failed call: %v", err)
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

// packetPerReadConn delivers each of its reads in its own Read call, so a test
// controls where a packet boundary falls relative to the read buffer.
type packetPerReadConn struct {
	mockConn
	reads [][]byte
}

func (c *packetPerReadConn) Read(b []byte) (int, error) {
	if len(c.reads) == 0 {
		return 0, io.EOF
	}
	n := copy(b, c.reads[0])
	if c.reads[0] = c.reads[0][n:]; len(c.reads[0]) == 0 {
		c.reads = c.reads[1:]
	}
	return n, nil
}

// TestServerIdentitySurvivesSeparateTerminator: the row's cells alias the
// connection's read buffer, and closing the rows reads the result's
// terminator into that buffer. When the row and the terminator arrive in
// separate reads, the terminator lands where the row was, so the identity
// must be copied out of the row before the rows are closed.
func TestServerIdentitySurvivesSeparateTerminator(t *testing.T) {
	const (
		uuid     = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
		hostname = "db-1"
	)
	_, mc := newRWMockConn(0)
	conn := &packetPerReadConn{reads: [][]byte{identityHeaderPackets(), identityRowPacket(uuid, hostname), identityEOFPacket(6)}}
	mc.netConn = conn

	gotUUID, gotHostname, err := mc.readServerIdentity(context.Background())
	if err != nil {
		t.Fatalf("readServerIdentity: %v", err)
	}
	if gotUUID != uuid || gotHostname != hostname {
		t.Fatalf("readServerIdentity: got (%q, %q), want (%q, %q)", gotUUID, gotHostname, uuid, hostname)
	}
	if len(conn.reads) != 0 {
		t.Fatalf("%d reads left unconsumed", len(conn.reads))
	}
}

// stallingConn delivers its reads, then blocks until it is closed, as a
// server would that stops sending mid-result.
type stallingConn struct {
	packetPerReadConn
	closed    chan struct{}
	closeOnce sync.Once
}

func (c *stallingConn) Read(b []byte) (int, error) {
	if len(c.reads) > 0 {
		return c.packetPerReadConn.Read(b)
	}
	<-c.closed
	return 0, net.ErrClosed
}

func (c *stallingConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

// TestServerIdentityHonoursTheContextWhileReadingTheTerminator: a terminator
// that never arrives must end with the context, not block the read forever.
// Rows.Close stops watching the context before it reads the terminator, so
// the terminator has to be read before the rows are closed.
func TestServerIdentityHonoursTheContextWhileReadingTheTerminator(t *testing.T) {
	_, mc := newRWMockConn(0)
	conn := &stallingConn{
		packetPerReadConn: packetPerReadConn{reads: [][]byte{identityHeaderPackets(), identityRowPacket("uuid", "db-1")}},
		closed:            make(chan struct{}),
	}
	mc.netConn, mc.rawConn = conn, conn
	mc.startWatcher()
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, _, err := mc.readServerIdentity(ctx)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("readServerIdentity: got nil error from a result with no terminator")
		}
		if mc.serverIdentity != nil {
			t.Error("a failed read must not be cached")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("readServerIdentity outlived its context waiting for the terminator")
	}
}

// The packets of serverIdentityQuery's result: the header (sequence 1-4),
// one row (5), and the terminator.

func identityLenenc(s string) []byte { return append([]byte{byte(len(s))}, s...) }

func identityPacket(seq byte, payload ...[]byte) []byte {
	var body []byte
	for _, p := range payload {
		body = append(body, p...)
	}
	return append([]byte{byte(len(body)), byte(len(body) >> 8), byte(len(body) >> 16), seq}, body...)
}

func identityColumnPacket(seq byte, name string) []byte {
	l := identityLenenc
	return identityPacket(seq, l("def"), l(""), l(""), l(""), l(name), l(""),
		[]byte{0x0c, 0x21, 0x00, 0xff, 0x00, 0x00, 0x00, byte(fieldTypeVarString), 0x00, 0x00, 0x00, 0x00, 0x00})
}

func identityEOFPacket(seq byte) []byte {
	return identityPacket(seq, []byte{iEOF, 0x00, 0x00, 0x02, 0x00})
}

func identityHeaderPackets() []byte {
	var header []byte
	header = append(header, identityPacket(1, []byte{0x02})...)
	header = append(header, identityColumnPacket(2, "@@server_uuid")...)
	header = append(header, identityColumnPacket(3, "@@hostname")...)
	header = append(header, identityEOFPacket(4)...)
	return header
}

func identityRowPacket(uuid, hostname string) []byte {
	return identityPacket(5, identityLenenc(uuid), identityLenenc(hostname))
}
