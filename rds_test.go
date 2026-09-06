// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"strings"
	"testing"
	"time"
)

func TestIsRDSAddr(t *testing.T) {
	for _, tt := range []struct {
		addr string
		want bool
	}{
		// Real endpoint shapes: instance, cluster writer, cluster reader,
		// and a GovCloud region (still under .rds.amazonaws.com).
		{"mydb.cxyz123.us-east-1.rds.amazonaws.com:3306", true},
		{"mydb.cxyz123.us-east-1.rds.amazonaws.com", true},
		{"mycluster.cluster-cxyz123.eu-west-1.rds.amazonaws.com:3306", true},
		{"mycluster.cluster-ro-cxyz123.eu-west-1.rds.amazonaws.com:3306", true},
		{"mydb.cxyz123.us-gov-west-1.rds.amazonaws.com:3306", true},

		// The leading dot in the pattern: a host that merely ends with the
		// string is not an RDS endpoint.
		{"notrds.amazonaws.com:3306", false},
		{"my-rds.amazonaws.com", false},

		// Anchored at the end: an RDS-looking label in the middle of some
		// other domain does not match.
		{"mydb.cxyz123.us-east-1.rds.amazonaws.com.example.test:3306", false},

		// Other partitions publish their own trust stores, so the embedded
		// bundle would not verify them anyway (see rdsGlobalBundle).
		{"mydb.cxyz123.rds.cn-north-1.amazonaws.com.cn:3306", false},

		{"127.0.0.1:3306", false},
		{"localhost", false},
		{"/tmp/mysql.sock", false},
		{"", false},
	} {
		if got := IsRDSAddr(tt.addr); got != tt.want {
			t.Errorf("IsRDSAddr(%q) = %v, want %v", tt.addr, got, tt.want)
		}
	}
}

// TestRDSGlobalBundleParses guards the embedded bundle itself. A malformed or
// truncated PEM yields an empty pool and fails every RDS connection at
// handshake time, which is a long way from the mistake; refreshing the bundle
// is the moment that can happen.
func TestRDSGlobalBundleParses(t *testing.T) {
	blocks := strings.Count(string(rdsGlobalBundle), "-----BEGIN CERTIFICATE-----")
	if blocks == 0 {
		t.Fatal("embedded RDS bundle contains no PEM certificate blocks")
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(rdsGlobalBundle) {
		t.Fatal("x509: no certificates in the embedded RDS bundle could be parsed")
	}
	// AppendCertsFromPEM reports success if it parsed *any* certificate, so
	// walk the bundle to catch the case where most of it failed to parse.
	// Counting unexpired roots at the same time: a bundle whose roots have all
	// expired parses fine and then fails every verification. Expired roots are
	// normal (Amazon leaves superseded ones in place), so require only that
	// some usable ones remain.
	var parsed, live int
	for rest := rdsGlobalBundle; ; {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Errorf("certificate %d in the embedded RDS bundle does not parse: %v", parsed+1, err)
			continue
		}
		parsed++
		if time.Now().Before(cert.NotAfter) {
			live++
		}
	}
	if parsed != blocks {
		t.Errorf("parsed %d certificates, bundle has %d PEM blocks", parsed, blocks)
	}
	if live == 0 {
		t.Error("every root in the embedded RDS bundle has expired; refresh it (see rdsGlobalBundle)")
	}
	t.Logf("embedded RDS bundle: %d roots, %d unexpired", parsed, live)
}

// TestRDSAutoTLS covers what normalize() does with an RDS address, which is
// where the DSN's own TLS setting has to keep winning.
func TestRDSAutoTLS(t *testing.T) {
	const rdsHost = "mydb.cxyz123.us-east-1.rds.amazonaws.com"

	t.Run("applied when the DSN says nothing", func(t *testing.T) {
		cfg, err := ParseDSN("user:pass@tcp(" + rdsHost + ":3306)/db")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.TLS == nil {
			t.Fatal("cfg.TLS is nil; RDS auto-TLS did not fire")
		}
		if cfg.TLS.RootCAs == nil {
			t.Error("cfg.TLS.RootCAs is nil; the connection would verify against the system pool, not the RDS bundle")
		}
		if cfg.TLS.InsecureSkipVerify {
			t.Error("cfg.TLS.InsecureSkipVerify is true; auto-TLS must verify")
		}
		if cfg.TLS.MinVersion != tls.VersionTLS12 {
			t.Errorf("cfg.TLS.MinVersion = %#04x, want TLS 1.2 (%#04x)", cfg.TLS.MinVersion, tls.VersionTLS12)
		}
		// normalize() fills ServerName from the address, which is what makes
		// this identity verification rather than just encryption.
		if cfg.TLS.ServerName != rdsHost {
			t.Errorf("cfg.TLS.ServerName = %q, want %q", cfg.TLS.ServerName, rdsHost)
		}
		if cfg.AllowFallbackToPlaintext {
			t.Error("AllowFallbackToPlaintext is true; auto-TLS must not silently downgrade")
		}
	})

	t.Run("tls=false wins", func(t *testing.T) {
		// The documented opt-out. If this ever regresses, an operator who
		// deliberately disabled TLS gets it back without being told.
		cfg, err := ParseDSN("user:pass@tcp(" + rdsHost + ":3306)/db?tls=false")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.TLS != nil {
			t.Error("cfg.TLS is set despite tls=false")
		}
	})

	t.Run("tls=skip-verify wins", func(t *testing.T) {
		cfg, err := ParseDSN("user:pass@tcp(" + rdsHost + ":3306)/db?tls=skip-verify")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.TLS == nil {
			t.Fatal("cfg.TLS is nil")
		}
		if !cfg.TLS.InsecureSkipVerify {
			t.Error("InsecureSkipVerify is false; the DSN asked for skip-verify")
		}
		if cfg.TLS.RootCAs != nil {
			t.Error("RootCAs is set; auto-TLS overrode an explicit skip-verify")
		}
	})

	t.Run("a registered config wins", func(t *testing.T) {
		const name = "rds-test-custom"
		if err := RegisterTLSConfig(name, &tls.Config{ServerName: "sentinel", MinVersion: tls.VersionTLS13}); err != nil {
			t.Fatal(err)
		}
		defer DeregisterTLSConfig(name)

		cfg, err := ParseDSN("user:pass@tcp(" + rdsHost + ":3306)/db?tls=" + name)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.TLS == nil {
			t.Fatal("cfg.TLS is nil")
		}
		if cfg.TLS.ServerName != "sentinel" {
			t.Errorf("cfg.TLS.ServerName = %q, want the registered config's %q", cfg.TLS.ServerName, "sentinel")
		}
	})

	t.Run("a config set on the struct wins", func(t *testing.T) {
		// NewConnector's path: no DSN string involved.
		cfg := NewConfig()
		cfg.User, cfg.Passwd, cfg.Net, cfg.DBName = "user", "pass", "tcp", "db"
		cfg.Addr = rdsHost + ":3306"
		cfg.TLS = &tls.Config{ServerName: "sentinel", MinVersion: tls.VersionTLS13}
		if err := cfg.normalize(); err != nil {
			t.Fatal(err)
		}
		if cfg.TLS.ServerName != "sentinel" {
			t.Errorf("cfg.TLS.ServerName = %q, want the caller's %q", cfg.TLS.ServerName, "sentinel")
		}
	})

	t.Run("not applied to a non-RDS host", func(t *testing.T) {
		cfg, err := ParseDSN("user:pass@tcp(127.0.0.1:3306)/db")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.TLS != nil {
			t.Error("cfg.TLS is set for a non-RDS address")
		}
	})

	t.Run("each config is independent", func(t *testing.T) {
		// normalize() writes ServerName into cfg.TLS. Were the configs shared,
		// the second parse would rename the first connection's expected
		// identity — and both would then verify against one of the two names.
		a, err := ParseDSN("user:pass@tcp(a.cxyz123.us-east-1.rds.amazonaws.com:3306)/db")
		if err != nil {
			t.Fatal(err)
		}
		b, err := ParseDSN("user:pass@tcp(b.cxyz123.us-east-1.rds.amazonaws.com:3306)/db")
		if err != nil {
			t.Fatal(err)
		}
		if a.TLS == b.TLS {
			t.Fatal("both configs share one *tls.Config")
		}
		if a.TLS.ServerName == b.TLS.ServerName {
			t.Errorf("both configs verify %q; the second parse overwrote the first", a.TLS.ServerName)
		}
	})
}
