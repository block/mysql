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
	_ "embed"
	"regexp"
	"sync"
)

// rdsGlobalBundle is Amazon's global RDS certificate bundle, containing the
// root CAs for every RDS region in the aws partition.
//
// Refresh it from the canonical location, which is a concatenation of the
// per-region bundles and is what the AWS documentation tells operators to
// download:
//
//	curl -o rdsGlobalBundle.pem https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
//
// Adding a root is backwards compatible, so refreshing early costs nothing.
// The bundle covers the `aws` partition only; the China partition
// (`amazonaws.com.cn`) publishes a separate trust store, which is why rdsAddr
// does not match its endpoint forms — see RDSTLSConfig for how to use a
// different bundle.
//
//go:embed rdsGlobalBundle.pem
var rdsGlobalBundle []byte

// rdsAddr matches an Amazon RDS or Aurora endpoint, with an optional port.
//
// The leading dot is load-bearing: without it the pattern also accepts
// `notrds.amazonaws.com`, so a host outside RDS could pull a connection onto
// the RDS trust store. That misfires safely — verification against RDS roots
// fails, rather than trusting the wrong CA — but a confusing handshake error
// is still worse than not matching.
var rdsAddr = regexp.MustCompile(`\.rds\.amazonaws\.com(:\d+)?$`)

// IsRDSAddr reports whether addr is an Amazon RDS or Aurora endpoint, with or
// without a port. Connections to such an address are given TLS automatically;
// see RDSTLSConfig.
func IsRDSAddr(addr string) bool {
	return rdsAddr.MatchString(addr)
}

// rdsRootCAs parses the embedded bundle once. A CertPool is safe for
// concurrent use once built, and every TLS config below shares this one.
var rdsRootCAs = sync.OnceValue(func() *x509.CertPool {
	pool := x509.NewCertPool()
	// A parse failure here would mean the embedded bundle is malformed, which
	// is a build-time mistake rather than a runtime condition: the result is an
	// empty pool, and every RDS connection then fails to verify. There is
	// nothing useful to do about it at this point in the call path, and
	// TestRDSGlobalBundleParses catches it before it can ship.
	pool.AppendCertsFromPEM(rdsGlobalBundle)
	return pool
})

// RDSTLSConfig returns a TLS configuration that verifies an Amazon RDS or
// Aurora server against the embedded RDS root bundle.
//
// Connections to an address IsRDSAddr recognizes get this automatically, so
// most callers never need it. Use it for an RDS instance reached under a name
// that does not look like one — a CNAME, or a proxy — by registering it and
// naming it in the DSN:
//
//	mysql.RegisterTLSConfig("rds", mysql.RDSTLSConfig())
//	db, _ := sql.Open("block-mysql", "user:pass@tcp(db.internal:3306)/schema?tls=rds")
//
// Each call returns a new config, so it can be modified freely — to trust a
// different partition's bundle, for instance. The returned config verifies the
// server name, so a proxy must present a certificate for the name dialed.
func RDSTLSConfig() *tls.Config {
	return &tls.Config{
		RootCAs: rdsRootCAs(),
		// RDS has supported TLS 1.2 everywhere for years, and 1.0/1.1 are
		// deprecated. Upstream leaves this to the Go default (currently 1.2 for
		// clients); pinning it means a future default change cannot quietly
		// weaken an RDS connection.
		MinVersion: tls.VersionTLS12,
	}
}

// applyRDSAutoTLS gives a connection to an RDS endpoint a verified TLS
// configuration when the DSN did not ask for one.
//
// This is a fork addition (see README.md). Upstream leaves TLS entirely to the
// DSN, which in an RDS deployment means every caller has to carry the bundle
// and wire up `tls=` itself — Block had three separate copies of exactly that
// before this existed. Doing it in the driver makes the safe thing the default
// while leaving it fully overridable: any explicit `tls=` in the DSN, including
// `tls=false`, is honoured, because this only fires when nothing else set one.
func (cfg *Config) applyRDSAutoTLS() {
	if cfg.TLS != nil || cfg.TLSConfig != "" {
		return
	}
	if !IsRDSAddr(cfg.Addr) {
		return
	}
	cfg.TLS = RDSTLSConfig()
}
