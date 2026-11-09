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

// TestColumnLengthIsTheServersLength checks ColumnLength against the lengths
// MySQL sends for DECIMAL columns, signed and unsigned, on both the text and
// the binary protocol. The unsigned columns are the ones a length rebuilt from
// ColumnTypePrecisionScale gets wrong.
func TestColumnLengthIsTheServersLength(t *testing.T) {
	runTestsParallel(t, dsn, func(dbt *DBTest, tbl string) {
		ctx := context.Background()
		dbt.mustExec("CREATE TABLE " + tbl + " (dc DECIMAL(12,4), whole DECIMAL(10,0), udc DECIMAL(12,4) UNSIGNED, u11 DECIMAL(1,1) UNSIGNED, u10 DECIMAL(1,0) UNSIGNED)")
		want := []uint32{14, 11, 13, 2, 1}
		query := "SELECT dc, whole, udc, u11, u10 FROM " + tbl

		conn, err := dbt.db.Conn(ctx)
		if err != nil {
			dbt.Fatalf("Conn: %v", err)
		}
		defer conn.Close()
		check := func(protocol string, rows driver.Rows) {
			defer rows.Close()
			lengther, ok := rows.(interface{ ColumnLength(int) uint32 })
			if !ok {
				dbt.Fatalf("%s rows %T have no ColumnLength", protocol, rows)
			}
			for i, w := range want {
				if got := lengther.ColumnLength(i); got != w {
					dbt.Errorf("%s column %d: ColumnLength %d, want %d", protocol, i, got, w)
				}
			}
		}
		if err := conn.Raw(func(dc any) error {
			text, err := dc.(driver.QueryerContext).QueryContext(ctx, query, nil)
			if err != nil {
				return err
			}
			check("text", text)

			stmt, err := dc.(driver.ConnPrepareContext).PrepareContext(ctx, query)
			if err != nil {
				return err
			}
			defer stmt.Close()
			binary, err := stmt.(driver.StmtQueryContext).QueryContext(ctx, nil)
			if err != nil {
				return err
			}
			check("binary", binary)
			return nil
		}); err != nil {
			dbt.Fatalf("Raw: %v", err)
		}
	})
}
