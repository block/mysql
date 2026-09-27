# Canceled transaction returns a connection with LOCK TABLES still held

🤖 Drafted by Morgan's AI agent. Not yet filed upstream.

## Description

Canceling the context of an idle transaction can return its connection to the pool with explicit table locks still held. The next borrower then fails with error 1100 when it accesses another table. Other sessions can also remain blocked on the locked table while that connection sits idle.

This happens after `LOCK TABLES` has completed, with no query in flight when the context is canceled. Cancellation during an executing query takes a different path and can close the connection instead.

## Expected behavior

Before a canceled transaction's connection becomes available to another borrower, release its session locks or discard the connection. The next borrower should be able to access an unrelated table.

## Actual behavior

The next borrower receives the locked session and gets:

```text
next borrower inherited table locks: Error 1100 (HY000): Table 'cancel_lock_<timestamp>_other' was not locked with LOCK TABLES
```

## Reproducer

Use a disposable MySQL database with permission to create tables and acquire table locks. Save this as `cancel_test.go` in an empty directory. The test creates uniquely named tables and cleans them up, including an explicit unlock after the assertion.

```go
package cancel_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func TestCancelledTransactionReleasesTableLocks(t *testing.T) {
	db, err := sql.Open("mysql", os.Getenv("MYSQL_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	a := fmt.Sprintf("cancel_lock_%d", time.Now().UnixNano())
	b := a + "_other"
	for _, name := range []string{a, b} {
		if _, err := db.Exec("CREATE TABLE " + name + " (id INT PRIMARY KEY)"); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			// The one-connection pool makes cleanup reach the same leaked session.
			db.Exec("UNLOCK TABLES")
			db.Exec("DROP TABLE " + name)
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "LOCK TABLES "+a+" WRITE"); err != nil {
		t.Fatal(err)
	}
	cancel() // Idle transaction: no query is in flight when cancellation happens.

	nextCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	// Wait for automatic rollback to return the only connection. Do not begin
	// another transaction: START TRANSACTION would itself release table locks.
	conn, err := db.Conn(nextCtx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(nextCtx, "UPDATE "+b+" SET id = id"); err != nil {
		t.Fatalf("next borrower inherited table locks: %v", err)
	}
}
```

Run against the upstream driver, without the Block fork or a `replace` directive:

```sh
go mod init example.com/mysql-cancel-repro
go get github.com/go-sql-driver/mysql@789a82a35d04f8ab5a7b28707615ef8bf9d4f09b
MYSQL_DSN='root:password@tcp(127.0.0.1:3306)/test' go test -v -count=10 -timeout=30s
```

## Environment and result

- Upstream revision: `789a82a35d04f8ab5a7b28707615ef8bf9d4f09b` (`v1.10.2-0.20260919065905-789a82a35d04`).
- Go: 1.26.6.
- MySQL: Community Server 8.0.46, InnoDB.
- OS: macOS, arm64; local Unix socket.
- Result: all 10 runs failed with error 1100. The TCP DSN above is an example; the verified run used a Unix-socket DSN.

## Why this appears to happen

MySQL documents that `ROLLBACK` does not release locks acquired by `LOCK TABLES`. The concern here is the automatic return to the pool after cancellation, when the application no longer has a usable `sql.Tx` through which to unlock.

Go's `database/sql` automatically rolls back when the `BeginTx` context is canceled. It permits connection reuse after this rollback when the driver implements both `driver.SessionResetter` and `driver.Validator`. The driver's rollback sends `ROLLBACK`; `ResetSession` checks connection liveness, and `IsValid` checks that the connection is open and its buffer is not busy. Neither releases the remaining table locks.

The reproducer deliberately uses a plain statement for the next borrower. Starting another transaction would itself release the table locks and hide the problem. It also waits by borrowing the only connection, rather than using a sleep or assuming automatic rollback has already finished.

A detached context for cleanup on the old `sql.Tx` does not solve the problem once automatic rollback has completed: that transaction returns `sql.ErrTxDone`.

Possible remedies include discarding canceled transaction sessions or resetting them before pool return. A full `COM_RESET_CONNECTION` implementation would also need to restore configured session settings and handle invalidated prepared statements. This report does not assume that full resets for every transaction are the only solution.

References: [MySQL table-lock semantics](https://dev.mysql.com/doc/refman/8.0/en/lock-tables.html), [upstream driver connection handling](https://github.com/go-sql-driver/mysql/blob/789a82a35d04f8ab5a7b28707615ef8bf9d4f09b/connection.go), and the original downstream failure in [block/spirit#1276](https://github.com/block/spirit/issues/1276).
