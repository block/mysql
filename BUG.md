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
committing the transaction failed but that was expected: context canceled
ExecContext could not update lockable_table2: Error 1100 (HY000): Table 'lockable_table2' was not locked with LOCK TABLES
Exec could not update lockable_table2: Error 1100 (HY000): Table 'lockable_table2' was not locked with LOCK TABLES
ExecContext can now update lockable_table2!
```

## Reproducer

Use a disposable MySQL database with permission to create tables and acquire table locks. Save this as `main.go` in an empty directory and adjust the DSN. The program leaves two tables behind and updates any rows they contain.

The commit error can be either `context canceled` or `sql: transaction has already been committed or rolled back`, depending on when automatic rollback runs.

```go
package main

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	db, err := sql.Open("mysql", "root:@tcp(127.0.0.1:3306)/test")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS lockable_table1 (a int)")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS lockable_table2 (a int)")
	if err != nil {
		panic(err)
	}

	// Start a transaction that we will use to acquire a lock.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		panic(err)
	}
	if _, err := tx.ExecContext(ctx, "LOCK TABLES lockable_table1 WRITE"); err != nil {
		panic(err)
	}
	// Cancel after acquiring the lock, with no query in flight. This could
	// happen when the caller cancels the operation or its deadline expires.
	cancel()

	// The operation might still try to commit, unaware of the cancellation.
	// The canceled transaction can no longer be used to release its locks.
	if err := tx.Commit(); err != nil {
		fmt.Printf("committing the transaction failed but that was expected: %v\n", err)
	}

	// The next borrower inherits the table locks. With only one connection,
	// ExecContext waits for automatic rollback before borrowing that session.
	// A locked session cannot modify tables outside its LOCK TABLES list.
	_, err = db.ExecContext(context.Background(), "UPDATE lockable_table2 SET a = 1234")
	if err != nil {
		fmt.Printf("ExecContext could not update lockable_table2: %v\n", err)
	}

	// Exec without an explicit context has the same problem.
	_, err = db.Exec("UPDATE lockable_table2 SET a = 1234")
	if err != nil {
		fmt.Printf("Exec could not update lockable_table2: %v\n", err)
	}

	// Starting a fresh transaction implicitly releases the old table locks.
	// It is START TRANSACTION, not the rollback below, that releases them.
	tx, err = db.BeginTx(context.Background(), nil)
	if err != nil {
		panic(err)
	}
	if err := tx.Rollback(); err != nil {
		panic(err)
	}

	// Now the same update works.
	_, err = db.ExecContext(context.Background(), "UPDATE lockable_table2 SET a = 1234")
	if err != nil {
		panic(err)
	}
	fmt.Println("ExecContext can now update lockable_table2!")
}
```

Run against the upstream driver, without the Block fork or a `replace` directive:

```sh
go mod init example.com/mysql-cancel-repro
go get github.com/go-sql-driver/mysql@789a82a35d04f8ab5a7b28707615ef8bf9d4f09b
go run main.go
```

## Environment and result

- Upstream revision: `789a82a35d04f8ab5a7b28707615ef8bf9d4f09b` (`v1.10.2-0.20260919065905-789a82a35d04`).
- Go: 1.26.6.
- MySQL: Community Server 8.0.46, InnoDB.
- OS: macOS, arm64; local Unix socket.
- Result: all 10 runs printed error 1100 for both plain updates, then completed the final update successfully. The TCP DSN above is an example; the verified run used a Unix-socket DSN.

## Why this appears to happen

MySQL documents that `ROLLBACK` does not release locks acquired by `LOCK TABLES`. The concern here is the automatic return to the pool after cancellation, when the application no longer has a usable `sql.Tx` through which to unlock.

Go's `database/sql` automatically rolls back when the `BeginTx` context is canceled. It permits connection reuse after this rollback when the driver implements both `driver.SessionResetter` and `driver.Validator`. The driver's rollback sends `ROLLBACK`; `ResetSession` checks connection liveness, and `IsValid` checks that the connection is open and its buffer is not busy. Neither releases the remaining table locks.

The reproducer first uses plain statements for the next borrower, with and without an explicit context. The one-connection pool makes these calls wait for automatic rollback without a sleep. It then starts a new transaction to demonstrate that `START TRANSACTION` implicitly releases the old table locks, making the final update succeed. That recovery does not mean `ROLLBACK` releases table locks.

A detached context for cleanup on the old `sql.Tx` does not solve the problem once automatic rollback has completed: that transaction returns `sql.ErrTxDone`.

Possible remedies include discarding canceled transaction sessions or resetting them before pool return. A full `COM_RESET_CONNECTION` implementation would also need to restore configured session settings and handle invalidated prepared statements. This report does not assume that full resets for every transaction are the only solution.

References: [MySQL table-lock semantics](https://dev.mysql.com/doc/refman/8.0/en/lock-tables.html), [upstream driver connection handling](https://github.com/go-sql-driver/mysql/blob/789a82a35d04f8ab5a7b28707615ef8bf9d4f09b/connection.go), and the original downstream failure in [block/spirit#1276](https://github.com/block/spirit/issues/1276).
