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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeKiller records KillQuery calls and answers them with err. When block is
// set, each call waits for it to be closed first.
type fakeKiller struct {
	err     error
	block   chan struct{}
	calls   atomic.Int32
	targets chan KillTarget
}

func newFakeKiller(err error) *fakeKiller {
	return &fakeKiller{err: err, targets: make(chan KillTarget, 100)}
}

func (k *fakeKiller) KillQuery(ctx context.Context, target KillTarget) error {
	k.calls.Add(1)
	k.targets <- target
	if k.block != nil {
		<-k.block
	}
	return k.err
}

var testKillTarget = KillTarget{ThreadID: 42, ServerUUID: "uuid", Hostname: "host"}

func TestArmInterruptHasNothingToArm(t *testing.T) {
	cancellable, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancelled, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	killer := newFakeKiller(nil)
	for name, tc := range map[string]struct {
		ctx    context.Context
		killer QueryKiller
		target KillTarget
	}{
		"context never ends":    {context.Background(), killer, testKillTarget},
		"no killer":             {cancellable, nil, testKillTarget},
		"no thread":             {cancellable, killer, KillTarget{ServerUUID: "uuid"}},
		"context already ended": {cancelled, killer, testKillTarget},
	} {
		runCtx, armed := ArmInterrupt(tc.ctx, tc.killer, tc.target, InterruptOptions{})
		if armed != nil {
			t.Errorf("%s: ArmInterrupt armed a statement", name)
		}
		if runCtx != tc.ctx {
			t.Errorf("%s: the statement must run on the caller's context", name)
		}
		if outcome := armed.Finish(); outcome != (InterruptOutcome{}) {
			t.Errorf("%s: Finish: got %+v, want the zero outcome", name, outcome)
		}
	}
	// An already-ended context stays ended, so the driver refuses the
	// statement before it is sent instead of racing it against a KILL QUERY.
	if n := killer.calls.Load(); n != 0 {
		t.Errorf("KillQuery called %d times", n)
	}
}

// TestArmedStatementFinishRightAfterCancelReportsInterrupted: the interrupt's
// goroutine may not have run when Finish begins, and then sends nothing, but
// the caller's context did end first.
func TestArmedStatementFinishRightAfterCancelReportsInterrupted(t *testing.T) {
	for range 100 {
		ctx, cancel := context.WithCancel(context.Background())
		killer := newFakeKiller(nil)
		_, armed := ArmInterrupt(ctx, killer, testKillTarget, InterruptOptions{})
		cancel()
		outcome := armed.Finish()
		if !outcome.Interrupted || outcome.Unacknowledged || outcome.FellBack {
			t.Fatalf("Finish: got %+v, want only Interrupted", outcome)
		}
	}
}

func TestArmedStatementThatFinishesIsNeverInterrupted(t *testing.T) {
	type key struct{}
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, "value"))
	killer := newFakeKiller(nil)
	runCtx, armed := ArmInterrupt(ctx, killer, testKillTarget, InterruptOptions{})
	if armed == nil {
		t.Fatal("ArmInterrupt: nil")
	}
	if got := runCtx.Value(key{}); got != "value" {
		t.Errorf("Context lost the caller's values: got %v", got)
	}
	if outcome := armed.Finish(); outcome != (InterruptOutcome{}) {
		t.Errorf("Finish: got %+v, want the zero outcome", outcome)
	}
	if runCtx.Err() == nil {
		t.Error("the statement's context must end once the statement has finished")
	}
	cancel()
	// The interrupt was deregistered: ending the caller's context now sends
	// nothing.
	time.Sleep(20 * time.Millisecond)
	if n := killer.calls.Load(); n != 0 {
		t.Errorf("KillQuery called %d times after Finish", n)
	}
}

func TestArmedStatementIsInterruptedWhenTheContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	killer := newFakeKiller(nil)
	runCtx, armed := ArmInterrupt(ctx, killer, testKillTarget, InterruptOptions{})
	cancel()
	if got := <-killer.targets; got != testKillTarget {
		t.Errorf("KillQuery target: got %+v, want %+v", got, testKillTarget)
	}
	if runCtx.Err() != nil {
		t.Error("an acknowledged KILL QUERY must leave the statement's context alone")
	}
	outcome := armed.Finish()
	if !outcome.Interrupted || outcome.Unacknowledged || outcome.FellBack {
		t.Errorf("Finish: got %+v, want only Interrupted", outcome)
	}
}

func TestArmedStatementFallsBackWhenKillQueryFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	killErr := errors.New("kill failed")
	killer := newFakeKiller(killErr)
	var fallbackErr error
	fellBack := make(chan struct{})
	runCtx, armed := ArmInterrupt(ctx, killer, testKillTarget, InterruptOptions{
		OnFallback: func(target KillTarget, err error) {
			fallbackErr = err
			close(fellBack)
		},
	})
	cancel()
	<-fellBack
	select {
	case <-runCtx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the statement's context must be cancelled when KILL QUERY fails")
	}
	outcome := armed.Finish()
	if !outcome.Interrupted || !outcome.Unacknowledged || !outcome.FellBack {
		t.Errorf("Finish: got %+v, want Interrupted, Unacknowledged and FellBack", outcome)
	}
	if !errors.Is(fallbackErr, killErr) {
		t.Errorf("OnFallback: got %v, want %v", fallbackErr, killErr)
	}
}

func TestArmedStatementThatOutlivesKillQueryFallsBack(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	killer := newFakeKiller(nil)
	var fallbackErr error
	var once sync.Once
	fellBack := make(chan struct{})
	runCtx, armed := ArmInterrupt(ctx, killer, testKillTarget, InterruptOptions{
		Timeout:       200 * time.Millisecond,
		RetryInterval: 20 * time.Millisecond,
		OnFallback: func(target KillTarget, err error) {
			once.Do(func() {
				fallbackErr = err
				close(fellBack)
			})
		},
	})
	cancel()
	select {
	case <-runCtx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the statement's context must be cancelled once it outlives the timeout")
	}
	<-fellBack
	outcome := armed.Finish()
	if !outcome.Interrupted || outcome.Unacknowledged || !outcome.FellBack {
		t.Errorf("Finish: got %+v, want Interrupted and FellBack", outcome)
	}
	if !errors.Is(fallbackErr, ErrStatementOutlivedKill) {
		t.Errorf("OnFallback: got %v, want ErrStatementOutlivedKill", fallbackErr)
	}
	if n := killer.calls.Load(); n < 2 {
		t.Errorf("KillQuery called %d times, want it resent while the statement ran", n)
	}
}

// TestArmedStatementFinishWaitsForKillQuery pins the rule that makes reusing
// the connection safe: a KILL QUERY still on its way must not reach the
// connection's next statement, so Finish returns only after it has.
func TestArmedStatementFinishWaitsForKillQuery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	killer := newFakeKiller(nil)
	killer.block = make(chan struct{})
	_, armed := ArmInterrupt(ctx, killer, testKillTarget, InterruptOptions{})
	cancel()
	<-killer.targets

	finished := make(chan InterruptOutcome)
	go func() { finished <- armed.Finish() }()
	select {
	case <-finished:
		t.Fatal("Finish returned while KILL QUERY was in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(killer.block)
	select {
	case outcome := <-finished:
		if !outcome.Interrupted || outcome.Unacknowledged {
			t.Errorf("Finish: got %+v, want Interrupted", outcome)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Finish did not return after KILL QUERY did")
	}
	if n := killer.calls.Load(); n != 1 {
		t.Errorf("KillQuery called %d times, want 1: Finish must stop the retries", n)
	}
}

func TestArmInterruptEndsALockWaitAndKeepsTheConnection(t *testing.T) {
	db, _ := openLockWaitTest(t)
	connector, err := NewConnector(mustParseDSN(t, dsn))
	if err != nil {
		t.Fatal(err)
	}
	interrupter := NewInterrupter(connector)
	defer interrupter.Close()

	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	var outcome InterruptOutcome
	err = conn.Raw(func(driverConn any) error {
		target, err := ReadKillTarget(ctx, driverConn)
		if err != nil {
			return err
		}
		runCtx, armed := ArmInterrupt(ctx, interrupter, target, InterruptOptions{})
		_, err = driverConn.(*mysqlConn).ExecContext(runCtx, lockWaitQuery, nil)
		outcome = armed.Finish()
		return err
	})
	var mysqlErr *MySQLError
	if !errors.As(err, &mysqlErr) || mysqlErr.Number != 1317 {
		t.Fatalf("statement: got %v, want error 1317", err)
	}
	if elapsed := time.Since(start); elapsed > DefaultInterruptTimeout {
		t.Errorf("statement took %v to end", elapsed)
	}
	if !outcome.Interrupted || outcome.Unacknowledged || outcome.FellBack {
		t.Errorf("Finish: got %+v, want only Interrupted", outcome)
	}
	var one int
	if err := conn.QueryRowContext(t.Context(), "SELECT 1").Scan(&one); err != nil {
		t.Errorf("the connection must survive the interrupt: %v", err)
	}
}
