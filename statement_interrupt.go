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
	"time"
)

const (
	// DefaultInterruptTimeout is InterruptOptions.Timeout when it is zero.
	DefaultInterruptTimeout = 5 * time.Second
	// DefaultInterruptRetryInterval is InterruptOptions.RetryInterval when it
	// is zero.
	DefaultInterruptRetryInterval = 250 * time.Millisecond
)

// ErrStatementOutlivedKill is the error InterruptOptions.OnFallback receives
// when the statement was still running InterruptOptions.Timeout after the
// interrupt started, although every KILL QUERY was acknowledged.
var ErrStatementOutlivedKill = errors.New("mysql: statement did not end after KILL QUERY")

// InterruptOptions configures ArmInterrupt.
type InterruptOptions struct {
	// Timeout bounds the interrupt: the KILL QUERY round trips, and the
	// wait for the statement to end. Past it, the interrupt falls back to
	// cancelling the statement's context. Zero means
	// DefaultInterruptTimeout.
	Timeout time.Duration
	// RetryInterval is how long the statement may keep running after an
	// acknowledged KILL QUERY before KILL QUERY is sent again. A KILL QUERY
	// that reaches the server before the statement does interrupts nothing.
	// Zero means DefaultInterruptRetryInterval.
	RetryInterval time.Duration
	// OnFallback, when set, is called from the interrupt's goroutine just
	// before it falls back to cancelling the statement's context. err is the
	// KILL QUERY error, or ErrStatementOutlivedKill.
	OnFallback func(target KillTarget, err error)
}

// InterruptOutcome is what ArmedStatement.Finish reports.
type InterruptOutcome struct {
	// Interrupted reports that the caller's context ended before Finish,
	// so the interrupt ran: the statement's result is no longer only its
	// own. What that means depends on the response; see ArmInterrupt.
	Interrupted bool
	// Unacknowledged reports that a KILL QUERY failed without learning
	// whether the server ran it. It may still arrive and end whatever runs
	// next on the connection, so the connection must be discarded.
	Unacknowledged bool
	// FellBack reports that the interrupt cancelled the statement's context
	// because KILL QUERY failed or the statement outlived Timeout. If the
	// statement was still running, the driver closed the connection, and
	// the server may still be running the statement.
	FellBack bool
}

// ArmedStatement is one statement armed by ArmInterrupt.
//
// It is built for the path where nothing is interrupted, which is nearly
// every statement: one allocation beyond the contexts, and no channel until
// an interrupt starts.
type ArmedStatement struct {
	killer  QueryKiller
	target  KillTarget
	options InterruptOptions
	// ctx is the context the statement runs on. It keeps the caller's
	// values but not its cancellation.
	ctx context.Context
	// cancel cancels ctx: the fallback that makes the driver close the
	// connection.
	cancel context.CancelFunc
	// stop deregisters the interrupt, reporting false when it has started.
	stop func() bool
	// running counts the interrupt from ArmInterrupt until it returns, or
	// until stop guarantees it never runs.
	running sync.WaitGroup

	// mu guards finished, outcome and wake, the handshake between Finish
	// and a running interrupt.
	mu       sync.Mutex
	finished bool
	outcome  InterruptOutcome
	// wake is created by an interrupt that has started, and closed by
	// Finish so the interrupt stops sending KILL QUERY.
	wake chan struct{}
}

// ArmInterrupt arms a statement about to run on the connection target names
// so that, when ctx ends, the statement is ended on the server with KILL
// QUERY sent through killer, instead of being abandoned there.
//
// Left to the driver, a context that ends mid-statement closes the
// connection's socket and returns at once. The server does not notice a
// closed socket while it executes, so the statement keeps running, holding
// its locks, until it finishes on its own. A DDL waiting for a metadata lock
// still takes effect once the lock is free. KILL QUERY ends the statement
// with error 1317 instead, and leaves the connection usable, along with a
// transaction or locks held on it.
//
// The statement must run on the returned ArmedStatement's Context, which
// keeps ctx's values but does not end with it. When ctx ends, the interrupt
// sends KILL QUERY, and sends it again every RetryInterval while the
// statement keeps running. If KILL QUERY fails, or the statement outlives
// Timeout, the interrupt cancels Context, which makes the driver close the
// connection: the behaviour without an interrupt.
//
// Finish must be called once the statement's response is complete: for a
// resultset, after the rows are closed. It waits for a running interrupt, so
// no KILL QUERY it sent can reach the connection's next statement.
//
// ArmInterrupt returns nil when there is nothing to arm: ctx never ends,
// killer is nil, or target has no thread. The statement then runs on ctx.
//
// Reading the outcome: an OK response from an interrupted statement is
// still the statement's result. The server acknowledged it, so a write
// happened (in autocommit, it is committed), and the interrupt only lost the
// race with it. A resultset from an interrupted statement is not: some
// statements answer KILL QUERY with a result instead of an error (SELECT
// SLEEP(n) returns 1), so the rows may be cut short.
func ArmInterrupt(ctx context.Context, killer QueryKiller, target KillTarget, options InterruptOptions) *ArmedStatement {
	if killer == nil || target.ThreadID == 0 || ctx.Done() == nil {
		return nil
	}
	if options.Timeout <= 0 {
		options.Timeout = DefaultInterruptTimeout
	}
	if options.RetryInterval <= 0 {
		options.RetryInterval = DefaultInterruptRetryInterval
	}
	a := &ArmedStatement{killer: killer, target: target, options: options}
	a.ctx, a.cancel = context.WithCancel(context.WithoutCancel(ctx))
	a.running.Add(1)
	a.stop = context.AfterFunc(ctx, a.interrupt)
	return a
}

// Context returns the context the statement must run on.
func (a *ArmedStatement) Context() context.Context {
	return a.ctx
}

// Finish marks the statement complete and reports what the interrupt did.
// When an interrupt has started it waits for it to return. Call it exactly
// once.
func (a *ArmedStatement) Finish() InterruptOutcome {
	var outcome InterruptOutcome
	if a.stop() {
		// The interrupt will never run.
		a.running.Done()
	} else {
		a.mu.Lock()
		a.finished = true
		if a.wake != nil {
			close(a.wake)
		}
		a.mu.Unlock()
		a.running.Wait()
		a.mu.Lock()
		outcome = a.outcome
		a.mu.Unlock()
	}
	a.cancel()
	return outcome
}

// interrupt runs once the caller's context ends while the statement is in
// flight.
func (a *ArmedStatement) interrupt() {
	defer a.running.Done()
	a.mu.Lock()
	if a.finished {
		a.mu.Unlock()
		return
	}
	a.outcome.Interrupted = true
	a.wake = make(chan struct{})
	a.mu.Unlock()
	acknowledged, fellBack := a.kill(a.wake)
	a.mu.Lock()
	a.outcome.Unacknowledged = !acknowledged
	a.outcome.FellBack = fellBack
	a.mu.Unlock()
}

// kill ends the statement with KILL QUERY, sent again for as long as it keeps
// running, until finished is closed. When KILL QUERY fails, or the statement
// outlives the timeout, it cancels the statement's context instead.
//
// It reports whether every KILL QUERY it sent was acknowledged, and whether
// it fell back. A KILL QUERY that failed may still have been sent (the usual
// failure is its context expiring mid-round trip, on the slow server that
// made the statement time out), and the server may yet run it. By then the
// statement may have finished on its own, so cancelling closed nothing, and
// the KILL would end the connection's next statement.
func (a *ArmedStatement) kill(finished <-chan struct{}) (acknowledged, fellBack bool) {
	deadline := time.NewTimer(a.options.Timeout)
	defer deadline.Stop()
	killCtx, cancelKill := context.WithTimeout(context.Background(), a.options.Timeout)
	defer cancelKill()
	for {
		select {
		case <-finished:
			return true, false
		default:
		}
		if err := a.killer.KillQuery(killCtx, a.target); err != nil {
			a.fallBack(err)
			return false, true
		}
		retry := time.NewTimer(a.options.RetryInterval)
		select {
		case <-finished:
			retry.Stop()
			return true, false
		case <-deadline.C:
			retry.Stop()
			a.fallBack(ErrStatementOutlivedKill)
			return true, true
		case <-retry.C:
		}
	}
}

func (a *ArmedStatement) fallBack(err error) {
	if a.options.OnFallback != nil {
		a.options.OnFallback(a.target, err)
	}
	a.cancel()
}
