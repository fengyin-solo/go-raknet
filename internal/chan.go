package internal

import (
	"context"
	"sync"
)

// ElasticChan is a channel that grows if its capacity is reached, up to the
// maximum size passed to Chan. ElasticChan is safe for concurrent use with
// any number of senders and receivers.
type ElasticChan[T any] struct {
	mu sync.Mutex
	// q holds the buffered values. Its backing array grows on demand up to
	// lim, so values never have to be moved between channels while callers
	// may be blocked on them.
	q []T
	// lim is the maximum number of values buffered at once. Send blocks once
	// q holds lim values until a receiver makes room.
	lim int

	// nonEmpty is closed and replaced with a new channel whenever q goes
	// from empty to non-empty. Closing it broadcasts the transition to every
	// receiver waiting on the previous channel.
	nonEmpty chan struct{}
	// nonFull is closed and replaced whenever q drops from lim to lim-1.
	// Closing it broadcasts the transition to every waiting sender.
	nonFull chan struct{}
}

// Chan creates an ElasticChan with the initial and maximum buffer sizes
// passed.
func Chan[T any](size, max int) *ElasticChan[T] {
	return &ElasticChan[T]{
		q:        make([]T, 0, size),
		lim:      max,
		nonEmpty: make(chan struct{}),
		nonFull:  make(chan struct{}),
	}
}

// Recv attempts to read a value from the channel. If ctx is canceled while no
// value is available, Recv returns ok = false.
func (c *ElasticChan[T]) Recv(ctx context.Context) (val T, ok bool) {
	c.mu.Lock()
	for len(c.q) == 0 {
		wake := c.nonEmpty
		// Release the mutex while blocked: holding it would prevent a Send
		// from making a value available and could deadlock with multiple
		// receivers.
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return val, false
		case <-wake:
		}
		c.mu.Lock()
	}
	val = c.q[0]
	// Clear the slot before advancing so values that are no longer queued
	// (in particular pointer-containing payloads) can be garbage collected.
	var zero T
	c.q[0] = zero
	c.q = c.q[1:]
	if len(c.q)+1 == c.lim {
		// The queue was full before this receive: wake senders waiting for
		// room and replace the signal so future senders can block again.
		close(c.nonFull)
		c.nonFull = make(chan struct{})
	}
	c.mu.Unlock()
	return val, true
}

// Send sends a value to the channel. It never blocks unless the maximum
// buffer size passed to Chan has been reached, in which case it blocks until a
// receiver makes room.
func (c *ElasticChan[T]) Send(val T) {
	c.mu.Lock()
	for len(c.q) >= c.lim {
		room := c.nonFull
		c.mu.Unlock()
		<-room
		c.mu.Lock()
	}
	if len(c.q) == 0 {
		// Broadcast that a value is available and replace the signal so that
		// receivers waking up after this transition block on a fresh one.
		close(c.nonEmpty)
		c.nonEmpty = make(chan struct{})
	}
	c.q = append(c.q, val)
	c.mu.Unlock()
}
