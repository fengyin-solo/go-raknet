package internal

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestElasticChanSendRecv(t *testing.T) {
	c := Chan[int](4, 64)
	for i := range 10 {
		c.Send(i)
	}
	for i := range 10 {
		v, ok := c.Recv(context.Background())
		if !ok || v != i {
			t.Fatalf("Recv = (%d, %v), want (%d, true)", v, ok, i)
		}
	}
}

// TestElasticChanConcurrentReceivers ensures many concurrent receivers never
// deadlock when the buffer grows while receivers are blocked. A previous
// implementation held a read lock across a blocking channel receive.
func TestElasticChanConcurrentReceivers(t *testing.T) {
	for range 500 {
		const readers, total = 8, 64
		c := Chan[int](4, 4096)
		ctx, cancel := context.WithCancel(context.Background())

		var received atomic.Int64
		var wg sync.WaitGroup
		for range readers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					if _, ok := c.Recv(ctx); !ok {
						return
					}
					received.Add(1)
				}
			}()
		}
		for i := range total {
			c.Send(i)
		}
		deadline := time.Now().Add(time.Second)
		for received.Load() < total {
			if time.Now().After(deadline) {
				cancel()
				wg.Wait()
				t.Fatalf("deadlocked with %d/%d values received", received.Load(), total)
			}
		}
		cancel()
		wg.Wait()
	}
}

func TestElasticChanRecvCancel(t *testing.T) {
	c := Chan[int](2, 8)
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan bool, 1)
	go func() {
		_, ok := c.Recv(ctx)
		errCh <- ok
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	if ok := <-errCh; ok {
		t.Fatal("Recv on canceled context returned ok = true, want false")
	}

	// A value sent afterwards must still be delivered to a new receiver.
	c.Send(42)
	v, ok := c.Recv(context.Background())
	if !ok || v != 42 {
		t.Fatalf("Recv after cancel = (%d, %v), want (42, true)", v, ok)
	}
}

// TestElasticChanLimitBlocks verifies Send blocks at the maximum buffer size
// and proceeds again as soon as a receiver makes room.
func TestElasticChanLimitBlocks(t *testing.T) {
	const max = 4
	c := Chan[int](1, max)

	for i := range max {
		c.Send(i)
	}
	sent := make(chan struct{})
	go func() {
		c.Send(max)
		close(sent)
	}()
	select {
	case <-sent:
		t.Fatal("Send did not block at the maximum buffer size")
	case <-time.After(20 * time.Millisecond):
	}

	v, _ := c.Recv(context.Background())
	if v != 0 {
		t.Fatalf("Recv = %d, want 0", v)
	}
	select {
	case <-sent:
	case <-time.After(time.Second):
		t.Fatal("blocked Send was not unblocked after a receiver made room")
	}

	for i := 1; i <= max; i++ {
		v, ok := c.Recv(context.Background())
		if !ok || v != i {
			t.Fatalf("Recv = (%d, %v), want (%d, true)", v, ok, i)
		}
	}
}
