package raknet_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandertv/go-raknet"
)

const (
	tagOrdered    byte = 1
	tagReliable   byte = 2
	tagUnreliable byte = 3
)

// dialPair establishes a RakNet connection over loopback and returns the
// client and server ends. The handshake succeeding also covers regression of
// the connection establishment behaviour.
func dialPair(t *testing.T) (client, server *raknet.Conn) {
	t.Helper()
	l, err := raknet.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	accepted := make(chan *raknet.Conn, 1)
	go func() {
		c, err := l.Accept()
		if err == nil {
			accepted <- c.(*raknet.Conn)
		}
	}()

	client, err = raknet.Dial(l.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	select {
	case server = <-accepted:
		t.Cleanup(func() { _ = server.Close() })
	case <-time.After(time.Second * 3):
		t.Fatal("timed out accepting connection")
	}
	return client, server
}

// recvPacket reads one packet from c or fails the test after the timeout.
func recvPacket(t *testing.T, c *raknet.Conn, timeout time.Duration) []byte {
	t.Helper()
	got := make(chan []byte, 1)
	go func() {
		b, err := c.ReadPacket()
		if err == nil {
			got <- b
		}
	}()
	select {
	case b := <-got:
		return b
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for a packet")
		return nil
	}
}

// testPayload builds a deterministic payload with a method tag and sequence
// number. The leading 0xAA byte ensures RakNet does not handle the packet
// internally.
func testPayload(tag byte, seq uint32, size int) []byte {
	b := make([]byte, size)
	b[0] = 0xAA
	b[1] = tag
	binary.BigEndian.PutUint32(b[2:6], seq)
	for i := 6; i < size; i++ {
		b[i] = byte(int(seq)*31 + i)
	}
	return b
}

// payloadSeq reads the sequence number encoded by testPayload.
func payloadSeq(b []byte) uint32 {
	return binary.BigEndian.Uint32(b[2:6])
}

// TestWritesShort sends one short packet through each of the three entries
// and checks the returned byte count, error and received content.
func TestWritesShort(t *testing.T) {
	client, server := dialPair(t)

	cases := []struct {
		name  string
		tag   byte
		write func([]byte) (int, error)
	}{
		{"Write", tagOrdered, client.Write},
		{"WriteReliable", tagReliable, client.WriteReliable},
		{"WriteUnreliable", tagUnreliable, client.WriteUnreliable},
	}
	for _, c := range cases {
		payload := testPayload(c.tag, 7, 32)
		n, err := c.write(payload)
		if err != nil || n != len(payload) {
			t.Fatalf("%s returned n=%v, err=%v", c.name, n, err)
		}
		if got := recvPacket(t, server, time.Second*3); !bytes.Equal(got, payload) {
			t.Fatalf("%s delivered %x, expected %x", c.name, got, payload)
		}
	}
}

// TestWritesFragmented sends packets larger than the negotiated MTU through
// each entry and verifies fragments are reassembled on the receiving end.
func TestWritesFragmented(t *testing.T) {
	client, server := dialPair(t)

	// 2000 exceeds even the largest effective MTU (1492-28 = 1464), so these
	// are always fragmented regardless of MTU negotiation.
	sizes := []int{2000, 5000}
	writes := []struct {
		name  string
		tag   byte
		write func([]byte) (int, error)
	}{
		{"Write", tagOrdered, client.Write},
		{"WriteReliable", tagReliable, client.WriteReliable},
		{"WriteUnreliable", tagUnreliable, client.WriteUnreliable},
	}
	var expected [][]byte
	seq := uint32(0)
	for _, size := range sizes {
		for _, w := range writes {
			payload := testPayload(w.tag, seq, size)
			seq++
			n, err := w.write(payload)
			if err != nil || n != size {
				t.Fatalf("%s size %v returned n=%v, err=%v", w.name, size, n, err)
			}
			expected = append(expected, payload)
		}
	}
	for i, want := range expected {
		got := recvPacket(t, server, time.Second*5)
		if !bytes.Equal(got, want) {
			t.Fatalf("fragmented packet %v (len %v) mismatch: got len %v", i, len(want), len(got))
		}
	}
}

// TestConsecutiveReliableUnordered sends many reliable unordered packets back
// to back and verifies every single one arrives.
func TestConsecutiveReliableUnordered(t *testing.T) {
	client, server := dialPair(t)

	const count = 200
	for i := range count {
		payload := testPayload(tagReliable, uint32(i), 16)
		if n, err := client.WriteReliable(payload); err != nil || n != len(payload) {
			t.Fatalf("WriteReliable %v returned n=%v, err=%v", i, n, err)
		}
	}
	seen := make(map[uint32]bool, count)
	for range count {
		b := recvPacket(t, server, time.Second*5)
		if b[1] != tagReliable {
			t.Fatalf("unexpected tag %v", b[1])
		}
		seq := payloadSeq(b)
		if seen[seq] {
			t.Fatalf("reliable unordered packet %v delivered twice", seq)
		}
		seen[seq] = true
	}
	if len(seen) != count {
		t.Fatalf("received %v distinct reliable unordered packets, expected %v", len(seen), count)
	}
}

// TestWriteOrderingInterleaved verifies Write keeps its reliable ordered
// semantics when reliable unordered packets are interleaved: ordered packets
// must arrive strictly in sequence, and unordered packets independently all
// arrive.
func TestWriteOrderingInterleaved(t *testing.T) {
	client, server := dialPair(t)

	const count = 200
	for i := range count {
		if _, err := client.Write(testPayload(tagOrdered, uint32(i), 16)); err != nil {
			t.Fatalf("write ordered %v: %v", i, err)
		}
		if _, err := client.WriteReliable(testPayload(tagReliable, uint32(i), 16)); err != nil {
			t.Fatalf("write reliable %v: %v", i, err)
		}
	}
	nextOrdered := uint32(0)
	unordered := make(map[uint32]bool, count)
	for range count * 2 {
		b := recvPacket(t, server, time.Second*5)
		switch b[1] {
		case tagOrdered:
			seq := payloadSeq(b)
			if seq != nextOrdered {
				t.Fatalf("ordered packet out of order: got %v, expected %v", seq, nextOrdered)
			}
			nextOrdered++
		case tagReliable:
			unordered[payloadSeq(b)] = true
		default:
			t.Fatalf("unexpected tag %v", b[1])
		}
	}
	if nextOrdered != count {
		t.Fatalf("received %v ordered packets, expected %v", nextOrdered, count)
	}
	if len(unordered) != count {
		t.Fatalf("received %v unordered packets, expected %v", len(unordered), count)
	}
}

// TestConcurrentWrites hammers all three entries from multiple goroutines and
// verifies write results, payload integrity and exactly-once delivery. Run
// with -race to verify send buffer serialisation.
func TestConcurrentWrites(t *testing.T) {
	client, server := dialPair(t)

	const (
		goroutines = 15
		perRoutine = 60
		total      = goroutines * perRoutine
	)
	var wg sync.WaitGroup
	var errCount atomic.Int64
	write := func(tag byte, g int, fn func([]byte) (int, error)) {
		defer wg.Done()
		for i := range perRoutine {
			payload := testPayload(tag, uint32(g*perRoutine+i), 128)
			if n, err := fn(payload); err != nil || n != len(payload) {
				errCount.Add(1)
				t.Errorf("write tag %v g %v i %v returned n=%v, err=%v", tag, g, i, n, err)
			}
		}
	}
	for g := range goroutines {
		wg.Add(1)
		switch g % 3 {
		case 0:
			go write(tagOrdered, g, client.Write)
		case 1:
			go write(tagReliable, g, client.WriteReliable)
		default:
			go write(tagUnreliable, g, client.WriteUnreliable)
		}
	}

	type key struct {
		tag byte
		seq uint32
	}
	received := make(map[key]bool, total)
	var mu sync.Mutex
	var duplicates atomic.Int64
	done := make(chan struct{})
	go func() {
		for i := 0; i < total; i++ {
			b, err := server.ReadPacket()
			if err != nil {
				return
			}
			k := key{b[1], payloadSeq(b)}
			mu.Lock()
			if received[k] {
				duplicates.Add(1)
			}
			received[k] = true
			mu.Unlock()
		}
		close(done)
	}()

	wg.Wait()
	select {
	case <-done:
	case <-time.After(time.Second * 10):
	}
	mu.Lock()
	got := len(received)
	mu.Unlock()
	if got != total {
		t.Fatalf("received %v packets, expected %v", got, total)
	}
	if errCount.Load() != 0 {
		t.Fatalf("%v writes returned errors", errCount.Load())
	}
	if duplicates.Load() != 0 {
		t.Fatalf("%v packets were delivered twice", duplicates.Load())
	}
}

// TestWritesAfterCloseLoopback verifies that after a connection has fully
// closed every entry returns n == 0 and a net.ErrClosed-wrapped error. The
// server end is closed; the client closes as soon as it processes the
// disconnect notification.
func TestWritesAfterCloseLoopback(t *testing.T) {
	client, server := dialPair(t)
	_ = server.Close()

	writers := map[string]func([]byte) (int, error){
		"Write":           client.Write,
		"WriteReliable":   client.WriteReliable,
		"WriteUnreliable": client.WriteUnreliable,
	}
	deadline := time.Now().Add(time.Second * 8)
	var ns map[string]int
	var errs map[string]error
	for time.Now().Before(deadline) {
		ns = make(map[string]int)
		errs = make(map[string]error)
		closed := 0
		for name, write := range writers {
			n, err := write([]byte{0xAA, 0x01})
			ns[name], errs[name] = n, err
			if errors.Is(err, net.ErrClosed) {
				closed++
			}
		}
		if closed == len(writers) {
			for name, err := range errs {
				if !errors.Is(err, net.ErrClosed) {
					t.Fatalf("%s error %v does not wrap net.ErrClosed", name, err)
				}
				var opErr *net.OpError
				if !errors.As(err, &opErr) || opErr.Op != "write" {
					t.Fatalf("%s returned %v, expected net.OpError op write", name, err)
				}
				if ns[name] != 0 {
					t.Fatalf("%s returned n=%v after close, expected 0", name, ns[name])
				}
			}
			return
		}
		time.Sleep(time.Millisecond * 20)
	}
	for name, err := range errs {
		t.Errorf("%s last error: %v (n=%v)", name, err, ns[name])
	}
	t.Fatalf("client did not report net.ErrClosed on all entries in time: %v", fmt.Sprint(errs))
}
