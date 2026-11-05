package raknet_test

import (
	"bytes"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sandertv/go-raknet"
)

// negotiatedMTU is the MTU negotiated over loopback with default options.
const negotiatedMTU = 1492

// statsPair is a connected client/server pair on loopback used by the stats
// tests.
type statsPair struct {
	client *raknet.Conn
	server *raknet.Conn
	l      *raknet.Listener
}

func dialStatsPair(t *testing.T) *statsPair {
	t.Helper()
	l, err := raknet.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			t.Error(err)
			return
		}
		accepted <- c
	}()
	client, err := raknet.Dial(l.Addr().String())
	if err != nil {
		_ = l.Close()
		t.Fatalf("dial: %v", err)
	}
	var server *raknet.Conn
	select {
	case c := <-accepted:
		server = c.(*raknet.Conn)
	case <-time.After(time.Second * 3):
		_ = client.Close()
		_ = l.Close()
		t.Fatal("accept timed out")
	}
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
		_ = l.Close()
	})
	return &statsPair{client: client, server: server, l: l}
}

// waitFor polls cond until it returns true or the timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond * 5)
	}
	t.Fatal(msg)
}

// waitClosed blocks until Read on the connection fails, meaning the
// connection was fully closed.
func waitClosed(t *testing.T, c *raknet.Conn) {
	t.Helper()
	buf := make([]byte, 1)
	deadline := time.Now().Add(time.Second * 8)
	for time.Now().Before(deadline) {
		if _, err := c.Read(buf); err != nil {
			return
		}
	}
	t.Fatal("connection was not closed in time")
}

// TestStatsNewConnection verifies that a fresh, fully negotiated connection
// exposes the negotiated MTU, handshake traffic at the transport layer, no
// user traffic and zeroed gauges.
func TestStatsNewConnection(t *testing.T) {
	p := dialStatsPair(t)

	for name, s := range map[string]raknet.Stats{
		"client": p.client.Stats(),
		"server": p.server.Stats(),
	} {
		t.Run(name, func(t *testing.T) {
			if s.MTU != negotiatedMTU {
				t.Errorf("MTU = %v, want %v", s.MTU, negotiatedMTU)
			}
			// The handshake itself produces transport-layer traffic.
			if s.BytesSent == 0 {
				t.Error("BytesSent = 0, want handshake traffic")
			}
			if s.BytesReceived == 0 {
				t.Error("BytesReceived = 0, want handshake traffic")
			}
			if s.DatagramsSent == 0 {
				t.Error("DatagramsSent = 0, want handshake datagrams")
			}
			if s.DatagramsReceived == 0 {
				t.Error("DatagramsReceived = 0, want handshake datagrams")
			}
			// No user message was written or delivered yet.
			if s.PacketsSent != 0 || s.UserBytesSent != 0 {
				t.Errorf("user send counters = (%v, %v), want (0, 0)", s.PacketsSent, s.UserBytesSent)
			}
			if s.PacketsReceived != 0 || s.UserBytesReceived != 0 {
				t.Errorf("user receive counters = (%v, %v), want (0, 0)", s.PacketsReceived, s.UserBytesReceived)
			}
			if s.Retransmissions != 0 {
				t.Errorf("Retransmissions = %v, want 0 on loopback", s.Retransmissions)
			}
			if s.Unacknowledged < 0 {
				t.Errorf("Unacknowledged = %v, must not be negative", s.Unacknowledged)
			}
			if s.OrderedQueueWindow != 0 {
				t.Errorf("OrderedQueueWindow = %v, want 0", s.OrderedQueueWindow)
			}
			if s.LastActivity.IsZero() {
				t.Error("LastActivity is zero, want a recent timestamp")
			} else if d := time.Since(s.LastActivity); d > time.Second*10 || d < 0 {
				t.Errorf("LastActivity age = %v, want a recent timestamp", d)
			}
		})
	}
}

// TestStatsReadWrite verifies counters for ordinary one-way user traffic and
// checks that transport and data layer statistics never get mixed up.
func TestStatsReadWrite(t *testing.T) {
	p := dialStatsPair(t)

	payload := bytes.Repeat([]byte("hello raknet! "), 8) // 112 bytes, one datagram
	if _, err := p.client.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := p.server.ReadPacket()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: got %d bytes, want %d", len(got), len(payload))
	}

	cs := p.client.Stats()
	if cs.PacketsSent != 1 {
		t.Errorf("client PacketsSent = %v, want 1", cs.PacketsSent)
	}
	if cs.UserBytesSent != uint64(len(payload)) {
		t.Errorf("client UserBytesSent = %v, want %v", cs.UserBytesSent, len(payload))
	}
	if cs.DatagramsSent < 1 {
		t.Errorf("client DatagramsSent = %v, want at least 1", cs.DatagramsSent)
	}
	// Transport bytes carry headers, so they exceed the payload size.
	if cs.BytesSent < uint64(len(payload)) {
		t.Errorf("client BytesSent = %v, want at least %v", cs.BytesSent, len(payload))
	}
	// The single user message produced at most one fresh data datagram
	// beyond handshake traffic: datagram count and packet count must be
	// independent counters.
	if cs.PacketsSent != 1 {
		t.Errorf("datagram/packet counting mixed up: PacketsSent = %v", cs.PacketsSent)
	}

	ss := p.server.Stats()
	if ss.PacketsReceived != 1 {
		t.Errorf("server PacketsReceived = %v, want 1", ss.PacketsReceived)
	}
	if ss.UserBytesReceived != uint64(len(payload)) {
		t.Errorf("server UserBytesReceived = %v, want %v", ss.UserBytesReceived, len(payload))
	}
	if ss.DatagramsReceived < 1 {
		t.Errorf("server DatagramsReceived = %v, want at least 1", ss.DatagramsReceived)
	}
	if ss.BytesReceived < uint64(len(payload)) {
		t.Errorf("server BytesReceived = %v, want at least %v", ss.BytesReceived, len(payload))
	}

	// The server must ACK the client datagram and the client must receive it.
	waitFor(t, time.Second*3, func() bool {
		return p.server.Stats().ACKsSent >= 1 && p.client.Stats().ACKsReceived >= 1
	}, "ACK for user datagram was not exchanged")
}

// TestStatsFragmentation verifies that a user message larger than the MTU is
// sent in multiple datagrams but counted exactly once at the data layer, and
// that the receiver counts one completed split and one delivered message.
func TestStatsFragmentation(t *testing.T) {
	p := dialStatsPair(t)

	const size = 5000 // 4 fragments at MTU 1492.
	big := bytes.Repeat([]byte{'F'}, size)
	if _, err := p.client.Write(big); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := p.server.ReadPacket()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != size {
		t.Fatalf("reassembled payload = %v bytes, want %v", len(got), size)
	}

	cs := p.client.Stats()
	if cs.PacketsSent != 1 {
		t.Errorf("client PacketsSent = %v, want 1 for one fragmented message", cs.PacketsSent)
	}
	if cs.UserBytesSent != size {
		t.Errorf("client UserBytesSent = %v, want %v (fragments must not double count)", cs.UserBytesSent, size)
	}
	if cs.DatagramsSent < 4 {
		t.Errorf("client DatagramsSent = %v, want at least 4 fragments", cs.DatagramsSent)
	}

	ss := p.server.Stats()
	if ss.SplitsCompleted < 1 {
		t.Errorf("server SplitsCompleted = %v, want at least 1", ss.SplitsCompleted)
	}
	if ss.PacketsReceived != 1 {
		t.Errorf("server PacketsReceived = %v, want 1 reassembled message", ss.PacketsReceived)
	}
	if ss.UserBytesReceived != size {
		t.Errorf("server UserBytesReceived = %v, want %v", ss.UserBytesReceived, size)
	}
}

// TestStatsRepeatedSnapshot verifies that repeated snapshots return
// independent copies with monotonic counters and no side effects.
func TestStatsRepeatedSnapshot(t *testing.T) {
	p := dialStatsPair(t)

	s1 := p.client.Stats()
	s2 := p.client.Stats()
	// Nothing happens between two immediate snapshots: counters must match.
	if s1 != s2 {
		t.Errorf("back-to-back snapshots differ:\n%+v\n%+v", s1, s2)
	}

	// Mutating a snapshot must not affect later snapshots.
	s1.PacketsSent = 1<<32 - 1
	s1.UserBytesSent = 1<<64 - 1
	s1.MTU = 0
	s3 := p.client.Stats()
	if s3.PacketsSent != s2.PacketsSent || s3.UserBytesSent != s2.UserBytesSent || s3.MTU != s2.MTU {
		t.Error("Stats snapshot is not an independent copy")
	}

	// After traffic, every cumulative counter is at least what it was.
	if _, err := p.client.Write([]byte("snapshot monotonic")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := p.server.ReadPacket(); err != nil {
		t.Fatalf("read: %v", err)
	}
	waitFor(t, time.Second*3, func() bool {
		return p.client.Stats().PacketsSent >= s2.PacketsSent+1
	}, "counter did not advance after write")
	s4 := p.client.Stats()
	if s4.BytesSent < s2.BytesSent || s4.DatagramsSent < s2.DatagramsSent ||
		s4.UserBytesSent < s2.UserBytesSent || s4.PacketsSent < s2.PacketsSent {
		t.Errorf("counters are not monotonic:\nbefore %+v\nafter  %+v", s2, s4)
	}
}

// TestStatsConcurrentWrites reads snapshots continuously while many goroutines
// write concurrently. Run with -race. Final data-layer counters must add up
// exactly, without split or duplicate counting.
func TestStatsConcurrentWrites(t *testing.T) {
	p := dialStatsPair(t)

	const writers = 16
	const perWriter = 25
	const msgLen = 64

	stopStats := make(chan struct{})
	var statsWG sync.WaitGroup
	statsWG.Add(2)
	readSnapshots := func(c *raknet.Conn) {
		defer statsWG.Done()
		for {
			select {
			case <-stopStats:
				return
			default:
				_ = c.Stats()
			}
		}
	}
	go readSnapshots(p.client)
	go readSnapshots(p.server)

	var writersWG sync.WaitGroup
	for i := 0; i < writers; i++ {
		writersWG.Add(1)
		go func(i int) {
			defer writersWG.Done()
			b := make([]byte, msgLen)
			b[0] = 'M'
			for j := 0; j < perWriter; j++ {
				b[msgLen-1] = byte(j)
				if _, err := p.client.Write(b); err != nil {
					t.Errorf("write: %v", err)
					return
				}
			}
		}(i)
	}
	writersWG.Wait()
	close(stopStats)
	statsWG.Wait()

	total := writers * perWriter
	totalBytes := uint64(total * msgLen)

	// Receive every message on the server.
	var received int
	for received < total {
		b, err := p.server.ReadPacket()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if len(b) != msgLen {
			t.Fatalf("got message of %d bytes, want %d", len(b), msgLen)
		}
		received++
	}

	cs := p.client.Stats()
	if cs.PacketsSent != uint64(total) {
		t.Errorf("PacketsSent = %v, want %v", cs.PacketsSent, total)
	}
	if cs.UserBytesSent != totalBytes {
		t.Errorf("UserBytesSent = %v, want %v", cs.UserBytesSent, totalBytes)
	}
	ss := p.server.Stats()
	if ss.PacketsReceived != uint64(total) {
		t.Errorf("PacketsReceived = %v, want %v", ss.PacketsReceived, total)
	}
	if ss.UserBytesReceived != totalBytes {
		t.Errorf("UserBytesReceived = %v, want %v", ss.UserBytesReceived, totalBytes)
	}
}

// TestStatsAfterClose verifies that snapshots remain available after the
// connection is closed and keep reporting the final, frozen counter values.
func TestStatsAfterClose(t *testing.T) {
	p := dialStatsPair(t)

	if _, err := p.client.Write([]byte("final values")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := p.server.ReadPacket(); err != nil {
		t.Fatalf("read: %v", err)
	}

	// Wait until reliable traffic is acknowledged so graceful close is
	// immediate.
	waitFor(t, time.Second*3, func() bool {
		return p.client.Stats().Unacknowledged == 0
	}, "sent datagrams were not acknowledged before close")

	if err := p.client.Close(); err != nil {
		t.Fatalf("client close: %v", err)
	}
	if err := p.server.Close(); err != nil {
		t.Fatalf("server close: %v", err)
	}
	waitClosed(t, p.client)
	waitClosed(t, p.server)

	cs := p.client.Stats()
	ss := p.server.Stats()
	// Final cumulative values are still present.
	if cs.PacketsSent == 0 || cs.UserBytesSent == 0 || cs.BytesSent == 0 {
		t.Errorf("client counters lost after close: %+v", cs)
	}
	if ss.PacketsReceived == 0 || ss.UserBytesReceived == 0 || ss.BytesReceived == 0 {
		t.Errorf("server counters lost after close: %+v", ss)
	}
	if cs.Unacknowledged != 0 {
		t.Errorf("client Unacknowledged = %v, want 0 after close", cs.Unacknowledged)
	}
	if ss.Unacknowledged != 0 {
		t.Errorf("server Unacknowledged = %v, want 0 after close", ss.Unacknowledged)
	}
	if !cs.LastActivity.IsZero() && !ss.LastActivity.IsZero() {
		// Fine: timestamps remain readable.
	}

	// Repeated reads keep working and values stay frozen.
	time.Sleep(time.Millisecond * 50)
	cs2 := p.client.Stats()
	ss2 := p.server.Stats()
	if cs2 != cs {
		t.Errorf("client stats changed after close:\nbefore %+v\nafter  %+v", cs, cs2)
	}
	if ss2 != ss {
		t.Errorf("server stats changed after close:\nbefore %+v\nafter  %+v", ss, ss2)
	}
}

// TestStatsLatencyAndUptimeConsistency verifies that snapshots stay
// consistent with Latency and SystemUptime: uptime is positive and
// monotonic, latency is non-negative, last activity advances with traffic,
// and all of them can be read concurrently.
func TestStatsLatencyAndUptimeConsistency(t *testing.T) {
	p := dialStatsPair(t)

	// The server learns the client's system start during the handshake.
	up1 := p.server.SystemUptime()
	if up1 <= 0 {
		t.Errorf("server SystemUptime = %v, want positive after handshake", up1)
	}
	time.Sleep(time.Millisecond * 20)
	if up2 := p.server.SystemUptime(); up2 < up1 {
		t.Errorf("SystemUptime went backwards: %v -> %v", up1, up2)
	}

	// Exchange traffic both ways and let the resend ticker update RTT.
	if _, err := p.client.Write([]byte("ping consistency 1")); err != nil {
		t.Fatalf("client write: %v", err)
	}
	if _, err := p.server.ReadPacket(); err != nil {
		t.Fatalf("server read: %v", err)
	}
	if _, err := p.server.Write([]byte("ping consistency 2")); err != nil {
		t.Fatalf("server write: %v", err)
	}
	if _, err := p.client.ReadPacket(); err != nil {
		t.Fatalf("client read: %v", err)
	}

	// Receiving traffic must advance LastActivity on the server.
	waitFor(t, time.Second, func() bool {
		return time.Since(p.server.Stats().LastActivity) < time.Second
	}, "LastActivity did not reflect received traffic")

	// Let checkResend run at least once so an RTT sample is stored.
	waitFor(t, time.Second*3, func() bool {
		return p.client.Latency() >= 0 && p.server.Latency() >= 0
	}, "latency became invalid")

	stop := make(chan struct{})
	var wg sync.WaitGroup
	readers := []*raknet.Conn{p.client, p.server}
	for range 4 {
		for _, c := range readers {
			wg.Add(1)
			go func(c *raknet.Conn) {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
						s := c.Stats()
						if c.Latency() < 0 {
							t.Error("negative latency")
						}
						if c.SystemUptime() < 0 {
							t.Error("negative uptime")
						}
						if s.OrderedQueueWindow > 2048 {
							t.Error("ordered queue window out of range")
						}
					}
				}
			}(c)
		}
	}
	time.Sleep(time.Millisecond * 200)
	close(stop)
	wg.Wait()
}
