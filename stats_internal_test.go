package raknet

import (
	"bytes"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/sandertv/go-raknet/internal"
)

// newWhiteBoxConn creates a Conn backed by a real UDP socket pair. Packets
// sent by the Conn arrive at peer, but no receive loop is running: the test
// drives the Conn directly. The ticker goroutine is stopped on cleanup.
func newWhiteBoxConn(t *testing.T, mtu uint16) (conn *Conn, peer net.PacketConn) {
	t.Helper()
	a, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen a: %v", err)
	}
	b, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		_ = a.Close()
		t.Fatalf("listen b: %v", err)
	}
	t.Cleanup(func() {
		_ = a.Close()
		_ = b.Close()
	})
	logger := slog.New(internal.DiscardHandler{})
	c := newConn(a, b.LocalAddr(), mtu, dialerConnectionHandler{l: logger})
	t.Cleanup(c.cancelFunc)
	return c, b
}

// encodeAcknowledgement builds a complete ACK or NACK packet for the sequence
// numbers passed, including the leading datagram bitflag byte.
func encodeAcknowledgement(t *testing.T, bitflag byte, sequences ...uint24) []byte {
	t.Helper()
	var buf bytes.Buffer
	ack := &acknowledgement{packets: sequences}
	buf.WriteByte(bitflag | bitFlagDatagram)
	if n := ack.write(&buf, maxMTUSize); n != len(sequences) {
		t.Fatalf("encoded %v/%v acknowledgement sequences", n, len(sequences))
	}
	return buf.Bytes()
}

// makeOrderedDatagram builds a data datagram body (without the outer header
// byte) carrying one reliable ordered packet.
func makeOrderedDatagram(seq uint24, content byte) []byte {
	pk := &packet{
		reliability:  reliabilityReliableOrdered,
		messageIndex: seq,
		orderIndex:   seq,
		content:      []byte{content, 0x01, 0x02},
	}
	var buf bytes.Buffer
	writeUint24(&buf, seq)
	pk.write(&buf)
	return buf.Bytes()
}

// TestStatsZeroSnapshot verifies a brand new connection reports zeroed
// counters, the negotiated MTU and a recent last activity timestamp.
func TestStatsZeroSnapshot(t *testing.T) {
	conn, _ := newWhiteBoxConn(t, 1200)

	s := conn.Stats()
	if s.MTU != 1200 {
		t.Errorf("MTU = %v, want 1200", s.MTU)
	}
	if s.BytesSent != 0 || s.BytesReceived != 0 ||
		s.DatagramsSent != 0 || s.DatagramsReceived != 0 ||
		s.ACKsSent != 0 || s.ACKsReceived != 0 ||
		s.NACKsSent != 0 || s.NACKsReceived != 0 ||
		s.Retransmissions != 0 || s.SplitsCompleted != 0 ||
		s.PacketsSent != 0 || s.PacketsReceived != 0 ||
		s.UserBytesSent != 0 || s.UserBytesReceived != 0 {
		t.Errorf("new connection has non-zero counters: %+v", s)
	}
	if s.Unacknowledged != 0 {
		t.Errorf("Unacknowledged = %v, want 0", s.Unacknowledged)
	}
	if s.OrderedQueueWindow != 0 {
		t.Errorf("OrderedQueueWindow = %v, want 0", s.OrderedQueueWindow)
	}
	if s.LastActivity.IsZero() || time.Since(s.LastActivity) > time.Second*5 {
		t.Errorf("LastActivity = %v, want a recent timestamp", s.LastActivity)
	}
}

// TestStatsDoesNotBlockOnLocks verifies Stats returns immediately even while
// the send lock and ACK lock are held, i.e. it never waits on connection
// locks or network operations.
func TestStatsDoesNotBlockOnLocks(t *testing.T) {
	conn, _ := newWhiteBoxConn(t, 1200)

	conn.mu.Lock()
	conn.ackMu.Lock()
	done := make(chan Stats, 1)
	go func() { done <- conn.Stats() }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stats blocked while conn.mu and ackMu were held")
	}
	conn.ackMu.Unlock()
	conn.mu.Unlock()
}

// TestStatsRetransmitAndAcknowledgement drives one reliable datagram through
// a NACK-triggered retransmission and an ACK, verifying all counters and the
// unacknowledged gauge, and ensuring user byte counters never move.
func TestStatsRetransmitAndAcknowledgement(t *testing.T) {
	conn, _ := newWhiteBoxConn(t, 1200)

	conn.mu.Lock()
	pk := packetPool.Get().(*packet)
	pk.reliability = reliabilityReliableOrdered
	pk.messageIndex = 1
	pk.orderIndex = 1
	pk.content = []byte{0x7a, 1, 2, 3, 4}
	if err := conn.sendDatagram(pk); err != nil {
		t.Fatalf("send datagram: %v", err)
	}
	firstSeq := conn.seq - 1
	conn.mu.Unlock()

	s := conn.Stats()
	if s.DatagramsSent != 1 {
		t.Errorf("DatagramsSent = %v, want 1", s.DatagramsSent)
	}
	if s.BytesSent == 0 {
		t.Error("BytesSent = 0 after sending a datagram")
	}
	if s.Unacknowledged != 1 {
		t.Errorf("Unacknowledged = %v, want 1", s.Unacknowledged)
	}
	if s.PacketsSent != 0 || s.UserBytesSent != 0 {
		t.Errorf("internal datagram counted as user traffic: %+v", s)
	}
	if s.Retransmissions != 0 {
		t.Errorf("Retransmissions = %v, want 0", s.Retransmissions)
	}

	// Receive a NACK: the datagram must be retransmitted with a new sequence
	// number, and its payload bytes must not be re-counted at the data layer.
	if err := conn.receive(encodeAcknowledgement(t, bitFlagNACK, firstSeq)); err != nil {
		t.Fatalf("receive NACK: %v", err)
	}
	s = conn.Stats()
	if s.NACKsReceived != 1 {
		t.Errorf("NACKsReceived = %v, want 1", s.NACKsReceived)
	}
	if s.Retransmissions != 1 {
		t.Errorf("Retransmissions = %v, want 1", s.Retransmissions)
	}
	if s.DatagramsSent != 2 {
		t.Errorf("DatagramsSent = %v, want 2 after retransmit", s.DatagramsSent)
	}
	if s.Unacknowledged != 1 {
		t.Errorf("Unacknowledged = %v, want 1 (old record replaced by new)", s.Unacknowledged)
	}
	if s.PacketsSent != 0 || s.UserBytesSent != 0 {
		t.Errorf("retransmit inflated user counters: packets=%v bytes=%v", s.PacketsSent, s.UserBytesSent)
	}

	// A NACK for an unknown sequence number changes nothing but the received
	// NACK is still counted.
	if err := conn.receive(encodeAcknowledgement(t, bitFlagNACK, 9999)); err != nil {
		t.Fatalf("receive stale NACK: %v", err)
	}
	s = conn.Stats()
	if s.NACKsReceived != 2 || s.Retransmissions != 1 || s.DatagramsSent != 2 || s.Unacknowledged != 1 {
		t.Errorf("stale NACK changed unrelated counters: %+v", s)
	}

	// ACK the retransmitted sequence number: the gauge must drop to zero.
	currentSeq := conn.seq - 1
	if currentSeq == firstSeq {
		t.Fatal("retransmitted datagram was not assigned a new sequence number")
	}
	if err := conn.receive(encodeAcknowledgement(t, bitFlagACK, currentSeq)); err != nil {
		t.Fatalf("receive ACK: %v", err)
	}
	s = conn.Stats()
	if s.ACKsReceived != 1 {
		t.Errorf("ACKsReceived = %v, want 1", s.ACKsReceived)
	}
	if s.Unacknowledged != 0 {
		t.Errorf("Unacknowledged = %v, want 0 after ACK", s.Unacknowledged)
	}
}

// TestStatsIncomingDatagrams verifies received datagram, NACK and ordered
// queue window accounting when datagrams arrive out of order with a gap.
func TestStatsIncomingDatagrams(t *testing.T) {
	conn, _ := newWhiteBoxConn(t, 1200)

	if err := conn.receiveDatagram(makeOrderedDatagram(0, 0x7a)); err != nil {
		t.Fatalf("receive datagram 0: %v", err)
	}
	if err := conn.receiveDatagram(makeOrderedDatagram(2, 0x7b)); err != nil {
		t.Fatalf("receive datagram 2: %v", err)
	}

	s := conn.Stats()
	if s.DatagramsReceived != 2 {
		t.Errorf("DatagramsReceived = %v, want 2", s.DatagramsReceived)
	}
	// Order index 0 delivered, order index 2 is waiting for order index 1.
	if s.OrderedQueueWindow != 2 {
		t.Errorf("OrderedQueueWindow = %v, want 2", s.OrderedQueueWindow)
	}
	if s.PacketsReceived != 1 {
		t.Errorf("PacketsReceived = %v, want 1 delivered message", s.PacketsReceived)
	}

	// Age the received sequence so the missing index is considered lost.
	conn.win.queue[2] = time.Now().Add(-time.Second)
	if err := conn.receiveDatagram(makeOrderedDatagram(3, 0x7c)); err != nil {
		t.Fatalf("receive datagram 3: %v", err)
	}
	s = conn.Stats()
	if s.DatagramsReceived != 3 {
		t.Errorf("DatagramsReceived = %v, want 3", s.DatagramsReceived)
	}
	if s.NACKsSent != 1 {
		t.Errorf("NACKsSent = %v, want 1 for the missing datagram", s.NACKsSent)
	}
	// Order indices 2 and 3 remain queued behind the missing order index 1.
	if s.OrderedQueueWindow != 3 {
		t.Errorf("OrderedQueueWindow = %v, want 3", s.OrderedQueueWindow)
	}
	if s.PacketsReceived != 1 {
		t.Errorf("PacketsReceived = %v, want still 1 while a gap exists", s.PacketsReceived)
	}

	// A duplicate datagram is not counted again.
	if err := conn.receiveDatagram(makeOrderedDatagram(2, 0x7b)); err != nil {
		t.Fatalf("receive duplicate datagram: %v", err)
	}
	s = conn.Stats()
	if s.DatagramsReceived != 3 {
		t.Errorf("DatagramsReceived = %v, want 3 (duplicate ignored)", s.DatagramsReceived)
	}
}

// TestStatsReadableAfterCancel verifies snapshots keep working after the
// connection context was cancelled and report the final values.
func TestStatsReadableAfterCancel(t *testing.T) {
	conn, _ := newWhiteBoxConn(t, 1200)

	conn.mu.Lock()
	pk := packetPool.Get().(*packet)
	pk.reliability = reliabilityReliableOrdered
	pk.messageIndex = 1
	pk.orderIndex = 1
	pk.content = []byte{0x7a}
	if err := conn.sendDatagram(pk); err != nil {
		t.Fatalf("send datagram: %v", err)
	}
	conn.mu.Unlock()

	before := conn.Stats()
	conn.cancelFunc()
	// Simulate the cleanup closeImmediately performs on the recovery queue.
	conn.stats.unacknowledged.Store(0)

	after := conn.Stats()
	if after.DatagramsSent != before.DatagramsSent || after.BytesSent != before.BytesSent {
		t.Error("cumulative counters changed after shutdown")
	}
	if after.Unacknowledged != 0 {
		t.Errorf("Unacknowledged = %v, want 0", after.Unacknowledged)
	}
	if after.MTU != 1200 {
		t.Errorf("MTU = %v, want 1200", after.MTU)
	}
}
