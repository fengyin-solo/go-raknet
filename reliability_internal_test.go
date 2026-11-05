package raknet

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/sandertv/go-raknet/internal"
)

// newTestConn returns a Conn backed by two real loopback UDP sockets: the
// *Conn writes to the returned peer socket, which tests may read from to
// inspect datagrams. The connection does not run a RakNet handshake.
func newTestConn(t *testing.T, mtu uint16) (*Conn, net.PacketConn) {
	t.Helper()
	a, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	b, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() {
		_ = a.Close()
		_ = b.Close()
	})
	c := newConn(a, b.LocalAddr(), mtu, dialerConnectionHandler{l: slog.New(internal.DiscardHandler{})})
	t.Cleanup(c.cancelFunc)
	return c, b
}

// countDatagrams reads datagrams from the peer socket until no datagram
// arrives within wait after the previous read. ACK and NACK packets are not
// counted.
func countDatagrams(t *testing.T, peer net.PacketConn, wait time.Duration) int {
	t.Helper()
	_ = peer.SetReadDeadline(time.Now().Add(wait))
	buf := make([]byte, 2048)
	var count int
	for {
		n, _, err := peer.ReadFrom(buf)
		if err != nil {
			return count
		}
		if n > 0 && buf[0]&bitFlagDatagram != 0 && buf[0]&bitFlagACK == 0 && buf[0]&bitFlagNACK == 0 {
			count++
		}
		_ = peer.SetReadDeadline(time.Now().Add(wait))
	}
}

// TestPacketReliabilityEncoding verifies that the reliability bits and the
// presence of message/order indices are encoded and decoded according to the
// reliability, covering the on-wire header of all three public entries.
func TestPacketReliabilityEncoding(t *testing.T) {
	cases := []struct {
		name             string
		rel              reliability
		encapsulatedSize int // header bytes before content (without split fields)
	}{
		{"unreliable", reliabilityUnreliable, 1 + 2},
		{"reliable unordered", reliabilityReliable, 1 + 2 + 3},
		{"reliable ordered", reliabilityReliableOrdered, 1 + 2 + 3 + 3 + 1},
	}
	content := []byte("hello")
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pk := &packet{reliability: c.rel, content: content, messageIndex: 0x112233, orderIndex: 0x445566}
			buf := bytes.NewBuffer(nil)
			pk.write(buf)
			raw := buf.Bytes()

			if len(raw) != c.encapsulatedSize+len(content) {
				t.Fatalf("encoded size is %v, expected %v", len(raw), c.encapsulatedSize+len(content))
			}
			if got := raw[0] & 224; got != byte(c.rel)<<5 {
				t.Fatalf("reliability bits are %02x, expected %02x", got, byte(c.rel)<<5)
			}
			if raw[0]&splitFlag != 0 {
				t.Fatalf("split flag should not be set for an unsplit packet")
			}
			if binary.BigEndian.Uint16(raw[1:]) != uint16(len(content))<<3 {
				t.Fatalf("encoded content length mismatch")
			}
			if c.rel.reliable() {
				if got := loadUint24(raw[3:]); got != 0x112233 {
					t.Fatalf("message index encoded as %x, expected 112233", got)
				}
			}
			if c.rel.sequencedOrOrdered() {
				if got := loadUint24(raw[6:]); got != 0x445566 {
					t.Fatalf("order index encoded as %x, expected 445566", got)
				}
				if raw[9] != 0 {
					t.Fatalf("order channel should be 0, got %v", raw[9])
				}
			}

			got := &packet{}
			n, err := got.read(raw)
			if err != nil {
				t.Fatalf("decoding packet: %v", err)
			}
			if n != len(raw) {
				t.Fatalf("decoded %v bytes, expected %v", n, len(raw))
			}
			if got.reliability != c.rel {
				t.Fatalf("decoded reliability %v, expected %v", got.reliability, c.rel)
			}
			if !bytes.Equal(got.content, content) {
				t.Fatalf("decoded content %q, expected %q", got.content, content)
			}
			if c.rel.reliable() && got.messageIndex != 0x112233 {
				t.Fatalf("decoded message index %x, expected 112233", got.messageIndex)
			}
			if c.rel == reliabilityReliableOrdered && got.orderIndex != 0x445566 {
				t.Fatalf("decoded order index %x, expected 445566", got.orderIndex)
			}
		})
	}
}

// TestPacketSplitEncoding verifies split metadata is encoded for a reliable
// unordered fragment without any order fields preceding it.
func TestPacketSplitEncoding(t *testing.T) {
	pk := &packet{
		reliability:  reliabilityReliable,
		messageIndex: 0x010203,
		content:      []byte("fragment"),
		split:        true,
		splitCount:   3,
		splitIndex:   1,
		splitID:      0x0a0b,
	}
	buf := bytes.NewBuffer(nil)
	pk.write(buf)
	raw := buf.Bytes()

	// header + length + message index + split fields + content.
	expected := 1 + 2 + 3 + splitAdditionalSize + len(pk.content)
	if len(raw) != expected {
		t.Fatalf("encoded split packet size is %v, expected %v", len(raw), expected)
	}
	if raw[0] != byte(reliabilityReliable)<<5|splitFlag {
		t.Fatalf("split packet header is %02x, expected %02x", raw[0], byte(reliabilityReliable)<<5|splitFlag)
	}
	if off := 1 + 2 + 3; binary.BigEndian.Uint32(raw[off:]) != 3 {
		t.Fatalf("split count mismatch")
	} else if binary.BigEndian.Uint16(raw[off+4:]) != 0x0a0b {
		t.Fatalf("split id mismatch")
	} else if binary.BigEndian.Uint32(raw[off+6:]) != 1 {
		t.Fatalf("split index mismatch")
	}

	got := &packet{}
	if _, err := got.read(raw); err != nil {
		t.Fatalf("decoding split packet: %v", err)
	}
	if !got.split || got.splitCount != 3 || got.splitIndex != 1 || got.splitID != 0x0a0b {
		t.Fatalf("decoded split metadata mismatch: %+v", got)
	}
	if got.reliability != reliabilityReliable || got.messageIndex != 0x010203 {
		t.Fatalf("decoded reliability/message index mismatch: %+v", got)
	}
}

// TestSplitFitsMTU verifies split fragments of every reliability fit inside
// the effective MTU once encapsulated in a datagram.
func TestSplitFitsMTU(t *testing.T) {
	const mtu = minMTUSize
	effective := int(mtu - 28)
	payload := make([]byte, 1000)
	for _, rel := range []reliability{reliabilityUnreliable, reliabilityReliable, reliabilityReliableOrdered} {
		fragments := split(payload, uint16(effective))
		if len(fragments) <= 1 {
			t.Fatalf("%v: expected payload to be split, got %v fragment", rel, len(fragments))
		}
		for i, content := range fragments {
			pk := &packet{
				reliability: rel,
				content:     content,
				split:       true,
				splitCount:  uint32(len(fragments)),
				splitIndex:  uint32(i),
				splitID:     0,
			}
			buf := bytes.NewBuffer(nil)
			// Datagram header and sequence number.
			buf.WriteByte(bitFlagDatagram | bitFlagNeedsBAndAS)
			writeUint24(buf, 0)
			pk.write(buf)
			if buf.Len() > effective {
				t.Fatalf("%v fragment %v datagram is %v bytes, exceeds effective MTU %v", rel, i, buf.Len(), effective)
			}
		}
	}
	if fragments := split(make([]byte, 2*(minMTUSize-28-packetAdditionalSize-splitAdditionalSize)), minMTUSize-28); len(fragments) != 2 {
		t.Fatalf("expected 2 fragments for exact 2-fragment payload, got %v", len(fragments))
	}
	if fragments := split([]byte{1}, minMTUSize-28); len(fragments) != 1 {
		t.Fatalf("expected 1 fragment for short payload, got %v", len(fragments))
	}
}

// TestWriteIndicesAndRetransmissionQueue verifies that unreliable packets
// never enter the retransmission queue or consume indices, reliable unordered
// packets consume only the message index (independent of the ordered channel)
// and ordered packets consume both, including per fragment.
func TestWriteIndicesAndRetransmissionQueue(t *testing.T) {
	c, _ := newTestConn(t, minMTUSize)

	if n, err := c.WriteUnreliable([]byte("unreliable")); err != nil || n != 10 {
		t.Fatalf("WriteUnreliable returned n=%v, err=%v", n, err)
	}
	if len(c.retransmission.unacknowledged) != 0 {
		t.Fatalf("unreliable packet entered retransmission queue: %v entries", len(c.retransmission.unacknowledged))
	}
	if c.messageIndex != 0 || c.orderIndex != 0 {
		t.Fatalf("unreliable packet consumed indices: message=%v order=%v", c.messageIndex, c.orderIndex)
	}
	seqAfterUnreliable := c.seq

	// Payload of exactly two fragments for an MTU of 400: max fragment content
	// is 400-28-packetAdditionalSize-splitAdditionalSize = 348 bytes.
	fragmentPayload := bytes.Repeat([]byte{'r'}, 2*(minMTUSize-28-packetAdditionalSize-splitAdditionalSize))
	if n, err := c.WriteReliable(fragmentPayload); err != nil || n != len(fragmentPayload) {
		t.Fatalf("WriteReliable returned n=%v, err=%v", n, err)
	}
	if c.seq != seqAfterUnreliable+2 {
		t.Fatalf("expected 2 datagram sequence numbers, seq went from %v to %v", seqAfterUnreliable, c.seq)
	}
	if len(c.retransmission.unacknowledged) != 2 {
		t.Fatalf("expected 2 reliable fragments in retransmission queue, got %v", len(c.retransmission.unacknowledged))
	}
	for _, record := range c.retransmission.unacknowledged {
		if record.pk.reliability != reliabilityReliable {
			t.Fatalf("queued packet has reliability %v, expected reliable", record.pk.reliability)
		}
		if !record.pk.split {
			t.Fatalf("queued packet should be marked as split")
		}
	}
	if c.messageIndex != 2 {
		t.Fatalf("expected message index 2 after two reliable fragments, got %v", c.messageIndex)
	}
	if c.orderIndex != 0 {
		t.Fatalf("reliable unordered packets must not consume an order index, got %v", c.orderIndex)
	}

	if n, err := c.Write([]byte("ordered")); err != nil || n != 7 {
		t.Fatalf("Write returned n=%v, err=%v", n, err)
	}
	if c.messageIndex != 3 || c.orderIndex != 1 {
		t.Fatalf("ordered packet should consume both indices, got message=%v order=%v", c.messageIndex, c.orderIndex)
	}
	if len(c.retransmission.unacknowledged) != 3 {
		t.Fatalf("expected 3 queued datagrams, got %v", len(c.retransmission.unacknowledged))
	}

	// A fragmented unreliable write must not end up in the queue either.
	before := len(c.retransmission.unacknowledged)
	if n, err := c.WriteUnreliable(bytes.Repeat([]byte{'u'}, 700)); err != nil || n != 700 {
		t.Fatalf("fragmented WriteUnreliable returned n=%v, err=%v", n, err)
	}
	if len(c.retransmission.unacknowledged) != before {
		t.Fatalf("unreliable fragments must not enter the retransmission queue")
	}
}

// TestUnreliableNotRetransmitted forces a resend tick and verifies unreliable
// packets are sent exactly once while reliable packets are retransmitted.
func TestUnreliableNotRetransmitted(t *testing.T) {
	t.Run("unreliable", func(t *testing.T) {
		c, peer := newTestConn(t, minMTUSize)
		if _, err := c.WriteUnreliable([]byte("u")); err != nil {
			t.Fatal(err)
		}
		// Past the resend threshold (rtt defaults to 50ms, threshold 1.5*rtt).
		c.checkResend(time.Now().Add(100 * time.Millisecond))
		if count := countDatagrams(t, peer, 150*time.Millisecond); count != 1 {
			t.Fatalf("unreliable packet sent %v times, expected exactly 1", count)
		}
	})
	t.Run("reliable", func(t *testing.T) {
		c, peer := newTestConn(t, minMTUSize)
		if _, err := c.WriteReliable([]byte("r")); err != nil {
			t.Fatal(err)
		}
		c.checkResend(time.Now().Add(100 * time.Millisecond))
		if count := countDatagrams(t, peer, 150*time.Millisecond); count < 2 {
			t.Fatalf("reliable packet sent %v times, expected at least 2 after resend", count)
		}
	})
}

// encDatagram builds a datagram containing a single encapsulated packet.
func encDatagram(seq uint24, pk *packet) []byte {
	buf := bytes.NewBuffer(nil)
	buf.WriteByte(bitFlagDatagram | bitFlagNeedsBAndAS)
	writeUint24(buf, seq)
	pk.write(buf)
	return buf.Bytes()
}

func encPacket(rel reliability, messageIndex, orderIndex uint24, content []byte) *packet {
	pk := &packet{reliability: rel, content: content}
	if rel.reliable() {
		pk.messageIndex = messageIndex
	}
	if rel.sequencedOrOrdered() {
		pk.orderIndex = orderIndex
	}
	return pk
}

// recvQueued receives one packet delivered to Conn.packets or times out.
func recvQueued(c *Conn, timeout time.Duration) ([]byte, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.packets.Recv(ctx)
}

// TestReceiveDispatchesByReliability feeds crafted datagrams with a gap in the
// ordered stream and verifies that reliable unordered and unreliable packets
// are handled immediately while the missing ordered index keeps later ordered
// packets queued.
func TestReceiveDispatchesByReliability(t *testing.T) {
	c, _ := newTestConn(t, minMTUSize)
	// 0xAA is not an internal message ID, so packets are passed through.
	ordered := func(seq, msg, ord uint24, s string) []byte {
		return encDatagram(seq, encPacket(reliabilityReliableOrdered, msg, ord, append([]byte{0xaa}, s...)))
	}

	feed := func(b []byte) {
		t.Helper()
		if err := c.receive(b); err != nil {
			t.Fatalf("receive: %v", err)
		}
	}
	expect := func(want string) {
		t.Helper()
		got, ok := recvQueued(c, time.Second)
		if !ok {
			t.Fatalf("expected packet %q, got none", want)
		}
		if string(got[1:]) != want {
			t.Fatalf("expected %q, got %q", want, got[1:])
		}
	}

	feed(ordered(0, 0, 0, "O0"))
	expect("O0")

	// Ordered index 1 is missing: index 2 must remain queued. Datagram seq 2
	// leaves datagram seq 1 missing as well, which only produces a NACK to the
	// (raw, non-answering) peer.
	feed(ordered(2, 1, 2, "O2"))
	feed(encDatagram(3, encPacket(reliabilityReliable, 2, 0, append([]byte{0xaa}, "R0"...))))
	feed(encDatagram(4, encPacket(reliabilityReliable, 3, 0, append([]byte{0xaa}, "R1"...))))
	feed(encDatagram(5, encPacket(reliabilityUnreliable, 0, 0, append([]byte{0xaa}, "U0"...))))

	if size := c.packetQueue.WindowSize(); size != 2 {
		t.Fatalf("ordered queue window size should be 2 (gap at index 1), got %v", size)
	}
	expect("R0")
	expect("R1")
	expect("U0")
	if _, ok := recvQueued(c, 150*time.Millisecond); ok {
		t.Fatalf("ordered packet behind a gap should not be delivered yet")
	}

	// Filling the gap flushes ordered index 1 and 2 in order, nothing stale
	// from the unordered packets is mixed in. Datagram seq 6 is used because
	// the missing seq 1 was already NACKed by the datagram window.
	feed(ordered(6, 4, 1, "O1"))
	expect("O1")
	expect("O2")
	if _, ok := recvQueued(c, 150*time.Millisecond); ok {
		t.Fatalf("did not expect any more packets")
	}
}

// TestWritesAfterClose verifies all three entries fail with a wrapped
// net.ErrClosed after the connection has been closed, even if reliable
// packets were still pending when closing.
func TestWritesAfterClose(t *testing.T) {
	c, _ := newTestConn(t, minMTUSize)
	if _, err := c.WriteReliable([]byte("pending")); err != nil {
		t.Fatal(err)
	}
	c.closeImmediately()

	writers := map[string]func([]byte) (int, error){
		"Write":           c.Write,
		"WriteReliable":   c.WriteReliable,
		"WriteUnreliable": c.WriteUnreliable,
	}
	for name, write := range writers {
		n, err := write([]byte("x"))
		if n != 0 {
			t.Fatalf("%s after close returned n=%v, expected 0", name, n)
		}
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("%s after close returned error %v, expected net.ErrClosed", name, err)
		}
		var opErr *net.OpError
		if !errors.As(err, &opErr) || opErr.Op != "write" {
			t.Fatalf("%s after close returned %v, expected a wrapped net.OpError with op write", name, err)
		}
	}
}

// TestWriteToClosedSocket ensures a write whose underlying socket is closed
// still reports n == 0 and a net.ErrClosed-wrapped error.
func TestWriteToClosedSocket(t *testing.T) {
	c, _ := newTestConn(t, minMTUSize)
	_ = c.conn.Close()
	for name, write := range map[string]func([]byte) (int, error){
		"Write":           c.Write,
		"WriteReliable":   c.WriteReliable,
		"WriteUnreliable": c.WriteUnreliable,
	} {
		if n, err := write([]byte("x")); n != 0 || !errors.Is(err, net.ErrClosed) {
			t.Fatalf("%s on closed socket returned n=%v, err=%v", name, n, err)
		}
	}
}
