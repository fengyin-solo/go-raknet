package raknet

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newTestConns creates two interconnected Conns over loopback UDP, each with
// a goroutine reading datagrams from its socket and feeding them to receive,
// mirroring the production read loops.
func newTestConns(t *testing.T, mtu uint16) (a, b *Conn) {
	t.Helper()
	ua, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	ub, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	// The dialer handler answers connected pings/pongs itself, so periodic
	// keep-alive traffic never ends up on the application packet channel.
	h := dialerConnectionHandler{l: slog.Default()}
	a = newConn(ua, ub.LocalAddr(), mtu, h)
	b = newConn(ub, ua.LocalAddr(), mtu, h)
	go testReadLoop(a)
	go testReadLoop(b)
	t.Cleanup(func() {
		a.closeImmediately()
		b.closeImmediately()
	})
	return a, b
}

func testReadLoop(c *Conn) {
	buf := make([]byte, maxMTUSize)
	for {
		n, _, err := c.conn.ReadFrom(buf)
		if err != nil {
			return
		}
		if n > 0 {
			_ = c.receive(buf[:n])
		}
	}
}

// newInjectConn creates a Conn with no read loop. Datagrams built with
// encodeDatagram can be fed to Conn.receive directly from the test goroutine
// without any other goroutine touching the receive state, which keeps the
// race detector quiet. Datagrams the Conn itself sends (ACKs, keep-alives)
// are read and discarded by a sink socket.
func newInjectConn(t *testing.T) *Conn {
	t.Helper()
	recv, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	sink, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	sinkDone := make(chan struct{})
	go func() {
		buf := make([]byte, maxMTUSize)
		for {
			if _, _, err := sink.ReadFromUDP(buf); err != nil {
				close(sinkDone)
				return
			}
		}
	}()
	c := newConn(recv, sink.LocalAddr(), 0, dialerConnectionHandler{l: slog.Default()})
	t.Cleanup(func() {
		c.closeImmediately()
		_ = sink.Close()
		<-sinkDone
	})
	return c
}

// encodeDatagram serialises one or more encapsulated packets into a single
// datagram with the sequence number passed.
func encodeDatagram(seq uint24, packets ...*packet) []byte {
	var buf bytes.Buffer
	buf.WriteByte(bitFlagDatagram | bitFlagNeedsBAndAS)
	writeUint24(&buf, seq)
	for _, pk := range packets {
		pk.write(&buf)
	}
	return buf.Bytes()
}

func recvPacket(t *testing.T, c *Conn) ReceivedPacket {
	t.Helper()
	type result struct {
		pk  ReceivedPacket
		err error
	}
	ch := make(chan result, 1)
	go func() {
		pk, err := c.ReadPacketWithMetadata()
		ch <- result{pk, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("ReadPacketWithMetadata: %v", r.err)
		}
		return r.pk
	case <-time.After(time.Second * 3):
		t.Fatal("timed out waiting for packet")
	}
	return ReceivedPacket{}
}

func TestReliabilityStableValuesAndString(t *testing.T) {
	cases := []struct {
		rel  Reliability
		n    byte
		name string
	}{
		{Unreliable, 0, "Unreliable"},
		{UnreliableSequenced, 1, "UnreliableSequenced"},
		{Reliable, 2, "Reliable"},
		{ReliableOrdered, 3, "ReliableOrdered"},
		{ReliableSequenced, 4, "ReliableSequenced"},
	}
	var _ fmt.Stringer = ReliableOrdered
	for _, c := range cases {
		if got := byte(c.rel); got != c.n {
			t.Errorf("%s has numeric value %d, want stable value %d", c.name, got, c.n)
		}
		if got := c.rel.String(); got != c.name {
			t.Errorf("String() for numeric value %d = %q, want %q", c.n, got, c.name)
		}
	}
	if got := (Reliability(99)).String(); got != "Reliability(99)" {
		t.Errorf("String() for unknown reliability = %q, want %q", got, "Reliability(99)")
	}
}

func TestReadPacketWithMetadataReliableOrdered(t *testing.T) {
	a, b := newTestConns(t, 0)

	if _, err := b.Write([]byte("first ordered")); err != nil {
		t.Fatal(err)
	}
	pk := recvPacket(t, a)
	if !bytes.Equal(pk.Payload, []byte("first ordered")) {
		t.Fatalf("payload = %q, want %q", pk.Payload, "first ordered")
	}
	if pk.Reliability != ReliableOrdered || pk.Reliability.String() != "ReliableOrdered" {
		t.Fatalf("reliability = %v, want ReliableOrdered", pk.Reliability)
	}
	if pk.MessageIndex != 0 || pk.OrderIndex != 0 {
		t.Fatalf("indices = (message %d, order %d), want (0, 0)", pk.MessageIndex, pk.OrderIndex)
	}
	if pk.Split || pk.SplitCount != 0 || pk.SplitIndex != 0 {
		t.Fatalf("split fields = (%v, %d, %d), want (false, 0, 0)", pk.Split, pk.SplitCount, pk.SplitIndex)
	}

	// A second ordered packet increments both indices independently, showing
	// message and order index are reported separately.
	if _, err := b.Write([]byte("second ordered")); err != nil {
		t.Fatal(err)
	}
	pk = recvPacket(t, a)
	if pk.MessageIndex != 1 || pk.OrderIndex != 1 {
		t.Fatalf("indices = (message %d, order %d), want (1, 1)", pk.MessageIndex, pk.OrderIndex)
	}
}

func TestReadPacketWithMetadataReliableUnordered(t *testing.T) {
	a, b := newTestConns(t, 0)

	if _, err := b.writeWithReliability([]byte("reliable one"), reliabilityReliable); err != nil {
		t.Fatal(err)
	}
	pk := recvPacket(t, a)
	if !bytes.Equal(pk.Payload, []byte("reliable one")) {
		t.Fatalf("payload = %q", pk.Payload)
	}
	if pk.Reliability != Reliable {
		t.Fatalf("reliability = %v, want Reliable", pk.Reliability)
	}
	if pk.MessageIndex != 0 || pk.OrderIndex != 0 {
		t.Fatalf("indices = (message %d, order %d), want (0, 0)", pk.MessageIndex, pk.OrderIndex)
	}

	if _, err := b.writeWithReliability([]byte("reliable two"), reliabilityReliable); err != nil {
		t.Fatal(err)
	}
	pk = recvPacket(t, a)
	// Reliable packets carry a message index but no order index.
	if pk.MessageIndex != 1 || pk.OrderIndex != 0 {
		t.Fatalf("indices = (message %d, order %d), want (1, 0)", pk.MessageIndex, pk.OrderIndex)
	}
}

func TestReadPacketWithMetadataUnreliable(t *testing.T) {
	a, b := newTestConns(t, 0)

	for i := range 2 {
		if _, err := b.writeWithReliability([]byte(fmt.Sprintf("unreliable %d", i)), reliabilityUnreliable); err != nil {
			t.Fatal(err)
		}
		pk := recvPacket(t, a)
		if pk.Reliability != Unreliable {
			t.Fatalf("reliability = %v, want Unreliable", pk.Reliability)
		}
		if pk.MessageIndex != 0 || pk.OrderIndex != 0 {
			t.Fatalf("indices = (message %d, order %d), want zero values for unreliable packet", pk.MessageIndex, pk.OrderIndex)
		}
		if pk.Split || pk.SplitCount != 0 || pk.SplitIndex != 0 {
			t.Fatalf("split fields = (%v, %d, %d), want (false, 0, 0)", pk.Split, pk.SplitCount, pk.SplitIndex)
		}
	}
}

func TestReadPacketWithMetadataMultiplePacketsDatagram(t *testing.T) {
	c := newInjectConn(t)

	err := c.receive(encodeDatagram(123,
		&packet{reliability: reliabilityReliable, messageIndex: 10, content: []byte("first")},
		&packet{reliability: reliabilityUnreliable, content: []byte("second")},
	))
	if err != nil {
		t.Fatal(err)
	}
	pk1 := recvPacket(t, c)
	pk2 := recvPacket(t, c)

	if !bytes.Equal(pk1.Payload, []byte("first")) || !bytes.Equal(pk2.Payload, []byte("second")) {
		t.Fatalf("payloads = %q, %q", pk1.Payload, pk2.Payload)
	}
	if pk1.Reliability != Reliable || pk1.MessageIndex != 10 {
		t.Fatalf("first packet metadata = %+v", pk1)
	}
	if pk2.Reliability != Unreliable || pk2.MessageIndex != 0 {
		t.Fatalf("second packet metadata = %+v", pk2)
	}
	// Both packets came from the same datagram and must share its sequence.
	if pk1.DatagramSequence != 123 || pk2.DatagramSequence != 123 {
		t.Fatalf("datagram sequences = %d and %d, want both 123", pk1.DatagramSequence, pk2.DatagramSequence)
	}
}

func TestReadPacketWithMetadataOrderedQueueKeepsMetadata(t *testing.T) {
	c := newInjectConn(t)

	// Datagrams arrive in sequence (0, 1, 2), but the order indices of the
	// reliable ordered packets inside them arrive out of order: 0, 2, 1.
	dg := []struct {
		seq        uint24
		order, msg uint24
		content    string
	}{
		{0, 0, 7, "o0"},
		{1, 2, 9, "o2"},
		{2, 1, 8, "o1"},
	}
	for _, d := range dg {
		err := c.receive(encodeDatagram(d.seq, &packet{
			reliability:  reliabilityReliableOrdered,
			messageIndex: d.msg,
			orderIndex:   d.order,
			content:      []byte(d.content),
		}))
		if err != nil {
			t.Fatal(err)
		}
	}
	// Delivery must follow order index, while each packet keeps the metadata
	// of the datagram it actually arrived in.
	want := []struct {
		content       string
		seq, msg, ord uint32
	}{
		{"o0", 0, 7, 0},
		{"o1", 2, 8, 1},
		{"o2", 1, 9, 2},
	}
	for _, w := range want {
		pk := recvPacket(t, c)
		if string(pk.Payload) != w.content ||
			pk.DatagramSequence != w.seq || pk.MessageIndex != w.msg || pk.OrderIndex != w.ord {
			t.Fatalf("got %+v, want content %q seq %d message %d order %d", pk, w.content, w.seq, w.msg, w.ord)
		}
	}
}

func TestReadPacketWithMetadataSplitAcrossMTU(t *testing.T) {
	// MTU 400 -> effective MTU 372 -> split fragment payloads are at most 348
	// bytes, so 1000 bytes require exactly 3 fragments.
	a, b := newTestConns(t, minMTUSize)

	// Pattern stays outside the internal RakNet message ID range so the
	// application payload is not consumed by the connection handler.
	payload := make([]byte, 1000)
	for i := range payload {
		payload[i] = byte('A' + i%26)
	}
	if n, err := b.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("Write returned (%d, %v), want (%d, nil)", n, err, len(payload))
	}
	pk := recvPacket(t, a)
	if !bytes.Equal(pk.Payload, payload) {
		t.Fatal("reassembled payload does not match")
	}
	if pk.Reliability != ReliableOrdered {
		t.Fatalf("reliability = %v", pk.Reliability)
	}
	if !pk.Split || pk.SplitCount != 3 || pk.SplitIndex != 2 {
		t.Fatalf("split fields = (%v, count %d, index %d), want (true, 3, 2)", pk.Split, pk.SplitCount, pk.SplitIndex)
	}
	// Each fragment gets its own message index; metadata reflects the final
	// fragment that completed the packet.
	if pk.MessageIndex != 2 {
		t.Fatalf("message index = %d, want 2 (final fragment)", pk.MessageIndex)
	}
}

func TestReadPacketWithMetadataSplitCompletionMetadata(t *testing.T) {
	c := newInjectConn(t)

	fragment := func(seq uint24, index uint32, content string, msg uint24) []byte {
		return encodeDatagram(seq, &packet{
			reliability:  reliabilityReliableOrdered,
			messageIndex: msg,
			orderIndex:   0,
			split:        true,
			splitCount:   2,
			splitIndex:   index,
			splitID:      42,
			content:      []byte(content),
		})
	}

	got := make(chan ReceivedPacket, 1)
	go func() {
		pk, err := c.ReadPacketWithMetadata()
		if err == nil {
			got <- pk
		}
	}()

	if err := c.receive(fragment(0, 0, "AAA", 5)); err != nil {
		t.Fatal(err)
	}
	select {
	case pk := <-got:
		t.Fatalf("incomplete split packet was delivered: %q", pk.Payload)
	case <-time.After(100 * time.Millisecond):
	}

	if err := c.receive(fragment(1, 1, "BBBB", 6)); err != nil {
		t.Fatal(err)
	}
	select {
	case pk := <-got:
		if string(pk.Payload) != "AAABBBB" {
			t.Fatalf("payload = %q, want AAABBBB", pk.Payload)
		}
		if !pk.Split || pk.SplitCount != 2 || pk.SplitIndex != 1 {
			t.Fatalf("split fields = (%v, count %d, index %d)", pk.Split, pk.SplitCount, pk.SplitIndex)
		}
		// Metadata must describe the second datagram/fragment that completed
		// the logical packet.
		if pk.DatagramSequence != 1 || pk.MessageIndex != 6 || pk.OrderIndex != 0 {
			t.Fatalf("metadata = seq %d message %d order %d, want 1/6/0", pk.DatagramSequence, pk.MessageIndex, pk.OrderIndex)
		}
	case <-time.After(time.Second * 3):
		t.Fatal("timed out waiting for completed split packet")
	}
}

func TestReadPacketWithMetadataPayloadIndependence(t *testing.T) {
	a, b := newTestConns(t, minMTUSize)

	check := func(name string, payload []byte) {
		if _, err := b.Write(payload); err != nil {
			t.Fatal(err)
		}
		first := recvPacket(t, a)
		if !bytes.Equal(first.Payload, payload) {
			t.Fatalf("%s: first payload mismatch", name)
		}
		// Mutate the returned payload: it is owned by the caller.
		for i := range first.Payload {
			first.Payload[i] = 0xFF
		}

		if _, err := b.Write(payload); err != nil {
			t.Fatal(err)
		}
		second := recvPacket(t, a)
		if !bytes.Equal(second.Payload, payload) {
			t.Fatalf("%s: later read affected by modification of earlier payload", name)
		}
		if len(second.Payload) > 0 && &first.Payload[0] == &second.Payload[0] {
			t.Fatalf("%s: consecutive payloads share the same backing array", name)
		}
	}

	check("small", bytes.Repeat([]byte{0xAB}, 64))

	big := make([]byte, 1000)
	for i := range big {
		big[i] = byte('A' + i%26)
	}
	check("split", big)
}

func TestReadPacketWithMetadataConcurrentReaders(t *testing.T) {
	// Drive the receive pipeline directly so the test is deterministic and
	// does not depend on loopback UDP delivery: ReadPacketWithMetadata
	// concurrency is what is under test here.
	c := newInjectConn(t)

	const readers, total = 8, 64
	var (
		wg       sync.WaitGroup
		received atomic.Int64
		mu       sync.Mutex
		got      = make(map[string]int)
	)
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				pk, err := c.ReadPacketWithMetadata()
				if err != nil {
					if errors.Is(err, net.ErrClosed) {
						return
					}
					t.Errorf("ReadPacketWithMetadata: %v", err)
					return
				}
				mu.Lock()
				got[string(pk.Payload)]++
				mu.Unlock()
				if received.Add(1) >= total {
					return
				}
			}
		}()
	}

	for i := range total {
		err := c.receive(encodeDatagram(uint24(i), &packet{
			reliability:  reliabilityReliableOrdered,
			messageIndex: uint24(i),
			orderIndex:   uint24(i),
			content:      []byte(fmt.Sprintf("msg-%03d", i)),
		}))
		if err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(time.Second * 3)
	for received.Load() < total {
		if time.Now().After(deadline) {
			t.Fatalf("only received %d of %d packets", received.Load(), total)
		}
		time.Sleep(time.Millisecond)
	}
	// Unblock readers still waiting on the channel.
	c.closeImmediately()
	wg.Wait()

	if len(got) != total {
		t.Fatalf("received %d distinct packets, want %d", len(got), total)
	}
	for payload, count := range got {
		if count != 1 {
			t.Errorf("packet %q received %d times, want exactly once", payload, count)
		}
	}
}

func TestReadPacketWithMetadataAfterClose(t *testing.T) {
	t.Run("read after close", func(t *testing.T) {
		a, _ := newTestConns(t, 0)
		a.closeImmediately()

		if _, err := a.ReadPacketWithMetadata(); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("error = %v, want net.ErrClosed", err)
		}
	})
	t.Run("blocking read unblocked by close", func(t *testing.T) {
		a, _ := newTestConns(t, 0)
		errCh := make(chan error, 1)
		go func() {
			_, err := a.ReadPacketWithMetadata()
			errCh <- err
		}()
		time.Sleep(100 * time.Millisecond)
		a.closeImmediately()

		select {
		case err := <-errCh:
			if !errors.Is(err, net.ErrClosed) {
				t.Fatalf("error = %v, want net.ErrClosed", err)
			}
		case <-time.After(time.Second * 3):
			t.Fatal("blocked read was not unblocked by closing the connection")
		}
	})
}

func TestLegacyReadInterfaces(t *testing.T) {
	a, b := newTestConns(t, 0)

	if _, err := b.Write([]byte("read-packet")); err != nil {
		t.Fatal(err)
	}
	pk, err := a.ReadPacket()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(pk, []byte("read-packet")) {
		t.Fatalf("ReadPacket = %q", pk)
	}

	// The new and legacy interfaces consume the same stream in order.
	if _, err := b.Write([]byte("metadata")); err != nil {
		t.Fatal(err)
	}
	withMeta := recvPacket(t, a)
	if !bytes.Equal(withMeta.Payload, []byte("metadata")) {
		t.Fatalf("metadata payload = %q", withMeta.Payload)
	}

	if _, err := b.Write([]byte("read")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	n, err := a.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 || !bytes.Equal(buf[:n], []byte("read")) {
		t.Fatalf("Read = %q (n=%d)", buf[:n], n)
	}

	// Read with a buffer that is too small keeps its existing error
	// behaviour and consumes the packet.
	if _, err := b.Write([]byte("xy")); err != nil {
		t.Fatal(err)
	}
	n, err = a.Read(make([]byte, 1))
	if n != 0 || !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("Read with small buffer = (%d, %v), want (0, ErrBufferTooSmall)", n, err)
	}

	a.closeImmediately()
	if _, err := a.ReadPacket(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("ReadPacket after close error = %v, want net.ErrClosed", err)
	}
}
