package raknet

import (
	"sync/atomic"
	"time"
)

// Stats is a point-in-time, concurrency-safe snapshot of the statistics of a
// Conn. All counters are cumulative since the connection was created and only
// increase. Gauges reflect the current connection state and may go up and
// down.
//
// Stats may be obtained at any time, including after the connection was
// closed: the final values of all counters remain available.
type Stats struct {
	// MTU is the maximum transmission unit negotiated for the connection.
	// Packets larger than the MTU are split into fragments.
	MTU uint16

	// BytesSent is the total number of bytes handed to the underlying UDP
	// connection at the transport layer. It counts every transmitted
	// datagram, including ACK and NACK packets and retransmitted datagrams.
	BytesSent uint64
	// BytesReceived is the total number of bytes read from the underlying
	// UDP connection at the transport layer and accepted as RakNet packets.
	// It counts every received datagram, including ACK and NACK packets and
	// duplicate datagrams.
	BytesReceived uint64

	// DatagramsSent is the number of data datagrams sent at the transport
	// layer. Retransmitted datagrams are counted each time they are sent.
	// ACK and NACK packets are not data datagrams and are not counted here.
	DatagramsSent uint64
	// DatagramsReceived is the number of unique data datagrams received.
	// Duplicate datagrams that were already delivered are not counted.
	DatagramsReceived uint64

	// ACKsSent is the number of ACK packets sent.
	ACKsSent uint64
	// ACKsReceived is the number of ACK packets received.
	ACKsReceived uint64
	// NACKsSent is the number of NACK packets sent.
	NACKsSent uint64
	// NACKsReceived is the number of NACK packets received.
	NACKsReceived uint64

	// Retransmissions is the number of datagrams that were retransmitted,
	// either because a NACK requested them or because they had been
	// unacknowledged for too long.
	Retransmissions uint64

	// SplitsCompleted is the number of fragmented messages of which all
	// fragments were received and that were reassembled into a single
	// message.
	SplitsCompleted uint64

	// PacketsSent is the number of user messages successfully written using
	// Write. A message that is split into multiple fragments, as well as a
	// message that is retransmitted, is counted exactly once.
	PacketsSent uint64
	// PacketsReceived is the number of user messages delivered to Read or
	// ReadPacket. A fragmented message is counted once, after all of its
	// fragments have been reassembled.
	PacketsReceived uint64

	// UserBytesSent is the total number of payload bytes written using Write
	// at the data layer. Fragmentation overhead and retransmissions are not
	// counted, so the value equals the sum of the lengths of all buffers
	// passed to successful Write calls.
	UserBytesSent uint64
	// UserBytesReceived is the total number of payload bytes delivered to
	// Read or ReadPacket at the data layer. It counts the reassembled
	// payload of each user message exactly once.
	UserBytesReceived uint64

	// Unacknowledged is the current number of sent reliable datagrams that
	// have not yet been acknowledged by the other end.
	Unacknowledged int
	// OrderedQueueWindow is the current window size of the ordered packet
	// queue: the distance between the oldest and newest reliable ordered
	// message that is waiting for missing messages to arrive.
	OrderedQueueWindow uint32

	// LastActivity is the time the last datagram, ACK or NACK packet was
	// received from the other end.
	LastActivity time.Time
}

// connStats holds the atomic counters that back Conn.Stats(). Every field is
// updated using atomic operations, which allows Stats to take a snapshot
// without locking conn.mu and without waiting for any network operation.
type connStats struct {
	bytesSent         atomic.Uint64
	bytesReceived     atomic.Uint64
	datagramsSent     atomic.Uint64
	datagramsReceived atomic.Uint64
	acksSent          atomic.Uint64
	acksReceived      atomic.Uint64
	nacksSent         atomic.Uint64
	nacksReceived     atomic.Uint64
	retransmissions   atomic.Uint64
	splitsCompleted   atomic.Uint64

	packetsSent       atomic.Uint64
	packetsReceived   atomic.Uint64
	userBytesSent     atomic.Uint64
	userBytesReceived atomic.Uint64

	// unacknowledged mirrors resendMap.unacknowledged and is updated while
	// conn.mu is held, but read without the lock.
	unacknowledged atomic.Int64
	// orderedWindow mirrors packetQueue.WindowSize() and is updated from the
	// receive goroutine, but read without the lock.
	orderedWindow atomic.Uint32
}

// Stats returns a snapshot of the connection's statistics. The call returns
// immediately: it only reads atomic counters and never waits for the send
// lock or any network operation. It is safe to call Stats concurrently with
// Read, Write and Close, and after the connection has been closed.
func (conn *Conn) Stats() Stats {
	s := Stats{
		MTU: conn.mtu,

		BytesSent:         conn.stats.bytesSent.Load(),
		BytesReceived:     conn.stats.bytesReceived.Load(),
		DatagramsSent:     conn.stats.datagramsSent.Load(),
		DatagramsReceived: conn.stats.datagramsReceived.Load(),
		ACKsSent:          conn.stats.acksSent.Load(),
		ACKsReceived:      conn.stats.acksReceived.Load(),
		NACKsSent:         conn.stats.nacksSent.Load(),
		NACKsReceived:     conn.stats.nacksReceived.Load(),
		Retransmissions:   conn.stats.retransmissions.Load(),
		SplitsCompleted:   conn.stats.splitsCompleted.Load(),

		PacketsSent:       conn.stats.packetsSent.Load(),
		PacketsReceived:   conn.stats.packetsReceived.Load(),
		UserBytesSent:     conn.stats.userBytesSent.Load(),
		UserBytesReceived: conn.stats.userBytesReceived.Load(),

		Unacknowledged:     int(conn.stats.unacknowledged.Load()),
		OrderedQueueWindow: conn.stats.orderedWindow.Load(),
	}
	if last := conn.lastActivity.Load(); last != nil {
		s.LastActivity = *last
	}
	return s
}
