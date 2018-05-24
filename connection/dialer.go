package connection

import (
	"net"
)

var (
	connectionPool []net.Conn
)

// Dialer is a function returning a connection used to connect
// to the message bus.
type Dialer func(string, string) (net.Conn, error)

// DefaultDialer is the RPC server default implementation of
// a dialer.
// func DefaultDialer(network, addr string) (net.Conn, error) {
// 	conn, err := net.DialTimeout(network, addr, 2*time.Second)
// 	if err != nil {
// 		return nil, err
// 	}
// 	// Heartbeating hasn't started yet, don't stall forever on a dead server.
// 	// A deadline is set for TLS and AMQP handshaking. After AMQP is established,
// 	// the deadline is cleared in openComplete.
// 	if err = conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
// 		return nil, err
// 	}

// 	// Add connection to the pool
// 	connectionPool = []net.Conn{conn}

// 	return conn, nil
// }

// GetConnection is a way to hook to the connection used to when connecting
// to the message bus.
// func GetConnection() net.Conn {
// 	s := rand.NewSource(time.Now().Unix())
// 	r := rand.New(s)

// 	return connectionPool[r.Intn(len(connectionPool))]
// }
