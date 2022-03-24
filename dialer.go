package amqprpc

import (
	"net"
	"time"
)

// Dialer is a function returning a connection used to connect
// to the message bus.
type Dialer func(string, string) (net.Conn, error)

// DialConf is a structure that you can pass to the default dialer to
// change the defautl timeouts for dailing and handshakes.
type DialConf struct {
	DialTimeout time.Duration
	Deadline    time.Duration
}

// DefaultDialer will return the default RPC server default implementation of
// a dialer. With the DialConf parameter the default timeous can be specified.
func DefaultDialer(config DialConf) Dialer {
	return func(network, addr string) (net.Conn, error) {
		conn, err := net.DialTimeout(network, addr, config.DialTimeout)
		if err != nil {
			return nil, err
		}
		// Heartbeating hasn't started yet, don't stall forever on a dead server.
		// A deadline is set for TLS and AMQP handshaking. After AMQP is established,
		// the deadline is cleared in openComplete.
		if err := conn.SetDeadline(time.Now().Add(config.Deadline)); err != nil {
			return nil, err
		}

		return conn, nil
	}
}
