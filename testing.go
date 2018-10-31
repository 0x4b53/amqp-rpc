package amqprpc

import (
	"net"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

// testDialer returns a dialing function that can be passed to amqp.Config as
// the Dial function. It also returns a function that can be used to get the
// net.Conn object used by amqp to connect.
// nolint: deadcode,megacheck
func testDialer() (func(string, string) (net.Conn, error), chan net.Conn) {
	var (
		conn net.Conn
		ch   = make(chan net.Conn, 100)
	)

	return func(network, addr string) (net.Conn, error) {
		var err error

		conn, err = net.DialTimeout(network, addr, 2*time.Second)
		if err != nil {
			return nil, err
		}
		// Heartbeating hasn't started yet, don't stall forever on a dead server.
		// A deadline is set for TLS and AMQP handshaking. After AMQP is established,
		// the deadline is cleared in openComplete.
		if err = conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
			return nil, err
		}

		ch <- conn
		return conn, nil
	}, ch
}

// startAndWait will start s by running ListenAndServe, it will then block
// until the server is started.
// nolint: deadcode,megacheck
func startAndWait(s *Server) func() {
	started := make(chan struct{})
	once := sync.Once{}
	s.OnStarted(func(_, _ *amqp.Connection, _, _ *amqp.Channel) {
		once.Do(func() {
			close(started)
		})
	})

	done := make(chan struct{})
	go func() {
		s.ListenAndServe()
		close(done)
	}()

	<-started

	return func() {
		s.Stop()
		<-done
	}
}
