package testhelpers

import (
	"time"

	amqprpc "github.com/bombsimon/amqp-rpc"
	"github.com/streadway/amqp"
)

// The interface is used to avoid circular imports in tests.
type serverer interface {
	ListenAndServe()
	Stop()
}

// StartServer will start the server s async and then return a function that
// can be used to stop the server.
func StartServer(s serverer) func() {
	done := make(chan struct{})

	go func() {
		s.ListenAndServe()
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)

	return func() {
		s.Stop()
		<-done
	}
}

// NewTestResponseWriter returns an empty *ResponseWriter for test purposes.
func NewTestResponseWriter() *amqprpc.ResponseWriter {
	return amqprpc.NewResponseWriter(&amqp.Publishing{})
}
