package amqprpc

import (
	"github.com/streadway/amqp"
)

// ackAwareChannel implements the amqp.Acknowledger interface with the addition
// that it can tell if a message has been acked, nacked or rejected.
type ackAwareChannel struct {
	ch      amqp.Acknowledger
	handled bool
}

func (a *ackAwareChannel) Ack(tag uint64, multiple bool) error {
	a.handled = true

	return a.ch.Ack(tag, multiple)
}

func (a *ackAwareChannel) Nack(tag uint64, multiple bool, requeue bool) error {
	a.handled = true

	return a.ch.Nack(tag, multiple, requeue)
}

func (a *ackAwareChannel) Reject(tag uint64, requeue bool) error {
	a.handled = true

	return a.ch.Reject(tag, requeue)
}

// IsHandled returns a boolean value telling if the acknowledger has been called.
func (a *ackAwareChannel) IsHandled() bool {
	return a.handled
}
