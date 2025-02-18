package amqprpc

import (
	"errors"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
)

type closer interface {
	Close() error
}

// Acknowledger implements the amqp.Acknowledger interface with the
// addition that it can tell if a message has been acked in any way, and if
// an ack action fails the Close function is called on the underlying
// acknowledger.
// The default Acknowledger on the amq.Delivery is just an *amq.Channel.
// By using the default Acknowledger here we ensure that this Acknowledger can
// close the channel to restore the connection without closing the queueu or discarding messages.
type Acknowledger struct {
	Acknowledger amqp.Acknowledger
	Handled      atomic.Bool
}

// NewAcknowledger returns the passed acknowledger as a Acknowledger.
func NewAcknowledger(acknowledger amqp.Acknowledger) *Acknowledger {
	return &Acknowledger{
		Acknowledger: acknowledger,
	}
}

// Ack passes the Ack down to the underlying Acknowledger.
func (a *Acknowledger) Ack(tag uint64, multiple bool) error {
	if !a.Handled.CompareAndSwap(false, true) {
		return nil
	}

	err := a.Acknowledger.Ack(tag, multiple)
	if err != nil {
		return errors.Join(err, a.close())
	}

	return nil
}

// Nack passes the Nack down to the underlying Acknowledger.
func (a *Acknowledger) Nack(tag uint64, multiple, requeue bool) error {
	if !a.Handled.CompareAndSwap(false, true) {
		return nil
	}

	err := a.Acknowledger.Nack(tag, multiple, requeue)
	if err != nil {
		return errors.Join(err, a.close())
	}

	return nil
}

// Reject passes the Reject down to the underlying Acknowledger.
func (a *Acknowledger) Reject(tag uint64, requeue bool) error {
	if !a.Handled.CompareAndSwap(false, true) {
		return nil
	}

	err := a.Acknowledger.Reject(tag, requeue)
	if err != nil {
		return errors.Join(err, a.close())
	}

	return nil
}

func (a *Acknowledger) close() error {
	ackCloser, ok := a.Acknowledger.(closer)
	if !ok {
		return nil
	}

	return ackCloser.Close()
}
