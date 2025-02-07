package amqprpc

import (
	"context"
	"errors"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
)

// SafeAcknowledgeHandler implements the amqp.Acknowledger interface with the
// addition that it can tell if a message has been acked in any way, and
// if the ack action fails - it will close the underlying connection.
type SafeAcknowledgeHandler struct {
	Acknowledger amqp.Acknowledger
	Handled      bool
	conn         *amqp.Channel
}

// NewSafeAcknowledgeHandler returns the passed acknowledger as an AwareAcknowledger.
func NewSafeAcknowledgeHandler(acknowledger amqp.Acknowledger, ch *amqp.Channel) *SafeAcknowledgeHandler {
	return &SafeAcknowledgeHandler{
		Acknowledger: acknowledger,
		conn:         ch,
	}
}

// Ack passes the Ack down to the underlying Acknowledger.
// Will close the underlying connection if ack fails.
func (a *SafeAcknowledgeHandler) Ack(tag uint64, multiple bool) error {
	if a.Handled {
		return nil
	}

	a.Handled = true

	err := a.Acknowledger.Ack(tag, multiple)
	if err == nil {
		return nil
	}

	slog.WarnContext(
		context.Background(),
		"error when acking",
		"error",
		err,
	)

	err = a.closeConnection()
	if err != nil {
		slog.ErrorContext(
			context.Background(),
			"could not close underlying connection",
			"error",
			err,
		)
	}

	return err
}

// Nack passes the Nack down to the underlying Acknowledger.
// Will close the underlying connection if nack fails.
func (a *SafeAcknowledgeHandler) Nack(tag uint64, multiple, requeue bool) error {
	if a.Handled {
		return nil
	}

	a.Handled = true

	err := a.Acknowledger.Nack(tag, multiple, requeue)
	if err == nil {
		return nil
	}

	slog.WarnContext(
		context.Background(),
		"error when nacking",
		"error",
		err,
	)

	err = a.closeConnection()
	if err != nil {
		slog.ErrorContext(
			context.Background(),
			"could not close underlying connection",
			"error",
			err,
		)
	}

	return err
}

// Reject passes the Reject down to the underlying Acknowledger.
// Will close the underlying connection if rejection fails.
func (a *SafeAcknowledgeHandler) Reject(tag uint64, requeue bool) error {
	if a.Handled {
		return nil
	}

	a.Handled = true

	err := a.Acknowledger.Reject(tag, requeue)
	if err == nil {
		return nil
	}

	slog.WarnContext(
		context.Background(),
		"error when rejecting",
		"error",
		err,
	)

	err = a.closeConnection()
	if err != nil {
		slog.ErrorContext(
			context.Background(),
			"could not close underlying connection",
			"error",
			err,
		)
	}

	return err
}

func (a *SafeAcknowledgeHandler) closeConnection() error {
	if a.conn == nil {
		return errors.New("can't stop connection - no underlying connection set")
	}

	return a.conn.Close()
}
