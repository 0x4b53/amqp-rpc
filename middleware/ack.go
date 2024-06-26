package middleware

import (
	"context"

	amqprpc "github.com/0x4b53/amqp-rpc/v4"
	amqp "github.com/rabbitmq/amqp091-go"
)

// OnErrFunc is the function that will be called when the middleware get an
// error from `Ack`. The error and the delivery will be passed.
type OnErrFunc func(err error, delivery amqp.Delivery)

// OnAckErrorLog is a built-in function that will log the error if any is
// returned from `Ack`.
//
//	middleware := AckDelivery(OnAckErrorLog(log.Printf))
func OnAckErrorLog(logFn amqprpc.LogFunc) OnErrFunc {
	return func(err error, delivery amqp.Delivery) {
		logFn("could not ack delivery (%s): %v\n", delivery.CorrelationId, err)
	}
}

// OnAckErrorSendOnChannel will first log the error and correlation ID and then
// try to send on the passed channel. If no one is consuming on the passed
// channel the middleware will not block but instead log a message about missing
// channel consumers.
func OnAckErrorSendOnChannel(logFn amqprpc.LogFunc, ch chan struct{}) OnErrFunc {
	logErr := OnAckErrorLog(logFn)

	return func(err error, delivery amqp.Delivery) {
		logErr(err, delivery)

		select {
		case ch <- struct{}{}:
		default:
			logFn("ack middleware: could not send on channel, no one is consuming\n")
		}
	}
}

// AckDelivery is a middleware that will acknowledge the delivery after the
// handler has been executed. If the Ack fails the error and the `amqp.Delivery`
// will be passed to the `OnErrFunc`.
func AckDelivery(onErrFn OnErrFunc) amqprpc.ServerMiddlewareFunc {
	return func(next amqprpc.HandlerFunc) amqprpc.HandlerFunc {
		return func(ctx context.Context, rw *amqprpc.ResponseWriter, d amqp.Delivery) {
			acknowledger := amqprpc.NewAwareAcknowledger(d.Acknowledger)
			d.Acknowledger = acknowledger

			next(ctx, rw, d)

			if acknowledger.Handled {
				return
			}

			if err := d.Ack(false); err != nil {
				onErrFn(err, d)
			}
		}
	}
}
