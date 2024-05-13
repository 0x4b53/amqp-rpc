package middleware

import (
	"context"

	amqprpc "github.com/0x4b53/amqp-rpc/v3"
	amqp "github.com/rabbitmq/amqp091-go"
)

// OnErrFunc is the function that will be called when the middleware get an
// error from `Ack`. The error and the correlation ID for the delivery will be
// passed.
type OnErrFunc func(err error, correlationID string)

// AckLogError is a built-in function that will log the error if any is returned
// from `Ack`.
//
//	middleware := AckDelivery(AckLogError(log.Printf))
func AckLogError(logFn amqprpc.LogFunc) OnErrFunc {
	return func(err error, correlationID string) {
		logFn("could not ack delivery (%s): %v\n", correlationID, err)
	}
}

// AckDelivery is a middleware that will acknowledge the delivery after the
// handler has been executed. Any error returned from d.Ack will be passed
// to the provided logFunc.
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
				onErrFn(err, d.CorrelationId)
			}
		}
	}
}
