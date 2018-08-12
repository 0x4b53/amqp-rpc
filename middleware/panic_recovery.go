package middleware

import (
	"context"
	"fmt"

	amqprpc "github.com/bombsimon/amqp-rpc"
	"github.com/bombsimon/amqp-rpc/logger"
	"github.com/streadway/amqp"
)

var (
	// HandlerCrashedHeader is the header that is set when a handelr panics.
	HandlerCrashedHeader = "X-Handler-Crashed"
)

// PanicRecovery is a middleware that will handle if a handler in an endpoint
// causes a panic.
func PanicRecovery(next amqprpc.HandlerFunc) amqprpc.HandlerFunc {
	return func(ctx context.Context, rw *amqprpc.ResponseWriter, d amqp.Delivery) {
		defer func() {
			if r := recover(); r != nil {
				logger.Warnf("handler caused a panic: %s", r)

				rw.WriteHeader(HandlerCrashedHeader, r.(string))
				fmt.Fprintf(rw, "crashed when running handler: %s", r.(string))

				// Nack message, do not requeue
				d.Nack(true, false)
			}
		}()

		next(ctx, rw, d)
	}
}
