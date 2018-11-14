package middleware

import (
	"context"
	"fmt"
	"log"

	"github.com/streadway/amqp"

	amqprpc "github.com/bombsimon/amqp-rpc"
)

const (
	// HandlerCrashedHeader is the header that is set when a handelr panics.
	HandlerCrashedHeader = "X-Handler-Crashed"
)

// PanicRecovery is a middleware that will handle if a handler in an endpoint
// causes a panic.
func PanicRecovery(next amqprpc.HandlerFunc) amqprpc.HandlerFunc {
	return func(ctx context.Context, rw *amqprpc.ResponseWriter, d amqp.Delivery) {
		defer func() {
			if r := recover(); r != nil {
				var crashMessage string

				log.Printf("handler caused a panic: %s", r)

				switch v := r.(type) {
				case error:
					crashMessage = v.Error()
				case string:
					crashMessage = v
				case fmt.Stringer:
					crashMessage = v.String()
				default:
					crashMessage = "unknown"
				}

				rw.WriteHeader(HandlerCrashedHeader, crashMessage)
				fmt.Fprintf(rw, "crashed when running handler: %s", crashMessage)

				// Nack message, do not requeue
				if err := d.Nack(true, false); err != nil {
					log.Printf("could not nack message: %s", err.Error())
				}
			}
		}()

		next(ctx, rw, d)
	}
}
