package middleware

import (
	"context"

	"github.com/streadway/amqp"
)

// ServerMiddleware is middleware attached to the server. A middleware is
// executed beforethe handleFunc and if an error is returned the handleFunc
// will not be executed.
type ServerMiddleware func(string, context.Context, *amqp.Delivery) error
