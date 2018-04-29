package middleware

import (
	"context"
	"errors"
	"testing"

	"github.com/streadway/amqp"
	. "gopkg.in/go-playground/assert.v1"
)

func TestMiddleware(t *testing.T) {
	m := func(rk string, c context.Context, d *amqp.Delivery) error {
		if _, ok := d.Headers["authorization"]; !ok {
			return errors.New("Missing authorization header")
		}

		return nil
	}

	Equal(t, m("test", context.TODO(), &amqp.Delivery{Headers: amqp.Table{"authorization": "Bearer"}}), nil)
}
