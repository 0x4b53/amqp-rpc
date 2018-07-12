package client

import (
	"testing"

	"github.com/streadway/amqp"
	. "gopkg.in/go-playground/assert.v1"
)

func preSend(next PreSendFunc) PreSendFunc {
	return func(r *Request) {
		r.Headers["unit"] = "testing"

		next(r)
	}
}
func postSend(next PostSendFunc) PostSendFunc {
	return func(d *amqp.Delivery, e error) {
		d.Body = []byte("unit-testing")
		next(d, e)
	}
}

func TestMiddlewareChain(t *testing.T) {
	var (
		mpre = []MiddlewarePreFunc{preSend}
		req  = NewRequest("")
	)

	MiddlewarePreChain(func(r *Request) {}, mpre...)(req)

	Equal(t, req.Headers["unit"], "testing")

	var (
		del   = amqp.Delivery{}
		mpost = []MiddlewarePostFunc{postSend}
	)

	MiddlewarePostChain(func(d *amqp.Delivery, e error) {}, mpost...)(&del, nil)

	Equal(t, del.Body, []byte("unit-testing"))
}

func TestClientAddMiddlewares(t *testing.T) {
	c := New("")

	Equal(t, len(c.preSendMiddlewares), 0)
	Equal(t, len(c.postSendMiddlewares), 0)

	c.AddPreSendMiddleware(
		func(n PreSendFunc) PreSendFunc {
			return func(r *Request) {
				n(r)
			}
		},
	)

	c.AddPostSendMiddleware(
		func(n PostSendFunc) PostSendFunc {
			return func(d *amqp.Delivery, e error) {
				n(d, e)
			}
		},
	)

	Equal(t, len(c.preSendMiddlewares), 1)
	Equal(t, len(c.postSendMiddlewares), 1)
}
