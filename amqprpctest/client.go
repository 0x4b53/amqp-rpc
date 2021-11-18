package amqprpctest

import (
	amqprpc "github.com/0x4b53/amqp-rpc/v2"
)

// NewTestClient returns a client with a custom send function to use for testing.
func NewTestClient(sf amqprpc.SendFunc) *amqprpc.Client {
	c := amqprpc.NewClient("")
	c.Sender = sf

	return c
}
