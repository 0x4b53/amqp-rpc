package amqprpctest

import (
	amqprpc "github.com/bombsimon/amqp-rpc"
)

// NewTestClient returns a client with a custom send function to use for testing.
func NewTestClient(sf amqprpc.SendFunc) *amqprpc.Client {
	c := amqprpc.NewClient("")
	c.Sender = sf

	return c
}
