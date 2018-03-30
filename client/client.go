package client

import (
	"context"
	"fmt"
)

// Client represents an AMQP client used within a RPC framework.
// This client can be used to communicate with RPC servers.
type Client struct {
	context context.Context
}

// New will return a pointer to a new Client.
func New() *Client {
	return &Client{
		context: context.TODO(),
	}
}

// NotTested is a function implemented to test code coverage.
func (c *Client) NotTested() {
	fmt.Println("A function not tested to evaluate Code Climate")
}

// AlsoNotTested a function implemented to test code coverage.
func (c *Client) AlsoNotTested(a, b int) int {
	fmt.Println("A function not tested to evaluate Code Climate")

	if a > 0 {
		return a + b
	}

	return a - b
}
