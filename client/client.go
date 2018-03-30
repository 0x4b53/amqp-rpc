package client

import (
	"context"
)

// Client represents an AMQP client used within a RPC framework.
// This client can be used to communicate with RPC servers.
type Client struct {
	context context.Context
}

// New will return a pointer to a new Client.
func New() *Client {
	return &Client{
		context: context.Background(),
	}
}
