package client

import (
	"context"
	"fmt"
)

type Client struct {
	context context.Context
}

func New() *Client {
	return &Client{
		context: context.TODO(),
	}
}

func (c *Client) NotTested() {
	fmt.Println("A function not tested to evaluate Code Climate")
}
