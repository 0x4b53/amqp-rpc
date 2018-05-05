package main

import (
	"fmt"

	"github.com/bombsimon/amqp-rpc/client"
)

func main() {
	c := client.New("amqp://guest:guest@localhost:5672/")

	request := client.NewRequest("hello_world").WithStringBody("Sent with a client")
	response, err := c.Send(request)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(string(response.Body))
}
