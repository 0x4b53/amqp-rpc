package main

import (
	"fmt"

	"github.com/bombsimon/amqp-rpc/client"
)

func main() {
	client := client.New("amqp://guest:guest@localhost:5672/")

	response, err := client.Publish("hello_world", []byte("Sent with a client"), true)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(string(response.Body))
}
