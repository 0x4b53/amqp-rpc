package main

import (
	"fmt"

	"github.com/bombsimon/amqp-rpc/client"
)

func main() {
	c := client.New("amqp://guest:guest@localhost:5672/")
	r := client.NewRequest("").WithExchange("cool-exchange").WithStringBody("Seding fanout")

	response, err := c.Send(r)
	if err != nil {
		fmt.Println("Woops: ", err)
	}

	fmt.Println(response.Body)

}
