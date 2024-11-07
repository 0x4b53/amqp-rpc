package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"time"

	amqprpc "github.com/0x4b53/amqp-rpc/v5"
)

func main() {
	c := amqprpc.NewClient("amqp://guest:guest@localhost:5672/")
	c.WithErrorLogger(log.New(os.Stdout, "ERROR - ", log.LstdFlags).Printf)

	reader := bufio.NewReader(os.Stdin)

	go heartbeat(c)

	for {
		fmt.Print("Enter text: ")

		text, _ := reader.ReadString('\n')
		request := amqprpc.NewRequest().WithRoutingKey("upper").WithBody(text)

		response, err := c.Send(request)
		if err != nil {
			fmt.Println("Woops: ", err)
		} else {
			fmt.Println(string(response.Body))
		}
	}
}

func heartbeat(c *amqprpc.Client) {
	counter := 0

	for {
		_, err := c.Send(
			amqprpc.NewRequest().WithRoutingKey("beat").
				WithBody(time.Now().String()).
				WithTimeout(5 * time.Second),
		)
		if err != nil {
			fmt.Println("Heartbeat error: ", err)
		}

		if counter%1000 == 0 {
			fmt.Println("Heartbeat")
		}

		counter++
	}
}
