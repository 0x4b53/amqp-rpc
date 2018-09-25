package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"time"

	amqprpc "github.com/bombsimon/amqp-rpc"
)

func main() {
	c := amqprpc.NewClient("amqp://guest:guest@localhost:5672/")
	c.WithErrorLogger(log.New(os.Stdout, "ERROR - ", log.LstdFlags).Printf)
	c.WithDebugLogger(log.New(os.Stdout, "DEBUG - ", log.LstdFlags).Printf)

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
	for {
		_, err := c.Send(
			amqprpc.NewRequest().WithRoutingKey("beat").
				WithBody(time.Now().String()).
				WithTimeout(100 * time.Millisecond),
		)
		if err != nil {
			fmt.Println("Heartbeat error: ", err)
		}
		time.Sleep(1 * time.Second)
	}
}
