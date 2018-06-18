package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"

	"log"

	"github.com/bombsimon/amqp-rpc/client"
	"github.com/bombsimon/amqp-rpc/logger"
)

func main() {
	l := log.New(ioutil.Discard, "", 0)
	logger.SetInfoLogger(l)
	logger.SetWarnLogger(l)

	c := client.New("amqp://guest:guest@localhost:5672/")
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("Enter text: ")
		text, _ := reader.ReadString('\n')
		request := client.NewRequest("upper").WithStringBody(text)
		response, err := c.Send(request)
		if err != nil {
			fmt.Println("Woops: ", err)
		} else {
			fmt.Println(string(response.Body))
		}

	}
}
