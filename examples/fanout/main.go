package main

import (
	"context"
	"fmt"

	"github.com/bombsimon/amqp-rpc/server"

	"github.com/streadway/amqp"
)

func main() {
	s1 := server.New("amqp://guest:guest@localhost:5672/")
	s2 := server.New("amqp://guest:guest@localhost:5672/")

	s1.AddFanoutHandler("cool-exchange", fanoutHandlerOne)
	s2.AddFanoutHandler("cool-exchange", fanoutHandlerTwo)

	forever := make(chan bool)

	go s1.ListenAndServe()
	go s2.ListenAndServe()

	<-forever
}

func fanoutHandlerOne(c context.Context, rw *server.ResponseWriter, d amqp.Delivery) {
	fmt.Println("FIRST")
	fmt.Fprint(rw, "First server handled request")
}

func fanoutHandlerTwo(c context.Context, rw *server.ResponseWriter, d amqp.Delivery) {
	fmt.Println("SECOND")
	fmt.Fprint(rw, "Second server handled request")
}
