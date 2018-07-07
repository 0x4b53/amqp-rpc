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

	s1.AddHandler("", fanoutHandlerOne)
	s2.AddHandler("", fanoutHandlerTwo)

	s1.ListenAndServe()
	s2.ListenAndServe()
}

func fanoutHandlerOne(c context.Context, rw *server.ResponseWriter, d amqp.Delivery) {
	fmt.Fprint(rw, "First server handled request")
}

func fanoutHandlerTwo(c context.Context, rw *server.ResponseWriter, d amqp.Delivery) {
	fmt.Fprint(rw, "Second server handled request")
}
