package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/bombsimon/amqp-rpc/logger"
	"github.com/bombsimon/amqp-rpc/server"

	"github.com/streadway/amqp"
)

func main() {
	s := server.New("amqp://guest:guest@localhost:5672/")

	s.AddHandler("upper", upper)

	s.ListenAndServe()
}

func upper(c context.Context, rw *server.ResponseWriter, d amqp.Delivery) {
	logger.Infof("Handling 'Hello world' request")
	fmt.Fprint(rw, strings.ToUpper(string(d.Body)))
}
