package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/bombsimon/amqp-rpc/server"

	"github.com/streadway/amqp"
)

func main() {
	s := server.New("amqp://guest:guest@localhost:5672/")

	s.Bind(server.DirectBinding("upper", upper))

	s.ListenAndServe()
}

func upper(c context.Context, rw *server.ResponseWriter, d amqp.Delivery) {
	fmt.Fprint(rw, strings.ToUpper(string(d.Body)))
}
