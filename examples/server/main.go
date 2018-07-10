package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/bombsimon/amqp-rpc/server"

	"github.com/streadway/amqp"
)

func main() {
	s := server.New("amqp://guest:guest@localhost:5672/")

	s.Bind(server.DirectBinding("upper", upper))
	s.Bind(server.DirectBinding("beat", beat))

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		<-sigs
		s.Stop()
	}()

	s.ListenAndServe()
}

func upper(c context.Context, rw *server.ResponseWriter, d amqp.Delivery) {
	fmt.Fprint(rw, strings.ToUpper(string(d.Body)))
}

func beat(c context.Context, rw *server.ResponseWriter, d amqp.Delivery) {
	fmt.Fprintf(rw, "beat")
}
