package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"

	amqprpc "github.com/0x4b53/amqp-rpc/v5"
	amqprpcmw "github.com/0x4b53/amqp-rpc/v5/middleware"
)

func main() {
	s := amqprpc.NewServer("amqp://guest:guest@localhost:5672/").
		AddMiddleware(amqprpcmw.PanicRecoveryLogging(slog.Default()))

	s.Bind(amqprpc.DirectBinding("upper", upper))
	s.Bind(amqprpc.DirectBinding("beat", beat))

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		<-sigs
		s.Stop()
	}()

	s.ListenAndServe()
}

func upper(c context.Context, rw *amqprpc.ResponseWriter, d amqp.Delivery) {
	if string(d.Body) == "crash\n" {
		panic("I died...")
	}

	fmt.Fprint(rw, strings.ToUpper(string(d.Body)))
}

func beat(c context.Context, rw *amqprpc.ResponseWriter, d amqp.Delivery) {
	fmt.Fprintf(rw, "beat")
}
