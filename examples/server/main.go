package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	amqprpc "github.com/bombsimon/amqp-rpc"
	amqprpcmw "github.com/bombsimon/amqp-rpc/middleware"

	"github.com/streadway/amqp"
)

func main() {
	s := amqprpc.NewServer("amqp://guest:guest@localhost:5672/").AddMiddleware(amqprpcmw.PanicRecovery)

	debugLogger := log.New(os.Stdout, "DEBUG - ", log.LstdFlags)
	errorLogger := log.New(os.Stdout, "ERROR - ", log.LstdFlags)

	s.WithErrorLogger(errorLogger.Printf)
	s.WithDebugLogger(debugLogger.Printf)

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
