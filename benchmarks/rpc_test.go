package benchmarks

import (
	"context"
	"io/ioutil"
	"log"
	"testing"
	"time"

	"github.com/bombsimon/amqp-rpc/client"
	"github.com/bombsimon/amqp-rpc/logger"
	"github.com/bombsimon/amqp-rpc/server"
	uuid "github.com/satori/go.uuid"
	"github.com/streadway/amqp"
)

var url = "amqp://guest:guest@localhost:5672/"
var result *amqp.Delivery

func Benchmark(b *testing.B) {
	l := log.New(ioutil.Discard, "", 0)
	logger.SetInfoLogger(l)
	logger.SetWarnLogger(l)

	s := server.New(url)
	queueName := uuid.Must(uuid.NewV4()).String()
	s.AddHandler(queueName, func(ctx context.Context, rw *server.ResponseWriter, d amqp.Delivery) {})

	go s.ListenAndServe()
	time.Sleep(1 * time.Second)

	c := client.New(url)

	b.Run("WithReplies", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, _ = c.Send(client.NewRequest(queueName))
		}
	})

	b.Run("WithReplies-Parallel", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				result, _ = c.Send(client.NewRequest(queueName))
			}
		})
	})
	b.Run("WithoutReplies", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, _ = c.Send(client.NewRequest(queueName).WithResponse(false))
		}
	})
	b.Run("WithoutReplies-Parallel", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				result, _ = c.Send(client.NewRequest(queueName).WithResponse(false))
			}
		})
	})
}
