package benchmarks

import (
	"context"
	"testing"
	"time"

	amqprpc "github.com/bombsimon/amqp-rpc"
	uuid "github.com/satori/go.uuid"
	"github.com/streadway/amqp"
)

var url = "amqp://guest:guest@localhost:5672/"
var result *amqp.Delivery

func Benchmark(b *testing.B) {
	s := amqprpc.NewServer(url)
	queueName := uuid.Must(uuid.NewV4()).String()
	s.Bind(amqprpc.DirectBinding(queueName, func(ctx context.Context, rw *amqprpc.ResponseWriter, d amqp.Delivery) {}))

	go s.ListenAndServe()
	time.Sleep(1 * time.Second)

	c := amqprpc.NewClient(url)

	b.Run("WithReplies", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, _ = c.Send(amqprpc.NewRequest().WithRoutingKey(queueName))
		}
	})

	b.Run("WithReplies-Parallel", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				result, _ = c.Send(amqprpc.NewRequest().WithRoutingKey(queueName))
			}
		})
	})
	b.Run("WithoutReplies", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, _ = c.Send(amqprpc.NewRequest().WithRoutingKey(queueName).WithResponse(false))
		}
	})
	b.Run("WithoutReplies-Parallel", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				result, _ = c.Send(amqprpc.NewRequest().WithRoutingKey(queueName).WithResponse(false))
			}
		})
	})
}
