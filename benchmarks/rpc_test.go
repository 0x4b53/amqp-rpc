package benchmarks

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/streadway/amqp"

	amqprpc "github.com/0x4b53/amqp-rpc" // nolint: goimports
)

const (
	url = "amqp://guest:guest@localhost:5672/"
)

func Benchmark(b *testing.B) {
	s := amqprpc.NewServer(url)
	queueName := uuid.New().String()
	s.Bind(amqprpc.DirectBinding(queueName, func(ctx context.Context, rw *amqprpc.ResponseWriter, d amqp.Delivery) {}))

	go s.ListenAndServe()
	time.Sleep(1 * time.Second)

	c := amqprpc.NewClient(url)
	defer c.Stop()

	b.Run("WithReplies", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = c.Send(amqprpc.NewRequest().WithRoutingKey(queueName))
		}
	})

	b.Run("WithReplies-Parallel", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _ = c.Send(amqprpc.NewRequest().WithRoutingKey(queueName))
			}
		})
	})
	b.Run("WithoutReplies", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = c.Send(amqprpc.NewRequest().WithRoutingKey(queueName).WithResponse(false))
		}
	})
	b.Run("WithoutReplies-Parallel", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _ = c.Send(amqprpc.NewRequest().WithRoutingKey(queueName).WithResponse(false))
			}
		})
	})
}
