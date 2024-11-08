package amqprpc

import (
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
)

func TestServerLogging(t *testing.T) {
	reader, writer := io.Pipe()

	go func() {
		logger := slog.New(slog.NewTextHandler(writer, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))
		logger = logger.With("my_attr", "TEST")

		s := NewServer(testURL).
			WithLogger(logger)

		stop := startAndWait(s)
		stop()

		assert.NoError(t, writer.Close())
	}()

	buf, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}

	assert.NotEqual(t, "", string(buf), "buffer contains logs")
	assert.Contains(t, string(buf), "TEST", "logs are prefixed with TEST")
}

func TestClientLogging(t *testing.T) {
	reader, writer := io.Pipe()

	go func() {
		logger := slog.New(
			slog.NewTextHandler(writer, &slog.HandlerOptions{
				Level: slog.LevelDebug,
			}),
		).With(
			"my_attr", "TEST",
		)

		c := NewClient("amqp://guest:guest@localhost:5672/")
		c.WithLogger(logger)

		_, err := c.Send(NewRequest().WithRoutingKey("foobar").WithTimeout(time.Millisecond))
		c.Stop()

		assert.Error(t, err, "did not get expected error")
		assert.Contains(t, err.Error(), "timed out")

		_ = writer.Close()
	}()

	buf, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}

	assert.NotEqual(t, "", string(buf), "buffer contains logs")
	assert.Contains(t, string(buf), "TEST", "logs are prefixed with TEST")
}

func Test_slogAttrs(t *testing.T) {
	headers := amqp.Table{
		"foo": "bar",
		"nested": amqp.Table{
			"baz": 13,
			"apa": 13,
		},
	}

	delivery := amqp.Delivery{
		CorrelationId: "coorelation1",
		Exchange:      "exchange",
		RoutingKey:    "routing_key",
		Type:          "type",
		UserId:        "jane",
		Headers:       headers,
	}

	request := Request{
		Exchange:   "exchange",
		RoutingKey: "routing_key",
		Publishing: amqp.Publishing{
			AppId:         "amqprpc",
			CorrelationId: "coorelation1",
			UserId:        "jane",
			Headers:       headers,
		},
	}

	ret := amqp.Return{
		AppId:         "amqprpc",
		CorrelationId: "coorelation1",
		Exchange:      "exchange",
		RoutingKey:    "routing_key",
		UserId:        "jane",
		ReplyText:     "NO_ROUTE",
		ReplyCode:     412,
		Headers:       headers,
	}

	tests := []struct {
		name string
		item interface{}
		want []slog.Attr
	}{
		{
			name: "delivery",
			item: &delivery,
			want: []slog.Attr{
				slog.String("correlation_id", "coorelation1"),
				slog.String("exchange", "exchange"),
				slog.Bool("redelivered", false),
				slog.String("routing_key", "routing_key"),
				slog.String("type", "type"),
				slog.String("user_id", "jane"),
				slog.Group("headers",
					slog.String("foo", "bar"),
					slog.Group("nested",
						slog.String("baz", "13"),
						slog.String("apa", "13"),
					),
				),
			},
		},
		{
			name: "request",
			item: &request,
			want: []slog.Attr{
				slog.String("exchange", "exchange"),
				slog.Bool("Reply", false),
				slog.String("routing_key", "routing_key"),
				slog.Group("publishing",
					slog.String("correlation_id", "coorelation1"),
					slog.String("user_id", "jane"),
					slog.String("app_id", "amqprpc"),
					slog.Group("headers",
						slog.String("foo", "bar"),
						slog.Group("nested",
							slog.String("apa", "13"),
							slog.String("baz", "13"),
						),
					),
				),
			},
		},
		{
			name: "return",
			item: &ret,
			want: []slog.Attr{
				slog.String("correlation_id", "coorelation1"),
				slog.Uint64("reply_code", 412),
				slog.String("reply_text", "NO_ROUTE"),
				slog.String("exchange", "exchange"),
				slog.String("routing_key", "routing_key"),
				slog.String("app_id", "amqprpc"),
				slog.String("user_id", "jane"),
				slog.Group("headers",
					slog.String("foo", "bar"),
					slog.Group("nested",
						slog.String("baz", "13"),
						slog.String("apa", "13"),
					),
				),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []slog.Attr

			switch v := tt.item.(type) {
			case *amqp.Delivery:
				got = slogAttrsForDelivery(v)

				assert.Nil(t, slogAttrsForDelivery(nil))
			case *amqp.Return:
				got = slogAttrsForReturn(v)

				assert.Nil(t, slogAttrsForDelivery(nil))
			case *Request:
				got = slogAttrsForRequest(v)

				assert.Nil(t, slogAttrsForRequest(nil))
			default:
				t.Fatalf("unknown type: %T", v)
			}

			slogAttrsSort(tt.want)
			slogAttrsSort(got)

			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func slogAttrsSort(attrs []slog.Attr) {
	slices.SortFunc(attrs, func(a, b slog.Attr) int {
		if a.Key < b.Key {
			return -1
		}

		if a.Key > b.Key {
			return 1
		}

		return 0
	})

	for _, attr := range attrs {
		if attr.Value.Kind() == slog.KindGroup {
			slogAttrsSort(attr.Value.Group())
		}
	}
}
