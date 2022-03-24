package amqprpc

import (
	"io"
	"log"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerLogging(t *testing.T) {
	reader, writer := io.Pipe()

	go func() {
		logger := log.New(writer, "TEST", log.LstdFlags)

		s := NewServer(testURL)
		s.WithDebugLogger(logger.Printf)
		s.WithErrorLogger(logger.Printf)

		stop := startAndWait(s)
		stop()

		require.NoError(t, writer.Close())
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
		logger := log.New(writer, "TEST", log.LstdFlags)

		c := NewClient("amqp://guest:guest@localhost:5672/")
		c.WithDebugLogger(logger.Printf)
		c.WithErrorLogger(logger.Printf)

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

func Test_stringifyForLog(t *testing.T) {
	headers := amqp.Table{
		"foo": "bar",
		"nested": amqp.Table{
			"baz": 13,
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
		want string
	}{
		{
			name: "delivery",
			item: delivery,
			want: "[Exchange=exchange, RoutingKey=routing_key, Type=type, CorrelationId=coorelation1, UserId=jane, Headers=[foo=bar, nested=[baz=13]]]",
		},
		{
			name: "request",
			item: request,
			want: "[Exchange=exchange, RoutingKey=routing_key, Publishing=[CorrelationID=coorelation1, AppId=amqprpc, UserId=jane, Headers=[foo=bar, nested=[baz=13]]]]",
		},
		{
			name: "return",
			item: ret,
			want: "[ReplyCode=412, ReplyText=NO_ROUTE, Exchange=exchange, RoutingKey=routing_key, CorrelationID=coorelation1, AppId=amqprpc, UserId=jane, Headers=[foo=bar, nested=[baz=13]]]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got string

			switch v := tt.item.(type) {
			case amqp.Delivery:
				got = stringifyDeliveryForLog(&v)
				assert.Equal(t, "[nil]", stringifyDeliveryForLog(nil))
			case amqp.Return:
				got = stringifyReturnForLog(v)
			case Request:
				got = stringifyRequestForLog(&v)
				assert.Equal(t, "[nil]", stringifyRequestForLog(nil))
			}

			assert.Equal(t, tt.want, got)
		})
	}
}
