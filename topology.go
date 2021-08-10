package amqprpc

import (
	"github.com/streadway/amqp"
)

type ExchangeKind string

const (
	ExchangeKindDirect  ExchangeKind = "direct"
	ExchangeKindFanout  ExchangeKind = "fanout"
	ExchangeKindTopic   ExchangeKind = "topic"
	ExchangeKindHeaders ExchangeKind = "headers"
)

const (
	ExchangeDefaultDirect  = "amq.direct"
	ExchangeDefaultFanout  = "amq.fanout"
	ExchangeDefaultTopic   = "amq.topic"
	ExchangeDefaultHeaders = "amq.match"
)

type ExchangeDefinition struct {
	Name       string
	Kind       ExchangeKind
	Durable    bool
	AutoDelete bool
	Internal   bool
	Args       amqp.Table
}

type QueueDefinition struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	Args       amqp.Table
}

type BindingDefinition struct{}
