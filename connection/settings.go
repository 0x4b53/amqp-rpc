package connection

import "github.com/streadway/amqp"

// ExchangeDeclareSettings is the settings that will be used when a handler
// is mapped to a fanout exchange and an exchange is declared.
type ExchangeDeclareSettings struct {
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
}

// QueueDeclareSettings is the settings that will be used when the response
// any kind of queue is declared. Se documentation for amqp.QueueDeclare
// for more information about these settings.
type QueueDeclareSettings struct {
	Durable          bool
	DeleteWhenUnused bool
	Exclusive        bool
	NoWait           bool
	Args             amqp.Table
}

// ConsumeSettings is the settings that will be used when the consumption
// on a specified queue is started.
type ConsumeSettings struct {
	Consumer  string
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      amqp.Table
}

// PublishSettings is the settings that will be used when a message is about
// to be published to the message bus.
type PublishSettings struct {
	Mandatory bool
	Immediate bool
}
