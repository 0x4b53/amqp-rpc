<h1 align="center">
  <img src="amqprpc.png" alt="AMQP RPC" width="300">
  <br>
  RabbitMQ RPC
  <br>
</h1>

<h4 align="center">RabbitMQ RPC Framework</h4>

<p align="center">
  <a href="https://godoc.org/github.com/0x4b53/amqp-rpc">
    <img src="https://godoc.org/github.com/0x4b53/amqp-rpc?status.svg" alt="GoDoc">
  </a>
  <a href="https://travis-ci.com/0x4b53/amqp-rpc">
    <img src="https://travis-ci.com/0x4b53/amqp-rpc.svg?branch=master">
  </a>
  <a href="https://goreportcard.com/report/github.com/0x4b53/amqp-rpc">
    <img src="https://goreportcard.com/badge/github.com/0x4b53/amqp-rpc">
  </a>
  <a href="https://codeclimate.com/github/0x4b53/amqp-rpc/maintainability">
    <img src="https://api.codeclimate.com/v1/badges/409b55c59ecc1b40595b/maintainability">
  </a>
  <a href="https://codeclimate.com/github/0x4b53/amqp-rpc/test_coverage">
    <img src="https://api.codeclimate.com/v1/badges/409b55c59ecc1b40595b/test_coverage">
  </a>
  <a href="https://golangci.com/r/github.com/0x4b53/amqp-rpc">
    <img src="https://golangci.com/badges/github.com/0x4b53/amqp-rpc.svg">
  </a>
</p>

## Description

This is a framework to use [RabbitMQ] as a client/server RPC setup togheter with
the [Go amqp] implementation. The framework can manage a fully funcitonal
message queue setup with reconnects, disconnects, graceful shutdown and other
stability mechanisms. By providing this RabbitMQ can be used as transport and
service discovery to quickly get up and running with a micro service
architecture.

Since the framework handles the majority of the communication with RabbitMQ and
the Go library the user does not need to know details about those systems.
However, since a few interfaces exposes the Go package types and that the
nomenclature is unique for RabbitMQ some prior experience is preferred.

## Project status

This project has been used in production since october 2018 handling millions of
requests both as server and client. The implementation is to be seen as stable
altough until a proper v1.0 release tag is set the API may change.

## Server

The server is inspired by the HTTP library where the user maps a [RabbitMQ
binding] to a handler function. A response writer is passed to the handler which
may be used as an `io.Writer` to write the response.

This is an an example of how to get up and running with a server responding to
all messages published to the given routing key.

```go
server := NewServer("amqp://guest:guest@localhost:5672")

server.Bind(DirectBinding("routing_key", func(c context.Context, rw *ResponseWriter d *amqp.Delivery) {
    // Print what the body and header was
    fmt.Println(d.Body, d.Headers)

    // Add a response to the publisher
    fmt.Fprint(rw, "Handled")
}))

server.ListenAndServe()
```

The example above uses a `DirectBinding` but all the supported bindings are
provided via an interface where the exchange type will be set to the proper
type.

```go
server.Bind(DirectBinding("routing_key", handleFunc))
server.Bind(FanoutBinding("fanout-exchange", handleFunc))
server.Bind(TopicBinding("queue-name", "routing_key.#", handleFunc))
server.Bind(HeadersBinding("queue-name", amqp.Table{"x-match": "all", "foo": "bar"}, handleFunc))
```

If the default variables doesn't result in the desired result you can setup the
binding with the type manually.

```go
customBinding := HandlerBinding{
    QueueName:    "oh-sweet-queue",
    ExchangeName: "my-exchange",
    ExchangeType: ExchangeDirect,
    RoutingKey:   "my-key",
    BindHeaders:  amqp.Table{},
    Handler:      handleFunc,
}

server.Bind(customBinding)
```

The server will not connect until `ListenAndServe()` is being called. This means
that you can configure settings for the server until that point. The settings
can be changed by calling chainable methods.

```go
server := NewServer("amqp://guest:guest@localhost:5672").
    WithConsumeSettings(ConsumeSettings{}).
    WithQueueDeclareSettings(QueueDeclareSettings{}).
    WithExchangeDeclareSettings(ExchangeDeclareSettings{}).
    WithDebugLogger(log.Printf).
    WithErrorLogger(log.Printf).
    WithDialConfig(amqp.Config{}).
    WithTLS(&tls.Config{})
```

QoS is by default set to a prefetch count of `10` and a prefetch size of `0` (no
limit). If you want to change this you can use the
`WithConsumeSettings(settings)` function.

## Client

The client is built around channels to be able to handle as many requests as
possible without the need to setup multiple clients. All messages published get
a unique correlation ID which is used to determine where a response should be
passed no matter what order it's received.

The client takes a `Request` as input which can hold all required information
about how to route and handle the message and response.

```go
client := NewClient("amqp://guest:guest@localhost:5672")

request := NewRequest().
    WithRoutingKey("routing_key").
    WithBody("This is my body)

response, err := client.Send(request)
if err != nil {
    log.Fatal(err.Error())
}

log.Print(string(response.Body))
```

The client will not connect while being created, instead this happens when the
first request is being published (while calling `Send()`). This allows you to
configure connection related parameters such as timeout by chaining the
methods.

```go
// Set timeout after NewClient is invoked by chaining.
client := NewClient("amqp://guest:guest@localhost:5672").
    WithTimeout(5000 * time.Milliseconds)

// Will not connect and may be changed untill this call.
client.Send(NewRequest().WithRoutingKey("routing_key"))
```

Example of available methods for chaining.

```go
client := NewClient("amqp://guest:guest@localhost:5672").
    WithDebugLogger(log.Printf).
    WithErrorLogger(log.Printf).
    WithDialConfig(amqp.Config{}).
    WithTLS(&tls.Config{}).
    WithQueueDeclareSettings(QueueDeclareSettings{}).
    WithConsumeSettings(ConsumeSettings{}).
    WithPublishSettings(PublishSettings{}).
    WithConfirmMode(true),
    WithTimeout(10 * Time.Second)
```

### Confirm mode

Confirm mode can be set on the client and will make the client wait for `Ack`
from the amqp-server. This makes sending requests more reliable at the cost of
some performance as each publishing must be confirmed by the amqp-server [Your
can read more here](https://www.rabbitmq.com/confirms.html#publisher-confirms)

The client is set in confirm mode by default.

You can use `WithPublishSettings` or `WithConfirmMode` to control this setting.

```go
client := NewClient("amqp://guest:guest@localhost:5672").
    WithConfirmMode(true)

client := NewClient("amqp://guest:guest@localhost:5672").
    WithPublishSettings(true)
```

### Request

The `Request` type is used as input to the clients send function and holds all
the information about how to route and handle the request. All the properties
may be set with chainable methods to make it easy to construct. A `Request` may
be re-used as many times as desired.

```go
request := NewRequest().
    WithBody(`{"hello":"world"}`).
    WithContentType("application/json").
    WithContext(context.TODO()).
    WithExchange("custom.exchange").
    WithRoutingKey("routing_key").
    WithHeaders(amqp.Headers{}).
    WithCorrelationID("custom-correlation-id").
    WithTimeout(5 * time.Second).
    WithResponse(true)
```

By default a `context.Background()` will be added and `WithResponse()` will be
set to `true`.

`WithTimeout` will also set the `Expiration` on the publishing since there is no
point of handling the message after the timeout has expired. Setting
`WithResponse(false)` will ensure that no `Expiration` is set.

The `Request` also implements the `io.Writer` interface which makes it possible
to use directly like that.

```go
request := NewRequest()

err := json.NewEncoder(request).Encode(serializableObject)
if err != nil {
    panic(err)
}
```

**Note**: If you request a response when sending to a fanout exchange the
response will be the first one responded from any of the subscribers. There's
currently no way to stream multiple responses for the same request.

### Sender

The client invokes a default `SendFunc` while calling `Send()` where all the
RabbitMQ communication is handled and the message is published. The sender is
attached to a public field (`Sender`) and may be overridden. This enables you to
handle unit testing without the need to implement an interface. This framework
comes with a package named `amqprpctest` which helps you create a client with
your own custom send function.

```go
unitTestSendFunc := func(r *Request) (*amqp.Delivery, error) {
    fmt.Println("will not connect or publish anything")

    mockResponse := &amqp.Delivery{
        Body: []byte("this is my mock response")
    }

    return mockResponse, nil
}

client := amqprpctest.NewTestClient(unitTestSendFunc)
```

## Middlewares

Both the client and the server aim to only handle the connectivity and routing
within the RPC setup. This means that there's no code interacting with the
request or the responses before or after a request is being published or
received. To handle this in a dynamic way and allow the user to affect as much
as possible for each request both the server and the client is built around
middlewares.

### Server middlewares

Server middlewares can be hooked both to a specific handler or to all messages
for the entire server. Middlewares can be chained to be executed in a specific
order.

The middleware is inspired by the [HTTP] library where the middleware is defined
as a function that takes a handler function as input and returns the same
handler func. This makes it possible to add middlewares in any order.

The handler function `HandlerFunc` takes a context, a response writer and a
delivery as input not returning anything. The `ServerMiddlewareFunc` thus takes
this type as input and returns the same type.

```go
type HandlerFunc func(context.Context, *ResponseWriter, amqp.Delivery)

type ServerMiddlewareFunc func(next HandlerFunc) HandlerFunc
```

To execute the next handler just call the received function.

```go
server := NewServer("amqp://guest:guest@localhost:5672")

// Define a custom middleware to use for the server.
func myMiddleware(next HandlerFunc) HandlerFunc {
    // Preinitialization of middleware here.

    return func(ctx context.Context, rw *ResponseWriter d amqp.Delivery) {
        fmt.Println("this will be executed before the actual handler")

        // Execute the handler.
        next(ctx, rw, d)

        fmt.Println("this will be executed after the actual handler")
    }
}

// Add a middleware to specific handler.
server.Bind(DirectBinding("foobar", myMiddleware(HandlerFunc)))

// Add multiple middlewares to specific handler by chainging them.
server.Bind(
    DirectBinding(
        "foobar",
        ServerMiddlewareChain(
            myHandler,
            middlewareOne,
            middlewareTwo,
            middlewareThree,
        ),
    )
)

// Add middleware to all handlers on the server.
server.AddMiddleware(myMiddleware)

server.ListenAndServe()
```

Note that an example of how to handle panic recovery with a middleware if a
handler would panic is located in the [middleware] folder in this project.

### Client middlewares

The client supports middlewares which may be executed before or after a request
has been published. This is a great way to i.e enrich headers or handle errors
returned.

The sender function `SendFunc` takes a `Request` as input and returns an
`amqp.Delivery` pointer and an error. The `ClientMiddlewareFunc` thus takes this
type as input and returns the same type.

```go
type SendFunc func(r *Request) (d *amqp.Delivery, e error)

type ClientMiddlewareFunc func(next SendFunc) SendFunc
```

A middleware can be added to the client to be executed for all requests or
attached to the `Request` type instead.

```go
func myMiddleware(next SendFunc) SendFunc {
    return func(r *Request) (*amqp.Delivery, error) (
        r.Publishing.Headers["enriched-header"] = "yes"
        r.Publishing.AppId = "my-app"

        return next(r)
    }
}

// Add the middleware to all request made with the client.
client := NewClient("amqp://guest:guest@localhost:5672").
    AddMiddleware(myMiddleware)

// Add the middleware to a singlerequest
reuqest := NewRequest().
    WithRoutingKey("routing_key").
    AddMiddleware(myMiddleware)

client.Send(request)
```

Due to the way middlewares are added and chained the middlewares attached to the
request will always be executed **after** the middlewares attached to the
client.

For more examples of client middlewares, see [examples/middleware].

## Connection hooks

You often want to know when a connection has been established and when it comes
to RabbitMQ also perform some post connection setup. This is enabled by the fact
that both the server and the client holds a list of `OnStarted`. The function
receives the incomming connection, outgoing connection, incomming channel and
outgoing channel.

```go
type OnStartedFunc func(inputConn, outputConn *amqp.Connection, inputChannel, outputChannel *amqp.Channel)
```

As an example this is a great place to do some initial QoS setup.

```go
server := NewServer("amqp://guest:guest@localhost:5672")

setupQoS(_, _ *amqp.Connection, inChan, _ *amqp.Channel) {
    err := inChan.Qos(
        10,   // Prefetch count
        1024, // Prefetch size
        true, // Global
    )

    if err != nil {
        panic(err.Error())
    }
}

// Setup QoS when the connection is established.
server.OnStarted(setupQoS)

server.ListenAndServe()
```

Both the server and the client follow the recommendations for [RabbitMQ
connections] which means separate connections for incomming and outgoing traffic
and separate channels for consuming and publishing messages. Because of this the
signature looks the same way for both the server and the client.

## TLS

Since this frameworks main responsibility is to handle connections it's shipped
with a type named `Certificates` which can hold the RabbitMQ message bus CA and
the clients certificate and private key. This is just a convenient way to keep
track of certificates to use but it also implements a method named `TLSConfig()`
which returns a `*tls.Config`. This may then be used to connect with a secured
protocol (AMQPS) to the message bus.

```go
certificates := Certificates{
    CA: "/path/to/rootCA.pem",
}

// Now we can get the TLS configuration to use for the client and server.
uri := "amqps://guest:guest@localhost:5671"

server := NewServer(uri).WithTLS(certificates.TLSConfig())
client := NewClient(uri).WithTLS(certificates.TLSConfig())

server.ListenAndServe()
```

## Logging

You can specifiy two optional loggers for debugging and errors or unexpected
behaviour. By default only error logging is turned on and is logged via the log
package's standard logging.

You can provide your own logging function for both error and debug on both the
client and the server.

```go
debugLogger := log.New(os.Stdout, "DEBUG - ", log.LstdFlags)
errorLogger := log.New(os.Stdout, "ERROR - ", log.LstdFlags)

server := NewServer(url).
    WithErrorLogger(errorLogger.Printf).
    WithDebugLogger(debugLogger.Printf)

client := NewClient(url).
    WithErrorLogger(errorLogger.Printf).
    WithDebugLogger(debugLogger.Printf)
```

This is perfect when using a logger which supports debugging as a separate
method such as the [logrus] logger which has `Debugf` and `Errorf` methods.

## Examples

There are a few examples included in the [examples] folder. For more information
about how to customize your setup, see the [documentation].

  [Go amqp]: https://github.com/streadway/amqp
  [RabbitMQ]: https://www.rabbitmq.com
  [RabbitMQ binding]: https://www.rabbitmq.com/tutorials/tutorial-four-go.html
  [HTTP]: https://godoc.org/net/http
  [middleware]: middleware/
  [examples/middleware]: examples/middleware/
  [RabbitMQ connections]: https://www.rabbitmq.com/connections.html
  [logrus]: https://github.com/sirupsen/logrus
  [examples]: examples/
  [documentation]: https://godoc.org/github.com/0x4b53/amqp-rpc
