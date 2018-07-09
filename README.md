# RabbitMQ RPC

[![GoDoc](https://godoc.org/github.com/bombsimon/amqp-rpc?status.svg)](https://godoc.org/github.com/bombsimon/amqp-rpc)
[![Build Status](https://travis-ci.org/bombsimon/amqp-rpc.svg?branch=master)](https://travis-ci.org/bombsimon/amqp-rpc)
[![Go Report Card](https://goreportcard.com/badge/github.com/bombsimon/amqp-rpc)](https://goreportcard.com/report/github.com/bombsimon/amqp-rpc)
[![Maintainability](https://api.codeclimate.com/v1/badges/77ecbf483dc76d4327a5/maintainability)](https://codeclimate.com/github/bombsimon/amqp-rpc/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/77ecbf483dc76d4327a5/test_coverage)](https://codeclimate.com/github/bombsimon/amqp-rpc/test_coverage)

## Description

This is a framework to use RabbitMQ with
[Go amqp](https://github.com/streadway/amqp) as RPC client/server setup. The
purpose of this package is to get all the advantages of using a message queue
but without the hassle of handling the core of setting up servers and clients,
handling network problems, reconnects and similar things.

## Components

The package consists of a few components that can be used together or separately.

### Server

One of the key features is the RPC server wich is an easy way to quckly hook
into the consumption of amqp messages. All you need to do to start consuming
messages published to `routing_key` looks like this:

```go
s := server.New("amqp://guest:guest@localhost:5672")

s.AddHandler("routing_key", func(c context.Context, rw *ResponseWriter d *amqp.Delivery) {
    fmt.Println(d.Body, d.Headers)
    fmt.Fprint(rw, "Handled")
})

s.ListenAndServe()
```

This will use the default exchange (`direct`) and use the routing key as queue
name. It's also possible to specify any kind of exchange such as topic or
fanout by using the `AddExchangeHandler`. To add a fanout exchange with auto
generated queue names one could do like this:

```go
s := server.New("amqp://guest:guest@localhost:5672)

var (
    routingKey   string
    exchangeName string = "fanout-exchange"
    exchangeType string = "fanout"
    options      amqp.Table
)

s.AddExchangeHandler(routingKey, exchangeName, exchangeType, options, handleFunc)
```

#### Middlewares

Middlewares can be hooked to both a handler and to the entire server. And are executed
when the handler is.

The middleware is defined as follows:

```go
type MiddlewareFunc func(next HandlerFunc) Handlerfunc
```

To execute the inner handler call `next` with the correct arguments:

```go
func myMiddle(next HandlerFunc) HandlerFunc {

	// Preinitialization of middleware here.

	return func(ctx context.Context, rw *ResponseWriter d amqp.Delivery) {
		// Before handler execution here.

		// Execute the handler.
		next(ctx, rw, d)

		// After execution here.
	}
}

s := server.New("amqp://guest:guest@localhost:5672")

// Add a middleware to specific handler.
s.AddHandler("foobar", myMiddle(HandlerFunc))

// Add multiple middlewares to specific handler.
s.AddHandler(
    "foobar",
    MiddlewareChain(
        myHandler,
        middlewareOne,
        middlewareTwo,
        middlewareThree,
    )
)

// Add middleware to all handlers on the server.
s.AddMiddleware(myMiddle)

s.ListenAndServe()
```

### Client

Similar to the server, a client is passed an URL when calling `New`. To setup
the connection with TLS either a complete dialconfig or just a certificate type
can be passed. Everything you usually handle is handled within the client
package such as creating channels, creating queues, subscribing to said queue
will be taken care of for you. The client will monitor the connection and
reconnect if it's lost.

The client is designed to handle publishing and consumption of the reply in a
non-blocking way to ensure that everyone using a client will get their message
published as fast as possible and also, if requested, the reply.

```go
c := client.New("amqp://guest:guest@localhost:5672")

request := client.NewRequest("my_endpoint").WithStringBody("My body").WithResponse(true)
response, err := c.Send(request)
if err != nil {
    logger.Warn("Something went wrong", err)
}

logger.Info(string(response.Body))
```

The client will not connect upon calling new, instead this is made the first
time a connection is required, usually when calling `Send`. By doint this you're
able to chain multiple methods after calling new to modify the client settings.

```go
c := client.New("amqp://guest:guest@localhost:5672").
    WithTimeout(5000 * time.Milliseconds).
    WithDialConfig(dialConfig).
    WithTLS(cert).
    WithQueueDeclareSettings(qdSettings).
    WithConsumeSettings(cSettings)

// Will not connect until this call.
c.Send(client.NewRequest("queue_one"))
```

You can also specify other exchanges than the default one. To send a request
to a fanout exchange subscribed to by multiple servers you can do this.

```go
c := client.New("amqp://guest:guest@localhost:5672")
r := client.NewRequest("").WithExchange("fanout-exchange").WithResponse(false)

_, err := c.Send(r)
```

**Note**: If you request a response when sending to a fanout exchange the
response will be the first one respondend from any of the subscribers. There is
currently no way to accept multiple responses or responses in a specific order.

### Logger

Usually you don't want to log much in a package but since this can tend to be
more of a part of an application there are some messages being logged. And an
easy way to log from the embedded application can be to use the very same
logger. The logger is two loggers implementing a `Logger` interface which only
requires `Print` and `Printf`. This means that the standard logger will work
just fine and will even be provided in the `init()` function called when
importing the logger.

The loggers can be overridden with your custom logger by calling `SetInfoLogger`
or `SetWarnLogger`.

```go
logrus.SetFormatter(&logrus.JSONFormatter{})
logrus.SetOutput(os.Stdout)

l := logrus.WithFields(logrus.Fields{})

// Use logrus with JSON format as info logger
logger.SetInfoLogger(l)

logger.Infof("Custom logger: %+v", l)
```

If you don't want anything to log at any time just set a logger pointed to
`/dev/null`, i.e like this:

```go
silentLogger := log.New(ioutil.Discard, "", log.LstdFlags)

logger.SetInfoLogger(silentLogger)
logger.SetWarnLogger(silentLogger)
```

### Connection and TLS

One part of the coding aimed to minimize is the handling of certificates and TLS
setup. Both the server and the client has the possibility to set an
`amqp.Config` which also includes the field `TLSClientConfig`.

You can either read more about `amqp.Config` and create your own or just used
the `Certificates` type to point out the path to different files needed for TLS.

```go
cert := connection.Certificates{
    Cert: "/path/to/cert.pem",
    Key:  "/path/to/key.pem",
    CA:   "/path/to/cacert.pem",
}

// Now we can pass this to New() and connect our server or client with TLS.
uri := "amqps://guest:guest@localhost:5671"
dialConfig := amqp.Config{
    TLSClientConfig: cert.TLSConfig(),
}

s := server.New(uri).WithDialConfig(dialConfig)

s.ListenAndServe()

c := client.New(uri).WithTLS(cert)
```

## Example

There are a few different examples included in the `examples` folder. For more
information about how to customize your setup, see the documentation (linked
above).

## Future

Basic functionallity is implemented but in the near future the hope is to add
more ways to easily customize things such as channel values, consumptions
values, publishing values. Without compromising the fast way to get started with
just a few lines of code, of course!
