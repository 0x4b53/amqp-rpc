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
s := server.New()
s.AddHandler("routing_key", func(c context.Context, d *amqp.Delivery) []byte {
    fmt.Println(d.Body, d.Headers)

    return []byte("Handled")
})

s.ListenAndServe("amqp://guest:guest@localhost:5672")
```

#### Middlewares

It's possible to hook middlewares in the server which will be executed before
each route. The server middleware is defined as follows:

```go
type ServerMiddleware func(string, context.Context, *amqp.Delivery) error
```

If the error returned is **not** `nil`, the handler for the given routing key
will **not** be called.

```go
noEmptyAuthHeader := func(rk string, c context.Context, d *amqp.Delivery) error {
    if _, ok := headers["authorization"]; !ok {
        return errors.New("Missing authorization header")
    }

    return nil
}

s := server.New()
s.ListenAndServe("amqp://guest:guest@localhost:5672", noEmptyAuthHeader)
```

### Client

Similar to the server, a client can be passed a `Certificates` type when calling
`New` to setup the connection with TLS. Everything you usually handle is handled
within the client package such as creating channels, creating queues,
subscribing to said queue will be taken care of for you. The client will monitor
the connection and reconnect if it's lost.

The client is designed to handle publishing and consumption of the reply in a
non-blocking way to ensure that everyone using a client will get their message
published as fast as possible and also, if requested, the reply.

```go
c := client.New("amqp://guest:guest@localhost:5672")

var (
    wantReply  bool   = true
    routingKey string = "endpoint"
    body              = []byte("My body")
)

response, err := c.Publish(routingKey, body, wantReply)
if err != nil {
    logger.Warn("Something went wrong", err)
}

logger.Info(string(response.Body))
```

If you're using the client but not the server, or for any other reason want to
handle your connection manually, see `NewWithConnection`. Note that the
connection passed will **not** be monitored so if you've recovered your
connection and need to set it again you should use `SetConnection`.

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
s := server.New(cert)
s.ListenAndServe(uri)

c := client.New(uri, cert)
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
