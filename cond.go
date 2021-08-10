package amqprpc

import "sync"

func AwaitCond(cond *sync.Cond, checkFunc func() bool) {}
