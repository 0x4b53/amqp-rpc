package amqprpc

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
)

type Publisher struct {
	url         string
	maxInFlight int
	dialConfig  amqp.Config

	didStop chan struct{}
	stop    chan struct{}

	confirmCallbacks   map[uint64]func(err error)
	confirmCallbacksMu sync.Mutex

	outstandingConfirms int32
	allConfirmedCond    chan (struct{})

	ch              *amqp.Channel
	conn            *amqp.Connection
	nextDeliveryTag uint64

	confirms chan amqp.Confirmation
	returns  chan amqp.Return
	flows    chan bool

	flowLock sync.RWMutex

	messages chan PublishData

	errs []error
}

func NewPublisher(url string) *Publisher {
	return &Publisher{
		url:              url,
		allConfirmedCond: make(chan struct{}),
		confirmCallbacks: make(map[uint64]func(err error)),
	}
}

func (p *Publisher) WithDialConfig(c amqp.Config) *Publisher {
	p.dialConfig = c
	return p
}

func (p *Publisher) Start() {
	once := sync.Once{}
	started := make(chan struct{})

	p.stop = make(chan struct{})
	p.didStop = make(chan struct{})

	go autoReconnect(p.didStop, p.stop, func() error {
		p.nextDeliveryTag = 0

		conn, err := amqp.DialConfig(p.url, p.dialConfig)
		if err != nil {
			return err
		}

		defer conn.Close()

		ch, err := conn.Channel()
		if err != nil {
			return err
		}

		defer ch.Close()

		p.ch = ch

		// Put the channel in confirm mode.
		err = ch.Confirm(false)
		if err != nil {
			return err
		}

		p.confirms = ch.NotifyPublish(make(chan amqp.Confirmation))
		p.returns = ch.NotifyReturn(make(chan amqp.Return))

		p.flows = ch.NotifyFlow(make(chan bool))

		if p.messages == nil {
			p.messages = make(chan PublishData)
		}

		go p.handleConfirmsAndReturns()
		go p.handleFlow()
		go p.runPublisher()

		once.Do(func() {
			close(started)
		})

		err = monitorAndWait(
			p.stop,
			ch.NotifyClose(make(chan *amqp.Error)),
			conn.NotifyClose(make(chan *amqp.Error)),
		)
		if err != nil {
			return err
		}

		// Finish sending all messages.
		close(p.messages)
		p.messages = nil

		return nil
	})

	<-started
}

func (p *Publisher) AwaitConfirms(timeout time.Duration) error {
	if atomic.LoadInt32(&p.outstandingConfirms) == 0 {
		return nil
	}

	select {
	case <-p.allConfirmedCond:
		return nil
	case <-time.After(timeout):
		return errors.New("timeout")
	}
}

func (p *Publisher) Close() error {
	close(p.stop)

	<-p.didStop

	return nil
}

func (p *Publisher) handleConfirmsAndReturns() {
	// This map is checked for returns on every confirmation so that we can
	// supply a correct error message back to the Publish() caller.
	returns := map[uint64]amqp.Return{}

	for {
		select {
		case ret, ok := <-p.returns:
			if !ok {
				return
			}

			deliveryTag, _ := strconv.ParseUint(ret.MessageId, 10, 64)

			if deliveryTag > 0 {
				returns[deliveryTag] = ret
			}
		case confirm, ok := <-p.confirms:
			if !ok {
				return
			}

			p.confirmCallbacksMu.Lock()
			callback := p.confirmCallbacks[confirm.DeliveryTag]
			delete(p.confirmCallbacks, confirm.DeliveryTag)
			p.confirmCallbacksMu.Unlock()

			ret, hasReturn := returns[confirm.DeliveryTag]

			switch {
			case hasReturn:
				delete(returns, confirm.DeliveryTag)

				if callback != nil {
					callback(fmt.Errorf("%w: %d, %s",
						ErrRequestReturned,
						ret.ReplyCode,
						ret.ReplyText,
					))
				}
			case !confirm.Ack:
				if callback != nil {
					callback(ErrRequestRejected)
				}
			default:
				if callback != nil {
					callback(nil)
				}
			}

			outstanding := atomic.AddInt32(&p.outstandingConfirms, -1)
			if outstanding == 0 {
				for {
					select {
					case p.allConfirmedCond <- struct{}{}:
						continue
					default:
					}

					break
				}
			}
		}
	}
}

func (p *Publisher) handleFlow() {
	isLocked := false

	for wantLock := range p.flows {
		if wantLock && !isLocked {
			p.flowLock.Lock()
			isLocked = true
			continue
		}

		if !wantLock && isLocked {
			p.flowLock.Unlock()
			isLocked = false
			continue
		}
	}
}

func (p *Publisher) runPublisher() {
	for wrapped := range p.messages {
		p.nextDeliveryTag++

		p.confirmCallbacksMu.Lock()
		p.confirmCallbacks[p.nextDeliveryTag] = wrapped.OnConfirm
		p.confirmCallbacksMu.Unlock()

		wrapped.Msg.MessageId = strconv.FormatUint(p.nextDeliveryTag, 10)

		err := p.ch.Publish(wrapped.Exchange, wrapped.Key, wrapped.Mandatory, wrapped.Immediate, wrapped.Msg)
		if err != nil {
			fmt.Println(err)
			_ = p.ch.Close()
			p.messages <- wrapped
		}
	}
}

func (p *Publisher) Publish(data PublishData) {
	atomic.AddInt32(&p.outstandingConfirms, 1)

	p.messages <- data
}

type PublishData struct {
	Exchange  string
	Key       string
	Mandatory bool
	Immediate bool
	Msg       amqp.Publishing

	OnConfirm func(err error)
}
