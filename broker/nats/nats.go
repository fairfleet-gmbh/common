package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"

	"bitbucket.org/fairfleet/common/broker"
)

const (
	defaultAckWait            = 60 * time.Second
	defaultReceiveChannelSize = 128
)

var _ broker.Broker = (*JetStream)(nil)

// JetStream implements Broker interface for NATS JetStream broker.
// Exported field Debug can be used for debugging.
type JetStream struct {
	c      nats.JetStreamContext
	config *JetStreamConfig
	Debug  func(s string)
}

// JetStreamConfig contains JetStream configuration parameters.
type JetStreamConfig struct {
	// Subscribe to this subject
	Subject string
	// Optional. If provided, queue group will be used.
	ConsumerGroup string
	// Produce into this subject
	ReceiveChannelSize int
	// How long to wait for ACK. If crossed, message will be redelivered. Default 60.s
	AckWait time.Duration
	// MaxRedeliveries defines how many times message will be redelivered if not acknowledged. Default 2.
	MaxRedeliveries uint8
}

// NewJetStream creates new NATS JetStream broker implementing broker.Broker interface.
func NewJetStream(client nats.JetStreamContext, config *JetStreamConfig, debug func(string)) *JetStream {
	if config.AckWait == 0 {
		config.AckWait = defaultAckWait
	}

	if config.MaxRedeliveries == 0 {
		config.MaxRedeliveries = 2
	}

	if config.ReceiveChannelSize == 0 {
		config.ReceiveChannelSize = defaultReceiveChannelSize
	}

	if debug == nil {
		debug = func(string) {}
	}

	return &JetStream{ //nolint:exhaustruct
		c:      client,
		config: config,
		Debug:  debug,
	}
}

// Sub implements broker.Broker interface.
func (b *JetStream) Sub(ctx context.Context) (<-chan broker.Message, error) { //nolint:funlen,cyclop
	messages := make(chan broker.Message)
	natsCh := make(chan *nats.Msg, b.config.ReceiveChannelSize)

	var sub *nats.Subscription

	var err error

	b.Debug(fmt.Sprintf("subscribe to subject: %s", b.config.Subject))
	if b.config.ConsumerGroup != "" {
		sub, err = b.c.ChanQueueSubscribe(b.config.Subject, b.config.ConsumerGroup, natsCh,
			nats.ManualAck(), nats.AckWait(b.config.AckWait), nats.MaxDeliver(int(b.config.MaxRedeliveries)),
			nats.DeliverNew())
	} else {
		sub, err = b.c.ChanSubscribe(b.config.Subject, natsCh, nats.ManualAck(),
			nats.AckWait(b.config.AckWait), nats.DeliverNew())
	}

	if err != nil {
		return nil, fmt.Errorf("subscribe: %w", err)
	}

	go func() {
		for {
			select {
			case msg := <-natsCh:
				b.Debug(fmt.Sprintf("receive message from NATS subject %s: %s",
					b.config.Subject, string(msg.Data)))
				messages <- broker.Message{
					Data: msg.Data,
					Ack: func() {
						if err := msg.Ack(); err != nil {
							b.Debug(fmt.Sprintf("error: ack: %s", err))
						}
					},
					InProgress: func() {
						if err := msg.InProgress(); err != nil {
							b.Debug(fmt.Sprintf("error: in progress: %s", err))
						}
					},
				}
			case <-ctx.Done():
				b.Debug(fmt.Sprintf("consuming from NATS subject %s done", b.config.Subject))
				if err := sub.Unsubscribe(); err != nil {
					b.Debug(fmt.Sprintf("error: unsubscribe: %s", err))
				}
				if err := sub.Drain(); err != nil {
					b.Debug(fmt.Sprintf("error: drain: %s", err))
				}

				close(natsCh)
				close(messages)

				return
			}
		}
	}()

	return messages, nil
}

// Pub implements broker.Broker interface.
func (b *JetStream) Pub(data []byte) error {
	b.Debug(fmt.Sprintf("publish to NATS subject %s: %s", b.config.Subject, string(data)))
	_, err := b.c.Publish(b.config.Subject, data)
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	return nil
}
