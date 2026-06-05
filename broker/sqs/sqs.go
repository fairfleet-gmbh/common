package sqs

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/fairfleet-gmbh/common/broker"
)

const (
	defaultVisibilityTimeout   = 30 // seconds
	defaultWaitTimeSeconds     = 10
	defaultReceiveChannelSize  = 128
	defaultMaxNumberOfMessages = 5
)

var _ broker.Broker = (*Queue)(nil)

// Queue implements Broker interface for AWS SQS.
// Exported field Debug can be used for debugging.
type Queue struct {
	c      *sqs.SQS
	config *QueueConfig
	Debug  func(s string)
}

// QueueConfig contains configuration for SQS broker.
type QueueConfig struct {
	// Full URL of the queue
	QueueURL string
	// Number of messages to prefetch per request (max 10)
	MaxNumberOfMessages int64
	// How long to wait for messages in long polling
	WaitTimeSeconds int64
	// Channel buffer size
	ReceiveChannelSize int
	// Visibility timeout for processing (in seconds)
	VisibilityTimeout int64
}

// NewQueue creates new SQS broker implementing broker.Broker interface.
func NewQueue(client *sqs.SQS, config *QueueConfig, debug func(string)) *Queue {
	if config.MaxNumberOfMessages == 0 {
		config.MaxNumberOfMessages = defaultMaxNumberOfMessages
	}
	if config.WaitTimeSeconds == 0 {
		config.WaitTimeSeconds = defaultWaitTimeSeconds
	}
	if config.ReceiveChannelSize == 0 {
		config.ReceiveChannelSize = defaultReceiveChannelSize
	}
	if config.VisibilityTimeout == 0 {
		config.VisibilityTimeout = defaultVisibilityTimeout
	}

	if debug == nil {
		debug = func(string) {}
	}

	return &Queue{ //nolint:exhaustruct
		c:      client,
		config: config,
		Debug:  debug,
	}
}

// Sub implements broker.Broker interface.
func (b *Queue) Sub(ctx context.Context) (<-chan broker.Message, error) {
	messages := make(chan broker.Message, b.config.ReceiveChannelSize)

	go func() {
		for {
			select {
			case <-ctx.Done():
				b.Debug(fmt.Sprintf("consuming from SQS queue %s stopped", b.config.QueueURL))
				close(messages)
				return
			default:
				input := &sqs.ReceiveMessageInput{
					QueueUrl:              aws.String(b.config.QueueURL),
					MaxNumberOfMessages:   aws.Int64(b.config.MaxNumberOfMessages),
					WaitTimeSeconds:       aws.Int64(b.config.WaitTimeSeconds),
					VisibilityTimeout:     aws.Int64(b.config.VisibilityTimeout),
					MessageAttributeNames: []*string{aws.String("All")},
				}
				out, err := b.c.ReceiveMessage(input)
				if err != nil {
					b.Debug(fmt.Sprintf("error: receive from SQS queue %s: %s", b.config.QueueURL, err))
					time.Sleep(2 * time.Second)
					continue
				}

				b.Debug(fmt.Sprintf("received %d messages from SQS queue %s", len(out.Messages), b.config.QueueURL))
				for _, msg := range out.Messages {
					b.Debug(fmt.Sprintf("receive message from SQS queue %s: %s", b.config.QueueURL, *msg.Body))
					messages <- broker.Message{
						Data: []byte(*msg.Body),
						Ack: func() {
							_, err := b.c.DeleteMessage(&sqs.DeleteMessageInput{
								QueueUrl:      aws.String(b.config.QueueURL),
								ReceiptHandle: msg.ReceiptHandle,
							})
							if err != nil {
								b.Debug(fmt.Sprintf("error: ack (delete): %s", err))
							}
						},
						InProgress: func() {
							_, err := b.c.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
								QueueUrl:          aws.String(b.config.QueueURL),
								ReceiptHandle:     msg.ReceiptHandle,
								VisibilityTimeout: aws.Int64(b.config.VisibilityTimeout),
							})
							if err != nil {
								b.Debug(fmt.Sprintf("error: in progress (visibility): %s", err))
							}
						},
					}
				}
			}
		}
	}()

	return messages, nil
}

// Pub implements broker.Broker interface.
func (b *Queue) Pub(data []byte) error {
	b.Debug(fmt.Sprintf("publish to SQS queue %s: %s", b.config.QueueURL, string(data)))
	_, err := b.c.SendMessage(&sqs.SendMessageInput{
		QueueUrl:    aws.String(b.config.QueueURL),
		MessageBody: aws.String(string(data)),
	})
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	return nil
}
