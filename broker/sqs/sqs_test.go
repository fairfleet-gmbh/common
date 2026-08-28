package sqs

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/awstesting/unit"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// newTestClient returns an *sqs.SQS whose Send handler is replaced by
// respond, so no real network call is made.
func newTestClient(respond func(r *request.Request)) *sqs.SQS {
	svc := sqs.New(unit.Session, &aws.Config{DisableComputeChecksums: aws.Bool(true)}) //nolint:exhaustruct
	svc.Handlers.Clear()
	svc.Handlers.Send.PushBack(respond)
	return svc
}

type logCapture struct {
	mu   sync.Mutex
	logs []string
}

func (c *logCapture) capture(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.logs = append(c.logs, s)
}

func (c *logCapture) assertNoRawPayload(t *testing.T, secret, wantPrefix string) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()

	found := false
	for _, l := range c.logs {
		if strings.Contains(l, secret) {
			t.Fatalf("Debug log leaked raw payload: %q", l)
		}
		if strings.Contains(l, wantPrefix) {
			found = true
			if !strings.Contains(l, "bytes") {
				t.Fatalf("expected byte count in log, got: %q", l)
			}
		}
	}
	if !found {
		t.Fatalf("expected a debug log line containing %q", wantPrefix)
	}
}

func TestQueuePubDoesNotLogRawPayload(t *testing.T) {
	client := newTestClient(func(r *request.Request) {
		if out, ok := r.Data.(*sqs.SendMessageOutput); ok {
			*out = sqs.SendMessageOutput{MessageId: aws.String("test-id")} //nolint:exhaustruct
		}
	})

	captured := &logCapture{}
	q := NewQueue(client, &QueueConfig{QueueURL: "https://example.com/queue"}, captured.capture) //nolint:exhaustruct

	secret := "super-secret-jwt"
	if err := q.Pub([]byte(`{"token":"` + secret + `"}`)); err != nil {
		t.Fatalf("Pub: %v", err)
	}

	captured.assertNoRawPayload(t, secret, "publish to SQS queue")
}

func TestQueueSubDoesNotLogRawPayload(t *testing.T) {
	secret := "super-secret-jwt"
	body := `{"token":"` + secret + `"}`

	client := newTestClient(func(r *request.Request) {
		if out, ok := r.Data.(*sqs.ReceiveMessageOutput); ok {
			*out = sqs.ReceiveMessageOutput{ //nolint:exhaustruct
				Messages: []*sqs.Message{
					{ //nolint:exhaustruct
						Body:          aws.String(body),
						ReceiptHandle: aws.String("rh-1"),
					},
				},
			}
		}
	})

	captured := &logCapture{}
	q := NewQueue(client, &QueueConfig{QueueURL: "https://example.com/queue"}, captured.capture) //nolint:exhaustruct

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	messages, err := q.Sub(ctx)
	if err != nil {
		t.Fatalf("Sub: %v", err)
	}

	select {
	case <-messages:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
	}

	captured.assertNoRawPayload(t, secret, "receive message from SQS queue")
}
