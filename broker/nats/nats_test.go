package nats

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// mockJetStream implements the subset of nats.JetStreamContext exercised by
// JetStream.Sub/Pub. Embedding the nil interface satisfies the rest, which
// must never be called in these tests.
type mockJetStream struct {
	nats.JetStreamContext
	subCh chan *nats.Msg
}

func (m *mockJetStream) ChanSubscribe(_ string, ch chan *nats.Msg, _ ...nats.SubOpt) (*nats.Subscription, error) {
	m.subCh = ch
	return nil, nil
}

func (m *mockJetStream) ChanQueueSubscribe(_, _ string, ch chan *nats.Msg, _ ...nats.SubOpt) (*nats.Subscription, error) {
	m.subCh = ch
	return nil, nil
}

func (m *mockJetStream) Publish(_ string, _ []byte, _ ...nats.PubOpt) (*nats.PubAck, error) {
	return &nats.PubAck{}, nil
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

func TestJetStreamPubDoesNotLogRawPayload(t *testing.T) {
	captured := &logCapture{}
	js := NewJetStream(&mockJetStream{}, &JetStreamConfig{Subject: "test.subject"}, captured.capture)

	secret := "super-secret-jwt"
	if err := js.Pub([]byte(`{"token":"` + secret + `"}`)); err != nil {
		t.Fatalf("Pub: %v", err)
	}

	captured.assertNoRawPayload(t, secret, "publish to NATS subject")
}

func TestJetStreamSubDoesNotLogRawPayload(t *testing.T) {
	mock := &mockJetStream{}
	captured := &logCapture{}
	js := NewJetStream(mock, &JetStreamConfig{Subject: "test.subject"}, captured.capture)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	messages, err := js.Sub(ctx)
	if err != nil {
		t.Fatalf("Sub: %v", err)
	}

	secret := "super-secret-jwt"
	mock.subCh <- &nats.Msg{Subject: "test.subject", Data: []byte(`{"token":"` + secret + `"}`)}

	select {
	case <-messages:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
	}

	captured.assertNoRawPayload(t, secret, "receive message from NATS subject")
}
