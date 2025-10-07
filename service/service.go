package service

import (
	"context"
	"encoding/json"
	"fmt"
	"os/signal"
	"sync"
	"syscall"

	"bitbucket.org/fairfleet/common/broker"
)

// Job defines common job methods.
type Job[IN, OUT any] interface {
	Execute(msg *IN) *OUT
}

// Service is a multithreaded service with configurable job to be executed.
type Service[IN, OUT any] struct {
	concurrency uint8
	brokerIn    broker.Broker
	brokerOut   broker.Broker
	done        chan struct{}
	wg          sync.WaitGroup
	job         Job[IN, OUT]
	Debug       func(s string)
}

// NewService creates new service.
func NewService[IN, OUT any](concurrency uint8, brokerIn, brokerOut broker.Broker, job Job[IN, OUT], debug func(string)) *Service[IN, OUT] {
	if debug == nil {
		debug = func(s string) {}
	}

	return &Service[IN, OUT]{
		concurrency: concurrency,
		brokerIn:    brokerIn,
		brokerOut:   brokerOut,
		done:        make(chan struct{}),
		job:         job,
		Debug:       debug,
	}
}

// Run executes service reacting on termination signals for graceful shutdown.
func (s *Service[IN, OUT]) Run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	s.Debug(fmt.Sprintf("starting worker pool with %d workers", s.concurrency))

	sub, err := s.brokerIn.Sub(ctx)
	if err != nil {
		return fmt.Errorf("broker: %w", err)
	}

	for workerID := uint8(1); workerID <= s.concurrency; workerID++ {
		s.wg.Add(1)
		go s.run(workerID, sub)
	}

	<-ctx.Done()
	s.Debug("termination signal received, finishing remaining messages...")

	close(s.done)

	s.wg.Wait()
	s.Debug("graceful shutdown complete")

	return nil
}

func (s *Service[IN, OUT]) run(workerID uint8, messages <-chan broker.Message) {
	defer s.wg.Done()
	s.Debug(fmt.Sprintf("starting worker %d", workerID))

	for {
		select {
		case msg, ok := <-messages:
			if !ok {
				s.Debug(fmt.Sprintf("worker %d: message channel closed", workerID))
				return
			}

			s.handleMessage(workerID, msg)

		case <-s.done:
			s.Debug(fmt.Sprintf("worker %d stopping gracefully (draining messages)", workerID))
			for msg := range messages {
				s.handleMessage(workerID, msg)
			}
			return
		}
	}
}

func (s *Service[IN, OUT]) handleMessage(workerID uint8, msg broker.Message) {
	var inMsg IN
	if err := json.Unmarshal(msg.Data, &inMsg); err != nil {
		s.Debug(fmt.Sprintf("worker %d decoding message type %T: %v", workerID, inMsg, err))
		return
	}

	msg.InProgress()

	outMsg := s.job.Execute(&inMsg)
	if outMsg == nil {
		msg.Ack()
		return
	}

	out, err := json.Marshal(outMsg)
	if err != nil {
		s.Debug(fmt.Sprintf("worker %d encoding message type %T: %v", workerID, outMsg, err))
		return
	}

	if err = s.brokerOut.Pub(out); err != nil {
		s.Debug(fmt.Sprintf("worker %d publishing message %v: %v", workerID, inMsg, err))
		return
	}

	msg.Ack()
}
