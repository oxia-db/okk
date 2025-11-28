package task

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-logr/logr"
	"github.com/oxia-io/okk/internal/proto"
	"github.com/oxia-io/okk/internal/task/generator"
	osserrors "github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	ErrRetryable        = errors.New("retryable error")
	ErrNonRetryable     = errors.New("non retryable error")
	ErrAssertionFailure = errors.New("assertion failure")

	operationLatencyHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "task_operation_duration_seconds",
		Help:    "",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
	}, []string{"task_name", "status"})
)

type Task interface {
	io.Closer

	Run()
}

var _ Task = &task{}

type task struct {
	*logr.Logger
	sync.WaitGroup
	context.Context
	context.CancelFunc

	generator       generator.Generator
	providerManager *ProviderManager
	name            string
	worker          string
}

func (t *task) Close() error {
	t.CancelFunc()
	t.Wait()

	return nil
}

func (t *task) Run() {
	err := backoff.RetryNotify(t.run, backoff.NewExponentialBackOff(), func(err error, duration time.Duration) {
		t.Error(err, "Task running failed.", "retry-after", duration)
	})
	if err != nil {
		t.Error(err, "Task running failed.")
	}
}

func (t *task) run() error {
	var provider proto.OkkClient
	var err error
	if provider, err = t.providerManager.GetProvider(t.worker); err != nil {
		return err
	}
	stream, err := provider.Execute(t.Context)
	if err != nil {
		return err
	}

	for {
		if operation, hasNext := t.generator.Next(); hasNext {
			err = backoff.RetryNotify(func() error {
				startTime := time.Now()
				if err := stream.Send(&proto.ExecuteCommand{
					Operation: operation,
				}); err != nil {
					if errors.Is(err, io.EOF) {
						return backoff.Permanent(errors.New("stream closed"))
					}
					return err
				}
				response, err := stream.Recv()
				if err != nil {
					if errors.Is(err, io.EOF) {
						return backoff.Permanent(errors.New("stream closed"))
					}
					return err
				}
				status := response.Status
				switch status {
				case proto.Status_Ok:
					operationLatencyHistogram.WithLabelValues(t.name, proto.Status_Ok.String()).Observe(time.Since(startTime).Seconds())
					return nil
				case proto.Status_RetryableFailure:
					operationLatencyHistogram.WithLabelValues(t.name, proto.Status_RetryableFailure.String()).Observe(time.Since(startTime).Seconds())
					return osserrors.Wrap(ErrRetryable, response.StatusInfo)
				case proto.Status_NonRetryableFailure:
					operationLatencyHistogram.WithLabelValues(t.name, proto.Status_NonRetryableFailure.String()).Observe(time.Since(startTime).Seconds())
					return backoff.Permanent(osserrors.Wrap(ErrNonRetryable, response.StatusInfo))
				case proto.Status_AssertionFailure:
					if IsEventually(operation.Assertion) {
						operationLatencyHistogram.WithLabelValues(t.name, proto.Status_RetryableFailure.String()).Observe(time.Since(startTime).Seconds())
						return osserrors.Wrap(ErrRetryable, response.StatusInfo)
					}
					operationLatencyHistogram.WithLabelValues(t.name, proto.Status_AssertionFailure.String()).Observe(time.Since(startTime).Seconds())
					return backoff.Permanent(osserrors.Wrap(ErrAssertionFailure, response.StatusInfo))
				default:
					operationLatencyHistogram.WithLabelValues(t.name, "Unknown").Observe(time.Since(startTime).Seconds())
					return backoff.Permanent(errors.New("unknown status"))
				}
			}, backoff.NewExponentialBackOff(), func(err error, duration time.Duration) {
				t.Error(err, "Send command failed.", "retry-after", duration)
			})
			if err != nil {
				if errors.Is(err, ErrAssertionFailure) {
					return backoff.Permanent(err)
				}
				return err
			}
		}
	}
}

func NewTask(ctx context.Context, logger *logr.Logger, providerManager *ProviderManager,
	name string, generator generator.Generator, worker string) Task {
	currentContext, contextCancel := context.WithCancel(ctx)
	taskLogger := logger.WithName(fmt.Sprintf("task-%s", name))
	t := &task{
		Context:         currentContext,
		CancelFunc:      contextCancel,
		Logger:          &taskLogger,
		WaitGroup:       sync.WaitGroup{},
		generator:       generator,
		name:            name,
		worker:          worker,
		providerManager: providerManager,
	}
	return t
}
