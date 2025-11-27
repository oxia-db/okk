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
)

var (
	ErrRetryable        = errors.New("retryable error")
	ErrNonRetryable     = errors.New("non retryable error")
	ErrAssertionFailure = errors.New("assertion failure")
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
					return nil
				case proto.Status_RetryableFailure:
					return osserrors.Wrap(ErrRetryable, response.StatusInfo)
				case proto.Status_NonRetryableFailure:
					return backoff.Permanent(osserrors.Wrap(ErrNonRetryable, response.StatusInfo))
				case proto.Status_AssertionFailure:
					if IsEventually(operation.Assertion) {
						return osserrors.Wrap(ErrRetryable, response.StatusInfo)
					}
					return backoff.Permanent(osserrors.Wrap(ErrAssertionFailure, response.StatusInfo))
				default:
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
	return &task{
		Context:         currentContext,
		CancelFunc:      contextCancel,
		Logger:          &taskLogger,
		WaitGroup:       sync.WaitGroup{},
		generator:       generator,
		worker:          worker,
		providerManager: providerManager,
	}
}
