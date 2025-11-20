package task

import (
	"context"
	"io"

	"github.com/go-logr/logr"
	"github.com/oxia-io/okk/internal/task/generator"
	"k8s.io/apimachinery/pkg/util/wait"
)

type Task interface {
	io.Closer

	Run() error
}

var _ Task = &task{}

type task struct {
	*logr.Logger
	wait.Group
	context.Context
	context.CancelFunc

	generator generator.Generator
	worker    string
}

func (t *task) Close() error {
	t.CancelFunc()
	t.Group.Wait()

	return nil
}

func (t *task) Run() error {
}

func NewTask(ctx context.Context, generator generator.Generator, worker string) Task {
	return &task{}
}
