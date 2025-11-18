package task

import (
	"context"
	"sync"

	"github.com/go-logr/logr"
	"github.com/oxia-io/okk/internal/task/generator"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

type Manager struct {
	*logr.Logger
	sync.Mutex

	tasks map[string]Task
}

func (m *Manager) ApplyTask(uid string, worker string, generator generator.Generator) error {
	m.Lock()
	defer m.Unlock()

	_, exist := m.tasks[uid]
	if exist {
		return nil
	}

	return nil
}

func NewManager(ctx context.Context) *Manager {
	log := logf.FromContext(ctx)
	return &Manager{
		Logger: &log,
		tasks:  make(map[string]Task),
	}
}
