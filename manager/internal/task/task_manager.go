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
	context.Context
	context.CancelFunc

	tasks           map[string]Task
	providerManager *ProviderManager
}

func (m *Manager) ApplyTask(name string, worker string, generatorFactory func() generator.Generator) error {
	m.Lock()
	defer m.Unlock()

	_, exist := m.tasks[name]
	if exist {
		return nil
	}

	newGenerator := generatorFactory()
	newTask := NewTask(m.Context, m.Logger, m.providerManager, name, newGenerator, worker)
	newTask.Run()

	m.tasks[name] = newTask
	return nil
}

func NewManager(ctx context.Context) *Manager {
	log := logf.FromContext(ctx)
	currentContext, currentContextCancel := context.WithCancel(ctx)

	providerManager := NewProviderManager()
	return &Manager{
		Logger:          &log,
		Context:         currentContext,
		CancelFunc:      currentContextCancel,
		tasks:           make(map[string]Task),
		providerManager: providerManager,
	}
}
