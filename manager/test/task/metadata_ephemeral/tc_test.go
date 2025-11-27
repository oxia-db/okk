package metadata_ephemeral

import (
	"testing"

	"github.com/oxia-io/okk/internal/task"
	"github.com/oxia-io/okk/internal/task/generator"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func TestRun(t *testing.T) {
	context := t.Context()
	manager := task.NewManager(context)
	name := "tc_metadata_ephemeral"
	opts := zap.Options{
		Development: true,
	}
	logger := zap.New(zap.UseFlagOptions(&opts))
	if err := manager.ApplyTask(name, "127.0.0.1:6666", func() generator.Generator {
		ops := 100
		return generator.NewMetadataEphemeralGenerator(&logger, context, name, nil, &ops)
	}); err != nil {
		t.Fatal(err)
	}
}
