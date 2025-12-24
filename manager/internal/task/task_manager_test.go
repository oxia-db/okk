package task

import (
	"fmt"
	"testing"
	"time"

	okkv1 "github.com/oxia-io/okk/api/v1"
	"github.com/oxia-io/okk/internal/task/generator"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var testLog = ctrl.Log.WithName("test")

func init() {
	opts := zap.Options{
		Development: true,
	}
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
}

func TestMetadataEphemeral(t *testing.T) {
	ctx := logf.IntoContext(t.Context(), testLog)
	taskManager := NewManager(ctx)

	err := taskManager.ApplyTask("test", "localhost:6666", func() generator.Generator {
		return generator.NewMetadataEphemeralGenerator(ctx, &okkv1.TestCase{
			ObjectMeta: metav1.ObjectMeta{
				Name: "metadata-ephemeral",
			},
			Spec: okkv1.TestCaseSpec{
				Type:     okkv1.TestCaseTypeMetadataWithEphemeral,
				OpRate:   pointer.Int(100),
				Duration: &metav1.Duration{Duration: 30 * time.Second},
			},
		})
	})
	assert.NoError(t, err)
}

func TestStreamingSequence(t *testing.T) {
	ctx := logf.IntoContext(t.Context(), testLog)
	taskManager := NewManager(ctx)

	err := taskManager.ApplyTask("test", "localhost:6666", func() generator.Generator {
		return generator.NewStreamingSequence(ctx, &okkv1.TestCase{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("sequence-%d", time.Now().UnixNano()),
			},
			Spec: okkv1.TestCaseSpec{
				Type:     okkv1.TestCaseTypeStreamingSequence,
				OpRate:   pointer.Int(100),
				Duration: &metav1.Duration{Duration: 30 * time.Second},
			},
		})
	})
	assert.NoError(t, err)
}
