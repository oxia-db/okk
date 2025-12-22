package generator

import (
	"context"
	"math/rand"
	"time"

	"github.com/bits-and-blooms/bitset"
	"github.com/go-logr/logr"
	v1 "github.com/oxia-io/okk/api/v1"
	"github.com/oxia-io/okk/internal/proto"
	"golang.org/x/time/rate"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var _ Generator = &basicKv{}

type basicKv struct {
	*logr.Logger
	context.Context
	context.CancelFunc
	name string

	duration  *time.Duration
	rateLimit *rate.Limiter
	startTime time.Time
	keySpace  int

	sequence int64
	keys     bitset.BitSet
	rand     *rand.Rand
}

func (b *basicKv) Name() string {
	return "basic-kv"
}

func (b *basicKv) Next() (*proto.Operation, bool) {
	if b.duration != nil && time.Since(b.startTime) > *b.duration {
		b.Info("Finish the metadata notification generator", "name", b.name)
		return nil, false
	}
	if err := b.rateLimit.Wait(b.Context); err != nil {
		b.Error(err, "Failed to wait for rate limiter")
		return nil, false
	}
	return nil, false
}

func NewBasicKv(ctx context.Context, tc *v1.TestCase) Generator {
	currentContext, currentContextCanceled := context.WithCancel(ctx)
	namedLogger := logf.FromContext(ctx).WithName("basic-kv-generator")
	namedLogger.Info("Starting basic kv generator ", "name", tc.Name)
	bkv := basicKv{
		Logger:     &namedLogger,
		Context:    currentContext,
		CancelFunc: currentContextCanceled,
		name:       tc.Name,
		duration:   tc.Duration(),
		startTime:  time.Now(),
		sequence:   0,
		rateLimit:  rate.NewLimiter(rate.Every(1*time.Second), tc.OpRate()),
	}
	return &bkv
}
