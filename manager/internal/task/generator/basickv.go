package generator

import (
	"context"
	"math/rand"
	"time"

	"github.com/bits-and-blooms/bitset"
	"github.com/go-logr/logr"
	"github.com/oxia-io/okk/internal/proto"
	"golang.org/x/time/rate"
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

func NewBasicKv(logger *logr.Logger, ctx context.Context, name string,
	duration *time.Duration, opPerSec int) Generator {
	currentContext, currentContextCanceled := context.WithCancel(ctx)
	namedLogger := logger.WithName("basic-kv-generator")
	namedLogger.Info("Starting basic kv generator ", "name", name)
	basicKv := basicKv{
		Logger:     &namedLogger,
		Context:    currentContext,
		CancelFunc: currentContextCanceled,
		name:       name,
		duration:   duration,
		startTime:  time.Now(),
		sequence:   0,
		rateLimit:  rate.NewLimiter(rate.Every(1*time.Second), opPerSec),
	}
	return &basicKv
}
