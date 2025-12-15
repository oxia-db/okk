package generator

import (
	"context"
	"math/rand"
	"time"

	"github.com/go-logr/logr"
	"github.com/oxia-io/okk/internal/proto"
	"golang.org/x/time/rate"
)

var _ Generator = &streamingSequence{}

type streamingSequence struct {
	*logr.Logger
	context.Context
	context.CancelFunc
	taskName string

	duration  *time.Duration
	rateLimit *rate.Limiter
	startTime time.Time

	sequence int64
}

func (s *streamingSequence) Name() string {
	return "streaming-sequence"
}

func (s *streamingSequence) Next() (*proto.Operation, bool) {
	//TODO implement me
	panic("implement me")
}

func NewStreamingSequence(logger *logr.Logger, ctx context.Context,
	taskName string, duration *time.Duration, opPerSec int) Generator {
	currentContext, currentContextCanceled := context.WithCancel(ctx)

	namedLogger := logger.WithName("streaming-sequence")
	namedLogger.Info("Starting metadata notification generator ", "name", taskName)

	return &metadataNotification{
		Logger:     &namedLogger,
		Context:    currentContext,
		CancelFunc: currentContextCanceled,
		taskName:   taskName,
		duration:   duration,
		startTime:  time.Now(),
		rateLimit:  rate.NewLimiter(rate.Every(1*time.Second), opPerSec),
		rand:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}
