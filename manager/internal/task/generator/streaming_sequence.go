package generator

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/go-logr/logr"
	v1 "github.com/oxia-io/okk/api/v1"
	"github.com/oxia-io/okk/internal/proto"
	"golang.org/x/time/rate"
	"k8s.io/utils/pointer"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
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
	if s.duration != nil && time.Since(s.startTime) > *s.duration {
		s.Info("Finish the metadata notification generator", "name", s.taskName)
		return nil, false
	}
	if err := s.rateLimit.Wait(s.Context); err != nil {
		s.Error(err, "Failed to wait for rate limiter")
		return nil, false
	}
	sequence := s.nextSequence()
	return &proto.Operation{
		Sequence: sequence,
		Operation: &proto.Operation_Put{
			Put: &proto.OperationPut{
				Key:              s.taskName,
				PartitionKey:     pointer.String(s.taskName),
				SequenceKeyDelta: []uint64{1, 2, 3},
			},
		},
		Precondition: &proto.Precondition{
			BypassIfAssertKeyExist: pointer.Bool(true),
		},
		Assertion: &proto.Assertion{
			Key: pointer.String(fmt.Sprintf("%s-%020d-%020d-%020d", s.taskName, sequence+1, sequence+2, sequence+3)),
		},
	}, true
}

func (s *streamingSequence) nextSequence() int64 {
	nextSequence := s.sequence
	s.sequence = s.sequence + 1
	return nextSequence
}

func NewStreamingSequence(ctx context.Context, tc v1.TestCase) Generator {
	currentContext, currentContextCanceled := context.WithCancel(ctx)
	namedLogger := logf.FromContext(ctx).WithName("streaming-sequence-generator")
	namedLogger.Info("Starting metadata notification generator ", "name", tc.Name)

	return &metadataNotification{
		Logger:     &namedLogger,
		Context:    currentContext,
		CancelFunc: currentContextCanceled,
		taskName:   tc.Name,
		duration:   tc.Duration(),
		startTime:  time.Now(),
		rateLimit:  rate.NewLimiter(rate.Every(1*time.Second), tc.OpRate()),
		rand:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}
