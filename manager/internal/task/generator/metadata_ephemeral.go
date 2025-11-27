package generator

import (
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/go-logr/logr"
	"github.com/oxia-io/okk/internal/proto"
	"golang.org/x/net/context"
	"golang.org/x/time/rate"
)

var _ Generator = &metadataEphemeral{}

type metadataEphemeral struct {
	*logr.Logger
	context.Context
	context.CancelFunc
	name string

	duration  *time.Duration
	rateLimit *rate.Limiter
	startTime time.Time

	sequence       int64
	counter        uint
	checkPoint     uint
	checkEphemeral bool
}

func (m *metadataEphemeral) Next() (*proto.Operation, bool) {
	if m.duration != nil && time.Since(m.startTime) > *m.duration {
		m.Info("Finish the metadata ephemeral generator", "name", m.name)
		return nil, false
	}
	if err := m.rateLimit.Wait(m.Context); err != nil {
		m.Error(err, "Failed to wait for rate limiter")
		return nil, false
	}
	if !m.checkEphemeral && !m.maybeResetCounter() {
		operation := &proto.Operation{
			Sequence: m.nextSequence(),
			Operation: &proto.Operation_Put{
				Put: &proto.OperationPut{
					Key:       fmt.Sprintf("/ephemeral/%s/%d", m.name, m.counter),
					Ephemeral: true,
				},
			},
		}
		return operation, true
	}
	var operation *proto.Operation
	if !m.checkEphemeral {
		operation = &proto.Operation{
			Sequence: m.nextSequence(),
			Operation: &proto.Operation_SessionRestart{
				SessionRestart: &proto.OperationSessionRestart{},
			},
		}
		m.checkEphemeral = true
		return operation, true
	}
	assertEmpty := true
	operation = &proto.Operation{
		Sequence: m.nextSequence(),
		Assertion: &proto.Assertion{
			EventuallyEmpty: &assertEmpty,
		},
		Operation: &proto.Operation_List{
			List: &proto.OperationList{
				KeyStart: fmt.Sprintf("/ephemeral/%s/", m.name),
				KeyEnd:   fmt.Sprintf("/ephemeral/%s//", m.name),
			},
		},
	}
	m.checkEphemeral = false
	return operation, true
}

func (m *metadataEphemeral) Name() string {
	return "metadataEphemera"
}

func (m *metadataEphemeral) nextSequence() int64 {
	nextSequence := m.sequence
	m.sequence = m.sequence + 1
	return nextSequence
}

func (m *metadataEphemeral) maybeResetCounter() bool {
	if m.counter < m.checkPoint {
		m.counter++
		return false
	}
	m.counter = 0
	m.checkPoint = rand.UintN(120)
	return true
}

func NewMetadataEphemeralGenerator(logger *logr.Logger, ctx context.Context,
	name string, duration *time.Duration, opPerSec *int) Generator {
	currentContext, currentContextCanceled := context.WithCancel(ctx)
	namedLogger := logger.WithName("ephemeral-generator")
	namedLogger.Info("Starting metadata ephemeral generator ", "name", name)
	ops := 10
	if opPerSec != nil && *opPerSec > 0 {
		ops = *opPerSec
	}
	me := metadataEphemeral{
		Logger:     &namedLogger,
		Context:    currentContext,
		CancelFunc: currentContextCanceled,
		name:       name,
		duration:   duration,
		startTime:  time.Now(),
		sequence:   0,
		rateLimit:  rate.NewLimiter(rate.Every(1*time.Second), ops),
	}
	me.maybeResetCounter()
	return &me
}
