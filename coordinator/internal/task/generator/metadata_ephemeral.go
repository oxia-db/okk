package generator

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/oxia-io/okk/coordinator/internal/config"
	"github.com/oxia-io/okk/internal/proto"
	"golang.org/x/time/rate"
)

const propertiesKeyCheckpointNum = "checkpointNum"

var _ Generator = &metadataEphemeral{}

type metadataEphemeral struct {
	ctx      context.Context
	cancel   context.CancelFunc
	logger   *slog.Logger
	taskName string

	duration      *time.Duration
	checkpointNum uint
	rateLimit     *rate.Limiter
	startTime     time.Time

	sequence       int64
	counter        uint
	checkPoint     uint
	checkEphemeral bool
}

func (m *metadataEphemeral) Next() (*proto.Operation, bool) {
	if m.duration != nil && time.Since(m.startTime) > *m.duration {
		m.logger.Info("Finish the metadata ephemeral generator", "name", m.taskName)
		return nil, false
	}
	if err := m.rateLimit.Wait(m.ctx); err != nil {
		m.logger.Error("Failed to wait for rate limiter", "error", err)
		return nil, false
	}
	if !m.checkEphemeral && !m.maybeResetCounter() {
		operation := &proto.Operation{
			Timestamp: time.Now().UnixNano(),
			Sequence:  m.nextSequence(),
			Operation: &proto.Operation_Put{
				Put: &proto.OperationPut{
					Key:       fmt.Sprintf("/ephemeral/%s/%d", m.taskName, m.counter),
					Ephemeral: true,
				},
			},
		}
		return operation, true
	}
	var operation *proto.Operation
	if !m.checkEphemeral {
		operation = &proto.Operation{
			Timestamp: time.Now().UnixNano(),
			Sequence:  m.nextSequence(),
			Operation: &proto.Operation_SessionRestart{
				SessionRestart: &proto.OperationSessionRestart{},
			},
		}
		m.checkEphemeral = true
		return operation, true
	}
	assertEmpty := true
	operation = &proto.Operation{
		Timestamp: time.Now().UnixNano(),
		Sequence:  m.nextSequence(),
		Assertion: &proto.Assertion{
			EventuallyEmpty: &assertEmpty,
		},
		Operation: &proto.Operation_List{
			List: &proto.OperationList{
				KeyStart: fmt.Sprintf("/ephemeral/%s/", m.taskName),
				KeyEnd:   fmt.Sprintf("/ephemeral/%s//", m.taskName),
			},
		},
	}
	m.checkEphemeral = false
	return operation, true
}

func (m *metadataEphemeral) Name() string {
	return "metadata-ephemeral"
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
	m.checkPoint = rand.UintN(m.checkpointNum)
	return true
}

func NewMetadataEphemeralGenerator(ctx context.Context, tc *config.TestCaseConfig) Generator {
	currentContext, currentContextCanceled := context.WithCancel(ctx)
	logger := slog.With("generator", "metadata-ephemeral", "name", tc.Name)

	checkpointNum := uint(1000)
	if properties := tc.Properties; properties != nil {
		if num, exist := properties[propertiesKeyCheckpointNum]; exist {
			intVal, err := strconv.Atoi(num)
			if err != nil {
				logger.Error("Failed to parse checkpointNum property, using default 1000", "value", num, "error", err)
			} else {
				checkpointNum = uint(intVal)
			}
		}
	}

	logger.Info("Starting metadata ephemeral generator", "checkpointNum", checkpointNum)

	opRate := tc.GetOpRate()
	me := metadataEphemeral{
		logger:        logger,
		ctx:           currentContext,
		cancel:        currentContextCanceled,
		taskName:      tc.Name,
		checkpointNum: checkpointNum,
		duration:      tc.GetDuration(),
		startTime:     time.Now(),
		sequence:      0,
		rateLimit:     rate.NewLimiter(rate.Limit(opRate), opRate),
	}
	me.maybeResetCounter()
	return &me
}
