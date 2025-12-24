package generator

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	v1 "github.com/oxia-io/okk/api/v1"
	"github.com/oxia-io/okk/internal/proto"
	"golang.org/x/net/context"
	"golang.org/x/time/rate"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const propertiesKeyCheckpointNum = "checkpointNum"

var _ Generator = &metadataEphemeral{}

type metadataEphemeral struct {
	*logr.Logger
	context.Context
	context.CancelFunc
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
		m.Info("Finish the metadata ephemeral generator", "task-name", m.taskName)
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
			Empty: &assertEmpty,
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
	return "metadata-ephemera"
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

func NewMetadataEphemeralGenerator(ctx context.Context, tc *v1.TestCase) Generator {
	currentContext, currentContextCanceled := context.WithCancel(ctx)
	namedLogger := logf.FromContext(ctx).WithName("metadata-ephemeral-generator")

	spec := tc.Spec

	checkpointNum := uint(1000)
	if properties := spec.Properties; properties != nil {
		if num, exist := properties[propertiesKeyCheckpointNum]; exist {
			intVal, err := strconv.Atoi(num)
			if err != nil {
				namedLogger.Error(err, "Failed to convert property '%s' to int, fallback to the default value 1000", "checkpoint-num", num)
			} else {
				checkpointNum = uint(intVal)
			}
		}
	}

	namedLogger.Info("Starting metadata ephemeral generator ", "task-name", tc.Name,
		"checkpoint-num", checkpointNum)

	me := metadataEphemeral{
		Logger:        &namedLogger,
		Context:       currentContext,
		CancelFunc:    currentContextCanceled,
		taskName:      tc.Name,
		checkpointNum: checkpointNum,
		duration:      tc.Duration(),
		startTime:     time.Now(),
		sequence:      0,
		rateLimit:     rate.NewLimiter(rate.Limit(tc.OpRate()), tc.OpRate()),
	}
	me.maybeResetCounter()
	return &me
}
