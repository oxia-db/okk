package generator

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/bits-and-blooms/bitset"
	"github.com/go-logr/logr"
	v1 "github.com/oxia-io/okk/api/v1"
	"github.com/oxia-io/okk/internal/proto"
	"golang.org/x/time/rate"
	"k8s.io/utils/pointer"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var _ Generator = &metadataNotification{}

type metadataNotification struct {
	*logr.Logger
	context.Context
	context.CancelFunc
	taskName string

	duration  *time.Duration
	rateLimit *rate.Limiter
	startTime time.Time

	sequence int64
	rand     *rand.Rand

	keys bitset.BitSet
}

func (m *metadataNotification) Name() string {
	return "metadata-notification"
}

func (m *metadataNotification) nextSequence() int64 {
	nextSequence := m.sequence
	m.sequence = m.sequence + 1
	return nextSequence
}

func (m *metadataNotification) Next() (*proto.Operation, bool) {
	if m.duration != nil && time.Since(m.startTime) > *m.duration {
		m.Info("Finish the metadata notification generator", "name", m.taskName)
		return nil, false
	}
	if err := m.rateLimit.Wait(m.Context); err != nil {
		m.Error(err, "Failed to wait for rate limiter")
		return nil, false
	}

	for {
		factor := m.rand.Intn(100)
		uintFactor := uint(factor)
		switch {
		case factor < 33:
			operation := m.executePut(uintFactor)
			return operation, true
		case factor < 66:
			if !m.keys.Test(uintFactor) {
				// put first if no key
				return m.executePut(uintFactor), true
			}
			m.keys.Clear(uintFactor)
			// delete
			operation := m.executeDelete(uintFactor)
			return operation, true
		default:
			if !m.keys.Test(uintFactor) {
				// put first if no key
				return m.executePut(uintFactor), true
			}
			m.keys.ClearAll()
			// delete range
			operation := m.executeDeleteRange()
			return operation, true
		}
	}
}

func (m *metadataNotification) executeDeleteRange() *proto.Operation {
	operation := &proto.Operation{
		Sequence: m.nextSequence(),
		Precondition: &proto.Precondition{
			WatchNotification: pointer.Bool(true),
		},
		Assertion: &proto.Assertion{
			Notification: &proto.Notification{
				Type:     proto.NotificationType_KEY_RANGE_DELETED,
				KeyStart: pointer.String(fmt.Sprintf("/notification/%s/", m.taskName)),
				KeyEnd:   pointer.String(fmt.Sprintf("/notification/%s//", m.taskName)),
			},
		},
		Operation: &proto.Operation_RangeDelete{
			RangeDelete: &proto.OperationRangeDelete{
				KeyStart: fmt.Sprintf("/notification/%s/", m.taskName),
				KeyEnd:   fmt.Sprintf("/notification/%s//", m.taskName),
			},
		},
	}
	return operation
}

func (m *metadataNotification) executeDelete(uintFactor uint) *proto.Operation {
	operation := &proto.Operation{
		Sequence: m.nextSequence(),
		Precondition: &proto.Precondition{
			WatchNotification: pointer.Bool(true),
		},
		Assertion: &proto.Assertion{
			Notification: &proto.Notification{
				Type: proto.NotificationType_KEY_DELETED,
				Key:  pointer.String(fmt.Sprintf("/notification/%s/%d", m.taskName, uintFactor)),
			},
		},
		Operation: &proto.Operation_Delete{
			Delete: &proto.OperationDelete{
				Key: fmt.Sprintf("/notification/%s/%d", m.taskName, uintFactor),
			},
		},
	}
	return operation
}

func (m *metadataNotification) executePut(uintFactor uint) *proto.Operation {
	var notificationType proto.NotificationType
	if m.keys.Test(uintFactor) {
		notificationType = proto.NotificationType_KEY_MODIFIED
	} else {
		notificationType = proto.NotificationType_KEY_CREATED
	}
	// put
	operation := &proto.Operation{
		Sequence: m.nextSequence(),
		Precondition: &proto.Precondition{
			WatchNotification: pointer.Bool(true),
		},
		Assertion: &proto.Assertion{
			Notification: &proto.Notification{
				Type: notificationType,
				Key:  pointer.String(fmt.Sprintf("/notification/%s/%d", m.taskName, uintFactor)),
			},
		},
		Operation: &proto.Operation_Put{
			Put: &proto.OperationPut{
				Key:   fmt.Sprintf("/notification/%s/%d", m.taskName, uintFactor),
				Value: []byte("notification"),
			},
		},
	}
	return operation
}

func NewMetadataNotificationGenerator(ctx context.Context, tc *v1.TestCase) Generator {
	currentContext, currentContextCanceled := context.WithCancel(ctx)

	namedLogger := logf.FromContext(ctx).WithName("metadata-notification-generator")
	namedLogger.Info("Starting metadata notification generator ", "task-name", tc.Name)

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
