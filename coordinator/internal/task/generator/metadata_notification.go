package generator

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/bits-and-blooms/bitset"
	"github.com/google/uuid"
	"github.com/oxia-io/okk/coordinator/internal/config"
	"github.com/oxia-io/okk/internal/proto"
	"golang.org/x/time/rate"
)

var _ Generator = &metadataNotification{}

type metadataNotification struct {
	ctx      context.Context
	cancel   context.CancelFunc
	logger   *slog.Logger
	taskName string

	duration  *time.Duration
	rateLimit *rate.Limiter
	startTime time.Time

	sequence uint

	keySpace        uint
	initialized     bool
	actionGenerator *ActionGenerator

	keys bitset.BitSet
}

func (m *metadataNotification) Name() string {
	return "metadata-notification"
}

func (m *metadataNotification) nextSequence() uint {
	nextSequence := m.sequence
	m.sequence = m.sequence + 1
	return nextSequence
}

func (m *metadataNotification) Next() (*proto.Operation, bool) {
	if m.duration != nil && time.Since(m.startTime) > *m.duration {
		m.logger.Info("Finish the metadata notification generator", "name", m.taskName)
		return nil, false
	}
	if err := m.rateLimit.Wait(m.ctx); err != nil {
		m.logger.Error("Failed to wait for rate limiter", "error", err)
		return nil, false
	}

	if !m.initialized {
		return m.processInitStage()
	}

	return m.processDataValidation()
}

func (m *metadataNotification) processDataValidation() (*proto.Operation, bool) {
	action := m.actionGenerator.Next()
	keyIndex := rand.UintN(m.keySpace)
	watchNotification := true
	switch action {
	case OpPut:
		{
			key := fmt.Sprintf("/notification/%s/%020d", m.taskName, keyIndex)

			var notificationType proto.NotificationType
			exist := m.keys.Test(keyIndex)
			if exist {
				notificationType = proto.NotificationType_KEY_MODIFIED
			} else {
				notificationType = proto.NotificationType_KEY_CREATED
			}
			m.keys.Set(keyIndex)

			return &proto.Operation{
				Precondition: &proto.Precondition{
					WatchNotification: &watchNotification,
				},
				Assertion: &proto.Assertion{
					Notification: &proto.Notification{
						Type: notificationType,
						Key:  &key,
					},
				},
				Operation: &proto.Operation_Put{
					Put: &proto.OperationPut{
						Key:   key,
						Value: makeValue(m.taskName, uuid.New().String()),
					},
				},
			}, true
		}
	case OpDelete:
		{
			key := fmt.Sprintf("/notification/%s/%020d", m.taskName, keyIndex)

			operation := proto.Operation{
				Precondition: &proto.Precondition{
					WatchNotification: &watchNotification,
				},
				Operation: &proto.Operation_Delete{
					Delete: &proto.OperationDelete{
						Key: key,
					},
				},
			}
			if m.keys.Test(keyIndex) {
				operation.Assertion = &proto.Assertion{
					Notification: &proto.Notification{
						Type: proto.NotificationType_KEY_DELETED,
						Key:  &key,
					},
				}
			}
			m.keys.Clear(keyIndex)
			return &operation, true
		}
	case OpDeleteRange:
		{
			keyIndexStart := keyIndex
			keyIndexEnd := keyIndex + rand.UintN(100)
			for i := keyIndexStart; i < keyIndexEnd; i++ {
				m.keys.Clear(i)
			}

			keyStart := fmt.Sprintf("/notification/%s/%020d", m.taskName, keyIndexStart)
			keyEnd := fmt.Sprintf("/notification/%s/%020d", m.taskName, keyIndexEnd)

			return &proto.Operation{
				Precondition: &proto.Precondition{
					WatchNotification: &watchNotification,
				},
				Assertion: &proto.Assertion{
					Notification: &proto.Notification{
						Type:     proto.NotificationType_KEY_RANGE_DELETED,
						KeyStart: &keyStart,
						KeyEnd:   &keyEnd,
					},
				},
				Operation: &proto.Operation_DeleteRange{
					DeleteRange: &proto.OperationDeleteRange{
						KeyStart: keyStart,
						KeyEnd:   keyEnd,
					},
				},
			}, true
		}
	}
	return nil, false
}

func (m *metadataNotification) processInitStage() (*proto.Operation, bool) {
	sequence := m.nextSequence()
	data := uuid.New().String()
	m.keys.Set(sequence)

	if sequence >= m.keySpace {
		m.initialized = true
	}
	key := fmt.Sprintf("/notification/%s/%020d", m.taskName, sequence)
	watchNotification := true
	return &proto.Operation{
		Precondition: &proto.Precondition{
			WatchNotification: &watchNotification,
		},
		Operation: &proto.Operation_Put{
			Put: &proto.OperationPut{
				Key:   key,
				Value: makeValue(m.taskName, data),
			},
		},
		Assertion: &proto.Assertion{
			Notification: &proto.Notification{
				Type: proto.NotificationType_KEY_CREATED,
				Key:  &key,
			},
		},
	}, true
}

func NewMetadataNotificationGenerator(ctx context.Context, tc *config.TestCaseConfig) Generator {
	currentContext, currentContextCanceled := context.WithCancel(ctx)
	logger := slog.With("generator", "metadata-notification", "name", tc.Name)
	logger.Info("Starting metadata notification generator")

	keySpace := uint(1000)
	if properties := tc.Properties; properties != nil {
		if num, exist := properties[propertiesKeyKeySpace]; exist {
			intVal, err := strconv.ParseUint(num, 10, 64)
			if intVal > math.MaxUint {
				intVal = math.MaxUint
				logger.Info("keySpace value too large, using max uint", "value", num)
			}
			if err != nil {
				logger.Error("Failed to parse keySpace property, using default 1000", "value", num, "error", err)
			} else {
				keySpace = uint(intVal)
			}
		}
	}

	opRate := tc.GetOpRate()
	actionGenerator := NewActionGenerator(map[OpType]int{
		OpPut:         34,
		OpDelete:      33,
		OpDeleteRange: 33,
	})

	return &metadataNotification{
		logger:          logger,
		ctx:             currentContext,
		cancel:          currentContextCanceled,
		taskName:        tc.Name,
		duration:        tc.GetDuration(),
		startTime:       time.Now(),
		rateLimit:       rate.NewLimiter(rate.Every(1*time.Second), opRate),
		actionGenerator: actionGenerator,
		initialized:     false,
		keySpace:        keySpace,
		keys:            bitset.BitSet{},
	}
}
