package generator

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/bits-and-blooms/bitset"
	"github.com/go-logr/logr"
	v1 "github.com/oxia-io/okk/api/v1"
	"github.com/oxia-io/okk/internal/proto"
	"golang.org/x/time/rate"
	"k8s.io/apimachinery/pkg/util/uuid"
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
		m.Info("Finish the metadata notification generator", "name", m.taskName)
		return nil, false
	}
	if err := m.rateLimit.Wait(m.Context); err != nil {
		m.Error(err, "Failed to wait for rate limiter")
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
					WatchNotification: pointer.Bool(true),
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
						Value: makeValue(m.taskName, string(uuid.NewUUID())),
					},
				},
			}, true
		}
	case OpDelete:
		{
			key := fmt.Sprintf("/notification/%s/%020d", m.taskName, keyIndex)

			operation := proto.Operation{
				Precondition: &proto.Precondition{
					WatchNotification: pointer.Bool(true),
				},
				Operation: &proto.Operation_Delete{
					Delete: &proto.OperationDelete{
						Key: key,
					},
				},
			}
			if m.keys.Test(keyIndex) {
				// only expect notification when key really deleted
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
					WatchNotification: pointer.Bool(true),
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
	// cache data first
	data := string(uuid.NewUUID())
	m.keys.Set(sequence)

	if sequence >= m.keySpace {
		m.initialized = true
	}
	key := fmt.Sprintf("/notification/%s/%020d", m.taskName, sequence)
	return &proto.Operation{
		Precondition: &proto.Precondition{
			WatchNotification: pointer.Bool(true),
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

func NewMetadataNotificationGenerator(ctx context.Context, tc *v1.TestCase) Generator {
	currentContext, currentContextCanceled := context.WithCancel(ctx)

	namedLogger := logf.FromContext(ctx).WithName("metadata-notification-generator")
	namedLogger.Info("Starting metadata notification generator ", "task-name", tc.Name)

	keySpace := uint(1000)
	if properties := tc.Spec.Properties; properties != nil {
		if num, exist := properties[propertiesKeyKeySpace]; exist {
			intVal, err := strconv.ParseUint(num, 10, 64)
			if intVal > math.MaxUint {
				intVal = math.MaxUint
				namedLogger.Info("Failed to convert property '%s' to uint, fallback to the the maximum value of uint", "key-space", num, "parsed-key-space", intVal)
			}
			if err != nil {
				namedLogger.Error(err, "Failed to convert property '%s' to int, fallback to the default value 1000", "key-space", num)
			} else {
				keySpace = uint(intVal)
			}
		}
	}

	actionGenerator := NewActionGenerator(map[OpType]int{
		OpPut:         34,
		OpDelete:      33,
		OpDeleteRange: 33,
	})

	return &metadataNotification{
		Logger:          &namedLogger,
		Context:         currentContext,
		CancelFunc:      currentContextCanceled,
		taskName:        tc.Name,
		duration:        tc.Duration(),
		startTime:       time.Now(),
		rateLimit:       rate.NewLimiter(rate.Every(1*time.Second), tc.OpRate()),
		actionGenerator: actionGenerator,
		initialized:     false,
		keySpace:        keySpace,
		keys:            bitset.BitSet{},
	}
}
