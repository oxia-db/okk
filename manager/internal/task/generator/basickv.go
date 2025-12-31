package generator

import (
	"context"
	"fmt"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/go-logr/logr"
	v1 "github.com/oxia-io/okk/api/v1"
	"github.com/oxia-io/okk/internal/proto"
	"golang.org/x/time/rate"
	"k8s.io/apimachinery/pkg/util/uuid"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var _ Generator = &basicKv{}

type basicKv struct {
	*logr.Logger
	context.Context
	context.CancelFunc
	name string

	duration        *time.Duration
	rateLimit       *rate.Limiter
	startTime       time.Time
	keySpace        int64
	actionGenerator *ActionGenerator

	sequence int64

	initialized bool
	data        map[int64]int32
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

	if !b.initialized {
		return b.processInitStage()
	}

	action := b.actionGenerator.Next()

	key := fmt.Sprintf("%s-%d", b.name, b.sequence)
	switch action {
	case OpPut:
		{
			return &proto.Operation{
				Operation: &proto.Operation_Put{
					Put: &proto.OperationPut{
						Key:   key,
						Value: []byte(key),
					},
				},
			}, true
		}
	case OpDelete:
		{
			return &proto.Operation{
				Operation: &proto.Operation_Delete{
					Delete: &proto.OperationDelete{
						Key: key,
					},
				},
			}, true
		}
	case OpGet:
		{
			return &proto.Operation{
				Operation: &proto.Operation_Get{
					Get: &proto.OperationGet{
						Key:            key,
						ComparisonType: proto.KeyComparisonType_EQUAL,
					},
				},
			}, true
		}
	case OpGetFloor:
		{
			return &proto.Operation{
				Operation: &proto.Operation_Get{
					Get: &proto.OperationGet{
						Key:            key,
						ComparisonType: proto.KeyComparisonType_FLOOR,
					},
				},
			}, true
		}
	case OpGetCeiling:
		{
			return &proto.Operation{
				Operation: &proto.Operation_Get{
					Get: &proto.OperationGet{
						Key:            key,
						ComparisonType: proto.KeyComparisonType_CEILING,
					},
				},
			}, true
		}
	case OpGetHigher:
		{
			return &proto.Operation{
				Operation: &proto.Operation_Get{
					Get: &proto.OperationGet{
						Key:            key,
						ComparisonType: proto.KeyComparisonType_HIGHER,
					},
				},
			}, true
		}
	case OpGetLower:
		{
			return &proto.Operation{
				Operation: &proto.Operation_Get{
					Get: &proto.OperationGet{
						Key:            key,
						ComparisonType: proto.KeyComparisonType_LOWER,
					},
				},
			}, true
		}
	case OpList:
		{
			start := b.sequence
			end := b.sequence + 100
			return &proto.Operation{
				Operation: &proto.Operation_List{
					List: &proto.OperationList{
						KeyStart: fmt.Sprintf("%s-%d", b.name, start),
						KeyEnd:   fmt.Sprintf("%s-%d", b.name, end),
					},
				},
			}, true
		}
	case OpScan:
		{
			return &proto.Operation{
				Operation: &proto.Operation_Scan{
					Scan: &proto.OperationScan{},
				},
			}, true
		}
	case OpDeleteRange:
		{
			start := b.sequence
			end := b.sequence + 100
			return &proto.Operation{
				Operation: &proto.Operation_DeleteRange{
					DeleteRange: &proto.OperationDeleteRange{
						KeyStart: fmt.Sprintf("%s-%d", b.name, start),
						KeyEnd:   fmt.Sprintf("%s-%d", b.name, end),
					},
				},
			}, true
		}
	}

	return nil, false
}

func (b *basicKv) processInitStage() (*proto.Operation, bool) {
	sequence := b.nextSequence()
	key := fmt.Sprintf("%s-%d", b.name, sequence)
	value := []byte(uuid.NewUUID())
	hash := xxhash.Sum64(value)
	// cache data first
	b.data[sequence] = int32(hash)
	if sequence >= b.keySpace {
		b.initialized = true
	}
	return &proto.Operation{
		Operation: &proto.Operation_Put{
			Put: &proto.OperationPut{
				Key:   key,
				Value: value,
			},
		},
	}, true
}

func (b *basicKv) nextSequence() int64 {
	nextSequence := b.sequence
	b.sequence = b.sequence + 1
	return nextSequence
}
func NewBasicKv(ctx context.Context, tc *v1.TestCase) Generator {
	currentContext, currentContextCanceled := context.WithCancel(ctx)
	namedLogger := logf.FromContext(ctx).WithName("basic-kv-generator")
	namedLogger.Info("Starting basic kv generator ", "name", tc.Name)

	actionGenerator := NewActionGenerator(map[OpType]int{
		OpPut:         10,
		OpDelete:      10,
		OpGet:         10,
		OpGetFloor:    10,
		OpGetCeiling:  10,
		OpGetHigher:   10,
		OpGetLower:    10,
		OpList:        10,
		OpScan:        10,
		OpDeleteRange: 10,
	})
	bkv := basicKv{
		Logger:          &namedLogger,
		Context:         currentContext,
		CancelFunc:      currentContextCanceled,
		actionGenerator: actionGenerator,
		name:            tc.Name,
		duration:        tc.Duration(),
		startTime:       time.Now(),
		sequence:        0,
		initialized:     false,
		rateLimit:       rate.NewLimiter(rate.Every(1*time.Second), tc.OpRate()),
	}
	return &bkv
}
