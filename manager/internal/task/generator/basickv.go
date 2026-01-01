package generator

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	v1 "github.com/oxia-io/okk/api/v1"
	"github.com/oxia-io/okk/internal/proto"
	"golang.org/x/time/rate"
	"k8s.io/apimachinery/pkg/util/uuid"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const propertiesKeyKeySpace = "keySpace"

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
	data        *DataTree
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

	return b.processDataValidation()
}

func (b *basicKv) processDataValidation() (*proto.Operation, bool) {
	action := b.actionGenerator.Next()
	keyIndex := rand.Int64N(b.keySpace)
	switch action {
	case OpPut:
		{
			uid := string(uuid.NewUUID())
			b.data.Put(makeFormatInt64(keyIndex), uid)
			return &proto.Operation{
				Operation: &proto.Operation_Put{
					Put: &proto.OperationPut{
						Key:   makeKey(b.name, keyIndex),
						Value: makeValue(b.name, uid),
					},
				},
			}, true
		}
	case OpDelete:
		{
			b.data.Delete(makeFormatInt64(keyIndex))
			return &proto.Operation{
				Operation: &proto.Operation_Delete{
					Delete: &proto.OperationDelete{
						Key: makeKey(b.name, keyIndex),
					},
				},
			}, true
		}
	case OpDeleteRange:
		{
			keyStart := keyIndex
			keyEnd := keyIndex + rand.Int64N(100)

			b.data.DeleteRange(makeFormatInt64(keyStart), makeFormatInt64(keyEnd))
			return &proto.Operation{
				Operation: &proto.Operation_DeleteRange{
					DeleteRange: &proto.OperationDeleteRange{
						KeyStart: makeKey(b.name, keyStart),
						KeyEnd:   makeKey(b.name, keyEnd),
					},
				},
			}, true
		}
	case OpGet:
		{
			value, found := b.data.Get(makeFormatInt64(keyIndex))
			key := makeKey(b.name, keyIndex)

			emptyRecord := !found
			assertion := &proto.Assertion{
				EmptyRecords: &emptyRecord,
			}
			if found {
				assertion.Records = []*proto.Record{
					{
						Key:   key,
						Value: makeValue(b.name, value),
					},
				}
			}
			return &proto.Operation{
				Assertion: assertion,
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
			entry, found := b.data.GetFloor(makeFormatInt64(keyIndex))
			key := makeKey(b.name, keyIndex)

			emptyRecord := !found
			assertion := &proto.Assertion{
				EmptyRecords: &emptyRecord,
			}
			if found {
				assertion.Records = []*proto.Record{
					{
						Key:   makeKeyWithFormattedIndex(b.name, entry.Key),
						Value: makeValue(b.name, entry.Value),
					},
				}
			}
			return &proto.Operation{
				Assertion: assertion,
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
			entry, found := b.data.GetCeiling(makeFormatInt64(keyIndex))
			key := makeKey(b.name, keyIndex)

			emptyRecord := !found
			assertion := &proto.Assertion{
				EmptyRecords: &emptyRecord,
			}
			if found {
				assertion.Records = []*proto.Record{
					{
						Key:   makeKeyWithFormattedIndex(b.name, entry.Key),
						Value: makeValue(b.name, entry.Value),
					},
				}
			}
			return &proto.Operation{
				Assertion: assertion,
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
			entry, found := b.data.GetHigher(makeFormatInt64(keyIndex))

			emptyRecord := !found
			assertion := &proto.Assertion{
				EmptyRecords: &emptyRecord,
			}
			if found {
				assertion.Records = []*proto.Record{
					{
						Key:   makeKeyWithFormattedIndex(b.name, entry.Key),
						Value: makeValue(b.name, entry.Value),
					},
				}
			}
			return &proto.Operation{
				Assertion: assertion,
				Operation: &proto.Operation_Get{
					Get: &proto.OperationGet{
						Key:            makeKey(b.name, keyIndex),
						ComparisonType: proto.KeyComparisonType_HIGHER,
					},
				},
			}, true
		}
	case OpGetLower:
		{
			entry, found := b.data.GetLower(makeFormatInt64(keyIndex))
			emptyRecord := !found
			assertion := &proto.Assertion{
				EmptyRecords: &emptyRecord,
			}
			if found {
				assertion.Records = []*proto.Record{
					{
						Key:   makeKeyWithFormattedIndex(b.name, entry.Key),
						Value: makeValue(b.name, entry.Value),
					},
				}
			}
			return &proto.Operation{
				Assertion: assertion,
				Operation: &proto.Operation_Get{
					Get: &proto.OperationGet{
						Key:            makeKey(b.name, keyIndex),
						ComparisonType: proto.KeyComparisonType_LOWER,
					},
				},
			}, true
		}
	case OpList:
		{
			keyStart := keyIndex
			keyEnd := keyIndex + rand.Int64N(100)

			keys := b.data.List(makeFormatInt64(keyStart), makeFormatInt64(keyEnd))
			records := make([]*proto.Record, 0)
			for _, key := range keys {
				records = append(records, &proto.Record{
					Key: makeKeyWithFormattedIndex(b.name, key),
				})
			}

			return &proto.Operation{
				Assertion: &proto.Assertion{
					Records: records,
				},
				Operation: &proto.Operation_List{
					List: &proto.OperationList{
						KeyStart: makeKey(b.name, keyStart),
						KeyEnd:   makeKey(b.name, keyEnd),
					},
				},
			}, true
		}
	case OpScan:
		{
			keyStart := keyIndex
			keyEnd := keyIndex + rand.Int64N(100)

			entries := b.data.RangeScan(makeFormatInt64(keyStart), makeFormatInt64(keyEnd))
			records := make([]*proto.Record, 0)
			for _, entry := range entries {
				records = append(records, &proto.Record{
					Key:   makeKeyWithFormattedIndex(b.name, entry.Key),
					Value: makeValue(b.name, entry.Value),
				})
			}

			return &proto.Operation{
				Assertion: &proto.Assertion{
					Records: records,
				},
				Operation: &proto.Operation_Scan{
					Scan: &proto.OperationScan{
						KeyStart: makeKey(b.name, keyStart),
						KeyEnd:   makeKey(b.name, keyEnd),
					},
				},
			}, true
		}
	}
	return nil, false
}

func (b *basicKv) processInitStage() (*proto.Operation, bool) {
	sequence := b.nextSequence()
	// cache data first
	data := string(uuid.NewUUID())
	b.data.Put(makeFormatInt64(sequence), data)

	if sequence >= b.keySpace {
		b.initialized = true
	}
	return &proto.Operation{
		Operation: &proto.Operation_Put{
			Put: &proto.OperationPut{
				Key:   makeKey(b.name, sequence),
				Value: makeValue(b.name, data),
			},
		},
	}, true
}
func makeFormatInt64(value int64) string {
	return fmt.Sprintf("%020d", value)
}
func makeKeyWithFormattedIndex(name string, index string) string {
	return fmt.Sprintf("%s-%s", name, index)
}
func makeKey(name string, index int64) string {
	return fmt.Sprintf("%s-%020d", name, index)
}

func makeValue(name string, uid string) []byte {
	return []byte(fmt.Sprintf("%s-%s", name, uid))
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

	keySpace := int64(1000)
	if properties := tc.Spec.Properties; properties != nil {
		if num, exist := properties[propertiesKeyKeySpace]; exist {
			intVal, err := strconv.ParseInt(num, 10, 64)
			if err != nil {
				namedLogger.Error(err, "Failed to convert property '%s' to int, fallback to the default value 1000", "key-space", num)
			} else {
				keySpace = intVal
			}
		}
	}

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
		keySpace:        keySpace,
		rateLimit:       rate.NewLimiter(rate.Limit(tc.OpRate()), tc.OpRate()),
		data:            NewDataTree(),
	}
	return &bkv
}
