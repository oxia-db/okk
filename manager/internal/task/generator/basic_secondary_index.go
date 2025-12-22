package generator

import (
	"context"
	"math/rand"
	"time"

	"github.com/bits-and-blooms/bitset"
	"github.com/go-logr/logr"
	v1 "github.com/oxia-io/okk/api/v1"
	"github.com/oxia-io/okk/internal/proto"
	"golang.org/x/time/rate"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var _ Generator = &secondaryIndex{}

type secondaryIndex struct {
	*logr.Logger
	context.Context
	context.CancelFunc
	name string

	duration  *time.Duration
	rateLimit *rate.Limiter
	startTime time.Time
	keySpace  int

	sequence int64
	keys     bitset.BitSet
	rand     *rand.Rand
}

func (s *secondaryIndex) Name() string {
	//TODO implement me
	panic("implement me")
}

func (s *secondaryIndex) Next() (*proto.Operation, bool) {
	//TODO implement me
	panic("implement me")
}

func NewSecondaryIndexGenerator(ctx context.Context, tc *v1.TestCase) {
	namedLogger := logf.FromContext(ctx).WithName("soncdary-index-generator")
	namedLogger.Info("Starting secondary index generator ", "task-name", tc.Name)
}
