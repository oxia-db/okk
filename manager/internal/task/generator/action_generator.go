package generator

import (
	"math/rand"
	"time"
)

type OpType = uint32

const OpPut OpType = 0
const OpDelete OpType = 1
const OpGet OpType = 2
const OpGetFloor OpType = 3
const OpGetCeiling OpType = 4
const OpGetHigher OpType = 5
const OpGetLower OpType = 6
const OpList OpType = 7
const OpScan OpType = 8
const OpDeleteRange OpType = 9

type ActionGenerator struct {
	r   *rand.Rand
	ops []OpType
}

func NewActionGenerator(weights map[OpType]int) *ActionGenerator {
	ops := make([]OpType, 100)
	totalWeights := 0
	for k, v := range weights {
		for range v {
			ops[totalWeights] = k
			totalWeights++
			if totalWeights > 100 {
				panic("unexpected action generator weights")
			}
		}
	}
	return &ActionGenerator{
		r:   rand.New(rand.NewSource(time.Now().UnixNano())),
		ops: ops,
	}
}

func (g *ActionGenerator) Next() OpType {
	target := g.r.Intn(100)
	return g.ops[target]
}
