package generator

import "github.com/oxia-io/okk/internal/proto"

type Generator interface {
	Next() (*proto.Operation, bool)
}
