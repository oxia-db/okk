package generator

import "github.com/oxia-io/okk/coordinator/internal/proto"

type Generator interface {
	Name() string

	Next() (*proto.Operation, bool)
}
