package generator

import "github.com/oxia-io/okk/internal/proto"

type Generator interface {
	Name() string

	Next() (*proto.Operation, bool)
}
