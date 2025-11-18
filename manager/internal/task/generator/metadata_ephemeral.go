package generator

import "github.com/oxia-io/okk/internal/proto"

var _ Generator = &metadataEphemeral{}

type metadataEphemeral struct {
}

func (m *metadataEphemeral) Next() (*proto.Operation, bool) {
	//TODO implement me
	panic("implement me")
}
