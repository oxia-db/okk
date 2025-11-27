package task

import (
	"fmt"
	"sync"
	"time"

	"github.com/oxia-io/okk/internal/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

type ProviderManager struct {
	sync.Mutex
	providers map[string]proto.OkkClient
}

func (pm *ProviderManager) GetProvider(worker string) (proto.OkkClient, error) {
	pm.Lock()
	defer pm.Unlock()
	if p, ok := pm.providers[worker]; ok {
		return p, nil
	}

	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			PermitWithoutStream: true,
			Time:                time.Second * 10,
			Timeout:             time.Second * 5,
		}),
	}
	var provider *grpc.ClientConn
	var err error
	if provider, err = grpc.NewClient(fmt.Sprintf("http://%s", worker), options...); err != nil {
		return nil, err
	}
	client := proto.NewOkkClient(provider)
	pm.providers[worker] = client
	return client, nil
}

func NewProviderManager() *ProviderManager {
	return &ProviderManager{
		providers: make(map[string]proto.OkkClient),
	}
}
