package services

import (
	"context"
	"fmt"
	"github.com/ethereum/go-ethereum/log"
	"github.com/huahaiwudi/multichain-sync-btc/database"
	dal_wallet_go "github.com/huahaiwudi/multichain-sync-btc/protobuf/dal-wallet-go"
	"github.com/huahaiwudi/multichain-sync-btc/rpcclient/syncclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
	"sync/atomic"
)

const MaxRecvMessageSize = 1024 * 1024 * 300

type BusinessMiddleConfig struct {
	GrpcHostName string
	GrpcPort     int
	ChainName    string
	NetWork      string
	CoinName     string
}

type BusinessMiddleWareService struct {
	*BusinessMiddleConfig
	syncClient *syncclient.WalletBtcAccountClient
	db         *database.DB
	stopped    atomic.Bool
}

func NewBusinessMiddleWareService(db *database.DB, config *BusinessMiddleConfig, syncClient *syncclient.WalletBtcAccountClient) (*BusinessMiddleWareService, error) {
	return &BusinessMiddleWareService{
		BusinessMiddleConfig: config,
		syncClient:           syncClient,
		db:                   db,
	}, nil
}

func (s *BusinessMiddleWareService) Stop(ctx context.Context) error {
	s.stopped.Store(true)
	return nil
}

func (s *BusinessMiddleWareService) Stopped() bool {
	return s.stopped.Load()
}

func (s *BusinessMiddleWareService) Start(ctx context.Context) error {
	go func(s *BusinessMiddleWareService) {
		addr := fmt.Sprintf("%s:%d", s.GrpcHostName, s.GrpcPort)
		log.Info("start rpc server", "addr", addr)
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			log.Error("failed to start tcp server", "err", err)
		}
		gs := grpc.NewServer(
			grpc.MaxRecvMsgSize(MaxRecvMessageSize),
			grpc.ChainUnaryInterceptor(nil),
		)
		reflection.Register(gs)

		dal_wallet_go.RegisterBusinessMiddleWireServicesServer(gs, s)

		log.Info("Grpc info", "port", s.GrpcPort, "addr", listener.Addr())
	}(s)
	return nil
}
