package worker

import (
	"context"
	"fmt"
	"github.com/0xshin-chan/multichain-sync-btc/common/tasks"
	"github.com/0xshin-chan/multichain-sync-btc/config"
	"github.com/0xshin-chan/multichain-sync-btc/database"
	"github.com/0xshin-chan/multichain-sync-btc/rpcclient/syncclient"
	"github.com/cockroachdb/errors"
	"github.com/ethereum/go-ethereum/log"
	"time"
)

type FallBack struct {
	rpcClient      *syncclient.WalletBtcAccountClient
	db             *database.DB
	resourceCtx    context.Context
	resourceCancel context.CancelFunc
	tasks          tasks.Group
	ticker         *time.Ticker
}

func NewFallBack(cfg *config.Config, db *database.DB, rpcClient *syncclient.WalletBtcAccountClient, shutdown context.CancelCauseFunc) (*FallBack, error) {
	resCtx, resCancel := context.WithCancel(context.Background())
	return &FallBack{
		rpcClient:      rpcClient,
		db:             db,
		resourceCtx:    resCtx,
		resourceCancel: resCancel,
		tasks: tasks.Group{HandleCrit: func(err error) {
			shutdown(fmt.Errorf("critical error in FallBack: %w", err))
		}},
		ticker: time.NewTicker(cfg.ChainNode.WorkerInterval),
	}, nil
}

func (f *FallBack) Close() error {
	var result error
	f.resourceCancel()
	f.ticker.Stop()
	log.Info("stop fallback...")
	if err := f.tasks.Wait(); err != nil {
		result = errors.Join(result, fmt.Errorf("failed to await FallBack %w", err))
		return result
	}
	log.Info("stop fallback success")
	return nil
}

func (f *FallBack) Start() error {
	log.Info("start fallback......")
	f.tasks.Go(func() error {
		for {
			select {
			case <-f.ticker.C:
				log.Info("fallback process start")
			case <-f.resourceCtx.Done():
				log.Info("stop fallback in worker")
				return nil
			}
		}
	})
	return nil
}
