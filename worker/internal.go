package worker

import (
	"context"
	"errors"
	"fmt"
	"github.com/0xshin-chan/multichain-sync-btc/common/retry"
	"github.com/0xshin-chan/multichain-sync-btc/common/tasks"
	"github.com/0xshin-chan/multichain-sync-btc/config"
	"github.com/0xshin-chan/multichain-sync-btc/database"
	"github.com/0xshin-chan/multichain-sync-btc/rpcclient/syncclient"
	"github.com/ethereum/go-ethereum/log"
	"math/big"
	"time"
)

type Internal struct {
	rpcClient      *syncclient.WalletBtcAccountClient
	db             *database.DB
	resourceCtx    context.Context
	resourceCancel context.CancelFunc
	tasks          tasks.Group
	ticker         *time.Ticker
}

func NewInternal(cfg *config.Config, db *database.DB, rpcclient *syncclient.WalletBtcAccountClient, shutdown context.CancelCauseFunc) (*Internal, error) {
	resCtx, resCancel := context.WithCancel(context.Background())
	return &Internal{
		rpcClient:      rpcclient,
		db:             db,
		resourceCtx:    resCtx,
		resourceCancel: resCancel,
		tasks: tasks.Group{HandleCrit: func(err error) {
			shutdown(fmt.Errorf("critical error in internals: %w", err))
		}},
		ticker: time.NewTicker(cfg.ChainNode.WorkerInterval),
	}, nil
}

func (i *Internal) Close() error {
	var result error
	i.resourceCancel()
	i.ticker.Stop()
	log.Info("stop internal...")
	if err := i.tasks.Wait(); err != nil {
		result = errors.Join(result, fmt.Errorf("wait internal tasks failed: %w", err))
		return result
	}
	log.Info("stop internal success")
	return nil
}

func (i *Internal) Start() error {
	log.Info("start internal...")
	i.tasks.Go(func() error {
		for {
			select {
			case <-i.ticker.C:
				log.Info("collection and hot to cold")
				businessList, err := i.db.Business.QueryBusinessList()
				if err != nil {
					log.Error("query business list failed", "err", err)
					continue
				}
				for _, business := range businessList {
					unSendInternalTxList, err := i.db.Internals.UnSendInternalsList(business.BusinessUid)
					if err != nil {
						log.Error("query un send internals list failed", "err", err)
						continue
					}

					var balanceList []database.Balances
					for _, unSendInternalTx := range unSendInternalTxList {
						childTxList, err := i.db.ChildTxs.QueryChildTxnByTxId(business.BusinessUid, unSendInternalTx.Guid.String())
						if err != nil {
							log.Error("query child txn fail", "err", err)
							return err
						}
						for _, childTx := range childTxList {
							lockBalance, _ := new(big.Int).SetString(childTx.Amount, 10)
							userBalanceItem := database.Balances{
								Address:     childTx.FromAddress,
								AddressType: 0,
								LockBalance: lockBalance,
							}
							balanceList = append(balanceList, userBalanceItem)
							hotBalanceItem := database.Balances{
								Address:     childTx.ToAddress,
								AddressType: 1,
								LockBalance: lockBalance,
							}
							balanceList = append(balanceList, hotBalanceItem)
						}

						txHash, err := i.rpcClient.SendTx(unSendInternalTx.TxSignHex)
						if err != nil {
							log.Error("send transaction fail", "err", err)
							continue
						} else {
							unSendInternalTx.Hash = txHash
							unSendInternalTx.Status = database.TxStatusSuccess
						}
					}

					retryStrategy := &retry.ExponentialStrategy{Min: 1000, Max: 20_000, MaxJitter: 250}
					if _, err := retry.Do[interface{}](i.resourceCtx, 10, retryStrategy, func() (interface{}, error) {
						if err := i.db.Transaction(func(tx *database.DB) error {
							if len(balanceList) > 0 {
								log.Info("update address balance", "totalTx", len(balanceList))
								if err := tx.Balances.UpdateBalances(business.BusinessUid, balanceList); err != nil {
									log.Error("update address balance fail", "err", err)
									return err
								}
							}

							if len(unSendInternalTxList) > 0 {
								if err := i.db.Internals.UpdateInternalStatus(business.BusinessUid, database.TxStatusSuccess, unSendInternalTxList); err != nil {
									log.Error("update internal status fail", "err", err)
									return err
								}
							}
							return nil
						}); err != nil {
							log.Error("unable to persist batch", "err", err)
							return nil, err
						}
						return nil, nil
					}); err != nil {
						return err
					}
				}
			case <-i.resourceCtx.Done():
				log.Info("stop internal...")
				return nil
			}
		}
	})
	return nil
}
