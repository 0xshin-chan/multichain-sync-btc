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
	"github.com/google/uuid"
	"math/big"
	"strings"
	"time"
)

type Deposit struct {
	BaseSynchronizer
	confirms       uint8
	latestHeader   syncclient.BlockHeader
	resourceCtx    context.Context
	resourceCancel context.CancelFunc
	tasks          tasks.Group
}

func NewDeposit(cfg config.Config, db *database.DB, rpcClient *syncclient.WalletBtcAccountClient, shutdown context.CancelCauseFunc) (*Deposit, error) {
	dbLatestBlockHeader, err := db.Blocks.LatestBlocks()
	if err != nil {
		log.Error("get latest block from database fail")
		return nil, err
	}
	var fromHeader *syncclient.BlockHeader

	if dbLatestBlockHeader != nil {
		log.Info("sync block", "number", dbLatestBlockHeader.Number)
		fromHeader = dbLatestBlockHeader
	} else if cfg.ChainNode.StartingHeight > 0 {
		chainLatestBlockHeader, er := rpcClient.GetBlockHeader(big.NewInt(int64(cfg.ChainNode.StartingHeight)))
		if er != nil {
			log.Error("get latest block from database fail")
			return nil, er
		}
		fromHeader = chainLatestBlockHeader
	} else {
		chainLatestBlockHeader, err := rpcClient.GetBlockHeader(nil)
		if err != nil {
			log.Error("get latest block from database fail")
			return nil, err
		}
		fromHeader = chainLatestBlockHeader
	}

	businessTxChannel := make(chan map[string]*TransactionsChannel)
	baseSyncer := BaseSynchronizer{
		loopInterval:     cfg.ChainNode.SynchronizerInterval,
		headerBufferSize: cfg.ChainNode.BlocksStep,
		businessChannels: businessTxChannel,
		rpcClient:        rpcClient,
		blockBatch:       syncclient.NewBatchBlock(rpcClient, fromHeader, big.NewInt(int64(cfg.ChainNode.Confirmations))),
		database:         db,
	}

	resCtx, resCancel := context.WithCancel(context.Background())

	return &Deposit{
		BaseSynchronizer: baseSyncer,
		confirms:         uint8(cfg.ChainNode.Confirmations),
		resourceCtx:      resCtx,
		resourceCancel:   resCancel,
		tasks: tasks.Group{HandleCrit: func(err error) {
			shutdown(fmt.Errorf("critical error in deposit: %w", err))
		}},
	}, nil
}

func (d *Deposit) Close() error {
	var result error
	if err := d.BaseSynchronizer.Close(); err != nil {
		result = errors.Join(result, fmt.Errorf("failed to close deposit base synchronizer: %w", err))
	}
	d.resourceCancel()
	if err := d.tasks.Wait(); err != nil {
		result = errors.Join(result, fmt.Errorf("failed to close deposit tasks: %w", err))
	}
	return result
}

func (d *Deposit) Start() error {
	log.Info("starting deposit...")
	if err := d.BaseSynchronizer.Start(); err != nil {
		return fmt.Errorf("failed to start deposit synchronizer: %w", err)
	}
	d.tasks.Go(func() error {
		log.Info("handle deposit task start")
		for batch := range d.businessChannels {
			log.Info("deposit business channel", "batch length", len(batch))
			if err := d.handleBatch(batch); err != nil {
				log.Error("handle batch fail", "err", err)
				return fmt.Errorf("failed to handle batch, stopping L2 Synchronizer: %w", err)
			}
		}
		return nil
	})
	return nil
}

func (d *Deposit) handleBatch(batch map[string]*TransactionsChannel) error {
	businessList, err := d.database.Business.QueryBusinessList()
	if err != nil {
		log.Error("query business list", "err", err)
		return err
	}
	for _, business := range businessList {
		_, exists := batch[business.BusinessUid]
		if !exists {
			continue
		}

		var (
			transactionFlowList         []database.Transactions
			transactionChildTxFlowList  []database.ChildTxs
			depositList                 []database.Deposits
			withdrawList                []database.Withdraws
			internalList                []database.Internals
			depositListChildTxFlowList  []database.ChildTxs
			withdrawListChildTxFlowList []database.ChildTxs
			internalListChildTxFlowList []database.ChildTxs
			vins                        []database.Vins
			vouts                       []database.Vouts
			balances                    []database.TokenBalance
		)

		log.Info(
			"handle business flow", "businessId", business.BusinessUid,
			"chanLatestBlock", batch[business.BusinessUid].BlockHeight,
			"txn", len(batch[business.BusinessUid].Transactions),
		)
		var pvList []*PrepareVoutList
		for _, tx := range batch[business.BusinessUid].Transactions {
			txItem, err := d.rpcClient.GetTransactionByHash(tx.Hash)
			if err != nil {
				log.Error("get transaction by hash", "err", err)
				return err
			}

			log.Info("get transaction success", "txHash", txItem.Hash)
			txFlow, txFlowChildTxs, err := d.HandleTransaction(tx)
			if err != nil {
				log.Error("handle transaction", "err", err)
				return err
			}
			transactionFlowList = append(transactionFlowList, txFlow)
			txFlowChildTxs = append(txFlowChildTxs, txFlowChildTxs...)
			vintListPre, vinBalances, err := d.HandleVin(tx)
			if err != nil {
				log.Error("handle vout fail", "err", err)
			}
			vins = append(vins, vintListPre...)
			balances = append(balances, vinBalances...)

			voutListPre, voutBalances, err := d.HandleVout(tx, business.BusinessUid)
			if err != nil {
				log.Error("handle vout fail", "err", err)
			}
			balances = append(balances, voutBalances...)

			pvList = append(pvList, voutListPre)
			vlist := voutListPre.VoutList
			vouts = append(vouts, vlist...)

			switch tx.TxType {
			case "deposit":
				depositItem, depositChildTxn, _ := d.HandleDeposit(tx)
				depositList = append(depositList, depositItem)
				depositListChildTxFlowList = append(depositListChildTxFlowList, depositChildTxn...)
				break
			case "withdraw":
				withdrawItem, withdrawChildTxn, _ := d.HandleWithdraw(tx)
				withdrawListChildTxFlowList = append(withdrawListChildTxFlowList, withdrawChildTxn...)
				withdrawList = append(withdrawList, withdrawItem)
				break
			case "collection", "hot2cold", "cold2hot":
				internelItem, internalChildTxn, _ := d.HandleInternalTx(tx)
				internalListChildTxFlowList = append(internalListChildTxFlowList, internalChildTxn...)
				internalList = append(internalList, internelItem)
				break
			default:
				break
			}
		}
		retryStrategy := &retry.ExponentialStrategy{Min: 1000, Max: 20_000, MaxJitter: 250}
		if _, err := retry.Do[interface{}](d.resourceCtx, 10, retryStrategy, func() (interface{}, error) {
			if err := d.database.Transaction(func(tx *database.DB) error {
				if len(depositList) > 0 {
					log.Info("Store deposit transaction success", "totalTx", len(depositList))
					if err := tx.Deposits.StoreDeposits(business.BusinessUid, depositList); err != nil {
						return err
					}

					if err := tx.ChildTxs.StoreChildTxs(business.BusinessUid, depositListChildTxFlowList); err != nil {
						return err
					}
				}
				if err := tx.Deposits.UpdateDepositsComfirms(business.BusinessUid, batch[business.BusinessUid].BlockHeight, uint64(d.confirms)); err != nil {
					log.Info("Handle confims fail", "totalTx", "err", err)
					return err
				}
				if len(balances) > 0 {
					log.Info("Handle balances success", "totalTx", len(balances))
					if err := tx.Balances.UpdateOrCreate(business.BusinessUid, balances); err != nil {
						return err
					}
				}
				if len(withdrawList) > 0 {
					if err := tx.Withdraws.UpdateWithdrawStatus(business.BusinessUid, database.TxStatusWithdrawed, withdrawList); err != nil {
						return err
					}
					if err := tx.ChildTxs.StoreChildTxs(business.BusinessUid, withdrawListChildTxFlowList); err != nil {
						return err
					}
				}
				if len(internalList) > 0 {
					if err := tx.Internals.UpdateInternalStatus(business.BusinessUid, database.TxStatusSuccess, internalList); err != nil {
						return err
					}
					if err := tx.ChildTxs.StoreChildTxs(business.BusinessUid, internalListChildTxFlowList); err != nil {
						return err
					}
				}
				if len(transactionFlowList) > 0 {
					if err := tx.Transactions.StoreTransactions(business.BusinessUid, transactionFlowList); err != nil {
						return err
					}
					if err := tx.ChildTxs.StoreChildTxs(business.BusinessUid, transactionChildTxFlowList); err != nil {
						return err
					}
				}
				if len(vins) > 0 {
					if err := tx.Vins.StoreVins(business.BusinessUid, vins); err != nil {
						return err
					}
				}
				if len(vouts) > 0 {
					if err := tx.Vouts.StoreVouts(business.BusinessUid, vouts); err != nil {
						return err
					}
				}

				if len(pvList) > 0 {
					for _, pvItem := range pvList {
						for _, voutItmepv := range pvItem.VoutList {
							if err := tx.Vins.UpdateVinsTx(business.BusinessUid, pvItem.TxId, voutItmepv.Address, true, pvItem.TxId, pvItem.BlockNumber); err != nil {
								return err
							}
						}
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
	return nil
}

func (d *Deposit) HandleTransaction(tx *Transaction) (database.Transactions, []database.ChildTxs, error) {
	txFee, _ := new(big.Int).SetString(tx.TxFee, 10)
	var childTxn []database.ChildTxs
	if tx.TxType == "deposit" {
		for _, voutItem := range tx.VoutList {
			childTx := database.ChildTxs{
				GUID:        uuid.New(),
				Hash:        tx.Hash,
				TxIndex:     big.NewInt(int64(voutItem.TxIndex)),
				TxType:      "deposit",
				FromAddress: "",
				ToAddress:   voutItem.Address,
				Amount:      voutItem.Amount.String(),
				Timestamp:   uint64(time.Now().Unix()),
			}
			childTxn = append(childTxn, childTx)
		}
	}
	if tx.TxType == "withdraw" {
		for _, vinItem := range tx.VinList {
			childTx := database.ChildTxs{
				GUID:        uuid.New(),
				Hash:        tx.Hash,
				TxIndex:     big.NewInt(int64(vinItem.Vout)),
				TxType:      "withdraw",
				FromAddress: "",
				ToAddress:   vinItem.Address,
				Amount:      vinItem.Amount.String(),
				Timestamp:   uint64(time.Now().Unix()),
			}
			childTxn = append(childTxn, childTx)
		}
	}
	transactionTx := database.Transactions{
		GUID:        uuid.New(),
		BlockHash:   "",
		BlockNumber: tx.BlockNumber,
		Hash:        tx.Hash,
		Fee:         txFee,
		Status:      database.TxStatusSuccess,
		TxType:      tx.TxType,
		Timestamp:   uint64(time.Now().Unix()),
	}
	return transactionTx, childTxn, nil
}

func (d *Deposit) HandleVin(tx *Transaction) ([]database.Vins, []database.TokenBalance, error) {
	var vinList []database.Vins
	var balanceList []database.TokenBalance
	for _, vout := range tx.VoutList {
		vinTx := database.Vins{
			GUID:             uuid.New(),
			Address:          vout.Address,
			TxId:             tx.Hash,
			Vout:             vout.TxIndex,
			Script:           "",
			Witness:          "",
			Amount:           vout.Amount,
			SpendTxHash:      "",
			SpendBlockHeight: big.NewInt(0),
			IsSpend:          false,
			Timestamp:        uint64(time.Now().Unix()),
		}

		if tx.TxType == "deposit" || tx.TxType == "collection" || tx.TxType == "hot2cold" || tx.TxType == "cold2hot" {
			balanceItem := database.TokenBalance{
				FromAddress:  "",
				ToAddress:    vout.Address,
				TokenAddress: "",
				Balance:      vout.Amount,
				TxType:       tx.TxType,
			}
			balanceList = append(balanceList, balanceItem)
		}
		vinList = append(vinList, vinTx)
	}
	return vinList, balanceList, nil
}

func (d *Deposit) HandleVout(tx *Transaction, business string) (*PrepareVoutList, []database.TokenBalance, error) {
	var voutList []database.Vouts
	var balanceList []database.TokenBalance
	for _, vin := range tx.VinList {
		vout := database.Vouts{
			GUID:      uuid.New(),
			Address:   vin.Address,
			N:         vin.Vout,
			Amount:    vin.Amount,
			Timestamp: uint64(time.Now().Unix()),
		}
		voutList = append(voutList, vout)
		if tx.TxType == "withdraw" || tx.TxType == "collection" || tx.TxType == "hot2cold" || tx.TxType == "cold2hot" {
			vinAddressess := strings.Split(vin.Address, "|")
			for _, addr := range vinAddressess {
				vinDetail, err := d.database.Vins.QueryVinByTxId(business, addr, tx.Hash)
				if err != nil {
					log.Error("query vins fail", "err", err)
				}
				balanceItem := database.TokenBalance{
					FromAddress:  addr,
					ToAddress:    "",
					TokenAddress: "",
					Balance:      vinDetail.Amount,
					TxType:       tx.TxType,
				}
				balanceList = append(balanceList, balanceItem)
			}
		}
	}
	return &PrepareVoutList{
		TxId:        tx.Hash,
		BlockNumber: tx.BlockNumber,
		VoutList:    voutList,
	}, balanceList, nil
}

func (deposit *Deposit) HandleDeposit(tx *Transaction) (database.Deposits, []database.ChildTxs, error) {
	var depositChildTx []database.ChildTxs
	for _, voutItem := range tx.VoutList {
		dChildTx := database.ChildTxs{
			GUID:        uuid.New(),
			Hash:        tx.Hash,
			TxIndex:     big.NewInt(int64(voutItem.TxIndex)),
			TxType:      "deposit",
			FromAddress: "",
			ToAddress:   voutItem.Address,
			Amount:      voutItem.Amount.String(),
			Timestamp:   uint64(time.Now().Unix()),
		}
		depositChildTx = append(depositChildTx, dChildTx)
	}
	txFee, _ := new(big.Int).SetString(tx.TxFee, 10)
	depositTx := database.Deposits{
		GUID:        uuid.New(),
		BlockHash:   "",
		BlockNumber: tx.BlockNumber,
		Hash:        tx.Hash,
		Fee:         txFee,
		Status:      database.TxStatusUnSafe,
		Timestamp:   uint64(time.Now().Unix()),
	}
	return depositTx, depositChildTx, nil
}

func (deposit *Deposit) HandleWithdraw(tx *Transaction) (database.Withdraws, []database.ChildTxs, error) {
	txFee, _ := new(big.Int).SetString(tx.TxFee, 10)
	var withdrawChildTx []database.ChildTxs
	for _, vinItem := range tx.VinList {
		wChildTx := database.ChildTxs{
			GUID:        uuid.New(),
			Hash:        tx.Hash,
			TxIndex:     big.NewInt(int64(vinItem.Vout)),
			TxType:      "withdraw",
			FromAddress: vinItem.Address,
			ToAddress:   "",
			Amount:      vinItem.Amount.String(),
			Timestamp:   uint64(time.Now().Unix()),
		}
		withdrawChildTx = append(withdrawChildTx, wChildTx)
	}
	withdrawTx := database.Withdraws{
		Guid:        uuid.New(),
		BlockHash:   "",
		BlockNumber: tx.BlockNumber,
		Hash:        tx.Hash,
		Fee:         txFee,
		Status:      database.TxStatusWithdrawed,
		Timestamp:   uint64(time.Now().Unix()),
	}
	return withdrawTx, withdrawChildTx, nil
}

func (deposit *Deposit) HandleInternalTx(tx *Transaction) (database.Internals, []database.ChildTxs, error) {
	txFee, _ := new(big.Int).SetString(tx.TxFee, 10)
	var childTxn []database.ChildTxs
	if tx.TxType == "collection" { // 用户地址到热钱包地址, 用户地址在 transactions vin, 对热钱包地址 vout
		for _, voutItem := range tx.VoutList {
			childTx := database.ChildTxs{
				GUID:        uuid.New(),
				Hash:        tx.Hash,
				TxIndex:     big.NewInt(int64(voutItem.TxIndex)),
				TxType:      "hot_input",
				FromAddress: "",
				ToAddress:   voutItem.Address,
				Amount:      voutItem.Amount.String(),
				Timestamp:   uint64(time.Now().Unix()),
			}
			childTxn = append(childTxn, childTx)
		}
		for _, vinItem := range tx.VinList {
			childTx := database.ChildTxs{
				GUID:        uuid.New(),
				Hash:        tx.Hash,
				TxIndex:     big.NewInt(int64(vinItem.Vout)),
				TxType:      "user_output",
				FromAddress: "",
				ToAddress:   vinItem.Address,
				Amount:      vinItem.Amount.String(),
				Timestamp:   uint64(time.Now().Unix()),
			}
			childTxn = append(childTxn, childTx)
		}
	}
	if tx.TxType == "hot2cold" { // 热转冷
		for _, voutItem := range tx.VoutList {
			childTx := database.ChildTxs{
				GUID:        uuid.New(),
				Hash:        tx.Hash,
				TxIndex:     big.NewInt(int64(voutItem.TxIndex)),
				TxType:      "cold_input",
				FromAddress: "",
				ToAddress:   voutItem.Address,
				Amount:      voutItem.Amount.String(),
				Timestamp:   uint64(time.Now().Unix()),
			}
			childTxn = append(childTxn, childTx)
		}
		for _, vinItem := range tx.VinList {
			childTx := database.ChildTxs{
				GUID:        uuid.New(),
				Hash:        tx.Hash,
				TxIndex:     big.NewInt(int64(vinItem.Vout)),
				TxType:      "hot_output",
				FromAddress: "",
				ToAddress:   vinItem.Address,
				Amount:      vinItem.Amount.String(),
				Timestamp:   uint64(time.Now().Unix()),
			}
			childTxn = append(childTxn, childTx)
		}
	}
	if tx.TxType == "cold2hot" { // 冷转热  to
		for _, voutItem := range tx.VoutList {
			childTx := database.ChildTxs{
				GUID:        uuid.New(),
				Hash:        tx.Hash,
				TxIndex:     big.NewInt(int64(voutItem.TxIndex)),
				TxType:      "hot_input",
				FromAddress: "",
				ToAddress:   voutItem.Address,
				Amount:      voutItem.Amount.String(),
				Timestamp:   uint64(time.Now().Unix()),
			}
			childTxn = append(childTxn, childTx)
		}

		for _, vinItem := range tx.VinList {
			childTx := database.ChildTxs{
				GUID:        uuid.New(),
				Hash:        tx.Hash,
				TxIndex:     big.NewInt(int64(vinItem.Vout)),
				TxType:      "cold_output",
				FromAddress: "",
				ToAddress:   vinItem.Address,
				Amount:      vinItem.Amount.String(),
				Timestamp:   uint64(time.Now().Unix()),
			}
			childTxn = append(childTxn, childTx)
		}
	}
	internalTx := database.Internals{
		Guid:        uuid.New(),
		BlockHash:   "",
		BlockNumber: tx.BlockNumber,
		Hash:        tx.Hash,
		Status:      database.TxStatusSuccess,
		Fee:         txFee,
		Timestamp:   uint64(time.Now().Unix()),
	}
	return internalTx, childTxn, nil
}

type PrepareVoutList struct {
	TxId        string
	BlockNumber *big.Int
	VoutList    []database.Vouts
}
