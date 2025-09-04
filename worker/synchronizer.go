package worker

import (
	"context"
	"errors"
	"github.com/0xshin-chan/multichain-sync-btc/common/clock"
	"github.com/0xshin-chan/multichain-sync-btc/database"
	"github.com/0xshin-chan/multichain-sync-btc/rpcclient/syncclient"
	"github.com/ethereum/go-ethereum/log"
	"math/big"
	"strings"
	"time"
)

type Vin struct {
	Address string
	TxId    string
	Vout    uint8
	Amount  *big.Int
}

type Vout struct {
	Address string
	TxIndex uint8
	Amount  *big.Int
}

type Transaction struct {
	BusinessId  string
	BlockNumber *big.Int
	Hash        string
	TxFee       string
	TxType      string
	VinList     []Vin
	VoutList    []Vout
}

type TransactionsChannel struct {
	BlockHeight  uint64
	ChannelId    string
	Transactions []*Transaction
}

type BaseSynchronizer struct {
	loopInterval     time.Duration
	headerBufferSize uint64
	businessChannels chan map[string]*TransactionsChannel
	rpcClient        *syncclient.WalletBtcAccountClient
	blockBatch       *syncclient.BatchBlock
	database         *database.DB
	headers          []syncclient.BlockHeader
	worker           *clock.LoopFn
}

func (syncer *BaseSynchronizer) Start() error {
	if syncer.worker != nil {
		return errors.New("already started")
	}
	syncer.worker = clock.NewLoopFn(clock.SystemClock, syncer.tick, func() error {
		log.Info("shutting down batch producer")
		close(syncer.businessChannels)
		return nil
	}, syncer.loopInterval)
	return nil
}

func (syncer *BaseSynchronizer) Close() error {
	if syncer.worker == nil {
		return nil
	}
	return syncer.worker.Close()
}

func (syncer *BaseSynchronizer) tick(_ context.Context) {
	if len(syncer.headers) > 0 {
		log.Info("retrying previous batch")
	} else {
		newHeaders, err := syncer.blockBatch.NextHeaders(syncer.headerBufferSize)
		if err != nil {
			log.Error("failed to fetch headers", "err", err)
		} else if len(newHeaders) == 0 {
			log.Warn("no new headers")
		} else {
			syncer.headers = newHeaders
		}
	}
	err := syncer.processBatch(syncer.headers)
	if err == nil {
		syncer.headers = nil
	}
}

func (syncer *BaseSynchronizer) processBatch(headers []syncclient.BlockHeader) error {
	if len(headers) == 0 {
		log.Info("headers is empty, no block waiting to handle")
		return nil
	}

	businessTxChannel := make(map[string]*TransactionsChannel)
	blockHeaders := make([]database.Blocks, len(headers))

	for i := range headers {
		log.Info("Sync block data", "height", headers[i].Number)
		blockHeaders[i] = database.Blocks{
			Hash:      headers[i].Hash,
			PrevHash:  headers[i].PrevHash,
			Number:    headers[i].Number,
			Timestamp: headers[i].Timestamp,
		}

		txList, err := syncer.rpcClient.GetBlockByNumber(headers[i].Number)
		if err != nil {
			return err
		}
		businessList, err := syncer.database.Business.QueryBusinessList()
		if err != nil {
			log.Error("failed to fetch business list", "err", err)
			return err
		}
		for _, business := range businessList {
			var businessTransactions []*Transaction
			for _, tx := range txList {
				txItem := &Transaction{
					BusinessId:  business.BusinessUid,
					BlockNumber: headers[i].Number,
					Hash:        tx.Hash,
					TxFee:       tx.Fee,
					TxType:      "unknown",
				}
				var toAddressList []string
				var voutArray []Vout
				var vinArray []Vin
				for _, vout := range tx.Vout {
					toAddressList = append(toAddressList, vout.Address)
					voutItem := Vout{
						Address: vout.Address,
						TxIndex: uint8(vout.Index),
						Amount:  big.NewInt(int64(vout.Amount)),
					}
					voutArray = append(voutArray, voutItem)
				}
				txItem.VoutList = voutArray

				var (
					existToAddress bool
					toAddressType  uint8
					isDeposit      bool = false
					isWithdraw     bool = false
					isCollection   bool = false
					isToCold       bool = false
					isToHot        bool = false
				)
				for index := range toAddressList {
					existToAddress, toAddressType = syncer.database.Addresses.AddressExist(business.BusinessUid, toAddressList[index])
					hotWalletAddress, errHot := syncer.database.Addresses.QueryHotWalletInfo(business.BusinessUid)
					if errHot != nil {
						log.Error("failed to fetch hot wallet address", "err", errHot)
						return errHot
					}
					coldWalletAddress, errCold := syncer.database.Addresses.QueryColdWalletInfo(business.BusinessUid)
					if errCold != nil {
						log.Error("failed to fetch cold wallet address", "err", errCold)
						return errCold
					}
					for _, txVin := range tx.Vin {
						vinItem := Vin{
							Address: txVin.Address,
							TxId:    tx.Hash,
							Vout:    uint8(txVin.Index),
							Amount:  big.NewInt(int64(txVin.Amount)),
						}
						vinArray = append(vinArray, vinItem)
						addressList := strings.Split(txVin.Address, "|")
						for _, address := range addressList {
							vinAddress, errQuery := syncer.database.Addresses.QueryAddressesByToAddress(business.BusinessUid, address)
							if errQuery != nil {
								log.Error("failed to fetch address", "err", errQuery)
								return errQuery
							}
							if vinAddress == nil && existToAddress && toAddressType == 0 {
								isDeposit = true
							}
							if existToAddress && toAddressType == 1 && vinAddress != nil {
								isCollection = true
							}
							if address == hotWalletAddress.Address && !existToAddress {
								isWithdraw = true
							}
							if existToAddress && toAddressType == 2 && address == hotWalletAddress.Address {
								isToCold = true
							}
							if existToAddress && toAddressType == 1 && address == coldWalletAddress.Address {
								isToHot = true
							}
						}
					}
				}

				if isDeposit {
					txItem.TxType = "deposit"
				}
				if isWithdraw { // 提现
					txItem.TxType = "withdraw"
				}
				if isCollection { // 归集； 1: 代表热钱包地址
					txItem.TxType = "collection"
				}
				if isToCold { // 热转冷；2 是冷钱包地址
					txItem.TxType = "hot2cold"
				}
				if isToHot { // 冷转热；
					txItem.TxType = "cold2hot"
				}

				businessTransactions = append(businessTransactions, txItem)
			}
			if len(businessTransactions) > 0 {
				if businessTxChannel[business.BusinessUid] == nil {
					businessTxChannel[business.BusinessUid] = &TransactionsChannel{
						BlockHeight: headers[i].Number.Uint64(),
						Transactions: businessTransactions,
					}
				} else {
					businessTxChannel[business.BusinessUid].BlockHeight = headers[i].Number.Uint64()
					businessTxChannel[business.BusinessUid].Transactions = append(businessTxChannel[business.BusinessUid].Transactions, businessTransactions...)
				}
			}
		}
	}
	if len(businessTxChannel) >= 0 {
		syncer.businessChannels <- businessTxChannel
	}
	if len(blockHeaders) > 0 {
		log.Info("Store block headers success", "totalBlockHeader", len(blockHeaders))
		if err := syncer.database.Blocks.StoreBlockss(blockHeaders); err != nil {
			return err
		}
	}
	return nil
}
