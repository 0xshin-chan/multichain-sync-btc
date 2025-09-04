package services

import (
	"context"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/google/uuid"

	"github.com/0xshin-chan/multichain-sync-btc/database"
	"github.com/0xshin-chan/multichain-sync-btc/database/dynamic"
	dal_wallet_go "github.com/0xshin-chan/multichain-sync-btc/protobuf/dal-wallet-go"
	"github.com/0xshin-chan/multichain-sync-btc/rpcclient/syncclient/utxo"
)

const (
	ConsumerToken = "slim"
)

func (s *BusinessMiddleWareService) BusinessRegister(ctx context.Context, request *dal_wallet_go.BusinessRegisterRequest) (*dal_wallet_go.BusinessRegisterResponse, error) {
	if request.RequestId == "" || request.NotifyUrl == "" {
		return &dal_wallet_go.BusinessRegisterResponse{
			Code: dal_wallet_go.ReturnCode_ERROR,
			Msg:  "invalid params",
		}, nil
	}
	business := &database.Business{
		GUID:        uuid.New(),
		BusinessUid: request.RequestId,
		NotifyUrl:   request.NotifyUrl,
		Timestamp:   uint64(time.Now().Unix()),
	}
	err := s.db.Business.StoreBusiness(business)
	if err != nil {
		log.Error("store business fail", "err", err)
		return &dal_wallet_go.BusinessRegisterResponse{
			Code: dal_wallet_go.ReturnCode_ERROR,
			Msg:  "store db fail",
		}, nil
	}
	dynamic.CreateTableFromTemplate(request.RequestId, s.db)
	return &dal_wallet_go.BusinessRegisterResponse{
		Code: dal_wallet_go.ReturnCode_SUCCESS,
		Msg:  "config business success",
	}, nil
}

func (s *BusinessMiddleWareService) ExportAddressesByPublicKeys(ctx context.Context, request *dal_wallet_go.ExportAddressesRequest) (*dal_wallet_go.ExportAddressesResponse, error) {
	var (
		retAddresses []*dal_wallet_go.Address
		dbAddresses  []database.Addresses
		balances     []database.Balances
	)
	for _, value := range request.PublicKeys {
		address := s.syncClient.ExportAddressByPubKey(value.Format, value.PublicKey)
		item := &dal_wallet_go.Address{
			Type:    value.Type,
			Address: address,
		}
		dbAddress := database.Addresses{
			GUID:        uuid.New(),
			Address:     address,
			AddressType: uint8(value.Type),
			PublicKey:   value.PublicKey,
			Timestamp:   uint64(time.Now().Unix()),
		}
		dbAddresses = append(dbAddresses, dbAddress)

		balanceItem := database.Balances{
			GUID:        uuid.New(),
			Address:     address,
			AddressType: uint8(value.Type),
			Balance:     big.NewInt(0),
			LockBalance: big.NewInt(0),
			Timestamp:   uint64(time.Now().Unix()),
		}
		balances = append(balances, balanceItem)

		retAddresses = append(retAddresses, item)
	}
	err := s.db.Addresses.StoreAddresses(request.RequestId, dbAddresses)
	if err != nil {
		return &dal_wallet_go.ExportAddressesResponse{
			Code: dal_wallet_go.ReturnCode_ERROR,
			Msg:  "store address to db fail",
		}, nil
	}
	err = s.db.Balances.StoreBalances(request.RequestId, balances)
	if err != nil {
		return &dal_wallet_go.ExportAddressesResponse{
			Code: dal_wallet_go.ReturnCode_ERROR,
			Msg:  "store balance to db fail",
		}, nil
	}
	return &dal_wallet_go.ExportAddressesResponse{
		Code:      dal_wallet_go.ReturnCode_SUCCESS,
		Msg:       "generate address success",
		Addresses: retAddresses,
	}, nil
}

func (s *BusinessMiddleWareService) BuildUnSignTransaction(ctx context.Context, request *dal_wallet_go.UnSignWithdrawTransactionRequest) (*dal_wallet_go.UnSignWithdrawTransactionResponse, error) {
	resp := &dal_wallet_go.UnSignWithdrawTransactionResponse{
		Code: dal_wallet_go.ReturnCode_ERROR,
		Msg:  "submit withdraw fail",
	}
	if request.ConsumerToken != ConsumerToken {
		resp.Msg = "consumer token is error"
		return resp, nil
	}

	feeReq := &utxo.FeeRequest{
		ConsumerToken: request.ConsumerToken,
		Chain:         s.ChainName,
		Network:       s.NetWork,
		Coin:          s.CoinName,
		RawTx:         "",
	}
	utxoFee, err := s.syncClient.BtcRpcClient.GetFee(ctx, feeReq)
	if err != nil {
		resp.Msg = "get fee fail"
		return resp, nil
	}
	// 假设的费用
	btcSt := utxoFee.FeeRate * 10e8 * 380
	btcStStr := fmt.Sprintf("%f", btcSt)

	// todo：预测模型实现
	/*
		根据是 taproot， 隔离见证或者legacy 的预估单个 input 和 output 字节数， 再根据 input 和 output 的数量做出总字节数，
		再乘以单个字节数需要消耗聪的手续费，得到的就是这笔交易的手续费

		如果铭文和符石，直接先进行一次预签名进行，铭文和符石，一个 witness， 一个 op-return， 不管是在哪个结构里面都是需要消耗手续费
	*/

	btcStBigIntFee, _ := new(big.Int).SetString(btcStStr, 10)

	hotWalletInfo, err := s.db.Addresses.QueryHotWalletInfo(request.RequestId)
	if err != nil {
		log.Error("query hotWalletInfo fail", "err", err)
		return nil, err
	}

	// todo:需要找到和提现交易匹配的热钱包地址的 vin
	// -暴力的形式： 将所有 utxo 输入进去， 然后找零
	// -将 utxo 进行排序， 选择和提现相近的交易放到 vin （可能导致 utxo 臃肿，需要通过合并 utxo 解决）

	vinsList, err := s.db.Vins.QueryVinsByAddress(request.RequestId, hotWalletInfo.Address)
	if err != nil {
		log.Error("query vins fail", "err", err)
		return nil, err
	}

	var utxoVins []*utxo.Vin
	for _, vin := range vinsList {
		vinItem := &utxo.Vin{
			Hash:    vin.TxId,
			Index:   uint32(vin.Vout),
			Amount:  vin.Amount.Int64(),
			Address: hotWalletInfo.Address,
		}
		utxoVins = append(utxoVins, vinItem)
	}

	var utxoVouts []*utxo.Vout
	for _, tx := range request.Txn {
		amount, _ := strconv.Atoi(tx.Value)
		voutItem := &utxo.Vout{
			Address: tx.To,
			Amount:  int64(amount),
			Index:   0,
		}
		utxoVouts = append(utxoVouts, voutItem)
	}

	utr := &utxo.UnSignTransactionRequest{
		ConsumerToken: request.ConsumerToken,
		Chain:         s.ChainName,
		Network:       s.NetWork,
		Fee:           btcStStr,
		Vin:           utxoVins,
		Vout:          utxoVouts,
	}

	// 构建 32 hash
	txMessageHash, err := s.syncClient.BtcRpcClient.CreateUnSignTransaction(ctx, utr)
	if err != nil {
		log.Error("create unsign transaction fail", "err", err)
		return nil, err
	}
	log.Info("create unsign transaction success", "txHash", txMessageHash)

	// 构建 withdraw 表
	txUuid := uuid.New()
	withdraw := &database.Withdraws{
		Guid:        txUuid,
		BlockHash:   "0x00",
		BlockNumber: big.NewInt(0),
		Hash:        "0x00",
		Fee:         btcStBigIntFee,
		LockTime:    big.NewInt(0),
		Version:     "0x00",
		TxSignHex:   "0x00",
		Status:      database.TxStatusWaitSign,
		Timestamp:   uint64(time.Now().Unix()),
	}

	if err := s.db.Withdraws.StoreWithdraws(request.RequestId, withdraw); err != nil {
		log.Error("store withdraws fail", "err", err)
		return nil, err
	}
	resp.Code = dal_wallet_go.ReturnCode_SUCCESS
	resp.Msg = "create unsign transaction success"

	var ReturnTxHashes []*dal_wallet_go.ReturnTransactionHashes
	var signHashesStr []string
	for _, tx := range txMessageHash.SignHashes {
		if tx != nil {
			signHashesStr = append(signHashesStr, string(tx))
		} else {
			signHashesStr = append(signHashesStr, "")
		}
	}
	var signHashStr string
	for _, signHash := range signHashesStr {
		signHashStr += signHash + "|"
	}
	retTxHash := &dal_wallet_go.ReturnTransactionHashes{
		TransactionUuid: txUuid.String(),
		UnSignTx:        signHashStr,
		TxData:          string(txMessageHash.TxData),
	}
	ReturnTxHashes = append(ReturnTxHashes, retTxHash)
	resp.ReturnTxHashes = ReturnTxHashes
	return resp, nil
}

func (s *BusinessMiddleWareService) BuildSignedTransaction(ctx context.Context, request *dal_wallet_go.SignedWithdrawTransactionRequest) (*dal_wallet_go.SignedWithdrawTransactionResponse, error) {
	resp := &dal_wallet_go.SignedWithdrawTransactionResponse{
		Code: dal_wallet_go.ReturnCode_ERROR,
		Msg:  "submit withdraw fail",
	}
	if request.ConsumerToken != ConsumerToken {
		resp.Msg = "consumer token is error"
		return resp, nil
	}

	var resultSignature [][]byte
	var txData []byte
	var transactionId string
	for _, SignTx := range request.SignTxn {
		if SignTx != nil {
			signatureItem := SignTx.Signature
			resultSignature = append(resultSignature, []byte(signatureItem))
		} else {
			resultSignature = append(resultSignature, []byte(nil))
		}

		txData = []byte(SignTx.TxData)
		transactionId = SignTx.TransactionUuid
	}

	hotWalletInfo, err := s.db.Addresses.QueryHotWalletInfo(request.RequestId)
	if err != nil {
		log.Error("query hotWalletInfo fail", "err", err)
		return nil, err
	}
	var publicKeys [][]byte
	publicKeys = append(publicKeys, []byte(hotWalletInfo.PublicKey))

	signedReq := &utxo.SignedTransactionRequest{
		ConsumerToken: ConsumerToken,
		Chain:         s.ChainName,
		Network:       s.NetWork,
		TxData:        txData,
		Signatures:    resultSignature,
		PublicKeys:    publicKeys,
	}
	completeTx, err := s.syncClient.BtcRpcClient.BuildSignedTransaction(ctx, signedReq)
	if err != nil {
		log.Error("build signed transaction fail", "err", err)
		return nil, err
	}
	log.Info("signed transaction data", "signedTxData", completeTx.SignedTxData)

	var retSignedTxn []*dal_wallet_go.ReturnSignedTransactions
	retSign := &dal_wallet_go.ReturnSignedTransactions{
		TransactionUuid: transactionId,
		SignedTx:        string(completeTx.SignedTxData),
	}
	retSignedTxn = append(retSignedTxn, retSign)

	err = s.db.Withdraws.UpdateWithdrawByGuid(request.RequestId, transactionId, string(completeTx.SignedTxData))
	if err != nil {
		log.Error("update withdraws fail", "err", err)
		return nil, err
	}

	resp.Code = dal_wallet_go.ReturnCode_SUCCESS
	resp.Msg = "build signed transaction success"
	resp.ReturnSignTxn = retSignedTxn
	return resp, nil
}

func (s *BusinessMiddleWareService) SubmitWithdraw(ctx context.Context, request *dal_wallet_go.SubmitWithdrawRequest) (*dal_wallet_go.SubmitWithdrawResponse, error) {
	resp := &dal_wallet_go.SubmitWithdrawResponse{
		Code: dal_wallet_go.ReturnCode_ERROR,
		Msg:  "submit withdraw fail",
	}
	if request.ConsumerToken != ConsumerToken {
		resp.Msg = "consumer token is error"
		return resp, nil
	}

	var childTxList []database.ChildTxs
	var txId = uuid.New()
	var withdrawTime = time.Now().Unix()
	for _, withdraw := range request.WithdrawList {
		childTx := database.ChildTxs{
			GUID:        uuid.New(),
			Hash:        "0x00",
			TxId:        txId.String(),
			TxIndex:     big.NewInt(0),
			TxType:      "withdraw",
			FromAddress: "hotwallet",
			ToAddress:   withdraw.Address,
			Amount:      withdraw.Value,
			Timestamp:   uint64(withdrawTime),
		}
		childTxList = append(childTxList, childTx)
	}

	withdraw := &database.Withdraws{
		Guid:        uuid.New(),
		BlockHash:   "0x00",
		BlockNumber: big.NewInt(0),
		Hash:        "0x00",
		Fee:         big.NewInt(0),
		LockTime:    big.NewInt(0),
		Version:     "0x00",
		TxSignHex:   "0x00",
		Status:      database.TxStatusWaitSign,
		Timestamp:   uint64(withdrawTime),
	}

	if err := s.db.Transaction(func(tx *database.DB) error {
		if len(childTxList) > 0 {
			if err := tx.ChildTxs.StoreChildTxs(request.RequestId, childTxList); err != nil {
				log.Error("store child tx to db fail", "err", err)
				return err
			}
		}
		if err := tx.Withdraws.StoreWithdraws(request.RequestId, withdraw); err != nil {
			log.Error("store withdraw to db fail", "err", err)
			return err
		}
		return nil
	}); err != nil {
		log.Error("unable to persist withdraw tx batch", "err", err)
		return nil, err
	}

	resp.Code = dal_wallet_go.ReturnCode_SUCCESS
	resp.Msg = "submit withdraw success"
	return resp, nil
}
