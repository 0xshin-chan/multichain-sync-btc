package services

import (
	"context"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/google/uuid"

	"github.com/0xshin-chan/multichain-sync-btc/database"
	"github.com/0xshin-chan/multichain-sync-btc/database/dynamic"
	dal_wallet_go "github.com/0xshin-chan/multichain-sync-btc/protobuf/dal-wallet-go"
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
	//TODO implement me
	panic("implement me")
}

func (s *BusinessMiddleWareService) BuildUnSignTransaction(ctx context.Context, request *dal_wallet_go.UnSignWithdrawTransactionRequest) (*dal_wallet_go.UnSignWithdrawTransactionResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (s *BusinessMiddleWareService) BuildSignedTransaction(ctx context.Context, request *dal_wallet_go.SignedWithdrawTransactionRequest) (*dal_wallet_go.SignedWithdrawTransactionResponse, error) {
	//TODO implement me
	panic("implement me")
}
