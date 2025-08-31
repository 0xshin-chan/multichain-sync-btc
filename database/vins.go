package database

import (
	"errors"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"math/big"
)

type Vins struct {
	GUID             uuid.UUID `gorm:"primaryKey" json:"guid"`
	Address          string    `json:"address"`                                   // 资金来源地址
	TxId             string    `json:"tx_id"`                                     // 本次交易id
	Vout             uint8     `json:"vout"`                                      // 上笔交易的输出序号
	Script           string    `json:"script"`                                    // 解锁脚本（scriptSig） ，证明可以花费上一个输出
	Witness          string    `json:"witness"`                                   // 隔离见证数据， 如果是 segwit 交易，放在这里
	Amount           *big.Int  `gorm:"serializer:u256" json:"amount"`             // 输入金额
	SpendTxHash      string    `json:"spend_tx_hash"`                             // 花费该输入的交易hash
	SpendBlockHeight *big.Int  `gorm:"serializer:u256" json:"spend_block_height"` // 被花费所在块高
	isSpend          bool      `json:"is_spend"`
	Timestamp        uint64    `json:"timestamp"`
}

type VinsView interface {
	QueryVinByTxId(businessId, address, txId string) (*Vins, error)
}

type VinsDB interface {
	VinsView

	StoreVins(businessId string, vins []Vins) error
	UpdateVinsTx(businessId, txId, address string, isSpend bool, spendTxHash string, spendBlockHeight *big.Int) error
}

type vinsDB struct {
	gorm *gorm.DB
}

func NewVinsDB(db *gorm.DB) VinsDB {
	return &vinsDB{gorm: db}
}

func (v vinsDB) QueryVinByTxId(businessId, address, txId string) (*Vins, error) {
	var vinEntry Vins
	err := v.gorm.Table("vins_"+businessId).Where("tx_id = ? and address = ?", txId, address).Take(&vinEntry).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, gorm.ErrRecordNotFound
		}
		return nil, err
	}
	return &vinEntry, nil
}

func (v vinsDB) StoreVins(businessId string, vins []Vins) error {
	result := v.gorm.Table("vins_"+businessId).CreateInBatches(vins, len(vins))
	return result.Error
}

func (v vinsDB) UpdateVinsTx(businessId, txId, address string, isSpend bool, spendTxHash string, spendBlockHeight *big.Int) error {

	updates := map[string]interface{}{
		"is_spend": false,
	}

	if (spendTxHash != "" && spendBlockHeight != big.NewInt(0)) || isSpend {
		updates["spend_tx_hash"] = spendTxHash
		updates["spend_block_height"] = spendBlockHeight
		updates["is_spend"] = true
	}

	result := v.gorm.Table("internals_"+businessId).
		Where("txId = ? and address = ?", txId, address).
		Updates(updates)

	if result.Error != nil {
		return result.Error
	}

	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}

	return nil
}
