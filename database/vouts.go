package database

import (
	"github.com/google/uuid"
	"gorm.io/gorm"
	"math/big"
)

type Vouts struct {
	GUID      uuid.UUID `gorm:"primaryKey" json:"guid"`
	Address   string    `json:"address"` // 资金接收方
	N         uint8     `json:"n"`       // 当前输出在交易里的序号
	Script    string    `json:"script"`  // 锁定脚本，用于与 vins 的scriptSig验证
	Amount    *big.Int  `gorm:"serializer:u256" json:"amount"`
	Timestamp uint64    `json:"timestamp"`
}

type VoutsView interface {
}

type VoutsDB interface {
	VoutsView
	StoreVouts(businessId string, vouts []Vouts) error
}

type voutsDB struct {
	gorm *gorm.DB
}

func NewVoutsDB(db *gorm.DB) VoutsDB {
	return &voutsDB{gorm: db}
}

func (v voutsDB) StoreVouts(businessId string, vouts []Vouts) error {
	result := v.gorm.Table("vouts"+businessId).CreateInBatches(&vouts, len(vouts))
	return result.Error
}
