package database

import (
	"gorm.io/gorm"

	"github.com/ethereum/go-ethereum/log"
)

type CreateTableDB interface {
	CreateTable(tableName, realTableName string)
}

type createTableDB struct {
	gorm *gorm.DB
}

func NewCreateTableDB(db *gorm.DB) CreateTableDB {
	return &createTableDB{gorm: db}
}

func (dao *createTableDB) CreateTable(tableName, realTableName string) {
	// "(like " + realTableName + " including all)" 是 PG 的语法， 会将 like 后面的表名中的字段、索引等赋给新建的表
	err := dao.gorm.Exec("CREATE TABLE IF NOT EXISTS " + tableName + "(like " + realTableName + " including all)").Error
	if err != nil {
		log.Error("create table from base table fail", "err", err)
	}
}
