package dynamic

import (
	"fmt"
	"github.com/huahaiwudi/multichain-sync-btc/database"
)

func CreateTableFromTemplate(requestId string, db *database.DB) {
	tables := []string{
		"address",
		"vins",
		"vouts",
		"balances",
		"deposits",
		"transactions",
		"withdraws",
		"internals",
		"child_txs",
	}

	for _, originTable := range tables {
		tableName := fmt.Sprintf("%s_%s", originTable, requestId)
		db.CreateTable.CreateTable(tableName, originTable)
	}
}
