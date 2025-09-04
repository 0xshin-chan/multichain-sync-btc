package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/urfave/cli/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/0xshin-chan/multichain-sync-btc/common/cliapp"
	"github.com/0xshin-chan/multichain-sync-btc/common/opio"
	"github.com/0xshin-chan/multichain-sync-btc/config"
	"github.com/0xshin-chan/multichain-sync-btc/database"
	flags2 "github.com/0xshin-chan/multichain-sync-btc/flags"
	"github.com/0xshin-chan/multichain-sync-btc/rpcclient/syncclient"
	"github.com/0xshin-chan/multichain-sync-btc/rpcclient/syncclient/utxo"
	"github.com/0xshin-chan/multichain-sync-btc/services"
)

const (
	POLLING_INTERVAL     = 1 * time.Second
	MAX_RPC_MESSAGE_SIZE = 1024 * 1024 * 300
)

func runMigrations(ctx *cli.Context) error {
	ctx.Context = opio.CancelOnInterrupt(ctx.Context)
	log.Info("running migrations...")
	cfg, err := config.LoadConfig(ctx)
	if err != nil {
		log.Error("failed to load config", "error", err)
		return err
	}
	db, err := database.NewDB(ctx.Context, cfg.MasterDB)
	if err != nil {
		log.Error("failed to open database", "error", err)
		return err
	}
	defer func(db *database.DB) {
		err := db.Close()
		if err != nil {
			log.Error("failed to close database", "error", err)
		}
	}(db)
	return db.ExecuteSQLMigration(cfg.Migrations)
}

func runRpc(ctx *cli.Context, shutdown context.CancelCauseFunc) (cliapp.Lifecycle, error) {
	fmt.Println("running grpc server...")
	cfg, err := config.LoadConfig(ctx)
	if err != nil {
		log.Error("failed to load config", "error", err)
		return nil, err
	}
	grpcServerCfg := &services.BusinessMiddleConfig{
		GrpcHostName: cfg.RpcServer.Host,
		GrpcPort:     cfg.RpcServer.Port,
	}
	db, err := database.NewDB(ctx.Context, cfg.MasterDB)
	if err != nil {
		log.Error("failed to open database", "error", err)
		return nil, err
	}

	log.Info("Chain utxo rpc", "rpc url", cfg.ChainBtcRpc)
	conn, err := grpc.NewClient(cfg.ChainBtcRpc, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error("failed to new grpc client", "error", err)
		return nil, err
	}
	client := utxo.NewWalletUtxoServiceClient(conn)
	utxoClient, err := syncclient.NewWalletBtcAccountClient(context.Background(), client, "Bitcoin")
	if err != nil {
		log.Error("failed to new grpc client", "error", err)
		return nil, err
	}
	return services.NewBusinessMiddleWareService(db, grpcServerCfg, utxoClient)
}

func NewCli(GitCommit string, GitData string) *cli.App {
	flags := flags2.Flags

	return &cli.App{
		Version:              "0.0.1",
		Description:          "An exchange wallet scanner services with rpc and rest api server",
		EnableBashCompletion: true,
		Commands: []*cli.Command{
			{
				Name:        "version",
				Description: "Show project version",
				Action: func(ctx *cli.Context) error {
					cli.ShowVersion(ctx)
					return nil
				},
			},
			{
				Name:        "migrate",
				Flags:       flags,
				Description: "Run database migrations",
				Action:      runMigrations,
			},
			{
				Name:        "rpc",
				Flags:       flags,
				Description: "Run rpc services",
				Action:      cliapp.LifecycleCmd(runRpc),
			},
		},
	}
}
