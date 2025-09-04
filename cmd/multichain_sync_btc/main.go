package main

import (
	"context"
	"os"

	"github.com/ethereum/go-ethereum/log"

	"github.com/0xshin-chan/multichain-sync-btc/common/opio"
)

var (
	GitCommit = "" // 编译时可以注入 Git 提交哈希
	gitData   = "" // 编译时可以注入额外信息（如构建时间）
)

func main() {
	// 设置默认日志器，输出到 stdout，日志级别 Info
	log.SetDefault(log.NewLogger(log.NewTerminalHandlerWithLevel(os.Stdout, log.LevelInfo, true)))
	app := NewCli(GitCommit, gitData)
	// 创建一个带中断阻塞的 context
	// 当接收到 Ctrl+C、SIGTERM 之类的信号时，这个 ctx 会被触发
	ctx := opio.WithInterruptBlocker(context.Background())
	// 启动 CLI 应用并运行
	// RunContext 会阻塞执行，直到 CLI 任务结束或者收到中断信号
	if err := app.RunContext(ctx, os.Args); err != nil {
		log.Error("Application failed")
		os.Exit(1)
	}
}
