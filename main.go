// Package main は Raft サーバーのエントリポイントです。
// 学習目的で Raft コンセンサスアルゴリズムを実装しています。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/taka/raft-sample/raft"
	"github.com/taka/raft-sample/transport"
)

func main() {
	// コマンドライン引数の解析
	nodeID := flag.String("id", "", "Node ID (required)")
	listenAddr := flag.String("listen", "", "Listen address (e.g., localhost:8001)")
	peersStr := flag.String("peers", "", "Comma-separated list of peer addresses")
	flag.Parse()

	if *nodeID == "" {
		fmt.Println("Usage: raft-sample -id <node-id> -listen <addr> -peers <peer1,peer2,...>")
		fmt.Println("\nExample (3-node cluster):")
		fmt.Println("  Terminal 1: raft-sample -id node1 -listen localhost:8001 -peers localhost:8002,localhost:8003")
		fmt.Println("  Terminal 2: raft-sample -id node2 -listen localhost:8002 -peers localhost:8001,localhost:8003")
		fmt.Println("  Terminal 3: raft-sample -id node3 -listen localhost:8003 -peers localhost:8001,localhost:8002")
		os.Exit(1)
	}

	// ピアのリストを作成
	var peers []string
	if *peersStr != "" {
		peers = strings.Split(*peersStr, ",")
	}

	// ロガーの設定
	logger := log.New(os.Stdout, fmt.Sprintf("[%s] ", *nodeID), log.Ltime|log.Lmicroseconds)

	// トランスポートの作成
	trans := transport.NewGRPCTransport(*listenAddr)
	if err := trans.Start(); err != nil {
		logger.Fatalf("Failed to start transport: %v", err)
	}

	actualAddr := trans.Addr()
	logger.Printf("Listening on %s", actualAddr)

	// Raft設定の作成
	config := &raft.Config{
		ID:                 *nodeID,
		Peers:              peers,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		ListenAddr:         actualAddr,
	}

	// Raftノードの作成
	node, err := raft.NewNode(config, logger)
	if err != nil {
		logger.Fatalf("Failed to create node: %v", err)
	}

	// トランスポートとノードを接続
	node.SetTransport(trans)
	trans.SetNode(node)

	// ステートマシンへの適用関数を設定（デモ用）
	node.SetApplyFunc(func(entry raft.LogEntry) {
		logger.Printf("Applied entry: index=%d, term=%d, command=%s",
			entry.Index, entry.Term, string(entry.Command.([]byte)))
	})

	// コンテキストの作成
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ノードの起動
	if err := node.Start(ctx); err != nil {
		logger.Fatalf("Failed to start node: %v", err)
	}

	logger.Printf("Raft node started (peers: %v)", peers)

	// 状態表示ゴルーチン
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				logger.Printf("Status: state=%s, term=%d", node.GetState(), node.GetTerm())
			}
		}
	}()

	// シグナルハンドリング
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	logger.Println("Shutting down...")

	node.Stop()
	trans.Stop()

	logger.Println("Shutdown complete")
}
