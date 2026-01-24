package transport

import (
	"context"
	"testing"
	"time"

	"github.com/taka/raft-sample/raft"
	pb "github.com/taka/raft-sample/proto"
)

func newTestConfig(id string, peers []string) *raft.Config {
	return &raft.Config{
		ID:                 id,
		Peers:              peers,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
	}
}

func TestGRPCTransportStartStop(t *testing.T) {
	transport := NewGRPCTransport("localhost:0")

	err := transport.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	addr := transport.Addr()
	if addr == "" {
		t.Error("expected non-empty address")
	}

	transport.Stop()
}

func TestGRPCTransportRequestVote(t *testing.T) {
	// サーバー側の設定
	serverTransport := NewGRPCTransport("localhost:0")
	serverConfig := newTestConfig("server", nil)
	serverNode, _ := raft.NewNode(serverConfig, nil)
	serverTransport.SetNode(serverNode)

	err := serverTransport.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer serverTransport.Stop()

	serverAddr := serverTransport.Addr()

	// クライアント側の設定
	clientTransport := NewGRPCTransport("localhost:0")
	err = clientTransport.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer clientTransport.Stop()

	// RequestVote RPC を送信
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb.RequestVoteRequest{
		Term:         1,
		CandidateId:  "candidate",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	resp, err := clientTransport.SendRequestVote(ctx, serverAddr, req)
	if err != nil {
		t.Fatalf("SendRequestVote failed: %v", err)
	}

	// サーバーは任期0で初期化されているので、任期1の候補者に投票するはず
	if !resp.VoteGranted {
		t.Error("expected vote to be granted")
	}
}

func TestGRPCTransportAppendEntries(t *testing.T) {
	// サーバー側の設定
	serverTransport := NewGRPCTransport("localhost:0")
	serverConfig := newTestConfig("server", nil)
	serverNode, _ := raft.NewNode(serverConfig, nil)
	serverTransport.SetNode(serverNode)

	err := serverTransport.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer serverTransport.Stop()

	serverAddr := serverTransport.Addr()

	// クライアント側の設定
	clientTransport := NewGRPCTransport("localhost:0")
	err = clientTransport.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer clientTransport.Stop()

	// AppendEntries RPC を送信（ハートビート）
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb.AppendEntriesRequest{
		Term:         1,
		LeaderId:     "leader",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 0,
	}

	resp, err := clientTransport.SendAppendEntries(ctx, serverAddr, req)
	if err != nil {
		t.Fatalf("SendAppendEntries failed: %v", err)
	}

	if !resp.Success {
		t.Error("expected success")
	}
}

func TestGRPCTransportClientCaching(t *testing.T) {
	// サーバー側の設定
	serverTransport := NewGRPCTransport("localhost:0")
	serverConfig := newTestConfig("server", nil)
	serverNode, _ := raft.NewNode(serverConfig, nil)
	serverTransport.SetNode(serverNode)

	err := serverTransport.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer serverTransport.Stop()

	serverAddr := serverTransport.Addr()

	// クライアント側の設定
	clientTransport := NewGRPCTransport("localhost:0")
	err = clientTransport.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer clientTransport.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb.RequestVoteRequest{
		Term:         1,
		CandidateId:  "candidate",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	// 複数回呼び出し
	for range 3 {
		_, err := clientTransport.SendRequestVote(ctx, serverAddr, req)
		if err != nil {
			t.Fatalf("SendRequestVote failed: %v", err)
		}
	}

	// クライアントがキャッシュされていることを確認
	clientTransport.mu.RLock()
	clientCount := len(clientTransport.clients)
	clientTransport.mu.RUnlock()

	if clientCount != 1 {
		t.Errorf("expected 1 cached client, got %d", clientCount)
	}
}
