// Package transport は Raft ノード間の通信を提供します。
package transport

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/taka/raft-sample/raft"
	pb "github.com/taka/raft-sample/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCTransport は gRPC を使用した通信層の実装です。
type GRPCTransport struct {
	pb.UnimplementedRaftServer

	mu       sync.RWMutex
	node     *raft.Node
	server   *grpc.Server
	listener net.Listener
	addr     string

	// ピアへの接続をキャッシュ
	clients map[string]pb.RaftClient
	conns   map[string]*grpc.ClientConn
}

// NewGRPCTransport は新しい gRPC トランスポートを作成します。
func NewGRPCTransport(addr string) *GRPCTransport {
	return &GRPCTransport{
		addr:    addr,
		clients: make(map[string]pb.RaftClient),
		conns:   make(map[string]*grpc.ClientConn),
	}
}

// SetNode はこのトランスポートが処理する Raft ノードを設定します。
func (t *GRPCTransport) SetNode(node *raft.Node) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.node = node
}

// Start は gRPC サーバーを起動します。
func (t *GRPCTransport) Start() error {
	listener, err := net.Listen("tcp", t.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", t.addr, err)
	}

	t.listener = listener
	t.server = grpc.NewServer()
	pb.RegisterRaftServer(t.server, t)

	go func() {
		if err := t.server.Serve(listener); err != nil {
			// サーバーが停止した場合
		}
	}()

	return nil
}

// Stop は gRPC サーバーを停止します。
func (t *GRPCTransport) Stop() {
	if t.server != nil {
		t.server.GracefulStop()
	}
	if t.listener != nil {
		t.listener.Close()
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	for _, conn := range t.conns {
		conn.Close()
	}
	t.clients = make(map[string]pb.RaftClient)
	t.conns = make(map[string]*grpc.ClientConn)
}

// Addr は実際にリッスンしているアドレスを返します。
func (t *GRPCTransport) Addr() string {
	if t.listener != nil {
		return t.listener.Addr().String()
	}
	return t.addr
}

// RequestVote は gRPC の RequestVote RPC を処理します。
func (t *GRPCTransport) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	t.mu.RLock()
	node := t.node
	t.mu.RUnlock()

	if node == nil {
		return nil, fmt.Errorf("node not set")
	}

	return node.HandleRequestVote(ctx, req)
}

// AppendEntries は gRPC の AppendEntries RPC を処理します。
func (t *GRPCTransport) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	t.mu.RLock()
	node := t.node
	t.mu.RUnlock()

	if node == nil {
		return nil, fmt.Errorf("node not set")
	}

	return node.HandleAppendEntries(ctx, req)
}

// SendRequestVote は指定されたピアに RequestVote RPC を送信します。
func (t *GRPCTransport) SendRequestVote(ctx context.Context, peer string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	client, err := t.getClient(peer)
	if err != nil {
		return nil, err
	}

	return client.RequestVote(ctx, req)
}

// SendAppendEntries は指定されたピアに AppendEntries RPC を送信します。
func (t *GRPCTransport) SendAppendEntries(ctx context.Context, peer string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	client, err := t.getClient(peer)
	if err != nil {
		return nil, err
	}

	return client.AppendEntries(ctx, req)
}

// getClient は指定されたピアへのクライアント接続を取得します。
func (t *GRPCTransport) getClient(peer string) (pb.RaftClient, error) {
	t.mu.RLock()
	client, ok := t.clients[peer]
	t.mu.RUnlock()

	if ok {
		return client, nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// ダブルチェック
	if client, ok := t.clients[peer]; ok {
		return client, nil
	}

	conn, err := grpc.NewClient(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", peer, err)
	}

	client = pb.NewRaftClient(conn)
	t.clients[peer] = client
	t.conns[peer] = conn

	return client, nil
}
