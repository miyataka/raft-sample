package raft

import (
	"context"
	"sync"
	"testing"
	"time"

	pb "github.com/taka/raft-sample/proto"
)

// mockTransport は Transport インターフェースのモック実装です。
type mockTransport struct {
	mu               sync.Mutex
	requestVoteCalls map[string][]*pb.RequestVoteRequest
	appendEntryCalls map[string][]*pb.AppendEntriesRequest
	voteResponses    map[string]*pb.RequestVoteResponse
	appendResponses  map[string]*pb.AppendEntriesResponse
}

func newMockTransport() *mockTransport {
	return &mockTransport{
		requestVoteCalls: make(map[string][]*pb.RequestVoteRequest),
		appendEntryCalls: make(map[string][]*pb.AppendEntriesRequest),
		voteResponses:    make(map[string]*pb.RequestVoteResponse),
		appendResponses:  make(map[string]*pb.AppendEntriesResponse),
	}
}

func (m *mockTransport) SendRequestVote(ctx context.Context, peer string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requestVoteCalls[peer] = append(m.requestVoteCalls[peer], req)
	if resp, ok := m.voteResponses[peer]; ok {
		return resp, nil
	}
	return &pb.RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
}

func (m *mockTransport) SendAppendEntries(ctx context.Context, peer string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.appendEntryCalls[peer] = append(m.appendEntryCalls[peer], req)
	if resp, ok := m.appendResponses[peer]; ok {
		return resp, nil
	}
	return &pb.AppendEntriesResponse{Term: req.Term, Success: true}, nil
}

func (m *mockTransport) setAppendResponse(peer string, resp *pb.AppendEntriesResponse) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.appendResponses[peer] = resp
}

func (m *mockTransport) getAppendEntryCalls(peer string) []*pb.AppendEntriesRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.appendEntryCalls[peer]
}

func TestPropose(t *testing.T) {
	config := newTestConfig("node1", []string{"node2", "node3"})
	node, _ := NewNode(config, nil)
	transport := newMockTransport()
	node.SetTransport(transport)

	// リーダーになる
	node.state.SetTerm(1)
	node.becomeLeader()

	// コマンドを提案
	ctx := context.Background()
	err := node.Propose(ctx, []byte("set x 1"))
	if err != nil {
		t.Fatalf("Propose failed: %v", err)
	}

	// ログにエントリが追加されていることを確認
	if node.state.Persistent.Log.Len() != 1 {
		t.Errorf("expected log length=1, got %d", node.state.Persistent.Log.Len())
	}

	entry := node.state.Persistent.Log.Get(1)
	if entry == nil {
		t.Fatal("expected entry at index 1")
	}
	if entry.Term != 1 {
		t.Errorf("expected term=1, got %d", entry.Term)
	}

	// 複製が開始されるまで待つ
	time.Sleep(50 * time.Millisecond)

	// フォロワーにAppendEntriesが送信されたことを確認
	for _, peer := range []string{"node2", "node3"} {
		calls := transport.getAppendEntryCalls(peer)
		if len(calls) == 0 {
			t.Errorf("expected AppendEntries to be sent to %s", peer)
		}
	}
}

func TestProposeNotLeader(t *testing.T) {
	config := newTestConfig("node1", []string{"node2", "node3"})
	node, _ := NewNode(config, nil)

	// フォロワー状態でPropose
	ctx := context.Background()
	err := node.Propose(ctx, []byte("set x 1"))
	if err != ErrNotLeader {
		t.Errorf("expected ErrNotLeader, got %v", err)
	}
}

func TestReplicateToPeer(t *testing.T) {
	config := newTestConfig("node1", []string{"node2"})
	node, _ := NewNode(config, nil)
	transport := newMockTransport()
	node.SetTransport(transport)

	// リーダーになる
	node.state.SetTerm(1)
	node.becomeLeader()

	// ログにエントリを追加
	node.state.Persistent.Log.Append(LogEntry{Term: 1, Index: 1, Command: []byte("cmd1")})

	// 複製
	ctx := context.Background()
	node.replicateToPeer(ctx, "node2")

	// AppendEntriesが送信されたことを確認
	calls := transport.getAppendEntryCalls("node2")
	if len(calls) != 1 {
		t.Fatalf("expected 1 AppendEntries call, got %d", len(calls))
	}

	req := calls[0]
	if req.Term != 1 {
		t.Errorf("expected term=1, got %d", req.Term)
	}
	if len(req.Entries) != 1 {
		t.Errorf("expected 1 entry, got %d", len(req.Entries))
	}
}

func TestReplicateToPeerFailure(t *testing.T) {
	config := newTestConfig("node1", []string{"node2"})
	node, _ := NewNode(config, nil)
	transport := newMockTransport()
	node.SetTransport(transport)

	// リーダーになる
	node.state.SetTerm(1)
	node.becomeLeader()

	// ログにエントリを追加
	node.state.Persistent.Log.Append(LogEntry{Term: 1, Index: 1, Command: []byte("cmd1")})
	node.state.Persistent.Log.Append(LogEntry{Term: 1, Index: 2, Command: []byte("cmd2")})

	// フォロワーの応答を設定（ログ不整合）
	transport.setAppendResponse("node2", &pb.AppendEntriesResponse{
		Term:          1,
		Success:       false,
		ConflictIndex: 1,
		ConflictTerm:  0,
	})

	// 複製
	ctx := context.Background()
	node.replicateToPeer(ctx, "node2")

	// nextIndex が調整されたことを確認
	if node.state.Leader.NextIndex["node2"] != 1 {
		t.Errorf("expected nextIndex=1, got %d", node.state.Leader.NextIndex["node2"])
	}
}

func TestUpdateCommitIndex(t *testing.T) {
	config := newTestConfig("node1", []string{"node2", "node3"})
	node, _ := NewNode(config, nil)

	// リーダーになる
	node.state.SetTerm(1)
	node.becomeLeader()

	// ログにエントリを追加
	node.state.Persistent.Log.Append(LogEntry{Term: 1, Index: 1, Command: []byte("cmd1")})
	node.state.Persistent.Log.Append(LogEntry{Term: 1, Index: 2, Command: []byte("cmd2")})

	// 過半数(2/3)が複製
	node.state.Leader.MatchIndex["node2"] = 2
	node.state.Leader.MatchIndex["node3"] = 1

	node.updateCommitIndex()

	// 過半数が複製したindex 2がコミットされる
	if node.state.Volatile.CommitIndex != 2 {
		t.Errorf("expected commitIndex=2, got %d", node.state.Volatile.CommitIndex)
	}
}

func TestUpdateCommitIndexOldTerm(t *testing.T) {
	config := newTestConfig("node1", []string{"node2", "node3"})
	node, _ := NewNode(config, nil)

	// リーダーになる（任期2）
	node.state.SetTerm(2)
	node.becomeLeader()

	// 古い任期のエントリ
	node.state.Persistent.Log.Append(LogEntry{Term: 1, Index: 1, Command: []byte("old")})

	// 過半数が複製
	node.state.Leader.MatchIndex["node2"] = 1
	node.state.Leader.MatchIndex["node3"] = 1

	node.updateCommitIndex()

	// 古い任期のエントリはコミットされない（Raft論文 5.4.2）
	if node.state.Volatile.CommitIndex != 0 {
		t.Errorf("expected commitIndex=0 (old term not committed), got %d", node.state.Volatile.CommitIndex)
	}
}

func TestApplyCommittedEntries(t *testing.T) {
	config := newTestConfig("node1", nil)
	node, _ := NewNode(config, nil)

	// ログにエントリを追加
	node.state.Persistent.Log.Append(LogEntry{Term: 1, Index: 1, Command: []byte("cmd1")})
	node.state.Persistent.Log.Append(LogEntry{Term: 1, Index: 2, Command: []byte("cmd2")})

	// コミットインデックスを設定
	node.state.Volatile.CommitIndex = 2

	// 適用されたエントリを追跡
	var appliedEntries []LogEntry
	node.SetApplyFunc(func(entry LogEntry) {
		appliedEntries = append(appliedEntries, entry)
	})

	// 適用
	node.applyCommittedEntries()

	// 2つのエントリが適用されたことを確認
	if len(appliedEntries) != 2 {
		t.Errorf("expected 2 entries applied, got %d", len(appliedEntries))
	}
	if node.state.Volatile.LastApplied != 2 {
		t.Errorf("expected lastApplied=2, got %d", node.state.Volatile.LastApplied)
	}
}

func TestSendHeartbeats(t *testing.T) {
	config := newTestConfig("node1", []string{"node2", "node3"})
	node, _ := NewNode(config, nil)
	transport := newMockTransport()
	node.SetTransport(transport)

	// リーダーになる
	node.state.SetTerm(1)
	node.becomeLeader()

	// ハートビートを送信
	node.sendHeartbeats()

	// 少し待つ（goroutineの完了を待つ）
	time.Sleep(50 * time.Millisecond)

	// 両方のピアにハートビートが送信されたことを確認
	for _, peer := range []string{"node2", "node3"} {
		calls := transport.getAppendEntryCalls(peer)
		if len(calls) == 0 {
			t.Errorf("expected heartbeat to be sent to %s", peer)
		} else if len(calls[0].Entries) != 0 {
			t.Errorf("heartbeat should have no entries, got %d", len(calls[0].Entries))
		}
	}
}

func TestLeaderStepsDownOnHigherTerm(t *testing.T) {
	config := newTestConfig("node1", []string{"node2"})
	node, _ := NewNode(config, nil)
	transport := newMockTransport()
	node.SetTransport(transport)

	// リーダーになる
	node.state.SetTerm(1)
	node.becomeLeader()

	// ログにエントリを追加
	node.state.Persistent.Log.Append(LogEntry{Term: 1, Index: 1, Command: []byte("cmd1")})

	// より高い任期の応答を設定
	transport.setAppendResponse("node2", &pb.AppendEntriesResponse{
		Term:    5,
		Success: false,
	})

	// 複製を試みる
	ctx := context.Background()
	node.replicateToPeer(ctx, "node2")

	// フォロワーに降格したことを確認
	if node.state.GetState() != Follower {
		t.Errorf("expected state=Follower, got %s", node.state.GetState())
	}
	if node.state.GetTerm() != 5 {
		t.Errorf("expected term=5, got %d", node.state.GetTerm())
	}
}
