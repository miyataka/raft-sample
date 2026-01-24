package raft

import (
	"context"
	"testing"
	"time"

	pb "github.com/taka/raft-sample/proto"
)

func TestHandleRequestVote_GrantVote(t *testing.T) {
	config := newTestConfig("node1", []string{"node2", "node3"})
	node, _ := NewNode(config, nil)
	node.state.SetTerm(1)

	req := &pb.RequestVoteRequest{
		Term:         2,
		CandidateId:  "node2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	resp, err := node.HandleRequestVote(context.Background(), req)
	if err != nil {
		t.Fatalf("HandleRequestVote failed: %v", err)
	}

	if !resp.VoteGranted {
		t.Error("expected vote to be granted")
	}
	if resp.Term != 2 {
		t.Errorf("expected term=2, got %d", resp.Term)
	}
	if node.state.GetVotedFor() != "node2" {
		t.Errorf("expected votedFor=node2, got %s", node.state.GetVotedFor())
	}
}

func TestHandleRequestVote_RejectOldTerm(t *testing.T) {
	config := newTestConfig("node1", []string{"node2", "node3"})
	node, _ := NewNode(config, nil)
	node.state.SetTerm(5)

	req := &pb.RequestVoteRequest{
		Term:         3, // 古い任期
		CandidateId:  "node2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	resp, err := node.HandleRequestVote(context.Background(), req)
	if err != nil {
		t.Fatalf("HandleRequestVote failed: %v", err)
	}

	if resp.VoteGranted {
		t.Error("expected vote to be rejected due to old term")
	}
	if resp.Term != 5 {
		t.Errorf("expected term=5, got %d", resp.Term)
	}
}

func TestHandleRequestVote_RejectAlreadyVoted(t *testing.T) {
	config := newTestConfig("node1", []string{"node2", "node3"})
	node, _ := NewNode(config, nil)
	node.state.SetTerm(2)
	node.state.SetVotedFor("node3") // 既に node3 に投票済み

	req := &pb.RequestVoteRequest{
		Term:         2,
		CandidateId:  "node2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	resp, err := node.HandleRequestVote(context.Background(), req)
	if err != nil {
		t.Fatalf("HandleRequestVote failed: %v", err)
	}

	if resp.VoteGranted {
		t.Error("expected vote to be rejected: already voted for another candidate")
	}
}

func TestHandleRequestVote_RejectOutdatedLog(t *testing.T) {
	config := newTestConfig("node1", []string{"node2", "node3"})
	node, _ := NewNode(config, nil)
	node.state.SetTerm(2)

	// ローカルログにエントリを追加（任期2でインデックス1と2）
	node.state.Persistent.Log.Append(LogEntry{Term: 2, Index: 1, Command: "a"})
	node.state.Persistent.Log.Append(LogEntry{Term: 2, Index: 2, Command: "b"})

	req := &pb.RequestVoteRequest{
		Term:         2,
		CandidateId:  "node2",
		LastLogIndex: 1, // 候補者のログは短い
		LastLogTerm:  2,
	}

	resp, err := node.HandleRequestVote(context.Background(), req)
	if err != nil {
		t.Fatalf("HandleRequestVote failed: %v", err)
	}

	if resp.VoteGranted {
		t.Error("expected vote to be rejected: candidate log is outdated")
	}
}

func TestHandleRequestVote_GrantVoteNewerTerm(t *testing.T) {
	config := newTestConfig("node1", []string{"node2", "node3"})
	node, _ := NewNode(config, nil)
	node.state.SetTerm(2)

	// ローカルログ: 任期1でインデックス1
	node.state.Persistent.Log.Append(LogEntry{Term: 1, Index: 1, Command: "a"})

	req := &pb.RequestVoteRequest{
		Term:         3,
		CandidateId:  "node2",
		LastLogIndex: 1,
		LastLogTerm:  2, // 候補者のログの任期が高い
	}

	resp, err := node.HandleRequestVote(context.Background(), req)
	if err != nil {
		t.Fatalf("HandleRequestVote failed: %v", err)
	}

	if !resp.VoteGranted {
		t.Error("expected vote to be granted: candidate has newer log term")
	}
}

func TestHandleAppendEntries_Heartbeat(t *testing.T) {
	config := newTestConfig("node1", []string{"node2", "node3"})
	node, _ := NewNode(config, nil)
	node.state.SetTerm(1)

	req := &pb.AppendEntriesRequest{
		Term:         1,
		LeaderId:     "node2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil, // ハートビート
		LeaderCommit: 0,
	}

	resp, err := node.HandleAppendEntries(context.Background(), req)
	if err != nil {
		t.Fatalf("HandleAppendEntries failed: %v", err)
	}

	if !resp.Success {
		t.Error("expected success for heartbeat")
	}
	if node.state.GetState() != Follower {
		t.Errorf("expected state=Follower, got %s", node.state.GetState())
	}
}

func TestHandleAppendEntries_RejectOldTerm(t *testing.T) {
	config := newTestConfig("node1", []string{"node2", "node3"})
	node, _ := NewNode(config, nil)
	node.state.SetTerm(5)

	req := &pb.AppendEntriesRequest{
		Term:     3, // 古い任期
		LeaderId: "node2",
	}

	resp, err := node.HandleAppendEntries(context.Background(), req)
	if err != nil {
		t.Fatalf("HandleAppendEntries failed: %v", err)
	}

	if resp.Success {
		t.Error("expected rejection for old term")
	}
	if resp.Term != 5 {
		t.Errorf("expected term=5, got %d", resp.Term)
	}
}

func TestHandleAppendEntries_AppendEntries(t *testing.T) {
	config := newTestConfig("node1", []string{"node2", "node3"})
	node, _ := NewNode(config, nil)
	node.state.SetTerm(1)

	// 最初のエントリを追加
	node.state.Persistent.Log.Append(LogEntry{Term: 1, Index: 1, Command: "a"})

	req := &pb.AppendEntriesRequest{
		Term:         1,
		LeaderId:     "node2",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []*pb.LogEntry{
			{Term: 1, Index: 2, Command: []byte("b")},
			{Term: 1, Index: 3, Command: []byte("c")},
		},
		LeaderCommit: 2,
	}

	resp, err := node.HandleAppendEntries(context.Background(), req)
	if err != nil {
		t.Fatalf("HandleAppendEntries failed: %v", err)
	}

	if !resp.Success {
		t.Error("expected success")
	}

	// ログの確認
	if node.state.Persistent.Log.Len() != 3 {
		t.Errorf("expected log length=3, got %d", node.state.Persistent.Log.Len())
	}

	// コミットインデックスの確認
	if node.state.Volatile.CommitIndex != 2 {
		t.Errorf("expected commitIndex=2, got %d", node.state.Volatile.CommitIndex)
	}
}

func TestHandleAppendEntries_LogInconsistency(t *testing.T) {
	config := newTestConfig("node1", []string{"node2", "node3"})
	node, _ := NewNode(config, nil)
	node.state.SetTerm(2)

	// ローカルログ: インデックス1に任期1のエントリ
	node.state.Persistent.Log.Append(LogEntry{Term: 1, Index: 1, Command: "a"})

	req := &pb.AppendEntriesRequest{
		Term:         2,
		LeaderId:     "node2",
		PrevLogIndex: 2, // 存在しないインデックス
		PrevLogTerm:  1,
		Entries:      nil,
	}

	resp, err := node.HandleAppendEntries(context.Background(), req)
	if err != nil {
		t.Fatalf("HandleAppendEntries failed: %v", err)
	}

	if resp.Success {
		t.Error("expected failure due to log inconsistency")
	}
	if resp.ConflictIndex != 2 {
		t.Errorf("expected conflictIndex=2, got %d", resp.ConflictIndex)
	}
}

func TestHandleAppendEntries_TruncateConflict(t *testing.T) {
	config := newTestConfig("node1", []string{"node2", "node3"})
	node, _ := NewNode(config, nil)
	node.state.SetTerm(2)

	// ローカルログ: 任期1のエントリ2つ
	node.state.Persistent.Log.Append(LogEntry{Term: 1, Index: 1, Command: "a"})
	node.state.Persistent.Log.Append(LogEntry{Term: 1, Index: 2, Command: "b"}) // これは競合する

	req := &pb.AppendEntriesRequest{
		Term:         2,
		LeaderId:     "node2",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []*pb.LogEntry{
			{Term: 2, Index: 2, Command: []byte("new_b")}, // 新しい任期のエントリ
		},
		LeaderCommit: 0,
	}

	resp, err := node.HandleAppendEntries(context.Background(), req)
	if err != nil {
		t.Fatalf("HandleAppendEntries failed: %v", err)
	}

	if !resp.Success {
		t.Error("expected success after truncating conflict")
	}

	// ログの確認: 古いエントリが削除され、新しいエントリに置き換わる
	if node.state.Persistent.Log.Len() != 2 {
		t.Errorf("expected log length=2, got %d", node.state.Persistent.Log.Len())
	}

	entry := node.state.Persistent.Log.Get(2)
	if entry == nil || entry.Term != 2 {
		t.Errorf("expected entry at index 2 with term 2, got %+v", entry)
	}
}

func TestIsLogUpToDate(t *testing.T) {
	config := newTestConfig("node1", nil)
	node, _ := NewNode(config, nil)

	// ローカルログ: 任期2でインデックス3
	node.state.Persistent.Log.Append(LogEntry{Term: 1, Index: 1, Command: "a"})
	node.state.Persistent.Log.Append(LogEntry{Term: 2, Index: 2, Command: "b"})
	node.state.Persistent.Log.Append(LogEntry{Term: 2, Index: 3, Command: "c"})

	tests := []struct {
		name          string
		lastLogIndex  int
		lastLogTerm   int
		expectedValid bool
	}{
		{"higher term", 1, 3, true},           // 任期が高ければ最新
		{"same term, higher index", 5, 2, true}, // 同じ任期で、インデックスが高い
		{"same term, same index", 3, 2, true},   // 同じ任期、同じインデックス
		{"same term, lower index", 2, 2, false}, // 同じ任期で、インデックスが低い
		{"lower term", 10, 1, false},            // 任期が低い
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := node.isLogUpToDate(tt.lastLogIndex, tt.lastLogTerm)
			if result != tt.expectedValid {
				t.Errorf("isLogUpToDate(%d, %d) = %v, want %v",
					tt.lastLogIndex, tt.lastLogTerm, result, tt.expectedValid)
			}
		})
	}
}

func TestElectionScenario(t *testing.T) {
	// 3ノードクラスタでの選挙シナリオをシミュレート
	config1 := newTestConfig("node1", []string{"node2", "node3"})
	config2 := newTestConfig("node2", []string{"node1", "node3"})
	config3 := newTestConfig("node3", []string{"node1", "node2"})

	node1, _ := NewNode(config1, newTestLogger("[node1]"))
	node2, _ := NewNode(config2, newTestLogger("[node2]"))
	node3, _ := NewNode(config3, newTestLogger("[node3]"))

	// 全ノードを任期0で開始
	node1.state.SetTerm(0)
	node2.state.SetTerm(0)
	node3.state.SetTerm(0)

	// node1 が候補者になる
	node1.becomeCandidate()

	// node1 が他のノードに投票を要求
	req := &pb.RequestVoteRequest{
		Term:         int64(node1.state.GetTerm()),
		CandidateId:  "node1",
		LastLogIndex: int64(node1.state.Persistent.Log.LastIndex()),
		LastLogTerm:  int64(node1.state.Persistent.Log.LastTerm()),
	}

	// node2 と node3 から投票を得る
	resp2, _ := node2.HandleRequestVote(context.Background(), req)
	resp3, _ := node3.HandleRequestVote(context.Background(), req)

	if !resp2.VoteGranted {
		t.Error("node2 should grant vote to node1")
	}
	if !resp3.VoteGranted {
		t.Error("node3 should grant vote to node1")
	}

	// node1 は過半数を得たのでリーダーになれる
	if node1.state.GetTerm() != 1 {
		t.Errorf("node1 term should be 1, got %d", node1.state.GetTerm())
	}
}

func TestCandidateReceivesHigherTerm(t *testing.T) {
	config := newTestConfig("node1", []string{"node2"})
	node, _ := NewNode(config, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	node.Start(ctx)
	defer node.Stop()

	// 候補者になるまで待つ
	time.Sleep(150 * time.Millisecond)

	// より高い任期のハートビートを受信
	req := &pb.AppendEntriesRequest{
		Term:     10,
		LeaderId: "node2",
	}

	node.HandleAppendEntries(context.Background(), req)

	// フォロワーに降格していることを確認
	time.Sleep(10 * time.Millisecond)
	if node.GetState() != Follower {
		t.Errorf("expected state=Follower after receiving higher term, got %s", node.GetState())
	}
	if node.GetTerm() != 10 {
		t.Errorf("expected term=10, got %d", node.GetTerm())
	}
}
