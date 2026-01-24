package raft

import (
	"context"
	"log"
	"os"
	"testing"
	"time"
)

func newTestConfig(id string, peers []string) *Config {
	return &Config{
		ID:                 id,
		Peers:              peers,
		ElectionTimeoutMin: 50 * time.Millisecond,
		ElectionTimeoutMax: 100 * time.Millisecond,
		HeartbeatInterval:  20 * time.Millisecond,
	}
}

func newTestLogger(prefix string) *log.Logger {
	return log.New(os.Stdout, prefix+" ", log.LstdFlags|log.Lmicroseconds)
}

func TestNewNode(t *testing.T) {
	config := newTestConfig("node1", []string{"node2", "node3"})
	logger := newTestLogger("[node1]")

	node, err := NewNode(config, logger)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	if node.GetID() != "node1" {
		t.Errorf("expected ID=node1, got %s", node.GetID())
	}
}

func TestNewNodeInvalidConfig(t *testing.T) {
	config := &Config{
		ID:                 "", // invalid: empty ID
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
	}

	_, err := NewNode(config, nil)
	if err != ErrEmptyID {
		t.Errorf("expected ErrEmptyID, got %v", err)
	}
}

func TestNodeStartsAsFollower(t *testing.T) {
	config := newTestConfig("node1", nil)
	logger := newTestLogger("[node1]")

	node, err := NewNode(config, logger)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err = node.Start(ctx)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer node.Stop()

	// 初期状態はフォロワー
	time.Sleep(10 * time.Millisecond)
	if node.GetState() != Follower {
		t.Errorf("expected initial state=Follower, got %s", node.GetState())
	}
}

func TestNodeBecomesCandidate(t *testing.T) {
	config := newTestConfig("node1", nil)
	logger := newTestLogger("[node1]")

	node, err := NewNode(config, logger)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err = node.Start(ctx)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer node.Stop()

	// 選挙タイムアウト後、候補者になる
	time.Sleep(150 * time.Millisecond)

	state := node.GetState()
	if state != Candidate && state != Leader {
		t.Errorf("expected state=Candidate or Leader after timeout, got %s", state)
	}
}

func TestNodeElectionTimerReset(t *testing.T) {
	config := newTestConfig("node1", nil)
	logger := newTestLogger("[node1]")

	node, err := NewNode(config, logger)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	err = node.Start(ctx)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer node.Stop()

	// タイマーをリセットし続けることで、フォロワー状態を維持
	for range 5 {
		time.Sleep(30 * time.Millisecond)
		node.ResetElectionTimer()
	}

	// まだフォロワーであるべき
	if node.GetState() != Follower {
		t.Errorf("expected state=Follower while receiving heartbeats, got %s", node.GetState())
	}
}

func TestNodeStepDown(t *testing.T) {
	config := newTestConfig("node1", nil)
	logger := newTestLogger("[node1]")

	node, err := NewNode(config, logger)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err = node.Start(ctx)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer node.Stop()

	// 候補者になるまで待機
	time.Sleep(150 * time.Millisecond)

	// より高い任期でステップダウン
	currentTerm := node.GetTerm()
	node.StepDown(currentTerm + 5)

	time.Sleep(10 * time.Millisecond)

	if node.GetState() != Follower {
		t.Errorf("expected state=Follower after step down, got %s", node.GetState())
	}

	if node.GetTerm() != currentTerm+5 {
		t.Errorf("expected term=%d, got %d", currentTerm+5, node.GetTerm())
	}
}

func TestBecomeFollower(t *testing.T) {
	config := newTestConfig("node1", nil)
	node, _ := NewNode(config, nil)

	node.becomeFollower(5, "node2")

	if node.GetState() != Follower {
		t.Errorf("expected state=Follower, got %s", node.GetState())
	}
	if node.GetTerm() != 5 {
		t.Errorf("expected term=5, got %d", node.GetTerm())
	}
}

func TestBecomeCandidate(t *testing.T) {
	config := newTestConfig("node1", nil)
	node, _ := NewNode(config, nil)

	node.state.SetTerm(3)
	node.becomeCandidate()

	if node.GetState() != Candidate {
		t.Errorf("expected state=Candidate, got %s", node.GetState())
	}
	if node.GetTerm() != 4 {
		t.Errorf("expected term=4, got %d", node.GetTerm())
	}
	if node.state.GetVotedFor() != "node1" {
		t.Errorf("expected votedFor=node1, got %s", node.state.GetVotedFor())
	}
}

func TestBecomeLeader(t *testing.T) {
	config := newTestConfig("node1", []string{"node2", "node3"})
	node, _ := NewNode(config, nil)

	node.becomeLeader()

	if node.GetState() != Leader {
		t.Errorf("expected state=Leader, got %s", node.GetState())
	}
}
