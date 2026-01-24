package raft

import "testing"

func TestNodeStateString(t *testing.T) {
	tests := []struct {
		state    NodeState
		expected string
	}{
		{Follower, "Follower"},
		{Candidate, "Candidate"},
		{Leader, "Leader"},
		{NodeState(99), "Unknown"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.expected {
			t.Errorf("NodeState(%d).String() = %q, want %q", tt.state, got, tt.expected)
		}
	}
}

func TestNewRaftState(t *testing.T) {
	peers := []string{"node2", "node3"}
	rs := NewRaftState("node1", peers)

	if rs.ID != "node1" {
		t.Errorf("expected ID=node1, got %s", rs.ID)
	}

	if rs.GetState() != Follower {
		t.Errorf("expected initial state=Follower, got %s", rs.GetState())
	}

	if rs.GetTerm() != 0 {
		t.Errorf("expected initial term=0, got %d", rs.GetTerm())
	}

	if rs.GetVotedFor() != "" {
		t.Errorf("expected initial votedFor='', got %s", rs.GetVotedFor())
	}

	if len(rs.Peers) != 2 {
		t.Errorf("expected 2 peers, got %d", len(rs.Peers))
	}
}

func TestRaftStateSetState(t *testing.T) {
	rs := NewRaftState("node1", nil)

	rs.SetState(Candidate)
	if rs.GetState() != Candidate {
		t.Errorf("expected state=Candidate, got %s", rs.GetState())
	}

	rs.SetState(Leader)
	if rs.GetState() != Leader {
		t.Errorf("expected state=Leader, got %s", rs.GetState())
	}
}

func TestRaftStateSetTerm(t *testing.T) {
	rs := NewRaftState("node1", nil)

	rs.SetVotedFor("node2")
	if rs.GetVotedFor() != "node2" {
		t.Errorf("expected votedFor=node2, got %s", rs.GetVotedFor())
	}

	// SetTerm should reset votedFor
	rs.SetTerm(5)
	if rs.GetTerm() != 5 {
		t.Errorf("expected term=5, got %d", rs.GetTerm())
	}
	if rs.GetVotedFor() != "" {
		t.Errorf("expected votedFor to be reset, got %s", rs.GetVotedFor())
	}
}

func TestRaftStateInitLeaderState(t *testing.T) {
	peers := []string{"node2", "node3"}
	rs := NewRaftState("node1", peers)

	// ログにエントリを追加
	rs.Persistent.Log.Append(LogEntry{Term: 1, Index: 1, Command: "a"})
	rs.Persistent.Log.Append(LogEntry{Term: 1, Index: 2, Command: "b"})

	rs.InitLeaderState()

	// NextIndex は lastLogIndex + 1 で初期化される
	for _, peer := range peers {
		if rs.Leader.NextIndex[peer] != 3 {
			t.Errorf("expected NextIndex[%s]=3, got %d", peer, rs.Leader.NextIndex[peer])
		}
		if rs.Leader.MatchIndex[peer] != 0 {
			t.Errorf("expected MatchIndex[%s]=0, got %d", peer, rs.Leader.MatchIndex[peer])
		}
	}
}
