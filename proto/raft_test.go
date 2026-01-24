package proto

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestRequestVoteRequestSerialization(t *testing.T) {
	req := &RequestVoteRequest{
		Term:         5,
		CandidateId:  "node1",
		LastLogIndex: 10,
		LastLogTerm:  3,
	}

	// シリアライズ
	data, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// デシリアライズ
	var decoded RequestVoteRequest
	if err := proto.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if decoded.Term != req.Term {
		t.Errorf("Term mismatch: got %d, want %d", decoded.Term, req.Term)
	}
	if decoded.CandidateId != req.CandidateId {
		t.Errorf("CandidateId mismatch: got %s, want %s", decoded.CandidateId, req.CandidateId)
	}
	if decoded.LastLogIndex != req.LastLogIndex {
		t.Errorf("LastLogIndex mismatch: got %d, want %d", decoded.LastLogIndex, req.LastLogIndex)
	}
	if decoded.LastLogTerm != req.LastLogTerm {
		t.Errorf("LastLogTerm mismatch: got %d, want %d", decoded.LastLogTerm, req.LastLogTerm)
	}
}

func TestRequestVoteResponseSerialization(t *testing.T) {
	resp := &RequestVoteResponse{
		Term:        5,
		VoteGranted: true,
	}

	data, err := proto.Marshal(resp)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var decoded RequestVoteResponse
	if err := proto.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if decoded.Term != resp.Term {
		t.Errorf("Term mismatch: got %d, want %d", decoded.Term, resp.Term)
	}
	if decoded.VoteGranted != resp.VoteGranted {
		t.Errorf("VoteGranted mismatch: got %v, want %v", decoded.VoteGranted, resp.VoteGranted)
	}
}

func TestAppendEntriesRequestSerialization(t *testing.T) {
	req := &AppendEntriesRequest{
		Term:         5,
		LeaderId:     "leader1",
		PrevLogIndex: 10,
		PrevLogTerm:  4,
		Entries: []*LogEntry{
			{Term: 5, Index: 11, Command: []byte("cmd1")},
			{Term: 5, Index: 12, Command: []byte("cmd2")},
		},
		LeaderCommit: 8,
	}

	data, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var decoded AppendEntriesRequest
	if err := proto.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if decoded.Term != req.Term {
		t.Errorf("Term mismatch: got %d, want %d", decoded.Term, req.Term)
	}
	if decoded.LeaderId != req.LeaderId {
		t.Errorf("LeaderId mismatch: got %s, want %s", decoded.LeaderId, req.LeaderId)
	}
	if len(decoded.Entries) != 2 {
		t.Errorf("Entries count mismatch: got %d, want 2", len(decoded.Entries))
	}
	if string(decoded.Entries[0].Command) != "cmd1" {
		t.Errorf("Entry command mismatch: got %s, want cmd1", string(decoded.Entries[0].Command))
	}
}

func TestAppendEntriesResponseSerialization(t *testing.T) {
	resp := &AppendEntriesResponse{
		Term:          5,
		Success:       false,
		ConflictIndex: 8,
		ConflictTerm:  3,
	}

	data, err := proto.Marshal(resp)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var decoded AppendEntriesResponse
	if err := proto.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if decoded.Term != resp.Term {
		t.Errorf("Term mismatch: got %d, want %d", decoded.Term, resp.Term)
	}
	if decoded.Success != resp.Success {
		t.Errorf("Success mismatch: got %v, want %v", decoded.Success, resp.Success)
	}
	if decoded.ConflictIndex != resp.ConflictIndex {
		t.Errorf("ConflictIndex mismatch: got %d, want %d", decoded.ConflictIndex, resp.ConflictIndex)
	}
}

func TestLogEntrySerialization(t *testing.T) {
	entry := &LogEntry{
		Term:    5,
		Index:   100,
		Command: []byte("set key value"),
	}

	data, err := proto.Marshal(entry)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var decoded LogEntry
	if err := proto.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if decoded.Term != entry.Term {
		t.Errorf("Term mismatch: got %d, want %d", decoded.Term, entry.Term)
	}
	if decoded.Index != entry.Index {
		t.Errorf("Index mismatch: got %d, want %d", decoded.Index, entry.Index)
	}
	if string(decoded.Command) != string(entry.Command) {
		t.Errorf("Command mismatch: got %s, want %s", string(decoded.Command), string(entry.Command))
	}
}
