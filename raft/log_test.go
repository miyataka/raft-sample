package raft

import "testing"

func TestNewLog(t *testing.T) {
	log := NewLog()
	if log == nil {
		t.Fatal("NewLog returned nil")
	}
	if log.Len() != 0 {
		t.Errorf("expected empty log, got len=%d", log.Len())
	}
	if log.LastIndex() != 0 {
		t.Errorf("expected LastIndex=0, got %d", log.LastIndex())
	}
	if log.LastTerm() != 0 {
		t.Errorf("expected LastTerm=0, got %d", log.LastTerm())
	}
}

func TestLogAppendAndGet(t *testing.T) {
	log := NewLog()

	// エントリを追加
	entry1 := LogEntry{Term: 1, Index: 1, Command: "cmd1"}
	entry2 := LogEntry{Term: 1, Index: 2, Command: "cmd2"}
	entry3 := LogEntry{Term: 2, Index: 3, Command: "cmd3"}

	log.Append(entry1)
	log.Append(entry2)
	log.Append(entry3)

	// 長さの確認
	if log.Len() != 3 {
		t.Errorf("expected len=3, got %d", log.Len())
	}

	// 各エントリの取得
	got := log.Get(1)
	if got == nil || got.Term != 1 || got.Command != "cmd1" {
		t.Errorf("Get(1) returned unexpected value: %+v", got)
	}

	got = log.Get(2)
	if got == nil || got.Term != 1 || got.Command != "cmd2" {
		t.Errorf("Get(2) returned unexpected value: %+v", got)
	}

	got = log.Get(3)
	if got == nil || got.Term != 2 || got.Command != "cmd3" {
		t.Errorf("Get(3) returned unexpected value: %+v", got)
	}

	// 範囲外のインデックス
	if log.Get(0) != nil {
		t.Error("Get(0) should return nil")
	}
	if log.Get(4) != nil {
		t.Error("Get(4) should return nil")
	}
}

func TestLogLastIndexAndTerm(t *testing.T) {
	log := NewLog()

	log.Append(LogEntry{Term: 1, Index: 1, Command: "a"})
	if log.LastIndex() != 1 || log.LastTerm() != 1 {
		t.Errorf("expected LastIndex=1, LastTerm=1, got %d, %d", log.LastIndex(), log.LastTerm())
	}

	log.Append(LogEntry{Term: 2, Index: 2, Command: "b"})
	if log.LastIndex() != 2 || log.LastTerm() != 2 {
		t.Errorf("expected LastIndex=2, LastTerm=2, got %d, %d", log.LastIndex(), log.LastTerm())
	}
}

func TestLogGetFrom(t *testing.T) {
	log := NewLog()
	log.Append(LogEntry{Term: 1, Index: 1, Command: "a"})
	log.Append(LogEntry{Term: 1, Index: 2, Command: "b"})
	log.Append(LogEntry{Term: 2, Index: 3, Command: "c"})

	entries := log.GetFrom(2)
	if len(entries) != 2 {
		t.Errorf("expected 2 entries, got %d", len(entries))
	}
	if entries[0].Command != "b" || entries[1].Command != "c" {
		t.Errorf("unexpected entries: %+v", entries)
	}

	// 範囲外
	if log.GetFrom(0) != nil {
		t.Error("GetFrom(0) should return nil")
	}
	if log.GetFrom(5) != nil {
		t.Error("GetFrom(5) should return nil")
	}
}

func TestLogTruncateFrom(t *testing.T) {
	log := NewLog()
	log.Append(LogEntry{Term: 1, Index: 1, Command: "a"})
	log.Append(LogEntry{Term: 1, Index: 2, Command: "b"})
	log.Append(LogEntry{Term: 2, Index: 3, Command: "c"})

	log.TruncateFrom(2)

	if log.Len() != 1 {
		t.Errorf("expected len=1 after truncate, got %d", log.Len())
	}
	if log.LastIndex() != 1 {
		t.Errorf("expected LastIndex=1, got %d", log.LastIndex())
	}
}
