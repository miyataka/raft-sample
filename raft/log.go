// Package raft は Raft コンセンサスアルゴリズムの実装を提供します。
package raft

// LogEntry は Raft ログの1エントリを表します。
// 各エントリはリーダーが受け取ったコマンドと、そのときの任期を保持します。
type LogEntry struct {
	// Term はこのエントリが作成されたときの任期番号
	Term int

	// Index はログ内でのこのエントリの位置（1から開始）
	Index int

	// Command はステートマシンに適用されるコマンド
	Command any
}

// Log は Raft ログを管理する構造体です。
type Log struct {
	entries []LogEntry
}

// NewLog は新しい空のログを作成します。
func NewLog() *Log {
	return &Log{
		entries: make([]LogEntry, 0),
	}
}

// Append は新しいエントリをログに追加します。
func (l *Log) Append(entry LogEntry) {
	l.entries = append(l.entries, entry)
}

// Get は指定されたインデックスのエントリを取得します。
// インデックスは1から始まります。存在しない場合は nil を返します。
func (l *Log) Get(index int) *LogEntry {
	if index < 1 || index > len(l.entries) {
		return nil
	}
	return &l.entries[index-1]
}

// LastIndex は最後のログエントリのインデックスを返します。
// ログが空の場合は 0 を返します。
func (l *Log) LastIndex() int {
	return len(l.entries)
}

// LastTerm は最後のログエントリの任期を返します。
// ログが空の場合は 0 を返します。
func (l *Log) LastTerm() int {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

// GetFrom は指定されたインデックス以降のすべてのエントリを返します。
func (l *Log) GetFrom(index int) []LogEntry {
	if index < 1 || index > len(l.entries) {
		return nil
	}
	return l.entries[index-1:]
}

// TruncateFrom は指定されたインデックス以降のエントリを削除します。
// 競合するログエントリを削除する際に使用します。
func (l *Log) TruncateFrom(index int) {
	if index < 1 || index > len(l.entries) {
		return
	}
	l.entries = l.entries[:index-1]
}

// Len はログのエントリ数を返します。
func (l *Log) Len() int {
	return len(l.entries)
}
