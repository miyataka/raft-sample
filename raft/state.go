package raft

import "sync"

// NodeState は Raft ノードの状態を表します。
type NodeState int

const (
	// Follower はフォロワー状態。リーダーからの指示を待ちます。
	Follower NodeState = iota
	// Candidate は候補者状態。リーダー選出中です。
	Candidate
	// Leader はリーダー状態。クラスタを管理します。
	Leader
)

// String は NodeState の文字列表現を返します。
func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// PersistentState は再起動後も保持する必要がある状態です。
type PersistentState struct {
	// CurrentTerm はこのサーバーが見た最新の任期番号（起動時に0で初期化）
	CurrentTerm int

	// VotedFor は現在の任期で投票した候補者のID（なければ空文字列）
	VotedFor string

	// Log はログエントリ
	Log *Log
}

// VolatileState はすべてのサーバーが持つ揮発的な状態です。
type VolatileState struct {
	// CommitIndex はコミット済みの最大ログインデックス（0で初期化）
	CommitIndex int

	// LastApplied はステートマシンに適用済みの最大ログインデックス（0で初期化）
	LastApplied int
}

// LeaderState はリーダーのみが持つ揮発的な状態です。
// 選出後に再初期化されます。
type LeaderState struct {
	// NextIndex は各サーバーに次に送るログインデックス
	// （リーダーの最後のログインデックス + 1 で初期化）
	NextIndex map[string]int

	// MatchIndex は各サーバーで複製済みの最大ログインデックス（0で初期化）
	MatchIndex map[string]int
}

// RaftState は Raft ノードの全状態を保持します。
type RaftState struct {
	mu sync.RWMutex

	// ノード識別子
	ID string

	// 現在の状態（Follower/Candidate/Leader）
	state NodeState

	// 永続的な状態
	Persistent PersistentState

	// 揮発的な状態
	Volatile VolatileState

	// リーダー状態（リーダーのみ使用）
	Leader LeaderState

	// クラスタ内の他のノードのアドレス
	Peers []string
}

// NewRaftState は新しい Raft 状態を作成します。
func NewRaftState(id string, peers []string) *RaftState {
	return &RaftState{
		ID:    id,
		state: Follower,
		Persistent: PersistentState{
			CurrentTerm: 0,
			VotedFor:    "",
			Log:         NewLog(),
		},
		Volatile: VolatileState{
			CommitIndex: 0,
			LastApplied: 0,
		},
		Leader: LeaderState{
			NextIndex:  make(map[string]int),
			MatchIndex: make(map[string]int),
		},
		Peers: peers,
	}
}

// GetState は現在のノード状態を返します。
func (rs *RaftState) GetState() NodeState {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.state
}

// SetState はノード状態を設定します。
func (rs *RaftState) SetState(state NodeState) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.state = state
}

// GetTerm は現在の任期を返します。
func (rs *RaftState) GetTerm() int {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.Persistent.CurrentTerm
}

// SetTerm は任期を設定し、投票記録をリセットします。
func (rs *RaftState) SetTerm(term int) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.Persistent.CurrentTerm = term
	rs.Persistent.VotedFor = ""
}

// GetVotedFor は現在の任期で投票した候補者を返します。
func (rs *RaftState) GetVotedFor() string {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.Persistent.VotedFor
}

// SetVotedFor は投票先を設定します。
func (rs *RaftState) SetVotedFor(candidateID string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.Persistent.VotedFor = candidateID
}

// InitLeaderState はリーダー状態を初期化します。
func (rs *RaftState) InitLeaderState() {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	lastLogIndex := rs.Persistent.Log.LastIndex()
	for _, peer := range rs.Peers {
		rs.Leader.NextIndex[peer] = lastLogIndex + 1
		rs.Leader.MatchIndex[peer] = 0
	}
}
