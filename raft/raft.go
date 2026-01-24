package raft

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"
)

// Node は Raft ノードを表します。
type Node struct {
	mu sync.RWMutex

	// 設定
	config *Config

	// 状態
	state *RaftState

	// タイマー
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	// チャネル
	stopCh     chan struct{}
	resetCh    chan struct{} // 選挙タイマーリセット用
	stepDownCh chan struct{} // リーダーからフォロワーへの降格通知

	// ロガー
	logger *log.Logger
}

// NewNode は新しい Raft ノードを作成します。
func NewNode(config *Config, logger *log.Logger) (*Node, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = log.Default()
	}

	n := &Node{
		config:     config,
		state:      NewRaftState(config.ID, config.Peers),
		stopCh:     make(chan struct{}),
		resetCh:    make(chan struct{}, 1),
		stepDownCh: make(chan struct{}, 1),
		logger:     logger,
	}

	return n, nil
}

// Start はノードを起動します。
func (n *Node) Start(ctx context.Context) error {
	n.mu.Lock()
	n.logger.Printf("[%s] starting node", n.config.ID)
	n.mu.Unlock()

	// 初期状態はフォロワー
	n.becomeFollower(0, "")

	// メインループ
	go n.run(ctx)

	return nil
}

// Stop はノードを停止します。
func (n *Node) Stop() {
	n.mu.Lock()
	defer n.mu.Unlock()

	close(n.stopCh)

	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
	}

	n.logger.Printf("[%s] node stopped", n.config.ID)
}

// run はメインイベントループです。
func (n *Node) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.stopCh:
			return
		default:
		}

		switch n.state.GetState() {
		case Follower:
			n.runFollower(ctx)
		case Candidate:
			n.runCandidate(ctx)
		case Leader:
			n.runLeader(ctx)
		}
	}
}

// runFollower はフォロワー状態のイベント処理を行います。
func (n *Node) runFollower(ctx context.Context) {
	n.logger.Printf("[%s] running as follower (term=%d)", n.config.ID, n.state.GetTerm())

	timeout := n.randomElectionTimeout()
	n.electionTimer = time.NewTimer(timeout)
	defer n.electionTimer.Stop()

	for n.state.GetState() == Follower {
		select {
		case <-ctx.Done():
			return
		case <-n.stopCh:
			return
		case <-n.resetCh:
			// リーダーからのハートビートを受信した場合、タイマーをリセット
			if !n.electionTimer.Stop() {
				select {
				case <-n.electionTimer.C:
				default:
				}
			}
			n.electionTimer.Reset(n.randomElectionTimeout())
		case <-n.electionTimer.C:
			// 選挙タイムアウト: 候補者になる
			n.logger.Printf("[%s] election timeout, becoming candidate", n.config.ID)
			n.becomeCandidate()
			return
		}
	}
}

// runCandidate は候補者状態のイベント処理を行います。
func (n *Node) runCandidate(ctx context.Context) {
	n.logger.Printf("[%s] running as candidate (term=%d)", n.config.ID, n.state.GetTerm())

	// 選挙タイムアウトを設定
	timeout := n.randomElectionTimeout()
	n.electionTimer = time.NewTimer(timeout)
	defer n.electionTimer.Stop()

	// 投票を開始
	voteCh := n.startElection()

	votesNeeded := (len(n.config.Peers)+1)/2 + 1 // 過半数
	votesGranted := 1                            // 自分自身への投票

	for n.state.GetState() == Candidate {
		select {
		case <-ctx.Done():
			return
		case <-n.stopCh:
			return
		case <-n.stepDownCh:
			// より高い任期を発見、フォロワーに降格
			return
		case vote := <-voteCh:
			if vote {
				votesGranted++
				n.logger.Printf("[%s] received vote, total=%d, needed=%d",
					n.config.ID, votesGranted, votesNeeded)
				if votesGranted >= votesNeeded {
					n.logger.Printf("[%s] won election, becoming leader", n.config.ID)
					n.becomeLeader()
					return
				}
			}
		case <-n.electionTimer.C:
			// 選挙タイムアウト: 新しい選挙を開始
			n.logger.Printf("[%s] election timeout, starting new election", n.config.ID)
			n.becomeCandidate()
			return
		}
	}
}

// runLeader はリーダー状態のイベント処理を行います。
func (n *Node) runLeader(ctx context.Context) {
	n.logger.Printf("[%s] running as leader (term=%d)", n.config.ID, n.state.GetTerm())

	// ハートビートタイマーを開始
	n.heartbeatTimer = time.NewTimer(0) // 即座に最初のハートビートを送信
	defer n.heartbeatTimer.Stop()

	for n.state.GetState() == Leader {
		select {
		case <-ctx.Done():
			return
		case <-n.stopCh:
			return
		case <-n.stepDownCh:
			// より高い任期を発見、フォロワーに降格
			return
		case <-n.heartbeatTimer.C:
			// ハートビートを送信
			n.sendHeartbeats()
			n.heartbeatTimer.Reset(n.config.HeartbeatInterval)
		}
	}
}

// becomeFollower はフォロワー状態に遷移します。
func (n *Node) becomeFollower(term int, leaderID string) {
	n.logger.Printf("[%s] becoming follower (term=%d, leader=%s)",
		n.config.ID, term, leaderID)

	n.state.SetState(Follower)
	if term > n.state.GetTerm() {
		n.state.SetTerm(term)
	}
}

// becomeCandidate は候補者状態に遷移します。
func (n *Node) becomeCandidate() {
	// 任期をインクリメント
	newTerm := n.state.GetTerm() + 1
	n.state.SetTerm(newTerm)

	// 自分自身に投票
	n.state.SetVotedFor(n.config.ID)

	n.state.SetState(Candidate)
	n.logger.Printf("[%s] became candidate (term=%d)", n.config.ID, newTerm)
}

// becomeLeader はリーダー状態に遷移します。
func (n *Node) becomeLeader() {
	n.state.SetState(Leader)
	n.state.InitLeaderState()
	n.logger.Printf("[%s] became leader (term=%d)", n.config.ID, n.state.GetTerm())
}

// startElection は選挙を開始し、投票結果を受け取るチャネルを返します。
func (n *Node) startElection() <-chan bool {
	voteCh := make(chan bool, len(n.config.Peers))

	// TODO: 各ピアに RequestVote RPC を送信
	// 現時点ではモック実装

	return voteCh
}

// sendHeartbeats は全てのフォロワーにハートビートを送信します。
func (n *Node) sendHeartbeats() {
	// TODO: 各ピアに AppendEntries RPC (空のエントリ) を送信
	n.logger.Printf("[%s] sending heartbeats", n.config.ID)
}

// ResetElectionTimer は選挙タイマーをリセットします。
// リーダーからの正当なメッセージを受け取った時に呼び出されます。
func (n *Node) ResetElectionTimer() {
	select {
	case n.resetCh <- struct{}{}:
	default:
	}
}

// StepDown はノードをフォロワーに降格させます。
// より高い任期を持つメッセージを受け取った時に呼び出されます。
func (n *Node) StepDown(term int) {
	if term > n.state.GetTerm() {
		n.becomeFollower(term, "")
		select {
		case n.stepDownCh <- struct{}{}:
		default:
		}
	}
}

// randomElectionTimeout はランダムな選挙タイムアウトを返します。
func (n *Node) randomElectionTimeout() time.Duration {
	min := n.config.ElectionTimeoutMin
	max := n.config.ElectionTimeoutMax
	diff := max - min
	return min + time.Duration(rand.Int63n(int64(diff)))
}

// GetState は現在のノード状態を返します。
func (n *Node) GetState() NodeState {
	return n.state.GetState()
}

// GetTerm は現在の任期を返します。
func (n *Node) GetTerm() int {
	return n.state.GetTerm()
}

// GetID はノードIDを返します。
func (n *Node) GetID() string {
	return n.config.ID
}
