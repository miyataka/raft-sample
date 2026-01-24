package raft

import (
	"context"
	"sync"

	pb "github.com/taka/raft-sample/proto"
)

// Transport はノード間の通信を抽象化するインターフェースです。
type Transport interface {
	// SendRequestVote は指定されたピアに RequestVote RPC を送信します。
	SendRequestVote(ctx context.Context, peer string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
	// SendAppendEntries は指定されたピアに AppendEntries RPC を送信します。
	SendAppendEntries(ctx context.Context, peer string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
}

// SetTransport はトランスポート層を設定します。
func (n *Node) SetTransport(t Transport) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.transport = t
}

// Propose はクライアントからのコマンドをログに追加します。
// リーダーのみが呼び出せます。
func (n *Node) Propose(ctx context.Context, command []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state.GetState() != Leader {
		return ErrNotLeader
	}

	// 新しいログエントリを作成
	entry := LogEntry{
		Term:    n.state.GetTerm(),
		Index:   n.state.Persistent.Log.LastIndex() + 1,
		Command: command,
	}

	n.state.Persistent.Log.Append(entry)
	n.logger.Printf("[%s] proposed command at index %d", n.config.ID, entry.Index)

	// フォロワーへの複製を開始（非同期）
	go n.replicateToFollowers(ctx)

	return nil
}

// replicateToFollowers はすべてのフォロワーにログを複製します。
func (n *Node) replicateToFollowers(ctx context.Context) {
	if n.transport == nil {
		return
	}

	var wg sync.WaitGroup
	for _, peer := range n.config.Peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			n.replicateToPeer(ctx, peer)
		}(peer)
	}
	wg.Wait()

	// 過半数がログを複製したらコミット
	n.updateCommitIndex()
}

// replicateToPeer は特定のピアにログを複製します。
func (n *Node) replicateToPeer(ctx context.Context, peer string) {
	n.mu.RLock()
	if n.state.GetState() != Leader {
		n.mu.RUnlock()
		return
	}

	nextIndex := n.state.Leader.NextIndex[peer]
	prevLogIndex := nextIndex - 1
	prevLogTerm := 0
	if prevLogIndex > 0 {
		if entry := n.state.Persistent.Log.Get(prevLogIndex); entry != nil {
			prevLogTerm = entry.Term
		}
	}

	// 送信するエントリを取得
	entries := n.state.Persistent.Log.GetFrom(nextIndex)
	pbEntries := make([]*pb.LogEntry, len(entries))
	for i, e := range entries {
		var cmd []byte
		if e.Command != nil {
			if b, ok := e.Command.([]byte); ok {
				cmd = b
			}
		}
		pbEntries[i] = &pb.LogEntry{
			Term:    int64(e.Term),
			Index:   int64(e.Index),
			Command: cmd,
		}
	}

	req := &pb.AppendEntriesRequest{
		Term:         int64(n.state.GetTerm()),
		LeaderId:     n.config.ID,
		PrevLogIndex: int64(prevLogIndex),
		PrevLogTerm:  int64(prevLogTerm),
		Entries:      pbEntries,
		LeaderCommit: int64(n.state.Volatile.CommitIndex),
	}
	n.mu.RUnlock()

	resp, err := n.transport.SendAppendEntries(ctx, peer, req)
	if err != nil {
		n.logger.Printf("[%s] failed to send AppendEntries to %s: %v", n.config.ID, peer, err)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// 任期が古い場合、フォロワーに降格
	if resp.Term > int64(n.state.GetTerm()) {
		n.becomeFollower(int(resp.Term), "")
		select {
		case n.stepDownCh <- struct{}{}:
		default:
		}
		return
	}

	if resp.Success {
		// 成功: nextIndex と matchIndex を更新
		newMatchIndex := int(req.PrevLogIndex) + len(req.Entries)
		n.state.Leader.NextIndex[peer] = newMatchIndex + 1
		n.state.Leader.MatchIndex[peer] = newMatchIndex
		n.logger.Printf("[%s] replicated to %s, matchIndex=%d", n.config.ID, peer, newMatchIndex)
	} else {
		// 失敗: nextIndex を減らしてリトライ
		if resp.ConflictTerm > 0 {
			// 最適化: 競合する任期の最後のインデックスを探す
			newNextIndex := int(resp.ConflictIndex)
			for i := n.state.Persistent.Log.Len(); i >= 1; i-- {
				if entry := n.state.Persistent.Log.Get(i); entry != nil && entry.Term == int(resp.ConflictTerm) {
					newNextIndex = i + 1
					break
				}
			}
			n.state.Leader.NextIndex[peer] = newNextIndex
		} else {
			n.state.Leader.NextIndex[peer] = int(resp.ConflictIndex)
		}
		n.logger.Printf("[%s] replication to %s failed, nextIndex=%d", n.config.ID, peer, n.state.Leader.NextIndex[peer])
	}
}

// updateCommitIndex は過半数が複製したログエントリをコミットします。
func (n *Node) updateCommitIndex() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state.GetState() != Leader {
		return
	}

	// 現在の任期のエントリのみコミット可能（Raft論文 5.4.2）
	for i := n.state.Persistent.Log.Len(); i > n.state.Volatile.CommitIndex; i-- {
		entry := n.state.Persistent.Log.Get(i)
		if entry == nil || entry.Term != n.state.GetTerm() {
			continue
		}

		// このインデックスを複製したノード数をカウント
		replicatedCount := 1 // 自分自身
		for _, peer := range n.config.Peers {
			if n.state.Leader.MatchIndex[peer] >= i {
				replicatedCount++
			}
		}

		// 過半数が複製していればコミット
		majority := (len(n.config.Peers)+1)/2 + 1
		if replicatedCount >= majority {
			n.state.Volatile.CommitIndex = i
			n.logger.Printf("[%s] committed up to index %d", n.config.ID, i)
			break
		}
	}
}

// sendHeartbeats は全てのフォロワーにハートビートを送信します。
func (n *Node) sendHeartbeats() {
	if n.transport == nil {
		n.logger.Printf("[%s] sending heartbeats (no transport)", n.config.ID)
		return
	}

	ctx := context.Background()
	for _, peer := range n.config.Peers {
		go n.sendHeartbeatToPeer(ctx, peer)
	}
}

// sendHeartbeatToPeer は特定のピアにハートビートを送信します。
func (n *Node) sendHeartbeatToPeer(ctx context.Context, peer string) {
	n.mu.RLock()
	if n.state.GetState() != Leader {
		n.mu.RUnlock()
		return
	}

	nextIndex := n.state.Leader.NextIndex[peer]
	prevLogIndex := nextIndex - 1
	prevLogTerm := 0
	if prevLogIndex > 0 {
		if entry := n.state.Persistent.Log.Get(prevLogIndex); entry != nil {
			prevLogTerm = entry.Term
		}
	}

	req := &pb.AppendEntriesRequest{
		Term:         int64(n.state.GetTerm()),
		LeaderId:     n.config.ID,
		PrevLogIndex: int64(prevLogIndex),
		PrevLogTerm:  int64(prevLogTerm),
		Entries:      nil, // ハートビートはエントリなし
		LeaderCommit: int64(n.state.Volatile.CommitIndex),
	}
	n.mu.RUnlock()

	resp, err := n.transport.SendAppendEntries(ctx, peer, req)
	if err != nil {
		n.logger.Printf("[%s] heartbeat to %s failed: %v", n.config.ID, peer, err)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if resp.Term > int64(n.state.GetTerm()) {
		n.becomeFollower(int(resp.Term), "")
		select {
		case n.stepDownCh <- struct{}{}:
		default:
		}
		return
	}

	if !resp.Success && n.state.GetState() == Leader {
		// ログの不整合がある場合、nextIndex を調整
		if resp.ConflictIndex > 0 {
			n.state.Leader.NextIndex[peer] = int(resp.ConflictIndex)
		}
	}
}

// ApplyFunc はコミットされたログエントリを適用するコールバック関数の型です。
type ApplyFunc func(entry LogEntry)

// SetApplyFunc はログエントリ適用のコールバックを設定します。
func (n *Node) SetApplyFunc(fn ApplyFunc) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.applyFunc = fn
}

// applyCommittedEntries はコミットされたエントリをステートマシンに適用します。
func (n *Node) applyCommittedEntries() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for n.state.Volatile.LastApplied < n.state.Volatile.CommitIndex {
		n.state.Volatile.LastApplied++
		entry := n.state.Persistent.Log.Get(n.state.Volatile.LastApplied)
		if entry != nil && n.applyFunc != nil {
			n.logger.Printf("[%s] applying entry at index %d", n.config.ID, entry.Index)
			n.applyFunc(*entry)
		}
	}
}
