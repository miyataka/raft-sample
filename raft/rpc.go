package raft

import (
	"context"

	pb "github.com/taka/raft-sample/proto"
)

// RPCHandler は Raft RPC を処理するインターフェースです。
type RPCHandler interface {
	// RequestVote は投票要求を処理します。
	RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
	// AppendEntries はログ追加要求を処理します。
	AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
}

// HandleRequestVote は RequestVote RPC を処理します。
func (n *Node) HandleRequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.logger.Printf("[%s] received RequestVote from %s (term=%d, lastLogIndex=%d, lastLogTerm=%d)",
		n.config.ID, req.CandidateId, req.Term, req.LastLogIndex, req.LastLogTerm)

	resp := &pb.RequestVoteResponse{
		Term:        int64(n.state.GetTerm()),
		VoteGranted: false,
	}

	// 候補者の任期が自分の任期より小さい場合、拒否
	if req.Term < int64(n.state.GetTerm()) {
		n.logger.Printf("[%s] rejecting vote: candidate term %d < current term %d",
			n.config.ID, req.Term, n.state.GetTerm())
		return resp, nil
	}

	// 候補者の任期が自分の任期より大きい場合、フォロワーに降格
	if req.Term > int64(n.state.GetTerm()) {
		n.logger.Printf("[%s] stepping down: candidate term %d > current term %d",
			n.config.ID, req.Term, n.state.GetTerm())
		n.becomeFollower(int(req.Term), "")
		resp.Term = req.Term
	}

	// 投票条件の確認
	// 1. まだ投票していない、または既に同じ候補者に投票している
	// 2. 候補者のログが少なくとも自分のログと同じくらい最新である
	votedFor := n.state.GetVotedFor()
	if votedFor == "" || votedFor == req.CandidateId {
		if n.isLogUpToDate(int(req.LastLogIndex), int(req.LastLogTerm)) {
			n.logger.Printf("[%s] granting vote to %s", n.config.ID, req.CandidateId)
			n.state.SetVotedFor(req.CandidateId)
			resp.VoteGranted = true
			// 投票したら選挙タイマーをリセット
			n.ResetElectionTimer()
		} else {
			n.logger.Printf("[%s] rejecting vote: candidate log not up-to-date", n.config.ID)
		}
	} else {
		n.logger.Printf("[%s] rejecting vote: already voted for %s", n.config.ID, votedFor)
	}

	return resp, nil
}

// isLogUpToDate は候補者のログが自分のログと同じくらい最新かどうかを判定します。
// Raft論文の5.4.1節に基づく実装:
// - 最後のログエントリの任期が異なる場合、任期が大きい方が最新
// - 同じ任期の場合、インデックスが大きい方が最新
func (n *Node) isLogUpToDate(lastLogIndex, lastLogTerm int) bool {
	myLastIndex := n.state.Persistent.Log.LastIndex()
	myLastTerm := n.state.Persistent.Log.LastTerm()

	// 候補者の最後の任期が大きければ最新
	if lastLogTerm > myLastTerm {
		return true
	}
	// 同じ任期で、候補者のインデックスが同じか大きければ最新
	if lastLogTerm == myLastTerm && lastLogIndex >= myLastIndex {
		return true
	}
	return false
}

// HandleAppendEntries は AppendEntries RPC を処理します。
func (n *Node) HandleAppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.logger.Printf("[%s] received AppendEntries from %s (term=%d, prevLogIndex=%d, entries=%d)",
		n.config.ID, req.LeaderId, req.Term, req.PrevLogIndex, len(req.Entries))

	resp := &pb.AppendEntriesResponse{
		Term:    int64(n.state.GetTerm()),
		Success: false,
	}

	// リーダーの任期が自分の任期より小さい場合、拒否
	if req.Term < int64(n.state.GetTerm()) {
		n.logger.Printf("[%s] rejecting AppendEntries: leader term %d < current term %d",
			n.config.ID, req.Term, n.state.GetTerm())
		return resp, nil
	}

	// リーダーの任期が自分の任期より大きい場合、フォロワーに降格
	if req.Term > int64(n.state.GetTerm()) {
		n.becomeFollower(int(req.Term), req.LeaderId)
		resp.Term = req.Term
		// メインループに降格を通知
		select {
		case n.stepDownCh <- struct{}{}:
		default:
		}
	} else if n.state.GetState() != Follower {
		// 同じ任期でも、候補者やリーダーはフォロワーに降格
		n.becomeFollower(int(req.Term), req.LeaderId)
		resp.Term = req.Term
		select {
		case n.stepDownCh <- struct{}{}:
		default:
		}
	}

	// 選挙タイマーをリセット（正当なリーダーからのメッセージ）
	n.ResetElectionTimer()

	// ログの一貫性チェック
	if req.PrevLogIndex > 0 {
		prevEntry := n.state.Persistent.Log.Get(int(req.PrevLogIndex))
		if prevEntry == nil {
			// 一致するエントリがない
			n.logger.Printf("[%s] log inconsistency: no entry at index %d", n.config.ID, req.PrevLogIndex)
			resp.ConflictIndex = int64(n.state.Persistent.Log.LastIndex() + 1)
			return resp, nil
		}
		if prevEntry.Term != int(req.PrevLogTerm) {
			// 任期が一致しない
			n.logger.Printf("[%s] log inconsistency: term mismatch at index %d (got %d, want %d)",
				n.config.ID, req.PrevLogIndex, prevEntry.Term, req.PrevLogTerm)
			resp.ConflictTerm = int64(prevEntry.Term)
			// この任期の最初のエントリを見つける
			resp.ConflictIndex = int64(n.findFirstIndexOfTerm(prevEntry.Term))
			return resp, nil
		}
	}

	// ログエントリを追加
	for i, entry := range req.Entries {
		index := int(req.PrevLogIndex) + 1 + i
		existing := n.state.Persistent.Log.Get(index)

		if existing != nil {
			// 既存のエントリと競合する場合、それ以降を削除
			if existing.Term != int(entry.Term) {
				n.state.Persistent.Log.TruncateFrom(index)
				n.appendEntry(entry)
			}
			// 同じエントリが既に存在する場合は何もしない
		} else {
			n.appendEntry(entry)
		}
	}

	// コミットインデックスを更新
	if req.LeaderCommit > int64(n.state.Volatile.CommitIndex) {
		lastNewEntry := int(req.PrevLogIndex) + len(req.Entries)
		n.state.Volatile.CommitIndex = min(int(req.LeaderCommit), lastNewEntry)
		n.logger.Printf("[%s] updated commitIndex to %d", n.config.ID, n.state.Volatile.CommitIndex)
	}

	resp.Success = true
	return resp, nil
}

// appendEntry は proto の LogEntry を内部のログに追加します。
func (n *Node) appendEntry(entry *pb.LogEntry) {
	n.state.Persistent.Log.Append(LogEntry{
		Term:    int(entry.Term),
		Index:   int(entry.Index),
		Command: entry.Command,
	})
}

// findFirstIndexOfTerm は指定された任期の最初のエントリのインデックスを返します。
func (n *Node) findFirstIndexOfTerm(term int) int {
	for i := 1; i <= n.state.Persistent.Log.Len(); i++ {
		entry := n.state.Persistent.Log.Get(i)
		if entry != nil && entry.Term == term {
			return i
		}
	}
	return 1
}
