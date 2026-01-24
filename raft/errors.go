package raft

import "errors"

var (
	// ErrEmptyID はノードIDが空の場合のエラー
	ErrEmptyID = errors.New("raft: node ID cannot be empty")

	// ErrInvalidElectionTimeout は選挙タイムアウトが無効な場合のエラー
	ErrInvalidElectionTimeout = errors.New("raft: invalid election timeout")

	// ErrInvalidHeartbeatInterval はハートビート間隔が無効な場合のエラー
	ErrInvalidHeartbeatInterval = errors.New("raft: invalid heartbeat interval")

	// ErrHeartbeatTooLong はハートビート間隔が選挙タイムアウトより長い場合のエラー
	ErrHeartbeatTooLong = errors.New("raft: heartbeat interval must be less than election timeout")

	// ErrNotLeader はリーダーでないノードがリーダー操作を試みた場合のエラー
	ErrNotLeader = errors.New("raft: not the leader")
)
