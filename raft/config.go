package raft

import "time"

// Config は Raft ノードの設定パラメータを保持します。
type Config struct {
	// ID はこのノードの一意な識別子
	ID string

	// Peers はクラスタ内の他のノードのアドレスリスト
	Peers []string

	// ElectionTimeoutMin は選挙タイムアウトの最小値
	ElectionTimeoutMin time.Duration

	// ElectionTimeoutMax は選挙タイムアウトの最大値
	// タイムアウトは [Min, Max] の範囲でランダムに選ばれます
	ElectionTimeoutMax time.Duration

	// HeartbeatInterval はリーダーがハートビートを送信する間隔
	HeartbeatInterval time.Duration

	// ListenAddr はこのノードがリッスンするアドレス
	ListenAddr string
}

// DefaultConfig はデフォルトの設定を返します。
func DefaultConfig() *Config {
	return &Config{
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
	}
}

// Validate は設定の妥当性を検証します。
func (c *Config) Validate() error {
	if c.ID == "" {
		return ErrEmptyID
	}
	if c.ElectionTimeoutMin <= 0 {
		return ErrInvalidElectionTimeout
	}
	if c.ElectionTimeoutMax < c.ElectionTimeoutMin {
		return ErrInvalidElectionTimeout
	}
	if c.HeartbeatInterval <= 0 {
		return ErrInvalidHeartbeatInterval
	}
	// ハートビートは選挙タイムアウトより短くなければならない
	if c.HeartbeatInterval >= c.ElectionTimeoutMin {
		return ErrHeartbeatTooLong
	}
	return nil
}
