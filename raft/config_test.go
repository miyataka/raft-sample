package raft

import (
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.ElectionTimeoutMin != 150*time.Millisecond {
		t.Errorf("expected ElectionTimeoutMin=150ms, got %v", cfg.ElectionTimeoutMin)
	}
	if cfg.ElectionTimeoutMax != 300*time.Millisecond {
		t.Errorf("expected ElectionTimeoutMax=300ms, got %v", cfg.ElectionTimeoutMax)
	}
	if cfg.HeartbeatInterval != 50*time.Millisecond {
		t.Errorf("expected HeartbeatInterval=50ms, got %v", cfg.HeartbeatInterval)
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr error
	}{
		{
			name: "valid config",
			config: &Config{
				ID:                 "node1",
				ElectionTimeoutMin: 150 * time.Millisecond,
				ElectionTimeoutMax: 300 * time.Millisecond,
				HeartbeatInterval:  50 * time.Millisecond,
			},
			wantErr: nil,
		},
		{
			name: "empty ID",
			config: &Config{
				ID:                 "",
				ElectionTimeoutMin: 150 * time.Millisecond,
				ElectionTimeoutMax: 300 * time.Millisecond,
				HeartbeatInterval:  50 * time.Millisecond,
			},
			wantErr: ErrEmptyID,
		},
		{
			name: "invalid election timeout min",
			config: &Config{
				ID:                 "node1",
				ElectionTimeoutMin: 0,
				ElectionTimeoutMax: 300 * time.Millisecond,
				HeartbeatInterval:  50 * time.Millisecond,
			},
			wantErr: ErrInvalidElectionTimeout,
		},
		{
			name: "election timeout max < min",
			config: &Config{
				ID:                 "node1",
				ElectionTimeoutMin: 300 * time.Millisecond,
				ElectionTimeoutMax: 150 * time.Millisecond,
				HeartbeatInterval:  50 * time.Millisecond,
			},
			wantErr: ErrInvalidElectionTimeout,
		},
		{
			name: "invalid heartbeat interval",
			config: &Config{
				ID:                 "node1",
				ElectionTimeoutMin: 150 * time.Millisecond,
				ElectionTimeoutMax: 300 * time.Millisecond,
				HeartbeatInterval:  0,
			},
			wantErr: ErrInvalidHeartbeatInterval,
		},
		{
			name: "heartbeat too long",
			config: &Config{
				ID:                 "node1",
				ElectionTimeoutMin: 150 * time.Millisecond,
				ElectionTimeoutMax: 300 * time.Millisecond,
				HeartbeatInterval:  200 * time.Millisecond,
			},
			wantErr: ErrHeartbeatTooLong,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if err != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
