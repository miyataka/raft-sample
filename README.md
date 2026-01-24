# Raft Sample

Go言語で実装した学習目的のRaftコンセンサスアルゴリズム。

## 概要

このプロジェクトは、分散システムにおける合意形成アルゴリズム「Raft」のコア機能を実装しています。
[Raft論文](https://raft.github.io/raft.pdf)に基づいた実装で、以下の機能を含みます：

- **リーダー選出 (Leader Election)**: ノード間でリーダーを決定
- **ログ複製 (Log Replication)**: リーダーからフォロワーへのログエントリ同期
- **状態管理**: Follower/Candidate/Leader の状態遷移

## 必要条件

- Go 1.21以上
- protoc (Protocol Buffers コンパイラ)
- protoc-gen-go, protoc-gen-go-grpc

## インストール

```bash
# リポジトリのクローン
git clone https://github.com/taka/raft-sample.git
cd raft-sample

# 依存関係のインストール
go mod tidy

# ビルド
go build
```

## 使い方

### 3ノードクラスタの起動

3つのターミナルで以下のコマンドを実行します：

```bash
# ターミナル1
./raft-sample -id node1 -listen localhost:8001 -peers localhost:8002,localhost:8003

# ターミナル2
./raft-sample -id node2 -listen localhost:8002 -peers localhost:8001,localhost:8003

# ターミナル3
./raft-sample -id node3 -listen localhost:8003 -peers localhost:8001,localhost:8002
```

### コマンドラインオプション

| オプション | 説明 | 例 |
|-----------|------|-----|
| `-id` | ノードID（必須） | `-id node1` |
| `-listen` | リッスンアドレス | `-listen localhost:8001` |
| `-peers` | 他のノードのアドレス（カンマ区切り） | `-peers localhost:8002,localhost:8003` |

## プロジェクト構造

```
raft-sample/
├── main.go                    # エントリポイント
├── raft/
│   ├── config.go              # 設定パラメータ
│   ├── errors.go              # エラー定義
│   ├── log.go                 # ログエントリ管理
│   ├── raft.go                # Raftノードのメイン実装
│   ├── replication.go         # ログ複製ロジック
│   ├── rpc.go                 # RPCハンドラ
│   ├── state.go               # 状態管理
│   └── *_test.go              # 単体テスト
├── proto/
│   ├── raft.proto             # Protocol Buffers定義
│   ├── raft.pb.go             # 生成されたGoコード
│   └── raft_grpc.pb.go        # 生成されたgRPCコード
└── transport/
    ├── grpc.go                # gRPC通信層
    └── grpc_test.go           # 通信層テスト
```

## アーキテクチャ

### ノード状態

```
    +----------+
    |          |
    v          |
Follower --> Candidate --> Leader
    ^                        |
    |                        |
    +------------------------+
```

- **Follower**: リーダーからのハートビートを待つ
- **Candidate**: 選挙タイムアウト後、リーダー選出を開始
- **Leader**: クラスタを管理し、ログを複製

### RPC

| RPC | 説明 |
|-----|------|
| RequestVote | 候補者がフォロワーに投票を要求 |
| AppendEntries | リーダーがログエントリを複製（ハートビートにも使用） |

## テスト

```bash
# 全テストの実行
go test ./...

# 詳細出力
go test ./... -v

# 統合テストのみ
go test ./raft/... -run "Cluster" -v
```

## 設定パラメータ

| パラメータ | デフォルト値 | 説明 |
|-----------|-------------|------|
| ElectionTimeoutMin | 150ms | 選挙タイムアウトの最小値 |
| ElectionTimeoutMax | 300ms | 選挙タイムアウトの最大値 |
| HeartbeatInterval | 50ms | ハートビート送信間隔 |

## 参考資料

- [Raft論文](https://raft.github.io/raft.pdf)
- [Raft可視化](https://raft.github.io/)
- [The Raft Consensus Algorithm](https://raft.github.io/)

## ライセンス

MIT License
