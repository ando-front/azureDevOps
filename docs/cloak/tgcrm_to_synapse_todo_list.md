# TG-CRM → Synapse 実装タスク一覧（ロードマップ）

| # | カテゴリ | タスク | 優先度 | 担当 | 状態 |
|---|---|---|---|---|---|
| 1 | 事前準備 | 既存 Docker/Compose 環境へ SQL/Synapse 代替コンテナと Blob モックを追加する | 高 | DevOps | ToDo |
| 2 | 事前準備 | ADF・Linked Service・Dataset 定義 JSON スケルトンをリポジトリにコミット | 高 | DataEng | Done |
| 3 | Synapse | ODS 物理テーブル名を確定し DDL 作成 | 高 | DBA | InProgress |
| 4 | Synapse | ローカル SQL コンテナで DDL 適用テスト | 中 | DBA | InProgress |
| 5 | ADF | Blob イベントトリガー JSON 実装＋ユニットテスト | 高 | DataEng | ToDo |
| 6 | ADF | CSV 取込 & 差分判定 DataFlow 実装（UPDATE_FLAG / UAD_FLAG） | 高 | DataEng | ToDo |
| 7 | ADF | Synapse への INSERT Sink 実装（必要に応じ MERGE に拡張） | 高 | DataEng | ToDo |
| 8 | テスト | 単体テスト: モック Blob/DB 環境で Import 検証 | 高 | QA | ToDo |
| 9 | テスト | E2E: Docker-compose + GitHub Actions で CI 実行 | 高 | QA | ToDo |
|10 | テスト | パフォーマンス試験（大容量 CSV） | 中 | QA | ToDo |
|11 | 監視 | ADF Diagnostics → Log Analytics ＋アラート IaC 化 | 中 | DevOps | ToDo |
|12 | デプロイ | bicep/ARM で dev → test → prod 昇格パイプライン構築 | 中 | DevOps | ToDo |
|13 | 運用 | Runbook / リカバリ手順を Wiki 整備 | 中 | Ops | ToDo |

> **運用メモ**: 各タスク完了時に「状態」列を `InProgress` → `Done` へ更新し、進捗を管理する。
