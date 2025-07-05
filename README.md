# Azure Data Factory 包括的開発・テスト環境

## Azure Data Factory (ADF) パイプライン開発・テスト・デプロイの統合環境

> **新機能**: 包括的なドキュメント体系と設計仕様書の追加 (2025/07/03)

## 📚 プロジェクト文書体系

### 設計・仕様書

- [**ARM テンプレート設計仕様書**](docs/ARM_TEMPLATE_DESIGN_SPECIFICATION.md) - パラメータ、リンクサービス、データセット、パイプライン、トリガーの詳細設計
- [**要件定義書**](docs/ARM_TEMPLATE_REQUIREMENTS_DEFINITION.md) - ビジネス要件、機能要件、非機能要件の包括的定義
- [**ドメイン駆動設計モデル**](docs/DOMAIN_DRIVEN_DESIGN_MODEL.md) - DDDプラクティスに基づくドメインモデル設計
- [**要件定義プロンプト設計ガイド**](docs/REQUIREMENTS_PROMPT_DESIGN_GUIDE.md) - 新機能開発における標準化された要件定義プロセス
- [**実用プロンプト例集**](docs/PRACTICAL_PROMPT_EXAMPLES.md) - 実際の開発で使用できる具体的なプロンプト例

### テスト仕様書

- [**テスト方針書**](docs/TEST_STRATEGY_DOCUMENT.md) - テスト戦略、アプローチ、品質基準の定義
- [**単体テスト仕様書**](docs/UNIT_TEST_SPECIFICATION.md) - コンポーネント別の詳細テストケース
- [**E2Eテスト仕様書**](docs/E2E_TEST_SPECIFICATION.md) - ビジネスシナリオベースの統合テスト
- [**総合テスト仕様書**](docs/COMPREHENSIVE_TEST_SPECIFICATION.md) - 品質保証と最終承認プロセス

### アーキテクチャ概要

```text
Azure Data Factory + DDD アーキテクチャ
├─ ドメイン層
│  ├─ パイプライン実行ドメイン (Core)
│  ├─ データ品質保証ドメイン (Core)  
│  ├─ テスト自動化ドメイン (Core)
│  ├─ 接続管理ドメイン (Supporting)
│  └─ 設定管理ドメイン (Supporting)
├─ ADF リソース層
│  ├─ 14 リンクサービス (DB, Storage, 外部システム)
│  ├─ 14 データセット (構造化・半構造化・バイナリ)
│  ├─ 38 パイプライン (ETL/ELT処理)
│  ├─ 30+ トリガー (スケジュール実行)
│  └─ 2 Integration Runtime (共有・Self-hosted)
└─ テスト・品質保証層
   ├─ 400+ E2Eテストケース
   ├─ Docker統合環境
   └─ CI/CD自動化パイプライン
```

### テスト結果サマリー

| テスト層 | 成功率 | 環境 | 用途 |
|---------|-------|------|------|
| 統合テスト | 100% (4/4) | ローカル | SQL外部化・テンプレート分割検証 |
| 単体テスト | 86% (24/28) | Docker/ローカル | 高速開発、ODBC不要 |
| E2Eテスト | **100% (409/409)** | Docker+SQL Server | パイプラインビジネスロジック完全検証 |
| 本番テスト | 準備中 | Azure | CI/CD統合検証 |

## 🎯 プロジェクト概要

### 対象と目的

- **対象**: ADFとPython/Docker/GitHub Actionsを使用するデータエンジニア
- **目標**: クローン後すぐにテスト実行可能、CI/CDパイプライン理解と拡張
- **特徴**: ODBC不要、Windows対応、高速テスト、Docker統合

### 技術的成果

- ✅ ODBC依存性の完全排除
- ✅ 4層テスト戦略の実装
- ✅ Docker統合環境の構築
- ✅ 24.3%のARMテンプレート軽量化
- ✅ 100%成功のDocker E2Eテスト環境

## 📊 テスト戦略とアーキテクチャ

### 4層テスト戦略

```text
統合テスト → 単体テスト → E2Eテスト → 本番テスト
(SQL外部化)  (Mock使用)   (実DB接続)   (実Azure)
```

### 4層テスト戦略の実績

## 🚀 クイックスタート

### 1. 環境セットアップ

```bash
git clone <repository-url>
cd azureDevOps
```

### 2. 単体テスト実行（推奨）

```bash
# Docker環境での高速テスト
docker-compose up --build adf-unit-test
```

**実行時間**: 約30秒  
**結果**: 24/28テスト成功  
**特徴**: ODBC不要、モック使用

### 3. E2Eテスト実行（プロキシ設定選択可能）

#### 自動判定実行（推奨）

```bash
# プロキシ環境を自動判定して実行
./run-e2e-flexible.sh --interactive full
```

#### 企業プロキシ環境

```bash
# プロキシありの環境で実行
./run-e2e-flexible.sh --proxy full
```

#### 開発者ローカル環境

```bash
# プロキシなしの環境で実行
./run-e2e-flexible.sh --no-proxy full
```

#### 従来の手動実行方法

```bash
# プロキシ版：フル機能IRシミュレーター付き
docker-compose -f docker-compose.e2e.yml up -d
docker exec -it ir-simulator-e2e pytest tests/e2e/ -v --tb=short
docker-compose -f docker-compose.e2e.yml down

# ノープロキシ版：ホストマシン実行
docker-compose -f docker-compose.e2e.no-proxy.yml up -d
pytest tests/e2e/ -v --tb=short
python -m pytest tests/e2e/ -v --tb=short
docker-compose -f docker-compose.e2e.no-proxy.yml down
```

**特徴**: 400以上のテストケース、プロキシ環境自動対応、ハイブリッドフォールバック機能

## 🧪 E2Eテスト完全実行ガイド

### 概要

本プロジェクトには**400個以上のE2Eテストケース**が実装されており、主に以下のカテゴリで構成されています：

| カテゴリ | 主要なテスト内容 | 関連テストファイル（例） |
|---------|----------------|--------------------|
| **パイプラインE2E** | 27本番パイプラインの個別・統合動作検証 | `test_e2e_pipeline_*.py` |
| **データ品質・整合性** | データ品質保証、スキーマ検証、セキュリティ | `test_data_quality_*.py`, `test_database_schema.py` |
| **業務シナリオ** | 支払い処理、契約管理、ポイント付与など | `test_e2e_pipeline_payment_*.py`, `test_e2e_pipeline_point_grant_email.py` |
| **基盤・接続性** | DB接続、Azurite接続、基本動作確認 | `test_basic_connections.py`, `test_basic_database_operations.py` |
| **包括シナリオ** | 複数の業務フローをまたぐエンドツーエンド検証 | `test_comprehensive_data_scenarios.py`, `test_final_integration.py` |

### 前提条件

| 要件 | 説明 |
|------|------|
| **Docker** | Docker Desktop 4.0+ または Docker Engine 20.10+ |
| **Docker Compose** | v2.0+ (最新版推奨) |
| **メモリ** | 最低4GB、推奨8GB以上 |
| **ディスク容量** | 最低5GB以上の空き容量 |
| **ネットワーク** | インターネット接続（初回のみDockerイメージダウンロード） |

### 🚀 確実な実行手順

#### ステップ1: 環境確認

```bash
# Docker動作確認
docker --version
docker-compose --version

# メモリ・ディスク確認
docker system df
docker system info | grep -E "Total Memory|CPUs"
```

#### ステップ2: プロジェクト準備

```bash
# リポジトリクローン（まだの場合）
git clone <repository-url>
cd azureDevOps

# 実行権限付与（Unix系OS）
chmod +x run-e2e-flexible.sh

# 環境クリーンアップ（念のため）
docker-compose -f docker-compose.e2e.yml down -v
docker system prune -f
```

#### ステップ3: 環境別実行方法

##### A) 自動判定実行（**推奨**）

```bash
# 環境を自動判定して最適な設定で実行
./run-e2e-flexible.sh --interactive full

# 【実行内容】
# - プロキシ環境を自動検出
# - Docker環境の健全性チェック
# - SQL Server 2022 + Azurite環境起動
# - 400個以上の全テストケース実行
# - 実行時間: 約10-15分
```

##### B) 企業プロキシ環境

```bash
# プロキシありの環境（企業ネットワーク等）
./run-e2e-flexible.sh --proxy full

# 【実行内容】
# - IRシミュレーター付きDocker環境
# - プロキシ設定済みコンテナで実行
# - 外部ネットワーク依存を最小化
```

##### C) 開発者ローカル環境

```bash
# プロキシなしの環境（家庭・開発環境等）
./run-e2e-flexible.sh --no-proxy full

# 【実行内容】
# - 軽量Docker環境
# - ホストマシンの設定を活用
# - 高速実行（プロキシオーバーヘッドなし）
```

##### D) 手動実行（詳細制御が必要な場合）

```bash
# プロキシ環境向け手動実行
docker-compose -f docker-compose.e2e.yml up -d
docker exec -it ir-simulator-e2e pytest tests/e2e/ -v --tb=short
docker-compose -f docker-compose.e2e.yml down

# ノープロキシ環境向け手動実行
docker-compose -f docker-compose.e2e.no-proxy.yml up -d
pytest tests/e2e/ -v --tb=short
docker-compose -f docker-compose.e2e.no-proxy.yml down
```

### 🔍 実行結果の確認

#### 成功パターン

```text
================= 409 passed in 600.12s =================

【期待される結果】
- `test_comprehensive_data_scenarios.py`: 多数のテストが成功
- `test_e2e_pipeline_*.py`: 各パイプラインテストが成功
- その他のテストファイル: 残り全て passed
```

#### 部分成功パターン

```text
================ 403 passed, 6 skipped ================

【許容される結果】
- 一部のテストがSKIPPED: 環境依存設定によるもの
- FAILEDが0件: 重要（ビジネスロジックに問題なし）
```

### 🛠️ トラブルシューティング

#### よくある問題と解決方法

| 問題 | 症状 | 解決方法 |
|------|------|----------|
| **メモリ不足** | `docker: Error response from daemon: Insufficient memory` | Docker設定でメモリを8GB以上に増加 |
| **ポート競合** | `port is already allocated` | `docker ps`で使用中コンテナを確認・停止 |
| **権限エラー** | `Permission denied` | `chmod +x run-e2e-flexible.sh`で実行権限付与 |
| **プロキシエラー** | `connection timeout` | `--proxy`オプション使用またはプロキシ設定確認 |
| **SQL接続エラー** | `Cannot connect to SQL Server` | Docker環境再起動: `docker-compose restart` |

#### 詳細トラブルシューティング

```bash
# Docker環境の完全リセット
docker-compose -f docker-compose.e2e.yml down -v
docker system prune -a -f

# ログ確認
docker-compose -f docker-compose.e2e.yml logs

# 個別コンテナ状況確認
docker ps -a
docker stats
```

### 📊 パフォーマンス指標

| 指標 | 目標値 | 実績値 |
|------|--------|--------|
| **総実行時間** | 15分以内 | 10-12分 |
| **成功率** | 98%以上 | 98.5% (403/409) |
| **環境起動時間** | 2分以内 | 1.5分 |
| **メモリ使用量** | 6GB以内 | 4.2GB |

### 🎯 カテゴリ別テスト実行

特定のカテゴリのみ実行したい場合：

```bash
# パイプラインE2E (マーケティングDMパイプライン)
pytest tests/e2e/test_e2e_pipeline_marketing_client_dm_comprehensive.py -v

# データ品質E2E
pytest tests/e2e/test_data_quality_operations.py -v

# 包括シナリオE2E
pytest tests/e2e/test_comprehensive_data_scenarios.py -v

# 支払い処理E2E (支払アラート)
pytest tests/e2e/test_e2e_pipeline_payment_alert.py -v

# 接続性テスト
pytest tests/e2e/test_basic_connections.py -v
```

### 📞 サポート体制

- **緊急時**: GitHub Issues に `urgent` ラベル付きで報告
- **一般的な質問**: GitHub Discussions
- **環境固有の問題**: [詳細E2Eテストガイド](./docs/E2E_TESTING.md) を参照

## 📁 ファイル構造

```text
azureDevOps/
├── docs/                           # 📚 詳細ドキュメント
│   ├── ADF_GIT_INTEGRATION.md     # Azure Data Factory Git統合ガイド
│   ├── E2E_TESTING.md             # E2Eテスト環境詳細ガイド
│   ├── CI_CD_GUIDE.md             # CI/CDパイプラインガイド
│   └── TROUBLESHOOTING.md         # トラブルシューティング
├── src/dev/                        # 🏭 Azure Data Factory定義
│   ├── pipeline/                  # 27個の本番パイプライン
│   ├── dataset/                   # データセット定義
│   └── linkedService/             # リンクサービス定義
├── tests/                          # 🧪 テストコード
│   ├── unit/                      # 単体テスト
│   ├── e2e/                       # E2Eテスト
│   └── fixtures/                  # テストデータ
├── docker-compose.yml             # 🐳 単体テスト環境
├── docker-compose.e2e.yml         # 🐳 E2Eテスト環境 (プロキシ有)
├── docker-compose.e2e.optimized.yml # 🐳 E2Eテスト環境 (高速化版)
└── .github/workflows/             # 🚀 CI/CDパイプライン
```

## 📚 詳細ドキュメント

### 専門ガイド

| ドキュメント | 内容 | 対象者 |
|-------------|------|--------|
| [🔗 ADF Git統合ガイド](./docs/ADF_GIT_INTEGRATION.md) | Azure Data Factory Git設定、27個の本番パイプライン一覧 | ADF開発者 |
| [🧪 E2Eテストガイド](./docs/E2E_TESTING.md) | Docker環境でのE2Eテスト詳細手順 | テストエンジニア |
| [🚀 CI/CDガイド](./docs/CI_CD_GUIDE.md) | GitHub Actions、ブランチ戦略、デプロイ | DevOpsエンジニア |
| [🔧 トラブルシューティング](./docs/TROUBLESHOOTING.md) | よくある問題と解決方法 | 全開発者 |

### 技術仕様

| 項目 | 詳細 |
|------|------|
| **Python版** | 3.9+ |
| **Docker環境** | Docker Compose v2対応 |
| **データベース** | SQL Server 2022 (E2E)、Mock (単体) |
| **Azure統合** | Data Factory、Storage Account |
| **CI/CD** | GitHub Actions、自動テスト |

## 🤝 貢献ガイド

### 開発フロー

1. **Issue作成** - バグ報告・機能要求
2. **Feature Branch** - `feature/機能名` でブランチ作成
3. **開発・テスト** - 単体テスト → E2Eテスト → 統合テスト
4. **Pull Request** - コードレビュー後マージ
5. **自動デプロイ** - CI/CDパイプラインで自動デプロイ

### 品質基準

- ✅ 単体テスト カバレッジ 80%以上
- ✅ E2Eテスト 全て成功
- ✅ Linting・フォーマット チェック合格
- ✅ セキュリティ脆弱性 スキャン合格

## 📞 サポート

### 問題報告

- **バグ報告**: GitHub Issues
- **機能要求**: GitHub Discussions
- **セキュリティ**: 直接連絡

### 関連リンク

- [Azure Data Factory ドキュメント](https://docs.microsoft.com/azure/data-factory/)
- [Docker Compose ガイド](https://docs.docker.com/compose/)
- [GitHub Actions ドキュメント](https://docs.github.com/actions)

## 📝 今後の課題 (TODO)

本プロジェクトをさらに発展させるための、現在認識されている課題と今後のタスク一覧です。

- **[ ] 単体テストの修正 (4件):**
  - 現在失敗している4件の単体テスト (`tests/unit/`) を修正し、成功率100%を目指す。

- **[ ] E2Eテストの安定化:**
  - スキップされているE2Eテストの原因を特定し、修正する。
  - 無効化されているテスト (`*.py.disabled`) をレビューし、修正・有効化または削除する。
  - `_legacy`, `_old`, `_fixed` といった接尾辞を持つテストファイルを整理・リファクタリングする。

- **[ ] プロキシ問題の恒久対策:**
  - 開発環境で頻発するプロキシ接続エラーに対し、より堅牢な対策を検討する。
  - (`run-e2e-flexible.sh` のような) 環境自動判別スクリプトの強化や、詳細なトラブルシューティングガイドの拡充。

- **[ ] 本番テスト環境の構築とCI/CD連携:**
  - 「準備中」となっている本番テスト環境を構築し、CI/CDパイプラインと統合してデプロイ検証を行う。

- **[ ] ドキュメントと実装の同期プロセス確立:**
  - コードの変更がドキュメントへ自動的に反映される仕組み（またはそれに準ずるレビュープロセス）を導入し、情報の陳腐化を防ぐ。

---

**更新日**: 2025年1月  
**メンテナー**: Data Engineering Team  
**ライセンス**: MIT License
