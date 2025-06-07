# ADF E2Eテスト環境 使用ガイド

## 概要

このドキュメントでは、Azure Data Factory (ADF) プロジェクト用のEnd-to-End (E2E) テスト環境の構築と実行方法について説明します。DockerとDocker Composeを使用して、完全に分離された再現可能なテスト環境を提供します。

## 🏗️ アーキテクチャ

E2Eテスト環境は以下のコンポーネントで構成されています：

```
┌─────────────────────────────────────────────────────────────┐
│                    E2E Test Environment                     │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │ E2E Test Runner │  │   SQL Server    │  │   Azurite    │ │
│  │   (Python)      │  │   (Database)    │  │  (Storage)   │ │
│  └─────────────────┘  └─────────────────┘  └──────────────┘ │
│  ┌─────────────────┐  ┌─────────────────┐                   │
│  │ IR Simulator    │  │     Redis       │                   │
│  │ (Integration)   │  │    (Cache)      │                   │
│  └─────────────────┘  └─────────────────┘                   │
└─────────────────────────────────────────────────────────────┘
```

## 📋 前提条件

### 必要なソフトウェア

- **Docker** (v20.10以上)
- **Docker Compose** (v2.0以上)
- **Git**

### 必要ファイル

- `Dockerfile.e2e.complete-light` - E2Eテスト用Dockerファイル
- `docker-compose.e2e.yml` - Docker Compose設定
- `requirements.e2e.txt` - E2Eテスト用Python依存関係
- `run-e2e-tests.sh` - 自動実行スクリプト

## 🚀 クイックスタート

### 1. 環境設定

環境変数テンプレートをコピーして設定：

```bash
cp .env.e2e.template .env.e2e
# .env.e2eファイルを編集してAzure認証情報を設定
```

### 2. E2Eテスト実行

#### 完全自動実行（推奨）

```bash
./run-e2e-tests.sh
```

#### 手動実行

```bash
# 1. イメージビルド
docker-compose -f docker-compose.e2e.yml build

# 2. テスト実行
docker-compose -f docker-compose.e2e.yml up --abort-on-container-exit
```

## 📝 実行オプション

### 自動実行スクリプトオプション

```bash
# ビルドのみ実行
./run-e2e-tests.sh --build-only

# テストのみ実行（ビルドスキップ）
./run-e2e-tests.sh --test-only

# テスト後にクリーンアップ
./run-e2e-tests.sh --cleanup

# タイムアウト設定（デフォルト: 600秒）
./run-e2e-tests.sh --timeout 900

# ヘルプ表示
./run-e2e-tests.sh --help
```

### Docker Composeコマンド

```bash
# 特定のサービスのみ起動
docker-compose -f docker-compose.e2e.yml up sqlserver-test azurite-test

# バックグラウンド実行
docker-compose -f docker-compose.e2e.yml up -d

# ログ確認
docker-compose -f docker-compose.e2e.yml logs e2e-test-runner

# コンテナ停止
docker-compose -f docker-compose.e2e.yml down

# ボリューム含めて削除
docker-compose -f docker-compose.e2e.yml down --volumes
```

## 📊 テスト結果

### 出力形式

テスト実行後、以下の形式で結果が出力されます：

```
test_results/
├── e2e_complete.xml         # 全体のJUnit XML結果
├── e2e_report.html          # HTMLテストレポート
├── coverage.xml             # カバレッジXML
├── coverage_html/           # HTMLカバレッジレポート
├── basic_connections.xml    # 基本接続テスト結果
├── docker_integration.xml   # Docker統合テスト結果
└── ...                      # その他個別テスト結果
```

### 結果確認方法

```bash
# テスト結果確認
ls -la test_results/

# HTMLレポートを開く
open test_results/e2e_report.html

# カバレッジレポートを開く
open test_results/coverage_html/index.html
```

## 🔧 カスタマイズ

### 環境変数設定

`.env.e2e`ファイルで以下の設定をカスタマイズできます：

- **Azure認証情報**: ADF_SUBSCRIPTION_ID, ADF_CLIENT_ID, etc.
- **データベース設定**: SQL_SERVER_HOST, SQL_SERVER_PASSWORD, etc.
- **テスト設定**: TEST_TIMEOUT, LOG_LEVEL, etc.

### テスト対象の変更

`docker-compose.e2e.yml`の`e2e-test-runner`サービスの`command`セクションを編集してテスト対象を変更できます。

```yaml
command: >
  sh -c "
    python -m pytest /app/tests/e2e/test_specific_module.py -v --tb=short
  "
```

### 追加サービス

新しいサービス（例：MongoDB、Elasticsearch）を追加する場合は、`docker-compose.e2e.yml`に新しいサービス定義を追加します。

## 🐛 トラブルシューティング

### よくある問題

#### 1. Docker Compose起動失敗

```bash
# エラー: ポートが既に使用中
ERROR: for sqlserver-test  Cannot start service sqlserver-test: 
driver failed programming external connectivity on endpoint

# 解決方法: 使用中のプロセスを停止
sudo lsof -i :1433
kill -9 <PID>
```

#### 2. SQL Server接続エラー

```bash
# ヘルスチェック確認
docker-compose -f docker-compose.e2e.yml exec sqlserver-test \
  /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd123' -Q 'SELECT 1' -C
```

#### 3. メモリ不足エラー

```bash
# Dockerメモリ設定を増加
# Docker Desktop > Settings > Resources > Memory を8GB以上に設定
```

#### 4. ビルド失敗

```bash
# キャッシュクリア後再ビルド
docker system prune -f
docker-compose -f docker-compose.e2e.yml build --no-cache
```

### ログ確認

```bash
# 全サービスのログ
docker-compose -f docker-compose.e2e.yml logs

# 特定サービスのログ
docker-compose -f docker-compose.e2e.yml logs e2e-test-runner

# ログをフォロー
docker-compose -f docker-compose.e2e.yml logs -f
```

### デバッグモード

```bash
# デバッグ用に対話モードでコンテナ起動
docker-compose -f docker-compose.e2e.yml run --rm e2e-test-runner bash

# 手動でテスト実行
python -m pytest tests/e2e/test_basic_connections.py -v -s
```

## 📈 パフォーマンス最適化

### 並列実行

```bash
# 並列テスト実行
export PYTEST_XDIST_WORKERS=4
python -m pytest tests/e2e/ -n 4
```

### リソース設定

`docker-compose.e2e.yml`でリソース制限を設定：

```yaml
services:
  e2e-test-runner:
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: '1.0'
```

## 🔒 セキュリティ

### 機密情報管理

- `.env.e2e`ファイルをgitignoreに追加
- 本番環境の認証情報は使用しない
- テスト専用のAzureリソースを使用

### ネットワークセキュリティ

- 内部ネットワーク使用
- 不要なポート公開を避ける

## 📚 関連ドキュメント

- [Docker Compose公式ドキュメント](https://docs.docker.com/compose/)
- [pytest公式ドキュメント](https://docs.pytest.org/)
- [Azure Data Factory公式ドキュメント](https://docs.microsoft.com/azure/data-factory/)

## 🤝 貢献

バグ報告や機能改善の提案は、プロジェクトのIssueトラッカーまでお願いします。

## 📄 ライセンス

このプロジェクトのライセンスについては、LICENSEファイルを参照してください。
