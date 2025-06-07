# Docker化されたE2Eテスト環境

## 概要

このドキュメントでは、Azure Data Factory パイプライン用のDocker化されたE2Eテスト環境について説明します。この環境では、実際のAzureサービスに依存せずに、コンテナ化されたサービスを使用してパイプライン全体のデータ整合性を検証できます。

## アーキテクチャ

### コンテナ構成

```
┌─────────────────────────────────────────────────────────────┐
│                  Docker E2E Test Environment                │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ SQL Server  │  │  Azurite    │  │  IR Simulator       │  │
│  │ 2022        │  │  (Storage)  │  │  (FastAPI)          │  │
│  │ Port: 11433 │  │ Port: 10000 │  │  Port: 8080         │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
│                                                             │
│  ┌───────────────────────────────────────────────────────┐  │
│  │              E2E Test Runner                          │  │
│  │              (Python + pytest)                       │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### サービス詳細

1. **SQL Server 2022** (`sqlserver-test`)
   - テスト用データベース: `TGMATestDB`
   - ポート: 11433 (ホスト) → 1433 (コンテナ)
   - 認証: sa / YourStrong!Passw0rd123

2. **Azurite** (`azurite-test`)
   - Azure Storage Emulator
   - Blob, Queue, Table サービス
   - ポート: 10000-10002

3. **Integration Runtime Simulator** (`ir-simulator`)
   - ADF パイプライン実行をシミュレート
   - FastAPI ベースのREST API
   - ポート: 8080

4. **E2E Test Runner** (`e2e-test-runner`)
   - pytest ベースのテスト実行環境
   - Python 3.11 + 必要な依存関係

## セットアップ

### 前提条件

- Docker Desktop (Windows/Mac) または Docker Engine (Linux)
- Docker Compose v2.0+
- PowerShell 5.0+ (Windows) または Bash (Linux/Mac)

### 環境構築

1. **リポジトリのクローン**
   ```bash
   git clone <repository-url>
   cd tg-ma-MA-ADF-TEST
   ```

2. **Docker環境の起動**
   ```powershell
   # PowerShell (Windows)
   .\manage-docker-e2e.ps1 -Action start

   # または直接Docker Composeを使用
   docker-compose -f docker-compose.test.yml up -d
   ```

3. **環境の確認**
   ```powershell
   .\manage-docker-e2e.ps1 -Action status
   ```

## 使用方法

### 管理スクリプトの使用

```powershell
# 環境の起動
.\manage-docker-e2e.ps1 -Action start

# 環境の状態確認
.\manage-docker-e2e.ps1 -Action status

# E2Eテストの実行
.\manage-docker-e2e.ps1 -Action test

# 特定のテストパターンのみ実行
.\manage-docker-e2e.ps1 -Action test -TestPattern "*client_dm*"

# ログの確認
.\manage-docker-e2e.ps1 -Action logs

# 環境の停止
.\manage-docker-e2e.ps1 -Action stop

# 環境のクリーンアップ
.\manage-docker-e2e.ps1 -Action cleanup
```

### 直接的なDocker Composeの使用

```bash
# 環境の起動
docker-compose -f docker-compose.test.yml up -d

# テストの実行
docker-compose -f docker-compose.test.yml run --rm e2e-test-runner

# ログの確認
docker-compose -f docker-compose.test.yml logs -f

# 環境の停止
docker-compose -f docker-compose.test.yml down -v
```

## テスト実装

### E2Eテストの構造

```
tests/e2e/
├── helpers/
│   └── docker_e2e_helper.py       # Docker環境用ヘルパークラス
├── test_docker_e2e_client_dm.py   # Client DM パイプラインテスト
├── test_docker_e2e_point_grant_email.py  # Point Grant Email テスト
└── __init__.py
```

### テストレベルの分離

#### Unit Tests (tests/unit/)
- **目的**: 個別のアクティビティレベルでの入出力データ整合性検証
- **依存関係**: モックのみ、外部サービスなし
- **実行速度**: 高速 (< 1秒/テスト)
- **検証範囲**: アクティビティ単体の動作

#### E2E Tests (tests/e2e/test_docker_e2e_*.py)
- **目的**: パイプライン全体での入出力データ整合性検証
- **依存関係**: Docker化されたサービス (SQL Server, Azurite, IR Simulator)
- **実行速度**: 中速 (10-60秒/テスト)
- **検証範囲**: パイプライン全体の統合動作

### テストパターン例

```python
@pytest.mark.e2e
class TestClientDataMartPipelineE2E:
    
    def test_pipeline_client_dm_complete_flow(self, e2e_connection, test_helper):
        """パイプライン全体のデータ整合性テスト"""
        # 1. テストデータのセットアップ
        e2e_connection.setup_test_scenario("client_dm_basic")
        
        # 2. パイプライン実行
        pipeline_result = e2e_connection.execute_pipeline_simulation(
            pipeline_name="pi_Copy_marketing_client_dm"
        )
        
        # 3. 実行成功の確認
        test_helper.assert_pipeline_success(pipeline_result)
        
        # 4. データ整合性の検証
        test_helper.assert_data_integrity("client_dm", "ClientDmBx")
        
        # 5. パイプライン実行ログの検証
        execution_logs = e2e_connection.get_pipeline_execution_logs("pi_Copy_marketing_client_dm")
        test_helper.assert_no_data_loss(execution_logs)
```

## データベーススキーマ

### メインテーブル

```sql
-- 顧客マスタ
CREATE TABLE [dbo].[client_dm] (
    [client_id] NVARCHAR(50) NOT NULL PRIMARY KEY,
    [client_name] NVARCHAR(100),
    [email] NVARCHAR(100),
    [phone] NVARCHAR(20),
    [address] NVARCHAR(200),
    [registration_date] DATETIME2,
    [status] NVARCHAR(20),
    [created_at] DATETIME2 DEFAULT GETDATE(),
    [updated_at] DATETIME2 DEFAULT GETDATE()
);

-- 顧客データマート
CREATE TABLE [dbo].[ClientDmBx] (
    [id] INT IDENTITY(1,1) PRIMARY KEY,
    [client_id] NVARCHAR(50),
    [segment] NVARCHAR(50),
    [score] DECIMAL(10,2),
    [last_transaction_date] DATETIME2,
    [total_amount] DECIMAL(18,2),
    [processed_date] DATETIME2 DEFAULT GETDATE(),
    [data_source] NVARCHAR(50)
);

-- ポイント付与メール履歴
CREATE TABLE [dbo].[point_grant_email] (
    [id] INT IDENTITY(1,1) PRIMARY KEY,
    [client_id] NVARCHAR(50),
    [email] NVARCHAR(100),
    [points_granted] INT,
    [email_sent_date] DATETIME2,
    [campaign_id] NVARCHAR(50),
    [status] NVARCHAR(20),
    [created_at] DATETIME2 DEFAULT GETDATE()
);
```

### ETL制御テーブル

```sql
-- パイプライン実行ログ
CREATE TABLE [etl].[pipeline_execution_log] (
    [execution_id] UNIQUEIDENTIFIER DEFAULT NEWID() PRIMARY KEY,
    [pipeline_name] NVARCHAR(100),
    [activity_name] NVARCHAR(100),
    [start_time] DATETIME2,
    [end_time] DATETIME2,
    [status] NVARCHAR(20),
    [input_rows] INT,
    [output_rows] INT,
    [error_message] NVARCHAR(MAX),
    [created_at] DATETIME2 DEFAULT GETDATE()
);
```

## Integration Runtime Simulator API

### エンドポイント

#### ヘルスチェック
```http
GET /health
```

#### パイプライン実行
```http
POST /pipeline-execution
Content-Type: application/json

{
    "pipeline_name": "pi_Copy_marketing_client_dm",
    "parameters": {
        "batch_size": 100,
        "test_mode": "e2e"
    }
}
```

#### Copy Activity実行
```http
POST /copy-activity
Content-Type: application/json

{
    "pipeline_name": "pi_Copy_marketing_client_dm",
    "activity_name": "CopyClientData",
    "source": {"type": "SqlServer", "table": "client_dm"},
    "sink": {"type": "SqlServer", "table": "ClientDmBx"}
}
```

#### 実行状態確認
```http
GET /execution-status/{execution_id}
```

## トラブルシューティング

### よくある問題

1. **SQL Serverに接続できない**
   ```powershell
   # コンテナの状態確認
   docker-compose -f docker-compose.test.yml ps
   
   # SQL Serverのログ確認
   docker-compose -f docker-compose.test.yml logs sqlserver-test
   ```

2. **テストがタイムアウトする**
   ```python
   # タイムアウト値を調整
   test_helper.wait_for_pipeline_completion(execution_id, timeout=300)
   ```

3. **ポートが既に使用されている**
   ```yaml
   # docker-compose.test.yml でポート番号を変更
   ports:
     - "11434:1433"  # 11433から11434に変更
   ```

### ログの確認

```powershell
# 全サービスのログ
.\manage-docker-e2e.ps1 -Action logs

# 特定サービスのログ
docker-compose -f docker-compose.test.yml logs -f sqlserver-test
docker-compose -f docker-compose.test.yml logs -f ir-simulator
```

### 環境のリセット

```powershell
# 完全なリセット
.\manage-docker-e2e.ps1 -Action reset

# データのみリセット
docker-compose -f docker-compose.test.yml down -v
docker-compose -f docker-compose.test.yml up -d
```

## CI/CD 統合

### GitHub Actions 例

```yaml
name: E2E Tests
on: [push, pull_request]

jobs:
  e2e-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Start Docker E2E Environment
        run: |
          docker-compose -f docker-compose.test.yml up -d
          sleep 30
      
      - name: Run E2E Tests
        run: |
          docker-compose -f docker-compose.test.yml run --rm e2e-test-runner
      
      - name: Cleanup
        if: always()
        run: |
          docker-compose -f docker-compose.test.yml down -v
```

## パフォーマンス考慮事項

### リソース要件

- **メモリ**: 最低 4GB、推奨 8GB
- **CPU**: 最低 2コア、推奨 4コア
- **ストレージ**: 最低 5GB 空き容量

### 最適化のヒント

1. **並列実行の制限**
   ```python
   # pytest-xdist を使用した並列実行
   pytest -n 2 tests/e2e/test_docker_e2e*.py
   ```

2. **テストデータのサイズ調整**
   ```python
   # 大量データテストは @pytest.mark.slow でマーク
   @pytest.mark.slow
   def test_large_dataset_processing(self):
       pass
   ```

3. **コンテナの事前ビルド**
   ```bash
   # CI環境での事前ビルド
   docker-compose -f docker-compose.test.yml build --parallel
   ```

## 今後の拡張

1. **追加パイプラインのサポート**
   - 新しいパイプライン用のE2Eテストクラス追加
   - 対応するテストデータとシナリオの実装

2. **モニタリングとメトリクス**
   - Prometheus/Grafana統合
   - テスト実行メトリクスの収集

3. **データ生成の自動化**
   - より現実的なテストデータの自動生成
   - データ依存関係の自動解決

4. **マルチ環境対応**
   - 開発、ステージング、本番環境の設定分離
   - 環境固有のテストシナリオ
