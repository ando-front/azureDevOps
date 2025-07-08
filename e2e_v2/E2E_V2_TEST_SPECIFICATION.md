# E2E V2 テストフレームワーク 詳細仕様書

## 目次
1. [概要](#概要)
2. [アーキテクチャ](#アーキテクチャ)
3. [テストカテゴリ](#テストカテゴリ)
4. [ドメイン構成](#ドメイン構成)
5. [パイプライン詳細](#パイプライン詳細)
6. [実行方法](#実行方法)
7. [品質基準](#品質基準)
8. [エラーハンドリング](#エラーハンドリング)
9. [レポート生成](#レポート生成)
10. [拡張方法](#拡張方法)

## 概要

### プロジェクト名
E2E V2 Test Framework - Azure Data Factory パイプライン包括的テストフレームワーク

### 目的
Azure Data Factory (ADF) パイプラインの品質保証を目的とした包括的なEnd-to-Endテストフレームワーク。38パイプライン中10パイプラインを7ドメインに分類し、機能・データ品質・パフォーマンス・統合の4カテゴリでテストを実行。

### 技術スタック
- **言語**: Python 3.10+
- **テストフレームワーク**: unittest
- **モックライブラリ**: カスタムMockサービス
- **データ処理**: CSV/TSV, JSON
- **レポート**: テキスト形式, JSON形式

### 成果指標
- **成功率**: 100% (52/52テスト)
- **実行時間**: 平均0.23秒/テスト
- **カバレッジ**: 7ドメイン, 10パイプライン, 52テスト

## アーキテクチャ

### システム全体構成図

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    E2E V2 Test Framework Architecture                       │
└─────────────────────────────────────────────────────────────────────────────┘

┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│   Test Runner    │    │   Reports        │    │   User Interface │
│  run_all_tests   │───▶│  - JSON Report   │◀───│  - CLI Commands  │
│                  │    │  - Text Report   │    │  - Log Output    │
└──────────────────┘    └──────────────────┘    └──────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Domain Layer                                     │
├─────────────┬─────────────┬─────────────┬─────────────┬─────────────────────┤
│  KENDENKI   │    SMC      │ ACTIONPOINT │ MARKETING   │  TGCONTRACT/        │
│             │             │             │             │  INFRASTRUCTURE     │
│ 3 Pipelines │ 2 Pipelines │ 2 Pipelines │ 1 Pipeline  │  2 Pipelines        │
└─────────────┴─────────────┴─────────────┴─────────────┴─────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Base Layer                                       │
├─────────────────────────────┬───────────────────────────────────────────────┤
│     DomainTestBase          │           PipelineTestBase                   │
│ - domain_specific_setup()   │ - Mock Services Setup                        │
│ - get_test_data_template()  │ - ETL Process Simulation                     │
│ - validate_business_rules() │ - Data Quality Validation                    │
└─────────────────────────────┴───────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Mock Services Layer                                 │
├───────────────────┬───────────────────┬─────────────────────────────────────┤
│  MockBlobStorage  │   MockSFTPServer  │        MockDatabase                 │
│ - File Upload     │ - Remote Transfer │ - CRUD Operations                   │
│ - File Download   │ - Directory Ops   │ - Query Execution                   │
│ - File Existence  │ - Transfer Log    │ - Transaction Log                   │
└───────────────────┴───────────────────┴─────────────────────────────────────┘
```

### ディレクトリ構造
```
e2e_v2/
├── base/
│   └── pipeline_test_base.py          # 基底クラス
├── common/
│   └── azure_mock.py                  # モックサービス
├── domains/
│   ├── kendenki/                      # 検電ドメイン
│   │   ├── test_point_grant_email.py
│   │   ├── test_point_lost_email.py
│   │   └── test_usage_service_mtgid.py
│   ├── smc/                           # SMCドメイン
│   │   ├── test_payment_alert.py
│   │   └── test_utility_bills.py
│   ├── actionpoint/                   # アクションポイントドメイン
│   │   ├── test_actionpoint_entry_event.py
│   │   └── test_actionpoint_transaction_history.py
│   ├── marketing/                     # マーケティングドメイン
│   │   └── test_client_dm.py
│   ├── tgcontract/                    # TG契約ドメイン
│   │   └── test_contract_score_info.py
│   └── infrastructure/                # インフラストラクチャドメイン
│       └── test_copy_marketing_client_dm.py
├── scripts/
│   └── run_all_tests.py               # 全テスト実行スクリプト
└── reports/                           # テストレポート出力先
```

### テスト実行環境図

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Test Execution Environment                         │
└─────────────────────────────────────────────────────────────────────────────┘

┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│   Development    │    │     Testing      │    │   Production     │
│   Environment    │    │   Environment    │    │   Environment    │
├──────────────────┤    ├──────────────────┤    ├──────────────────┤
│ - Local Testing  │    │ - CI/CD Pipeline │    │ - Monitoring     │
│ - Code Changes   │    │ - Automated Run  │    │ - Scheduled Run  │
│ - Debug Mode     │    │ - Quality Gates  │    │ - Alert System  │
└──────────────────┘    └──────────────────┘    └──────────────────┘
          │                        │                        │
          └────────────────────────┼────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     E2E V2 Test Framework                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│ Operating System: Linux/Windows WSL2                                       │
│ Python Version: 3.10+                                                      │
│ Working Directory: /mnt/c/Users/angie/git/azureDevOps                      │
│ Test Coverage: 10 Pipelines, 7 Domains, 52 Tests                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 階層設計

#### 1. Base Layer (基底層)
**PipelineTestBase**
- コア機能: モック初期化、ETL処理シミュレーション、データ品質検証
- 責務: 全パイプライン共通の基本機能提供

**DomainTestBase**
- 継承: PipelineTestBase
- 責務: ドメイン固有のセットアップとビジネスルール検証
- 抽象メソッド: `domain_specific_setup()`, `get_domain_test_data_template()`, `validate_domain_business_rules()`

#### 2. Domain Layer (ドメイン層)
各ドメインのテストクラス
- 継承: DomainTestBase
- 責務: パイプライン固有のテストロジック実装
- 実装内容: データ生成、変換処理、ビジネスルール検証

#### 3. Execution Layer (実行層)
**run_all_tests.py**
- 責務: テスト実行オーケストレーション、レポート生成
- 機能: 並列実行、エラーハンドリング、進捗表示

### クラス構成

#### PipelineStatus (Enum)
```python
class PipelineStatus(Enum):
    PENDING = "Pending"
    IN_PROGRESS = "InProgress"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    CANCELLED = "Cancelled"
    SKIPPED = "Skipped"
```

#### TestCategory (Enum)
```python
class TestCategory(Enum):
    FUNCTIONAL = "functional"      # 機能テスト
    DATA_QUALITY = "data_quality"  # データ品質テスト
    PERFORMANCE = "performance"    # パフォーマンステスト
    INTEGRATION = "integration"    # 統合テスト
```

#### PipelineTestResult (DataClass)
```python
@dataclass
class PipelineTestResult:
    test_id: str
    pipeline_name: str
    domain: str
    category: TestCategory
    status: PipelineStatus
    start_time: datetime
    end_time: datetime
    records_extracted: int
    records_transformed: int
    records_loaded: int
    data_quality_score: float
    errors: List[str]
    warnings: List[str]
```

## テストカテゴリ（ETL処理特化）

### 1. FUNCTIONAL (機能テスト) - ETL基本機能
**目的**: ADFパイプラインのETL基本機能の動作確認

**主要テスト**:
- ファイル有り処理: Copy Activityによる正常データ読み込み
- ファイル無し処理: ファイル不存在時のSKIPPED処理
- データ変換処理: Data Flowによるフォーマット変換（TSV→CSV、CSV→JSON）
- システム間連携: SFTP転送、データベース接続の成功

**検証項目**:
- Copy Activityの正常動作
- Data Flowの変換処理正確性
- 外部システム連携の成功
- エラー時の適切なハンドリング

### 2. DATA_QUALITY (データ品質テスト) - ETL品質保証
**目的**: ETL処理レベルでのデータ品質評価

**品質指標（ETL特化）**:
- **completeness** (完全性): NULL値・空値が適切に処理されている割合
- **validity** (有効性): データ型変換が正しく行われている割合
- **consistency** (一貫性): フィールド数・構造が一致している割合
- **accuracy** (精度): フィールドマッピングが正確な割合

**ETL品質基準**:
- completeness: ≥ 0.90 (NULL値処理)
- validity: ≥ 0.95 (データ型変換)
- consistency: ≥ 0.99 (構造整合性)
- accuracy: ≥ 0.95 (マッピング正確性)

### 3. PERFORMANCE (パフォーマンステスト) - ETL性能
**目的**: ETL処理の性能評価

**性能基準（ETL処理）**:
- 小規模データセット (< 10K): > 8,000 records/sec
- 中規模データセット (10K-100K): > 5,000 records/sec
- 大規模データセット (> 100K): > 3,000 records/sec
- 実行時間制限: 60秒以内 (パフォーマンステスト), 30秒以内 (その他)

**測定項目**:
- ETL処理スループット (records/sec)
- メモリ使用量（データ変換時）
- 実行時間（Copy Activity + Data Flow）

### 4. INTEGRATION (統合テスト) - システム間連携
**目的**: ADFパイプラインと外部システムの連携確認

**連携システム**:
- **Azure Blob Storage**: Copy Activityでのファイル読み書き
- **SFTP**: 外部システムへのファイル転送
- **Database**: データベースへのデータ挿入・更新
- **REST API**: 外部APIとの連携

**検証項目**:
- 各システムとの接続性確認
- データ転送の成功
- 連携失敗時のエラーハンドリング
- リトライ機能の動作

## ドメイン構成

### ドメイン全体構成図

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Domain Architecture                            │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│   KENDENKI      │  │      SMC        │  │  ACTIONPOINT    │  │   MARKETING     │
│  (検電ドメイン)    │  │ (支払い・請求)     │  │ (ポイントイベント)  │  │ (マーケティング)  │
├─────────────────┤  ├─────────────────┤  ├─────────────────┤  ├─────────────────┤
│📧 PointGrant     │  │💸 PaymentAlert   │  │🎯 EntryEvent     │  │📮 ClientDM       │
│📧 PointLost      │  │🧾 UtilityBills   │  │📊 TransactionHist│  │                 │
│⚡ UsageService   │  │                 │  │                 │  │                 │
├─────────────────┤  ├─────────────────┤  ├─────────────────┤  ├─────────────────┤
│ 3/3 Pipelines   │  │ 2/13 Pipelines  │  │ 2/2 Pipelines   │  │ 1/2 Pipelines   │
│ ✅ 100% Coverage │  │ 📈 15% Coverage  │  │ ✅ 100% Coverage │  │ 📈 50% Coverage  │
└─────────────────┘  └─────────────────┘  └─────────────────┘  └─────────────────┘

┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│  TGCONTRACT     │  │ INFRASTRUCTURE  │  │   MTGMASTER     │
│ (契約スコア)      │  │ (データ複製)      │  │ (マスター管理)    │
├─────────────────┤  ├─────────────────┤  ├─────────────────┤
│📊 ContractScore  │  │🔄 CopyClientDM   │  │ (未実装)         │
│                 │  │                 │  │                 │
│                 │  │                 │  │                 │
├─────────────────┤  ├─────────────────┤  ├─────────────────┤
│ 1/1 Pipelines   │  │ 1/4 Pipelines   │  │ 0/1 Pipelines   │
│ ✅ 100% Coverage │  │ 📈 25% Coverage  │  │ ⏳ Planned       │
└─────────────────┘  └─────────────────┘  └─────────────────┘

Legend: 📧 Email  💸 Payment  🎯 Event  📮 Marketing  📊 Analytics  🧾 Billing  🔄 Copy  ⚡ Service
```

### ドメイン間データフロー図

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Inter-Domain Data Flow                             │
└─────────────────────────────────────────────────────────────────────────────┘

[External Data Sources]
        │
        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Raw Data      │───▶│  Processing     │───▶│  Enriched Data  │
│   Integration   │    │   Pipeline      │    │   Distribution  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
        │                       │                       │
        ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  KENDENKI       │    │  ACTIONPOINT    │    │  MARKETING      │
│ - Usage Data    │    │ - Point Events  │    │ - Customer DM   │
│ - Point Grant   │    │ - Transactions  │    │ - Segmentation  │
│ - Point Lost    │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
        │                       │                       │
        └───────────┬───────────┴───────────┬───────────┘
                    │                       │
                    ▼                       ▼
            ┌─────────────────┐    ┌─────────────────┐
            │      SMC        │    │  TGCONTRACT     │
            │ - Payment Alert │    │ - Score Calc    │
            │ - Utility Bills │    │ - Risk Analysis │
            └─────────────────┘    └─────────────────┘
                    │                       │
                    └───────────┬───────────┘
                                │
                                ▼
                    ┌─────────────────┐
                    │ INFRASTRUCTURE  │
                    │ - Data Copy     │
                    │ - Quality Sync  │
                    └─────────────────┘
```

### 1. KENDENKI (検電ドメイン)
**概要**: ポイント管理・使用サービス関連処理

**パイプライン** (3/3実装):
1. **pi_PointGrantEmail**: ポイント付与メール送信
2. **pi_PointLostEmail**: ポイント失効メール送信  
3. **pi_Ins_usageservice_mtgid**: 使用サービスmTGID挿入

**特徴**:
- 大量顧客データ処理 (50K件)
- メール配信システム連携
- ポイント計算ロジック

### 2. SMC (SMCドメイン)
**概要**: 支払い・請求関連処理

**パイプライン** (2/13実装):
1. **pi_Send_PaymentAlert**: 支払いアラート送信
2. **pi_UtilityBills**: 公共料金処理

**特徴**:
- 支払い遅延検知
- 季節調整処理
- 高額請求アラート

### 3. ACTIONPOINT (アクションポイントドメイン)
**概要**: ポイントイベント・取引履歴管理

**パイプライン** (2/2実装):
1. **pi_Insert_ActionPointEntryEvent**: アクションポイント登録イベント
2. **pi_Insert_ActionPointTransactionHistory**: アクションポイント取引履歴

**特徴**:
- リアルタイムイベント処理
- キャンペーン管理
- 大量取引データ処理

### 4. MARKETING (マーケティングドメイン)
**概要**: 顧客マーケティング関連処理

**パイプライン** (1/2実装):
1. **pi_Send_ClientDM**: 顧客ダイレクトメール送信

**特徴**:
- 顧客セグメンテーション
- パーソナライゼーション
- オプトアウト管理

### 5. TGCONTRACT (TG契約ドメイン)
**概要**: 契約スコア・リスク評価

**パイプライン** (1/1実装):
1. **pi_Send_karte_contract_score_info**: 契約スコア情報送信

**特徴**:
- 信用スコア計算
- リスク評価
- プレミアム顧客特定

### 6. INFRASTRUCTURE (インフラストラクチャドメイン)
**概要**: データ複製・品質向上処理

**パイプライン** (1/4実装):
1. **pi_Copy_marketing_client_dm**: マーケティング顧客DM複製

**特徴**:
- 大規模データ複製
- 品質向上処理
- 増分処理対応

### 7. MTGMASTER (mTGマスタードメイン)
**概要**: 顧客マスター管理

**パイプライン** (0/1実装):
- 今後実装予定

## パイプライン詳細

### データフロー標準パターン

#### ETL処理フロー図

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        ETL Processing Flow                                 │
└─────────────────────────────────────────────────────────────────────────────┘

        [Input Source]
             │
             ▼
┌─────────────────────────┐
│     Extract Phase       │
├─────────────────────────┤     ┌──────────────────┐
│ - TSV File Reading      │────▶│  File Validation │
│ - Data Format Check     │     │ - Existence Check│
│ - Encoding Validation   │     │ - Size Check     │
└─────────────────────────┘     │ - Format Check   │
             │                  └──────────────────┘
             ▼
┌─────────────────────────┐
│    Transform Phase      │
├─────────────────────────┤     ┌──────────────────┐
│ - Data Type Conversion  │────▶│ Business Rules   │
│ - Field Mapping         │     │ - ID Validation  │
│ - Calculation Logic     │     │ - Range Check    │
│ - Quality Enhancement   │     │ - Logic Rules    │
└─────────────────────────┘     └──────────────────┘
             │
             ▼
┌─────────────────────────┐
│      Load Phase         │
├─────────────────────────┤     ┌──────────────────┐
│ - CSV Generation        │────▶│ Output Services  │
│ - Metadata Addition     │     │ - Blob Storage   │
│ - Status Assignment     │     │ - SFTP Transfer  │
│ - Timestamp Addition    │     │ - Database Insert│
└─────────────────────────┘     └──────────────────┘
             │
             ▼
        [Output Destination]

Quality Gates:
┌─────────────┬─────────────┬─────────────┬─────────────┐
│Completeness │  Validity   │ Consistency │  Accuracy   │
│   ≥ 0.95    │   ≥ 0.80    │   ≥ 0.95    │   ≥ 0.90    │
└─────────────┴─────────────┴─────────────┴─────────────┘
```

#### テストカテゴリ実行フロー図

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Test Category Execution Flow                        │
└─────────────────────────────────────────────────────────────────────────────┘

┌────────────────┐    ┌────────────────┐    ┌────────────────┐    ┌─────────────────┐
│   FUNCTIONAL   │    │  DATA_QUALITY  │    │  PERFORMANCE   │    │  INTEGRATION    │
│     Tests      │    │     Tests      │    │     Tests      │    │     Tests       │
├────────────────┤    ├────────────────┤    ├────────────────┤    ├─────────────────┤
│ ✓ File Exists  │    │ ✓ Completeness │    │ ✓ Large Dataset│    │ ✓ SFTP Transfer │
│ ✓ File Missing │    │ ✓ Validity     │    │ ✓ Throughput   │    │ ✓ DB Operations │
│ ✓ Normal Logic │    │ ✓ Consistency  │    │ ✓ Memory Usage │    │ ✓ External APIs │
│ ✓ Error Cases  │    │ ✓ Business Rules│    │ ✓ Time Limits  │    │ ✓ Connectivity  │
└────────────────┘    └────────────────┘    └────────────────┘    └─────────────────┘
        │                      │                      │                      │
        └──────────────────────┼──────────────────────┼──────────────────────┘
                               │                      │
                               ▼                      ▼
                    ┌─────────────────────────────────────┐
                    │        Test Results                 │
                    ├─────────────────────────────────────┤
                    │ Status: SUCCEEDED/FAILED/SKIPPED    │
                    │ Quality Score: 0.0 - 1.0            │
                    │ Execution Time: seconds              │
                    │ Records: Extract/Transform/Load      │
                    │ Errors: List[str]                   │
                    │ Warnings: List[str]                 │
                    └─────────────────────────────────────┘
```

#### 入力データ形式
- **ファイル形式**: TSV (Tab-Separated Values)
- **ファイルパス**: `{domain}/{date}/{filename}.tsv`
- **エンコーディング**: UTF-8

#### 変換処理
1. **Extract**: TSVファイル読み込み
2. **Transform**: 
   - データ型変換
   - ビジネスルール適用
   - 品質チェック
   - 集計・計算処理
3. **Load**: CSV形式で出力

#### 出力データ形式
- **ファイル形式**: CSV (Comma-Separated Values)
- **ファイルパス**: `{domain}/processed_{date}.csv`
- **追加フィールド**: タイムスタンプ、ステータス

### E2E テストスコープ定義（ETL処理に特化）

## E2Eテストの責任範囲

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    System Architecture & Test Scope                        │
└─────────────────────────────────────────────────────────────────────────────┘

[前段システム]     [ADF Pipeline - E2E Test Scope]     [後続システム]
     │                        │                            │
     ▼                        ▼                            ▼
┌────────────┐    ┌─────────────────────────────┐    ┌────────────┐
│Business    │    │        ETL Process          │    │Business    │
│Logic       │───▶│  Extract → Transform → Load │───▶│Logic       │
│Validation  │    │                             │    │Processing  │
└────────────┘    └─────────────────────────────┘    └────────────┘
     ↑                        ↑                            ↑
Unit Tests          E2E Tests (This Framework)      Integration Tests
- ビジネスルール      - データ移動・変換              - 後続処理
- 計算ロジック        - ファイル形式変換              - API連携
- 条件分岐           - システム連携                  - 通知処理
```

#### pi_PointGrantEmail
**ADF ETL処理**: ポイント付与メール用データ準備

**ETL処理フロー**:
```python
# ADFパイプラインのETL処理のみに特化
def adf_etl_process():
    1. Copy Activity: TSVファイル読み込み (Azure Blob Storage)
    2. Data Flow: 基本的なデータ変換
       - フィールドマッピング (TSV → CSV)
       - データ型変換
       - NULL値・空値の除外
       - 重複レコード除去
    3. Copy Activity: CSV出力 (Azure Blob Storage)
    4. Copy Activity: SFTP転送
```

**E2Eテスト検証内容**:
- ✅ **ファイル読み込み**: Copy Activityによるソースファイル取得
- ✅ **データ変換**: Data Flowによる基本的な形式変換
  - TSV → CSV形式変換
  - フィールドマッピング正確性
  - データ型変換（文字列、数値、日付）
- ✅ **データ品質**: ETL処理での基本品質チェック
  - NULL値除外処理
  - 重複除去処理
  - フィールド数一致
- ✅ **ファイル出力**: 指定形式での正常出力
- ✅ **システム連携**: SFTP転送の成功

#### pi_PointLostEmail
**ADF ETL処理**: ポイント失効メール用データ準備

**ETL処理フロー**:
```python
def adf_etl_process():
    1. Copy Activity: TSVファイル読み込み (Azure Blob Storage)
    2. Data Flow: 基本的なデータ変換
       - 日付フィールド変換 (YYYYMMDD → CSV出力用)
       - 数値フィールド変換 (ポイント額)
       - NULL値・空値の除外
    3. Copy Activity: CSV出力 (Azure Blob Storage)
    4. Copy Activity: SFTP転送
```

**E2Eテスト検証内容**:
- ✅ **ファイル読み込み**: ソースファイル取得
- ✅ **データ変換**: 基本的な形式変換
- ✅ **データ品質**: 基本品質チェック
- ✅ **ファイル出力**: CSV形式出力
- ✅ **システム連携**: SFTP転送

#### pi_Ins_usageservice_mtgid
**ADF ETL処理**: 使用サービスデータのDB挿入

**ETL処理フロー**:
```python
def adf_etl_process():
    1. Copy Activity: 使用量データ読み込み
    2. Data Flow: データ変換
       - 数値データ型変換
       - 必須フィールド検証
       - フィールドマッピング
    3. Copy Activity: データベース挿入
```

**E2Eテスト検証内容**:
- ✅ **ファイル読み込み**: 使用量データ取得
- ✅ **データ変換**: 型変換とマッピング
- ✅ **データベース連携**: INSERT操作の実行
- ✅ **データ整合性**: 挿入前後のレコード数確認

#### pi_Send_PaymentAlert
**ADF ETL処理**: 支払いアラート用データ準備

**ETL処理フロー**:
```python
def adf_etl_process():
    1. Copy Activity: 請求データ読み込み
    2. Data Flow: 基本変換
       - 日付フィールド変換
       - 金額フィールド変換
       - フィールドマッピング
    3. Copy Activity: アラート配信システムへ転送
```

**E2Eテスト検証内容**:
- ✅ **ファイル読み込み**: 請求データ取得
- ✅ **データ変換**: 基本的な形式変換
- ✅ **データ出力**: 配信システム用形式
- ✅ **システム連携**: 転送処理

#### pi_Send_ClientDM
**ADF ETL処理**: 顧客DM配信用データ準備

**ETL処理フロー**:
```python
def adf_etl_process():
    1. Copy Activity: 顧客マスターデータ読み込み
    2. Data Flow: データ変換
       - フィールドマッピング
       - NULL値除外
       - 重複除去
    3. Copy Activity: DM配信システムへ転送
```

**E2Eテスト検証内容**:
- ✅ **ファイル読み込み**: 顧客データ取得
- ✅ **データ変換**: 基本変換処理
- ✅ **重複除去**: ETL レベルでの重複処理
- ✅ **システム連携**: 配信システム転送

#### pi_Send_karte_contract_score_info
**ADF ETL処理**: 契約スコア情報の外部システム連携

**ETL処理フロー**:
```python
def adf_etl_process():
    1. Copy Activity: 契約データ読み込み
    2. Data Flow: JSON形式変換
       - CSV → JSON変換
       - フィールドマッピング
       - 必須項目検証
    3. Copy Activity: 外部API用ファイル出力
```

**E2Eテスト検証内容**:
- ✅ **ファイル読み込み**: 契約データ取得
- ✅ **データ変換**: CSV → JSON変換
- ✅ **フォーマット検証**: JSON構造確認
- ✅ **システム連携**: 外部システム用出力

#### pi_Copy_marketing_client_dm
**ADF ETL処理**: マーケティングデータ複製

**ETL処理フロー**:
```python
def adf_etl_process():
    1. Copy Activity: ソースデータ読み込み
    2. Data Flow: データクレンジング
       - 基本的なデータ品質チェック
       - フィールド標準化
       - 重複除去
    3. Copy Activity: ターゲットシステム書き込み
```

**E2Eテスト検証内容**:
- ✅ **大量データ処理**: スケーラビリティ確認
- ✅ **データ複製**: 正確な複製処理
- ✅ **基本品質チェック**: ETLレベルの品質処理
- ✅ **システム間連携**: ソース→ターゲット移行

## 適切なE2Eテストスコープ

### E2Eテストカテゴリの再定義

#### 1. **FUNCTIONAL (機能テスト)** - ETL基本機能
- ✅ **ファイル処理**: 正常読み込み、ファイル不存在時の適切な処理
- ✅ **データ変換**: フォーマット変換（TSV→CSV、CSV→JSON）の正確性
- ✅ **システム連携**: SFTP転送、データベース接続、API連携の成功
- ❌ ~~ビジネスルール検証~~ *(前段システムの責務)*
- ❌ ~~計算ロジック~~ *(前段システムの責務)*

#### 2. **DATA_QUALITY (データ品質テスト)** - ETL品質保証
- ✅ **構造品質**: フィールド数一致、データ型正確性
- ✅ **完全性**: NULL値処理、空レコード除外
- ✅ **一貫性**: 入出力レコード数整合性
- ✅ **重複処理**: ETLレベルでの重複除去
- ❌ ~~業務ルール妥当性~~ *(前段システムの責務)*
- ❌ ~~詳細なデータ検証~~ *(前段システムの責務)*

#### 3. **PERFORMANCE (パフォーマンステスト)** - ETL性能
- ✅ **スループット**: データ処理速度 (records/sec)
- ✅ **スケーラビリティ**: 大量データ処理能力
- ✅ **リソース使用量**: メモリ、CPU使用率
- ✅ **実行時間**: SLA準拠 (30-60秒以内)

#### 4. **INTEGRATION (統合テスト)** - システム間連携
- ✅ **ストレージ連携**: Azure Blob Storage読み書き
- ✅ **データベース連携**: SQL Server/Azure SQL接続
- ✅ **SFTP連携**: 外部システムファイル転送
- ✅ **API連携**: REST API呼び出し
- ✅ **エラーハンドリング**: 連携失敗時の適切な処理

### 修正されたテスト検証ポイント

#### **ETL処理のコア検証**

**入力検証**:
```python
# ファイル存在確認
assert file_exists(source_path), "ソースファイル不存在"

# 基本フォーマット検証
assert is_valid_format(input_data), "入力フォーマット不正"
```

**変換処理検証**:
```python
# データ型変換の正確性
assert all_fields_converted_correctly(output_data), "データ型変換失敗"

# フィールドマッピング確認
assert field_mapping_correct(input_schema, output_schema), "マッピング不正"

# NULL値・空値処理
assert null_values_handled(output_data), "NULL値処理失敗"
```

**出力検証**:
```python
# 出力フォーマット確認
assert output_format_valid(output_data), "出力フォーマット不正"

# レコード数整合性
assert record_count_consistent(input_count, output_count), "レコード数不整合"

# ファイル転送成功
assert transfer_successful(target_system), "転送失敗"
```

### ADFパイプライン要素との対応

| ADF要素 | E2Eテスト検証内容 | 対象外（他テストの責務） |
|---------|------------------|----------------------|
| **Copy Activity** | ファイル読み込み・書き込み成功 | ファイル内容の業務妥当性 |
| **Data Flow** | フォーマット変換・マッピング | 複雑な計算・条件分岐 |
| **Lookup Activity** | マスター参照接続性 | 参照結果の業務判定 |
| **Conditional Activity** | 条件による分岐動作 | 条件の業務ロジック |
| **ForEach Activity** | ループ処理実行 | ループ内の業務処理 |

### テスト実装方針の変更

#### **簡素化されたビジネスルール検証**
```python
def validate_etl_data_quality(self, data: str) -> Dict[str, float]:
    """ETL処理の基本品質のみ検証"""
    lines = data.strip().split('\n')
    
    if len(lines) < 2:
        return {"completeness": 0.0, "validity": 0.0, "consistency": 0.0}
    
    # 構造的品質のみ検証
    total_records = len(lines) - 1
    valid_records = 0
    complete_records = 0
    
    for line in lines[1:]:
        parts = line.split(',')
        
        # フィールド数一致（構造的整合性）
        if len(parts) == len(self.output_columns):
            valid_records += 1
            
            # 空値でない（完全性）
            if all(part.strip() for part in parts):
                complete_records += 1
    
    return {
        "completeness": complete_records / total_records if total_records > 0 else 0.0,
        "validity": valid_records / total_records if total_records > 0 else 0.0,
        "consistency": 1.0 if valid_records == total_records else 0.0
    }
```

#### **焦点を絞ったテスト項目**

**FUNCTIONAL テスト**:
- ファイル有り/無し時の適切な処理
- データフォーマット変換の正確性
- システム間連携の成功

**PERFORMANCE テスト**:
- 実運用規模でのスループット確認
- メモリ使用量の妥当性
- SLA準拠の実行時間

**INTEGRATION テスト**:
- 外部システム接続性
- エラー時の適切なハンドリング
- ログ出力の正確性

このように、E2Eテストのスコープを**ETL処理の技術的側面**に特化することで、テストの責任分界を明確にし、効率的で保守性の高いテストフレームワークを実現しています。

## 実行方法

### テスト実行フロー図

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Test Execution Workflow                            │
└─────────────────────────────────────────────────────────────────────────────┘

    [User Input]
         │
         ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Command Line   │    │   Test Runner   │    │   Test Results  │
│   Interface     │───▶│   Orchestrator  │───▶│   Generation    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │              ┌────────┴────────┐              │
         │              │                 │              │
         ▼              ▼                 ▼              ▼
┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│Single Test  │ │Domain Tests │ │All Tests    │ │Report Files │
│Execution    │ │Execution    │ │Execution    │ │.txt/.json   │
└─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘

Execution Types:
┌─────────────────┬─────────────────┬─────────────────┬─────────────────┐
│   Individual    │    By Domain    │   By Category   │   Full Suite    │
│ Single Method   │  All in Domain  │  All Functional │   All 52 Tests  │
│ Quick Debug     │  Domain Focus   │  Quality Focus  │ Complete Check  │
│ < 1 second      │ < 5 seconds     │ < 10 seconds    │ < 15 seconds    │
└─────────────────┴─────────────────┴─────────────────┴─────────────────┘
```

### 全テスト実行
```bash
python3 e2e_v2/scripts/run_all_tests.py
```

### ドメイン別実行
```bash
# 検電ドメインのみ
python3 -c "
import sys
sys.path.insert(0, '.')
from e2e_v2.domains.kendenki.test_point_grant_email import TestPointGrantEmailPipeline
test = TestPointGrantEmailPipeline()
test.test_functional_with_file_exists()
"
```

### 単一テスト実行
```bash
python3 -c "
import sys
sys.path.insert(0, '.')
from e2e_v2.domains.{domain}.test_{pipeline} import Test{Pipeline}Pipeline
test = Test{Pipeline}Pipeline()
test.test_{method_name}()
"
```

### 実行時パラメータ
- **タイムアウト**: 60秒 (パフォーマンステスト), 30秒 (その他)
- **並列実行**: ドメイン単位
- **リトライ**: なし (fail-fast方式)

## 品質基準（ETL特化）

### ETLテスト成功条件
```python
def is_successful(self) -> bool:
    return (
        (self.status == PipelineStatus.SUCCEEDED or self.status == PipelineStatus.SKIPPED) and
        len(self.errors) == 0 and
        self.data_quality_score >= 0.90  # ETL品質基準
    )
```

### ETLデータ品質計算
```python
def validate_etl_data_quality(self, data: str, expected_columns: List[str] = None) -> Dict[str, float]:
    # completeness: NULL値・空値が適切に処理されている
    # validity: データ型変換が正しく行われている
    # consistency: フィールド数・構造が一致している
    # accuracy: フィールドマッピングが正確
```

### ETLパフォーマンス基準
| データ規模 | 最小スループット | 最大実行時間 | 対象処理 |
|-----------|----------------|-------------|---------|
| < 10K件   | 8,000 rec/sec  | 30秒        | Copy + Data Flow |
| 10K-100K件 | 5,000 rec/sec  | 45秒        | Copy + Data Flow + Transfer |
| > 100K件   | 3,000 rec/sec  | 60秒        | 大量データETL処理 |

## エラーハンドリング

### ETLエラー分類

#### 1. CRITICAL (致命的エラー) - システム障害
- **原因**: ADF接続障害、インフラ問題
- **処理**: 即座にテスト停止
- **例**: Azure接続エラー、認証失敗

#### 2. ERROR (エラー) - ETL処理失敗
- **原因**: データ変換失敗、システム連携失敗
- **処理**: 該当テスト失敗、他テスト継続
- **例**: Copy Activity失敗、SFTP転送失敗

#### 3. WARNING (警告) - ETL品質問題
- **原因**: データ品質基準未達、軽微な処理問題
- **処理**: 警告記録、テスト継続
- **例**: NULL値多数、変換処理でのレコード減少

### リカバリ機能
- **自動リトライ**: なし (明確な失敗理由特定のため)
- **部分実行**: ドメイン・パイプライン単位で継続
- **ログ保存**: 全実行ログの詳細記録

## レポート生成

### 出力形式

#### 1. テキストレポート (.txt)
```
================================================================================
E2E V2 パイプラインテスト実行レポート
================================================================================
実行日時: 2025/07/08 12:39:19 - 12:39:31
実行時間: 11.87秒

## 実行サマリ
- 対象パイプライン数: 10
- 総テスト数: 52
- 成功テスト数: 52
- 失敗テスト数: 0
- 成功率: 100.0%
- 平均実行時間: 0.23秒/テスト
```

#### 2. JSONレポート (.json)
```json
{
  "summary": {
    "total_pipelines": 10,
    "total_tests": 52,
    "passed_tests": 52,
    "failed_tests": 0,
    "success_rate": 100.0,
    "execution_time": 11.87
  },
  "domains": {
    "kendenki": {
      "pipelines": 3,
      "success_rate": 100.0,
      "tests": [...]
    }
  }
}
```

### ファイル保存先
```
e2e_v2/reports/
├── test_report_YYYYMMDD_HHMMSS.txt
└── test_report_YYYYMMDD_HHMMSS.json
```

### メトリクス項目
- **実行時間**: 総実行時間、テスト別実行時間
- **成功率**: 全体・ドメイン別・パイプライン別
- **品質スコア**: データ品質指標の統計値
- **エラー分析**: エラータイプ別集計

## 拡張方法

### 新規パイプライン追加

#### 1. テストクラス作成
```python
class TestNewPipelinePipeline(DomainTestBase):
    def __init__(self):
        super().__init__("pi_NewPipeline", "new_domain")
    
    def domain_specific_setup(self):
        # ドメイン固有設定
        pass
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        # テストデータテンプレート
        return {}
    
    def validate_domain_business_rules(self, data: str) -> List[str]:
        # ビジネスルール検証
        return []
```

#### 2. run_all_tests.py更新
```python
self.pipeline_tests = {
    "new_domain": {
        "pi_NewPipeline": TestNewPipelinePipeline
    }
}
```

### 新規ドメイン追加

#### 1. ディレクトリ作成
```
e2e_v2/domains/new_domain/
├── __init__.py
└── test_new_pipeline.py
```

#### 2. ドメイン固有ロジック実装
- データ生成ロジック
- 変換処理ロジック
- ビジネスルール検証ロジック

### 新規テストカテゴリ追加

#### 1. TestCategory拡張
```python
class TestCategory(Enum):
    FUNCTIONAL = "functional"
    DATA_QUALITY = "data_quality"
    PERFORMANCE = "performance"
    INTEGRATION = "integration"
    SECURITY = "security"  # 新規追加
```

#### 2. テストメソッド実装
```python
def test_security_validation(self):
    # セキュリティテストロジック
    pass
```

### モックサービス拡張

#### 新規サービス追加
```python
class MockNewService:
    def __init__(self):
        self.connections = {}
    
    def connect(self, config):
        # 接続処理
        pass
    
    def execute(self, operation):
        # 操作実行
        pass
```

### 品質基準カスタマイズ

#### ドメイン固有基準設定
```python
def get_domain_quality_thresholds(self) -> Dict[str, float]:
    return {
        "completeness": 0.98,
        "validity": 0.95,
        "consistency": 0.99,
        "accuracy": 0.92
    }
```

## 付録

### ファイル命名規則
- テストクラス: `Test{PipelineName}Pipeline`
- テストメソッド: `test_{category}_{description}`
- ファイル名: `test_{pipeline_name}.py`

### 推奨開発フロー
1. 要件定義・設計
2. テストクラス作成
3. データ生成ロジック実装
4. 変換処理ロジック実装
5. ビジネスルール検証実装
6. 単体テスト実行
7. 統合テスト実行
8. レビュー・品質確認

### トラブルシューティング

#### よくある問題
1. **ImportError**: sys.path設定確認
2. **FileNotFoundError**: ファイルパス確認
3. **AssertionError**: テストデータ・期待値確認
4. **TimeoutError**: 処理時間・データサイズ確認

#### デバッグ方法
```python
# ログレベル調整
logging.basicConfig(level=logging.DEBUG)

# 個別テスト実行
test = TestPipelinePipeline()
test.test_functional_with_file_exists()

# データ内容確認
print(f"Generated data: {test_data}")
print(f"Transformed data: {transformed_data}")
```

---

**Document Version**: 1.0  
**Last Updated**: 2025-07-08  
**Author**: Claude Code Assistant  
**Status**: Production Ready