# ドメイン駆動設計モデル仕様書

## 📋 概要

本ドキュメントは、Azure Data Factory (ADF) パイプライン開発・テスト・デプロイ統合環境をドメイン駆動設計（DDD）のプラクティスに従ってモデリングした結果を記述します。

---

## 🏗️ ドメインアーキテクチャ

### ドメイン階層構造

```
データ処理プラットフォームドメイン
├── コアドメイン (Core Domain)
│   ├── パイプライン実行ドメイン
│   ├── データ品質保証ドメイン
│   └── テスト自動化ドメイン
├── サポートドメイン (Supporting Domain)
│   ├── 接続管理ドメイン
│   ├── 設定管理ドメイン
│   └── ログ・監視ドメイン
└── 汎用ドメイン (Generic Domain)
    ├── 認証・認可ドメイン
    ├── ファイル管理ドメイン
    └── 通知・アラートドメイン
```

---

## 🎯 コアドメイン

### 1. パイプライン実行ドメイン (Pipeline Execution Domain)

#### 集約ルート (Aggregate Root)
**`Pipeline`** - パイプライン実行の一貫性を保証

#### エンティティ (Entities)
- **`Pipeline`**: パイプラインの実行単位
- **`PipelineRun`**: パイプライン実行インスタンス
- **`Activity`**: パイプライン内の活動単位
- **`ActivityRun`**: 活動実行インスタンス

#### 値オブジェクト (Value Objects)
- **`PipelineId`**: パイプライン識別子
- **`RunId`**: 実行識別子
- **`ExecutionStatus`**: 実行状態（PENDING, RUNNING, SUCCESS, FAILED, CANCELLED）
- **`Duration`**: 実行時間
- **`TriggerType`**: トリガー種別（MANUAL, SCHEDULED, EVENT_DRIVEN）

#### ドメインサービス (Domain Services)
- **`PipelineExecutionService`**: パイプライン実行ロジック
- **`DependencyResolver`**: 依存関係解決
- **`ResourceAllocator`**: リソース割り当て

#### リポジトリ (Repository)
- **`PipelineRepository`**: パイプライン永続化
- **`PipelineRunRepository`**: 実行履歴永続化

```python
# ドメインモデル例
class Pipeline:
    def __init__(self, pipeline_id: PipelineId, name: str, definition: dict):
        self.pipeline_id = pipeline_id
        self.name = name
        self.definition = definition
        self.activities: List[Activity] = []
        
    def execute(self, trigger: TriggerType, parameters: dict = None) -> PipelineRun:
        # ビジネスルール: 実行前検証
        self._validate_execution_prerequisites()
        
        run_id = RunId.generate()
        pipeline_run = PipelineRun(run_id, self.pipeline_id, trigger, parameters)
        
        # ドメインイベント発行
        pipeline_run.add_domain_event(PipelineExecutionStarted(run_id))
        
        return pipeline_run
```

### 2. データ品質保証ドメイン (Data Quality Assurance Domain)

#### 集約ルート
**`DataQualityProfile`** - データ品質管理の一貫性を保証

#### エンティティ
- **`DataSource`**: データソース
- **`QualityRule`**: 品質ルール
- **`QualityCheck`**: 品質チェック実行
- **`QualityMetric`**: 品質メトリクス

#### 値オブジェクト
- **`DataSourceId`**: データソース識別子
- **`QualityScore`**: 品質スコア（0-100）
- **`ValidationRule`**: 検証ルール
- **`Threshold`**: 閾値設定

#### ドメインサービス
- **`QualityValidator`**: データ品質検証
- **`MetricsCalculator`**: メトリクス計算
- **`AnomalyDetector`**: 異常検知

### 3. テスト自動化ドメイン (Test Automation Domain)

#### 集約ルート
**`TestSuite`** - テスト実行の一貫性を保証

#### エンティティ
- **`TestCase`**: テストケース
- **`TestExecution`**: テスト実行
- **`TestResult`**: テスト結果
- **`TestEnvironment`**: テスト環境

#### 値オブジェクト
- **`TestId`**: テスト識別子
- **`TestType`**: テスト種別（UNIT, INTEGRATION, E2E, PERFORMANCE）
- **`TestStatus`**: テスト状態（PASS, FAIL, SKIP, ERROR）
- **`Coverage`**: カバレッジ率

---

## 🤝 サポートドメイン

### 1. 接続管理ドメイン (Connection Management Domain)

#### 集約ルート
**`ConnectionPool`** - 接続プールの管理

#### エンティティ
- **`Connection`**: データベース・サービス接続
- **`LinkedService`**: ADF リンクサービス
- **`Dataset`**: データセット

#### 値オブジェクト
- **`ConnectionString`**: 接続文字列
- **`Credential`**: 認証情報
- **`Endpoint`**: エンドポイント

### 2. 設定管理ドメイン (Configuration Management Domain)

#### 集約ルート
**`Configuration`** - 設定管理の一貫性

#### 値オブジェクト
- **`EnvironmentConfig`**: 環境設定
- **`ParameterSet`**: パラメータセット
- **`DeploymentConfig`**: デプロイ設定

---

## 🌐 境界づけられたコンテキスト (Bounded Context)

### 1. Pipeline Execution Context
**責務**: パイプライン実行・監視・制御

**主要エンティティ**:
- Pipeline, PipelineRun, Activity, ActivityRun

**境界**:
- パイプライン定義から実行完了まで
- 他コンテキストとはイベント経由で連携

### 2. Data Quality Context
**責務**: データ品質の定義・検証・改善

**主要エンティティ**:
- DataSource, QualityRule, QualityCheck

**境界**:
- データ品質ルール定義から検証結果まで
- Pipeline Execution Contextと品質チェック連携

### 3. Test Automation Context
**責務**: テスト計画・実行・レポート

**主要エンティティ**:
- TestSuite, TestCase, TestExecution

**境界**:
- テスト設計から結果レポートまで
- CI/CDパイプライン連携

### 4. Infrastructure Context
**責務**: インフラ管理・監視・運用

**主要エンティティ**:
- Connection, LinkedService, Configuration

---

## 📚 ユビキタス言語 (Ubiquitous Language)

### パイプライン実行ドメイン
| 用語 | 定義 | 例 |
|------|------|-----|
| **Pipeline** | データ処理フローの定義単位 | `pipeline_marketing_client_dm` |
| **Activity** | パイプライン内の処理単位 | Copy Activity, Execute Pipeline |
| **Trigger** | パイプライン実行の起動条件 | Schedule Trigger, Tumbling Window |
| **Run** | パイプライン・活動の実行インスタンス | `run_20250705_143025` |
| **Dependency** | 活動間の依存関係 | Success, Failure, Completion |

### データ品質ドメイン
| 用語 | 定義 | 例 |
|------|------|-----|
| **Data Source** | データの提供元 | SQL Database, Blob Storage |
| **Quality Rule** | データ品質を判定する規則 | NOT NULL制約, 値域チェック |
| **Quality Score** | データ品質の数値評価 | 0-100の百分率 |
| **Anomaly** | 品質基準から外れたデータ | 異常値, 欠損値 |

### テスト自動化ドメイン
| 用語 | 定義 | 例 |
|------|------|-----|
| **Test Case** | 検証すべき機能・シナリオ | `test_pipeline_execution_success` |
| **Test Suite** | 関連するテストケースのグループ | E2E Test Suite, Unit Test Suite |
| **Coverage** | テストがカバーする範囲の割合 | Code Coverage, Functional Coverage |
| **Mock** | テスト用の代替実装 | Database Mock, Service Mock |

---

## 🔄 ドメインイベント (Domain Events)

### Pipeline Execution Context
- **`PipelineExecutionStarted`**: パイプライン実行開始
- **`ActivityCompleted`**: 活動完了
- **`PipelineExecutionCompleted`**: パイプライン実行完了
- **`PipelineExecutionFailed`**: パイプライン実行失敗

### Data Quality Context
- **`QualityCheckCompleted`**: 品質チェック完了
- **`AnomalyDetected`**: 異常検知
- **`QualityThresholdExceeded`**: 品質閾値超過

### Test Automation Context
- **`TestSuiteStarted`**: テストスイート開始
- **`TestCaseCompleted`**: テストケース完了
- **`TestSuiteCompleted`**: テストスイート完了

---

## 🏛️ アーキテクチャパターン

### レイヤードアーキテクチャ
```
┌─────────────────────────────┐
│ Presentation Layer          │  ← CLI, Web UI, API
├─────────────────────────────┤
│ Application Layer           │  ← Use Cases, Application Services
├─────────────────────────────┤
│ Domain Layer                │  ← Entities, Value Objects, Domain Services
├─────────────────────────────┤
│ Infrastructure Layer        │  ← Repositories, External Services
└─────────────────────────────┘
```

### ヘキサゴナルアーキテクチャ（ポート＆アダプター）
```
        ┌─────────────────┐
        │   Azure Portal  │
        └─────────┬───────┘
                  │
    ┌─────────────▼─────────────┐
    │      Web API Adapter      │
    └─────────────┬─────────────┘
                  │
        ┌─────────▼─────────┐
        │   Application     │
        │     Services      │
        └─────────┬─────────┘
                  │
        ┌─────────▼─────────┐
        │   Domain Model    │
        └─────────┬─────────┘
                  │
    ┌─────────────▼─────────────┐
    │  Infrastructure Adapters  │
    ├───────────┬───────────────┤
    │ Azure ADF │ SQL Database  │
    └───────────┴───────────────┘
```

---

## 🧩 アプリケーションサービス (Application Services)

### PipelineExecutionApplicationService
```python
class PipelineExecutionApplicationService:
    def __init__(
        self, 
        pipeline_repo: PipelineRepository,
        execution_service: PipelineExecutionService,
        event_publisher: EventPublisher
    ):
        self.pipeline_repo = pipeline_repo
        self.execution_service = execution_service
        self.event_publisher = event_publisher
    
    def execute_pipeline(self, command: ExecutePipelineCommand) -> PipelineRunId:
        """パイプライン実行ユースケース"""
        pipeline = self.pipeline_repo.find_by_id(command.pipeline_id)
        if not pipeline:
            raise PipelineNotFoundException(command.pipeline_id)
        
        # ドメインロジック実行
        pipeline_run = self.execution_service.execute(
            pipeline, 
            command.trigger_type, 
            command.parameters
        )
        
        # イベント発行
        self.event_publisher.publish(pipeline_run.domain_events)
        
        return pipeline_run.run_id
```

### TestExecutionApplicationService
```python
class TestExecutionApplicationService:
    def execute_test_suite(self, command: ExecuteTestSuiteCommand) -> TestSuiteResult:
        """テストスイート実行ユースケース"""
        test_suite = self.test_repo.find_by_id(command.suite_id)
        
        # テスト環境準備
        environment = self.environment_service.prepare(command.environment_config)
        
        # テスト実行
        result = self.test_executor.execute(test_suite, environment)
        
        # 結果保存
        self.result_repo.save(result)
        
        return result
```

---

## 📊 メトリクスとKPI

### パイプライン実行メトリクス
- **成功率**: 正常完了したパイプライン実行の割合
- **平均実行時間**: パイプライン実行の平均所要時間
- **スループット**: 単位時間あたりの処理データ量
- **リソース使用率**: CPU、メモリ、ネットワークの使用率

### データ品質メトリクス
- **品質スコア**: データ品質の総合評価（0-100）
- **異常検知率**: 異常データの検出割合
- **修復率**: 検出された問題の修復割合

### テスト実行メトリクス
- **テストカバレッジ**: コード・機能カバレッジ率
- **テスト成功率**: 成功したテストケースの割合
- **実行時間**: テストスイート実行の所要時間

---

## 🔧 実装ガイドライン

### 1. 集約の境界設計
- 1つの集約は1つのトランザクション境界
- 集約間の参照はIDのみ
- 結果整合性を許容する設計

### 2. ドメインイベントの活用
- 集約内での状態変更時にイベント発行
- 非同期処理による疎結合化
- イベントソーシングの部分的適用

### 3. リポジトリパターン
- インターフェース定義はドメイン層
- 実装はインフラストラクチャ層
- ユニットテストではモック使用

### 4. 値オブジェクトの活用
- プリミティブ型の代替
- 不変性の保証
- ビジネスルールの封じ込め

---

## 📝 今後の拡張指針

### 1. イベントソーシング導入
- パイプライン実行履歴の完全保持
- 時系列データ分析の強化
- 監査要件への対応

### 2. CQRS パターン適用
- 読み取り専用モデルの最適化
- レポート生成の高速化
- スケーラビリティ向上

### 3. マイクロサービス化
- 境界づけられたコンテキスト単位での分割
- 独立したデプロイ・スケーリング
- 技術スタックの多様化

---

**更新日**: 2025年7月5日  
**作成者**: DDD モデリングチーム  
**承認者**: アーキテクチャチーム
