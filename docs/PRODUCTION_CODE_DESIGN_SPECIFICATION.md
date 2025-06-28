# Azure Data Factory プロダクションコード設計書

## 📋 目次

1. [システム概要](#システム概要)
2. [アーキテクチャ設計](#アーキテクチャ設計)
3. [パイプライン設計仕様](#パイプライン設計仕様)
4. [データ処理フロー](#データ処理フロー)
5. [セキュリティ設計](#セキュリティ設計)
6. [インフラ構成](#インフラ構成)
7. [モニタリング設計](#モニタリング設計)
8. [災害復旧設計](#災害復旧設計)

---

## 📊 システム概要

### システム名称

**Azure Data Factory 統合データパイプラインシステム**

### 目的・概要

Azure Data Factory（ADF）を中核とする大規模データ統合・変換・配信プラットフォーム。顧客データ、電力契約情報、支払い方法、マーケティング情報等の多様なデータソースを統合し、リアルタイム及びバッチ処理によるデータ変換・配信を実現。

### システム特徴

- **37+のパイプライン**による包括的データ処理
- **460列の大規模データ構造**対応
- **SQL外部化による保守性向上**
- **Docker化によるポータブルな開発環境**
- **CI/CD統合による継続的デプロイメント**

### 処理規模

- **データ処理件数**: 100万件/日以上
- **バッチ処理時間**: 1-4時間/実行
- **リアルタイム処理**: 1000件/秒
- **データ保持期間**: 7年間（法的要件準拠）

---

## 🏗️ アーキテクチャ設計

### システム全体アーキテクチャ

```
┌─────────────────────────────────────────────────────────────┐
│                    Azure Cloud Platform                     │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌───────────────┐ │
│  │   Data Sources  │  │  Data Factory   │  │  Data Sinks   │ │
│  ├─────────────────┤  ├─────────────────┤  ├───────────────┤ │
│  │ • SQL MI (CRM)  │──┤ • 37+ Pipelines │──┤ • Blob Storage│ │
│  │ • SQL DW        │  │ • Data Flows    │  │ • SFTP Server │ │
│  │ • API Services  │  │ • Integration   │  │ • Email Queue │ │
│  │ • File Sources  │  │   Runtime       │  │ • Analytics   │ │
│  └─────────────────┘  └─────────────────┘  └───────────────┘ │
│                           │                                  │
│  ┌─────────────────────────┼─────────────────────────────────┐ │
│  │        Monitoring & Management Layer                     │ │
│  ├─────────────────────────┼─────────────────────────────────┤ │
│  │ • Azure Monitor         │ • Key Vault (Secrets)          │ │
│  │ • Application Insights  │ • RBAC (Security)              │ │
│  │ • Log Analytics         │ • Private Endpoints            │ │
│  └─────────────────────────┼─────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### レイヤード・アーキテクチャ

#### 1. データ取得層 (Data Ingestion Layer)

```
┌──────────────────────────────────────────┐
│            Data Sources                  │
├──────────────────────────────────────────┤
│ • SQL Managed Instance (顧客マスタ)       │
│ • Azure SQL DW (データウェアハウス)       │
│ • External APIs (契約管理システム)        │
│ • File Systems (CSV/JSON/XML)           │
│ • Real-time Streams (IoT/Event Hub)     │
└──────────────────────────────────────────┘
```

#### 2. データ統合層 (Data Integration Layer)

```
┌──────────────────────────────────────────┐
│         Azure Data Factory               │
├──────────────────────────────────────────┤
│ [パイプライン分類]                        │
│ • 顧客関連 (12パイプライン)               │
│   - pi_Copy_marketing_client_dm          │
│   - pi_Insert_ClientDM_Bx                │
│   - pi_Send_ClientDM                     │
│                                          │
│ • 支払い関連 (8パイプライン)              │
│   - pi_Send_PaymentMethodMaster          │
│   - pi_Send_PaymentMethodChanged         │
│   - pi_Send_PaymentAlert                 │
│                                          │
│ • 契約関連 (7パイプライン)                │
│   - pi_Send_ElectricityContractThanks    │
│   - pi_Send_OpeningPaymentGuide          │
│   - pi_Send_karte_contract_score_info    │
│                                          │
│ • その他業務 (10パイプライン)             │
│   - pi_Send_ActionPointCurrentMonthEntry │
│   - pi_Send_MovingPromotionTemplate      │
│   - LIM系各種パイプライン                │
└──────────────────────────────────────────┘
```

#### 3. データ変換層 (Data Transformation Layer)

```
┌──────────────────────────────────────────┐
│          Data Flows                      │
├──────────────────────────────────────────┤
│ • df_json_data_blob_only                 │
│ • df_csv_data_transformation             │
│ • df_marketing_client_dm_460_columns     │
│ • df_payment_method_validation           │
│ • df_contract_score_calculation          │
└──────────────────────────────────────────┘
```

#### 4. データ配信層 (Data Distribution Layer)

```
┌──────────────────────────────────────────┐
│           Output Destinations            │
├──────────────────────────────────────────┤
│ • Azure Blob Storage (CSV/JSON/Parquet) │
│ • SFTP Servers (外部システム連携)        │
│ • Email Services (通知・レポート)        │
│ • Analytics Platforms (BI/レポート)      │
│ • API Endpoints (リアルタイム配信)       │
└──────────────────────────────────────────┘
```

---

## 🔄 パイプライン設計仕様

### 主要パイプライン群

#### 1. 顧客データ管理パイプライン群

##### `pi_Copy_marketing_client_dm`

- **目的**: マーケティング用顧客DMの460列データ統合
- **入力**: SQL MI 顧客マスタ、契約情報、行動履歴
- **処理**: 460列データ統合、NULL値補完、データ品質検証
- **出力**: omni_ods_marketing_trn_client_dm テーブル
- **実行頻度**: 日次 (午前3時)
- **想定処理時間**: 45-60分
- **重要度**: 極高 (他パイプラインの依存元)

```sql
-- 主要カラムグループ
CRITICAL_COLUMN_GROUPS = {
    "core_client": ["CLIENT_KEY_AX", "REC_REG_YMD", "REC_UPD_YMD"],
    "liveness": ["LIV0EU_*"],  -- 157カラム
    "tes_system": ["TESHSMC_*", "TESHSEQ_*"],  -- 89カラム
    "electric_contract": ["EPCISCRT_*"],  -- 76カラム
    "web_history": ["WEBHIS_*"]  -- 138カラム
}
```

##### `pi_Insert_ClientDM_Bx`

- **目的**: 顧客DMにBXフラグ付与とセグメント分析
- **依存**: pi_Copy_marketing_client_dm
- **処理**: BXフラグ算出、顧客セグメント分類、スコア計算
- **出力**: ClientDmBx テーブル
- **実行頻度**: 日次 (顧客DMパイプライン後)

#### 2. 支払い関連パイプライン群

##### `pi_Send_PaymentMethodMaster`

- **目的**: 支払い方法マスタデータの SFMC 連携
- **入力**: 支払い方法マスタ、顧客支払い設定
- **処理**: 支払い方法変更検知、データ整形、CSV生成
- **出力**: PaymentMethodMaster_YYYYMMDD.csv.gz (SFTP)
- **実行頻度**: 日次

##### `pi_Send_PaymentMethodChanged`

- **目的**: 支払い方法変更通知の自動配信
- **入力**: 支払い方法変更ログ、顧客連絡先
- **処理**: 変更検知、通知対象判定、メール内容生成
- **出力**: 支払い方法変更通知メール
- **実行頻度**: リアルタイム (変更検知トリガー)

#### 3. 契約関連パイプライン群

##### `pi_Send_ElectricityContractThanks`

- **目的**: 電力契約完了感謝メールの配信
- **入力**: 契約完了情報、顧客基本情報
- **処理**: 契約内容確認、感謝メール生成、配信タイミング制御
- **出力**: 契約感謝メール、配信ログ
- **実行頻度**: イベント駆動

##### `pi_Send_karte_contract_score_info`

- **目的**: KARTE向け契約スコア情報の JSON 配信
- **入力**: 契約情報、顧客スコア、行動履歴
- **処理**: スコア計算、JSON形式変換、ハッシュ化
- **出力**: TGContractScore_YYYYMMDD.json (S3)
- **実行頻度**: 日次

### データフロー設計

#### 共通データフロー: `df_json_data_blob_only`

```json
{
  "transformations": [
    {
      "name": "contractscoreColumn",
      "description": "TG契約情報をJSON形式に変換",
      "mapping": {
        "HASHED_MTGID": "string",
        "INTERNAL_AREA_GAS": "string", 
        "EXTERNAL_AREA_GAS": "string",
        "POWER": "string",
        "OUTPUT_DATETIME": "timestamp"
      }
    }
  ],
  "source": "ds_contract_score",
  "sink": "ds_Json_Blob",
  "parameters": {
    "filename": "動的ファイル名生成"
  }
}
```

---

## 📊 データ処理フロー

### 1. バッチ処理フロー

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Source Extract │───▶│  Transform      │───▶│  Load & Deliver │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ • SQL Query     │    │ • Data Mapping  │    │ • CSV Generation│
│ • API Call      │    │ • Validation    │    │ • Compression   │
│ • File Read     │    │ • Enrichment    │    │ • SFTP Transfer │
│ • Incremental   │    │ • Aggregation   │    │ • Notification  │
│   Delta Load    │    │ • Quality Check │    │ • Log Recording │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### 2. リアルタイム処理フロー

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Event Trigger  │───▶│  Stream Process │───▶│  Immediate Act. │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ • Data Change   │    │ • Event Filter  │    │ • Email Send    │
│ • File Upload   │    │ • Enrichment    │    │ • API Call      │
│ • Schedule      │    │ • Validation    │    │ • Status Update │
│ • Manual Start  │    │ • Routing       │    │ • Alert         │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### 3. データ品質管理フロー

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Quality Check  │───▶│  Error Handle   │───▶│  Recovery Act.  │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ • Schema Valid. │    │ • Error Detect  │    │ • Retry Logic   │
│ • Data Type     │    │ • Alert Send    │    │ • Fallback      │
│ • Business Rule │    │ • Log Record    │    │ • Manual Inter. │
│ • Completeness  │    │ • Stop Pipeline │    │ • Data Repair   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

---

## 🔒 セキュリティ設計

### 認証・認可設計

#### 1. Azure Active Directory 統合

```
┌─────────────────────────────────────────┐
│           AAD Integration               │
├─────────────────────────────────────────┤
│ • Service Principal Authentication      │
│ • Managed Identity (推奨)               │
│ • RBAC Role Assignment                  │
│ • Conditional Access Policy            │
│ • Multi-Factor Authentication          │
└─────────────────────────────────────────┘
```

#### 2. シークレット管理

```
┌─────────────────────────────────────────┐
│           Azure Key Vault               │
├─────────────────────────────────────────┤
│ • Connection Strings                    │
│ • API Keys                              │
│ • Certificates                          │
│ • Encryption Keys                       │
│ • Access Policies                       │
└─────────────────────────────────────────┘
```

### データ保護設計

#### 1. 暗号化戦略

- **保存時暗号化**: Azure Storage SSE、SQL TDE
- **転送時暗号化**: HTTPS/TLS 1.2+、SFTP
- **処理時暗号化**: Always Encrypted（機密データ）

#### 2. 個人情報保護

```sql
-- ハッシュ化処理例 (GDPR準拠)
HASHED_MTGID = HASHBYTES('SHA256', CONCAT(MTGID, SECRET_SALT))
MASKED_EMAIL = CONCAT(LEFT(email, 3), '***@', SUBSTRING(email, CHARINDEX('@', email) + 1, LEN(email)))
```

#### 3. アクセス制御

- **ネットワーク**: Private Endpoint、NSG
- **データ**: Column-level Security、Row-level Security
- **API**: API Management、Rate Limiting

---

## 🏭 インフラ構成

### Azure リソース構成

#### 1. コア・コンピューティング

```
Azure Data Factory (主要)
├── Integration Runtime
│   ├── Azure IR (メイン処理)
│   ├── Self-hosted IR (オンプレ連携)
│   └── SSIS IR (レガシー処理)
├── Data Flows (32個)
├── Pipelines (37個)
├── Datasets (50+個)
└── Linked Services (15個)
```

#### 2. データストレージ

```
Azure Storage Account
├── Blob Storage
│   ├── Raw Data Container
│   ├── Processed Data Container
│   ├── Archive Container
│   └── Log Container
├── File Shares (SFTP用)
└── Queue Storage (メッセージング)

Azure SQL Database
├── Metadata Store
├── Process Control DB
└── Audit Log DB
```

#### 3. セキュリティ・管理

```
Azure Key Vault
├── Connection Strings
├── API Keys
├── Certificates
└── Encryption Keys

Azure Monitor
├── Application Insights
├── Log Analytics
├── Alerts & Actions
└── Dashboards
```

### ネットワーク設計

#### 1. 仮想ネットワーク構成

```
┌─────────────────────────────────────────┐
│           Virtual Network               │
├─────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────────────┐ │
│  │ ADF Subnet  │  │ Storage Subnet      │ │
│  │ 10.0.1.0/24 │  │ 10.0.2.0/24         │ │
│  └─────────────┘  └─────────────────────┘ │
│  ┌─────────────┐  ┌─────────────────────┐ │
│  │ SQL Subnet  │  │ Management Subnet   │ │
│  │ 10.0.3.0/24 │  │ 10.0.4.0/24         │ │
│  └─────────────┘  └─────────────────────┘ │
└─────────────────────────────────────────┘
```

#### 2. プライベート エンドポイント

- Azure Data Factory: プライベート エンドポイント経由アクセス
- Storage Account: Blob、File、Queue サービス用
- SQL Database: プライベート DNS ゾーン統合
- Key Vault: シークレット アクセス専用

---

## 📊 モニタリング設計

### 1. パフォーマンス監視

#### パイプライン実行監視

```json
{
  "monitoring_metrics": {
    "execution_time": {
      "threshold": "60_minutes",
      "alert_level": "warning"
    },
    "success_rate": {
      "threshold": "95%",
      "alert_level": "critical"
    },
    "data_volume": {
      "threshold": "1M_records",
      "alert_level": "info"
    },
    "resource_utilization": {
      "cpu_threshold": "80%",
      "memory_threshold": "75%",
      "alert_level": "warning"
    }
  }
}
```

#### カスタム メトリクス

```sql
-- パイプライン実行統計
SELECT 
    pipeline_name,
    execution_date,
    start_time,
    end_time,
    DATEDIFF(MINUTE, start_time, end_time) as duration_minutes,
    status,
    records_processed,
    error_message
FROM pipeline_execution_log
WHERE execution_date >= DATEADD(DAY, -7, GETDATE())
```

### 2. ビジネス監視

#### データ品質ダッシュボード

```
┌─────────────────────────────────────────┐
│        Data Quality Dashboard           │
├─────────────────────────────────────────┤
│ • レコード処理数 (日次/累計)             │
│ • データ品質スコア (テーブル別)          │
│ • エラー率 (パイプライン別)             │
│ • SLA達成率 (99.5%目標)                │
│ • ビジネスルール違反件数                │
└─────────────────────────────────────────┘
```

### 3. アラート設計

#### 段階的アラート戦略

```
Level 1: Info (ログ記録のみ)
├── 正常完了通知
├── 軽微な警告
└── 性能情報

Level 2: Warning (担当者通知)
├── 実行時間超過
├── データ量異常
└── 品質スコア低下

Level 3: Critical (即座に対応)
├── パイプライン失敗
├── セキュリティ違反
└── システム停止
```

---

## 🔄 災害復旧設計

### 1. バックアップ戦略

#### データ バックアップ

```
Primary Region (East Japan)
├── Production Data Factory
├── Live Data Storage
└── Real-time Processing

Secondary Region (West Japan)
├── DR Data Factory (Cold Standby)
├── Backup Data Storage
└── Emergency Processing
```

#### メタデータ バックアップ

- ADF 定義: ARM テンプレート (Git管理)
- 設定情報: Key Vault レプリケーション
- スクリプト: GitHub Enterprise 保管

### 2. 復旧手順

#### RTO/RPO 目標

```
サービス分類          RTO     RPO     復旧優先度
────────────────────────────────────────────
Critical Pipeline    2時間   15分    最高
Business Pipeline    4時間   1時間   高
Report Pipeline      8時間   4時間   中
Archive Pipeline     24時間  24時間  低
```

#### 復旧シナリオ別手順

1. **部分障害**: 自動フェイルオーバー
2. **リージョン障害**: 手動 DR 切り替え
3. **データ破損**: バックアップからの復元
4. **セキュリティ侵害**: 緊急停止・調査・復旧

---

## 📈 スケーラビリティ設計

### 1. 水平スケーリング

- **Integration Runtime**: 複数ノード分散処理
- **Data Flow**: パラレル実行設定
- **Storage**: パーティション分割戦略

### 2. 垂直スケーリング

- **Compute**: 自動スケール設定
- **Memory**: Large Memory インスタンス
- **I/O**: Premium Storage 利用

### 3. コスト最適化

- **Reserved Instances**: 予約インスタンス利用
- **Spot Instances**: 非クリティカル処理用
- **Auto-pause**: 非使用時自動停止

---

## 🔧 運用・保守設計

### 1. デプロイメント戦略

#### CI/CD パイプライン

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Development   │───▶│     Testing     │───▶│   Production    │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ • Feature Branch│    │ • Integration   │    │ • Blue-Green    │
│ • Unit Tests    │    │ • E2E Tests     │    │ • Health Check  │
│ • Code Review   │    │ • Performance   │    │ • Rollback Plan │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### 2. 変更管理

#### 変更分類と承認プロセス

```
Standard Change (事前承認済み)
├── 設定値変更
├── スケジュール調整
└── 軽微な修正

Normal Change (変更審査委員会)
├── パイプライン追加
├── データフロー変更
└── 新機能追加

Emergency Change (緊急対応)
├── セキュリティ修正
├── 障害対応
└── 業務停止回避
```

### 3. 文書化戦略

- **API Documentation**: Swagger/OpenAPI
- **パイプライン仕様**: 自動生成
- **運用手順書**: Confluence
- **障害対応**: Run Book 形式

---

*このドキュメントは2024年12月作成。最新情報はGitHubリポジトリを参照。*
*設計者: Azure Data Engineering Team*
*レビュー: システムアーキテクト, セキュリティ担当者*
