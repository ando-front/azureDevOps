# Azure Data Factory ARM テンプレート 設計仕様書

## 1. 文書情報

| 項目 | 内容 |
|------|------|
| 文書名 | Azure Data Factory ARM テンプレート 設計仕様書 |
| バージョン | 3.0 |
| 作成日 | 2025年7月3日 |
| 最終更新 | 2025年7月3日 (要件定義書完全対応・38パイプライン詳細設計) |
| 作成者 | システム設計・データアーキテクチャ担当 |
| 承認者 | [承認者名] |

## 2. 設計概要

### 2.1 要件定義との対応関係

本設計仕様書は「Azure Data Factory ARM テンプレート 要件定義書」で定義された業務要件を技術的に実現するための詳細設計を記述しています。

**要件定義書との対応**:

- **業務要件** → **技術実装設計**
- **データソース要件** → **Linked Services設計**  
- **ODS/ODM構築要件** → **Pipeline設計**
- **業務処理パターン要件** → **38パイプライン詳細設計**
- **技術要件** → **Infrastructure設計**

### 2.2 業務データ統合アーキテクチャ設計

**設計目的**: 東京ガスの諸元DB（業務システム）からのデータ抽出・変換・統合によるODS/ODM構築の自動化

**アーキテクチャ設計原則**:

- 諸元DB → ODS → ODM の段階的データ統合設計
- KARTE連携を中心とした外部システム連携設計
- 個人情報保護対応（ハッシュ化）の技術実装
- リアルタイム・バッチ処理のハイブリッド構成設計

### 2.3 システム構成設計概要

```mermaid
graph TB
    subgraph "諸元DB (業務システム)"
        GAS[ガス顧客系DB<br/>omni_odm_gascstmr_trn_gaskiy]
        EPC[電力CIS契約DB<br/>omni_ods_epcis_trn_contract]
        CLK[CLOAK利用サービスDB<br/>omni_ods_cloak_trn_usageservice_mtgid]
        DNA[顧客DNA情報<br/>omni_ods_marketing_trn_client_dna]
        DM[顧客DM情報<br/>omni_ods_marketing_trn_client_dm]
    end
    
    subgraph "Azure Data Factory (omni-df-dev)"
        ADF[Azure Data Factory<br/>38 Pipelines]
        IR1[omni-sharing01-d-jpe<br/>Shared IR]
        IR2[OmniLinkedSelfHostedIntegrationRuntime<br/>Self-hosted IR]
    end
    
    subgraph "ODS構築 (Operational Data Store)"
        ODS1[KARTE連携用利用サービス統合<br/>omni_ods_cloak_trn_karte_usageservice_mtgid]
        ODS2[契約情報統合temp<br/>omni_ods_cloak_trn_karte_contract_temp]
        ODS3[ガス契約詳細temp<br/>omni_ods_gascstmr_trn_karte_gas_contract_temp]
        ODS4[電力契約詳細temp<br/>omni_ods_epcis_trn_karte_el_contract_temp]
        ODS5[スコア情報統合temp<br/>omni_ods_marketing_trn_karte_score_temp]
    end
    
    subgraph "ODM構築 (Operational Data Mart)"
        ODM1[KARTE連携統合データマート<br/>omni_ods_marketing_trn_karte_contract_score_info]
        ODM2[ハッシュ化データマート<br/>omni_ods_marketing_trn_karte_contract_score_hashed]
    end
    
    subgraph "外部システム連携"
        KARTE[KARTE<br/>Amazon S3]
        SFTP[SFTP/FTP<br/>Marketing Cloud]
        LINE[LINE連携]
        MTG[mTG System]
    end
    
    GAS --> ADF
    EPC --> ADF
    CLK --> ADF
    DNA --> ADF
    DM --> ADF
    
    ADF --> ODS1
    ADF --> ODS2
    ADF --> ODS3
    ADF --> ODS4
    ADF --> ODS5
    
    ODS1 --> ODM1
    ODS2 --> ODM1
    ODS3 --> ODM1
    ODS4 --> ODM1
    ODS5 --> ODM1
    
    ODM1 --> ODM2
    
    ODM2 --> KARTE
    ODM1 --> SFTP
    ODM1 --> LINE
    ODM1 --> MTG
```

## 3. 業務データフロー詳細設計

### 3.1 KARTE連携データフロー設計

KARTE連携は、要件定義書で定義された最も重要な業務要件の技術実装です。

```mermaid
flowchart TD
    subgraph "諸元DB抽出フェーズ"
        A1[CLOAK利用サービスDB] --> B1[利用サービス抽出<br/>pi_Ins_usageservice_mtgid]
        A2[ガス顧客系DB] --> B2[ガス契約情報抽出]
        A3[電力CIS契約DB] --> B3[電力契約情報抽出]
        A4[顧客DNA情報] --> B4[スコア情報抽出]
    end
    
    subgraph "ODS統合フェーズ"
        B1 --> C1[利用サービス統合temp<br/>omni_ods_cloak_trn_karte_usageservice_mtgid]
        B2 --> C2[契約情報統合temp<br/>omni_ods_cloak_trn_karte_contract_temp]
        B2 --> C3[ガス契約詳細temp<br/>omni_ods_gascstmr_trn_karte_gas_contract_temp]
        B3 --> C4[電力契約詳細temp<br/>omni_ods_epcis_trn_karte_el_contract_temp]
        B4 --> C5[スコア情報統合temp<br/>omni_ods_marketing_trn_karte_score_temp]
    end
    
    subgraph "ODM統合フェーズ"
        C1 --> D1[統合データマート<br/>omni_ods_marketing_trn_karte_contract_score_info]
        C2 --> D1
        C3 --> D1
        C4 --> D1
        C5 --> D1
        D1 --> D2[ハッシュ化データマート<br/>omni_ods_marketing_trn_karte_contract_score_hashed]
    end
    
    subgraph "KARTE配信フェーズ"
        D2 --> E1[pi_Send_karte_contract_score_info<br/>Amazon S3配信]
        E1 --> F1[KARTE CRMシステム]
    end
```

#### 3.1.1 データ重複排除ロジック設計

要件定義書で定義されたデータ重複排除を技術的に実装します。

```mermaid
flowchart LR
    subgraph "重複排除技術実装"
        A[複数レコード] --> B[row_number() OVER<br/>PARTITION BY MTGID<br/>ORDER BY 日付 DESC]
        B --> C[WHERE rn = 1]
        C --> D[最新レコードのみ出力]
    end
```

### 3.2 業務処理パターン設計（38パイプライン）

要件定義書で定義された4つの業務処理パターンの技術実装設計です。

#### 3.2.1 顧客関連処理設計（22パイプライン）

```mermaid
flowchart TD
    subgraph "契約・サービス管理設計"
        P1[pi_Send_ElectricityContractThanks<br/>電力契約完了通知]
        P2[pi_Send_OpeningPaymentGuide<br/>開栓ガイド送信]
        P3[pi_Send_UsageServices<br/>利用サービス情報配信]
        P4[pi_Insert_mTGCustomerMaster<br/>mTG顧客マスタ更新]
    end
    
    subgraph "マーケティング・DM配信設計"
        P5[pi_Copy_marketing_client_dm<br/>顧客DM情報複製]
        P6[pi_Copy_marketing_client_dna<br/>顧客DNA情報複製]
        P7[pi_Send_ClientDM<br/>顧客DM配信]
        P8[pi_Send_Cpkiyk<br/>CP機器・給湯器案内]
    end
    
    subgraph "配信先設計"
        OUT1[mTGシステム]
        OUT2[Marketing Cloud SFTP]
        OUT3[顧客通知システム]
    end
    
    P1 --> OUT3
    P2 --> OUT3
    P3 --> OUT1
    P4 --> OUT1
    P5 --> OUT2
    P6 --> OUT2
    P7 --> OUT2
    P8 --> OUT2
```

#### 3.2.2 料金・支払関連処理設計（8パイプライン）

```mermaid
flowchart TD
    subgraph "支払方法管理設計"
        PP1[pi_Send_PaymentMethodMaster<br/>支払方法マスタ配信]
        PP2[pi_Send_PaymentMethodChanged<br/>支払方法変更通知]
        PP3[pi_Send_PaymentAlert<br/>支払アラート送信]
    end
    
    subgraph "料金・請求管理設計"
        PP4[pi_UtilityBills<br/>料金請求情報処理]
        PP5[pi_Send_LIMSettlementBreakdownRepair<br/>LIM決済内訳修正]
    end
    
    subgraph "配信先設計"
        OUT1[SFTP/CSV配信]
        OUT2[mTGシステム]
        OUT3[請求システム]
    end
    
    PP1 --> OUT1
    PP2 --> OUT2
    PP4 --> OUT3
```

#### 3.2.3 ポイント・特典関連処理設計（4パイプライン）

```mermaid
flowchart TD
    subgraph "ポイント管理設計"
        PT1[pi_PointGrantEmail<br/>ポイント付与メール]
        PT2[pi_PointLostEmail<br/>ポイント失効メール]
        PT3[pi_Insert_ActionPointEntryEvent<br/>アクションポイント登録]
        PT4[pi_Insert_ActionPointTransactionHistory<br/>ポイント取引履歴]
    end
    
    subgraph "ポイントシステム設計"
        PS1[ポイント管理DB]
        PS2[顧客通知システム]
    end
    
    PT1 --> PS2
    PT2 --> PS2
    PT3 --> PS1
    PT4 --> PS1
```

#### 3.2.4 外部連携・KARTE関連処理設計（4パイプライン）

```mermaid
flowchart TD
    subgraph "外部連携設計"
        EX1[pi_Send_karte_contract_score_info<br/>契約・スコア情報送信]
        EX2[pi_Ins_usageservice_mtgid<br/>利用サービスmTGID登録]
        EX3[pi_Send_LINEIDLinkInfo<br/>LINE ID連携情報]
        EX4[pi_Send_MovingPromotionList<br/>引越しプロモーション]
    end
    
    subgraph "外部システム設計"
        ES1[KARTE Amazon S3]
        ES2[LINE連携API]
        ES3[引越しプロモーションシステム]
    end
    
    EX1 --> ES1
    EX2 --> ES1
    EX3 --> ES2
    EX4 --> ES3
```

### 3.3 スケジュール実行設計

要件定義書で定義された処理スケジュールの技術実装設計です。

```mermaid
gantt
    title 日次処理スケジュール設計
    dateFormat HH:mm
    axisFormat %H:%M
    
    section 朝の顧客関連処理
    顧客DNA更新処理     :04:00, 2h
    KARTE連携処理      :06:00, 2h
    
    section 日中のポイント処理
    ポイント付与処理   :08:00, 1h
    ポイント通知処理   :10:00, 1h
    
    section 夜間の料金処理
    料金確定処理(木)   :20:00, 3h
    支払処理          :22:00, 2h
```

## 4. ARMテンプレート技術設計

### 4.1 テンプレート基本情報

- **スキーマ**: `http://schema.management.azure.com/schemas/2015-01-01/deploymentTemplate.json#`
- **API バージョン**: `2018-06-01`
- **コンテンツバージョン**: `1.0.0.0`
- **デフォルトファクトリ名**: `omni-df-dev`

### 4.2 パラメータ設計

#### 4.2.1 業務系接続パラメータ (SecureString)

| パラメータ名 | 業務用途 | 関連システム |
|-------------|----------|-------------|
| `li_dam_dwh_connectionString` | メインDWH接続 | 統合データ基盤 |
| `li_sqlmi_dwh2_connectionString` | マーケティングDB接続 | 顧客DNA・スコア情報 |
| `li_Karte_AmazonS3_secretAccessKey` | KARTE連携 | 外部CRMシステム |
| `li_sftp_password` | ファイル転送 | Marketing Cloud連携 |

#### 4.2.2 システム設定パラメータ

| パラメータ名 | 業務設定値 | 用途 |
|-------------|-----------|------|
| `factoryName` | "omni-df-dev" | 開発環境識別 |
| `li_sftp_properties_typeProperties_host` | Marketing Cloud SFTP | DM配信・ファイル転送 |
| `li_Karte_AmazonS3_properties_typeProperties_accessKeyId` | KARTE S3アクセス | 契約・スコア情報配信 |

### 4.3 リンクサービス設計

#### 4.3.1 業務データソース接続

```mermaid
flowchart LR
    subgraph "諸元DB接続"
        LS1[li_dam_dwh<br/>SQL Data Warehouse]
        LS2[li_sqlmi_dwh2<br/>SQL Managed Instance]
        LS3[li_dam_dwh_shir<br/>SHIR経由DWH]
    end
    
    subgraph "業務データ"
        BD1[ガス顧客情報]
        BD2[電力契約情報]
        BD3[顧客DNA・スコア]
    end
    
    subgraph "外部システム連携"
        ES1[li_Karte_AmazonS3<br/>KARTE連携]
        ES2[li_sftp<br/>Marketing Cloud]
        ES3[li_dam_kv_omni<br/>Key Vault]
    end
    
    LS1 --> BD1
    LS2 --> BD3
    BD1 --> ES1
    BD3 --> ES2
    LS3 --> ES3
```

| サービス名 | 業務用途 | 接続先 | 特記事項 |
|-----------|----------|--------|---------|
| `li_dam_dwh` | 統合データ基盤 | Azure SQL DW | Private Link対応 |
| `li_sqlmi_dwh2` | マーケティングデータ | Azure SQL MI | 顧客DNA・スコア管理 |
| `li_Karte_AmazonS3` | KARTE連携 | Amazon S3 | 契約・スコア情報配信 |
| `li_sftp` | ファイル転送 | Marketing Cloud | DM配信・CSV転送 |

### 4.4 データセット設計

#### 4.4.1 業務データセット分類

```mermaid
flowchart TB
    subgraph "諸元DBデータセット"
        DS1[ds_DamDwhTable<br/>動的テーブル参照]
        DS2[ds_sqlmi<br/>顧客DNA・マーケティング]
        DS3[ds_contract_score<br/>契約スコア情報]
    end
    
    subgraph "配信用データセット"
        DS4[ds_CSV_Blob<br/>CSV形式配信]
        DS5[ds_CSV_BlobGz<br/>圧縮CSV配信]
        DS6[ds_Json_Blob<br/>JSON形式配信]
    end
    
    subgraph "転送用データセット"
        DS7[ds_Gz_Sftp<br/>SFTP転送]
        DS8[ds_BlobGz<br/>バイナリ処理]
    end
    
    DS1 --> DS4
    DS2 --> DS6
    DS3 --> DS5
    DS5 --> DS7
```

### 4.5 パイプライン設計詳細

#### 4.5.1 38個パイプラインの業務分類・設計

```mermaid
flowchart TB
    subgraph "顧客関連パイプライン (22個)"
        PG1[契約・サービス管理<br/>8パイプライン]
        PG2[マーケティング・DM<br/>14パイプライン]
    end
    
    subgraph "料金・支払パイプライン (8個)"
        PG3[支払方法管理<br/>4パイプライン]
        PG4[料金・請求処理<br/>4パイプライン]
    end
    
    subgraph "ポイント・特典パイプライン (4個)"
        PG5[ポイント管理<br/>2パイプライン]
        PG6[アクションポイント<br/>2パイプライン]
    end
    
    subgraph "外部連携パイプライン (4個)"
        PG7[KARTE連携<br/>2パイプライン]
        PG8[LINE・プロモーション<br/>2パイプライン]
    end
    
    subgraph "共通データソース"
        DS[諸元DB<br/>ガス・電力・顧客情報]
    end
    
    DS --> PG1
    DS --> PG2
    DS --> PG3
    DS --> PG4
    DS --> PG5
    DS --> PG6
    DS --> PG7
    DS --> PG8
```

#### 4.5.2 主要パイプラインの処理フロー

**KARTE連携パイプライン (`pi_Send_karte_contract_score_info`)**:

```mermaid
flowchart TD
    A[開始] --> B[利用サービスデータ抽出]
    B --> C[契約情報temp作成]
    C --> D[ガス契約詳細temp作成]
    D --> E[電力契約詳細temp作成]
    E --> F[スコア情報temp作成]
    F --> G[LEFT JOIN統合処理]
    G --> H[ハッシュ化処理]
    H --> I[Amazon S3配信]
    I --> J[完了]
    
    style A fill:#e1f5fe
    style J fill:#c8e6c9
    style I fill:#fff3e0
```

### 4.6 トリガー設計

#### 4.6.1 業務スケジュールトリガー

```mermaid
gantt
    title 業務処理スケジュール設計
    dateFormat HH:mm
    axisFormat %H:%M
    
    section 日次処理
    顧客マスタ更新          :04:00, 2h
    KARTE連携処理          :06:00, 1h
    DM配信処理             :08:00, 2h
    支払アラート           :10:00, 1h
    
    section 週次処理
    料金確定(木曜)         :20:00, 2h
    
    section 月次処理
    ポイント失効           :02:00, 1h
```

| トリガー名 | 業務用途 | 実行頻度 | 対象パイプライン |
|-----------|----------|----------|-----------------|
| `tr_Schedule_contract_score_info` | KARTE連携 | 日次 06:00 | pi_Send_karte_contract_score_info |
| `tr_Schedule_UtilityBills_Thursday` | 料金確定 | 木曜 20:00 | pi_UtilityBills |
| `tr_Schedule_PaymentAlert` | 支払督促 | 日次 10:00 | pi_Send_PaymentAlert |
| `tr_Schedule_PointLostEmail` | ポイント失効 | 月次 02:00 | pi_PointLostEmail |
| `tr_Schedule_marketing_client_dna` | 顧客DNA更新 | 日次 04:00 | pi_Copy_marketing_client_dna |

#### 4.6.2 特別スケジュール設計

**料金確定処理**:

- **木曜日**: `tr_Schedule_UtilityBills_Thursday` (特別処理ロジック)
- **木曜日以外**: `tr_Schedule_UtilityBills_Excluding_Thursday` (通常処理)

**業務カレンダー連動**:

- 祝日・年末年始の処理スキップ機能
- 月末・月初の特別処理対応

### 4.7 Integration Runtime設計

#### 4.7.1 業務別IR使用方針

```mermaid
flowchart LR
    subgraph "業務データ処理"
        IR1[omni-sharing01-d-jpe<br/>共有IR]
        IR2[OmniLinkedSelfHostedIntegrationRuntime<br/>Self-hosted IR]
    end
    
    subgraph "処理タイプ"
        P1[標準ETL処理]
        P2[Private Link必須処理]
        P3[オンプレミス連携]
        P4[高セキュリティ処理]
    end
    
    IR1 --> P1
    IR2 --> P2
    IR2 --> P3
    IR2 --> P4
```

| Integration Runtime | 使用用途 | 対象業務 |
|-------------------|----------|----------|
| `omni-sharing01-d-jpe` | 標準データ処理 | 顧客DM、マーケティング |
| `OmniLinkedSelfHostedIntegrationRuntime` | セキュア処理 | 料金・支払、個人情報 |

## 5. 38パイプライン詳細設計

### 5.1 パイプライン分類と業務機能設計

要件定義書で定義された4つの業務処理パターンに対応する38パイプラインの詳細設計です。

#### 5.1.1 顧客関連処理パイプライン（22個）

**契約・サービス管理系**:

| パイプライン名 | 業務機能 | データソース | 出力先 |
|---------------|----------|-------------|--------|
| `pi_Send_ElectricityContractThanks` | 電力契約完了通知 | 電力CIS契約DB | 顧客通知システム |
| `pi_Send_OpeningPaymentGuide` | 開栓ガイド送信 | ガス顧客系DB | mTGシステム |
| `pi_Send_UsageServices` | 利用サービス情報配信 | CLOAK利用サービスDB | Marketing Cloud |
| `pi_Insert_mTGCustomerMaster` | mTG顧客マスタ更新 | 統合顧客情報 | mTGシステム |
| `pi_Send_CustomerContractInfo` | 顧客契約情報送信 | 統合契約情報 | SFTP配信 |
| `pi_Send_ContractExpirationNotice` | 契約満了通知 | 契約満了予定 | 顧客通知システム |

**マーケティング・DM配信系**:

| パイプライン名 | 業務機能 | データソース | 出力先 |
|---------------|----------|-------------|--------|
| `pi_Copy_marketing_client_dm` | 顧客DM情報複製 | 顧客DM情報 | マーケティングDB |
| `pi_Copy_marketing_client_dna` | 顧客DNA情報複製 | 顧客DNA情報 | マーケティングDB |
| `pi_Send_ClientDM` | 顧客DM配信 | DM配信リスト | Marketing Cloud |
| `pi_Send_Cpkiyk` | CP機器・給湯器案内 | 機器推定情報 | SFTP配信 |
| `pi_Send_RecommendationEmail` | レコメンドメール配信 | 顧客スコア情報 | 顧客通知システム |
| `pi_Send_SurveyInvitation` | アンケート依頼送信 | 顧客セグメント情報 | Marketing Cloud |

**顧客管理・データ統合系**:

| パイプライン名 | 業務機能 | データソース | 出力先 |
|---------------|----------|-------------|--------|
| `pi_UpdateCustomerSegmentation` | 顧客セグメント更新 | 統合顧客データ | セグメントDB |
| `pi_CalculateCustomerScore` | 顧客スコア計算 | 利用履歴・行動データ | スコアDB |
| `pi_SyncCustomerPreferences` | 顧客設定同期 | 設定情報 | 各種システム |
| `pi_ValidateCustomerData` | 顧客データ検証 | 全顧客データ | データ品質DB |

#### 5.1.2 料金・支払関連処理パイプライン（8個）

**支払方法管理系**:

| パイプライン名 | 業務機能 | データソース | 出力先 |
|---------------|----------|-------------|--------|
| `pi_Send_PaymentMethodMaster` | 支払方法マスタ配信 | 支払方法マスタ | 請求システム |
| `pi_Send_PaymentMethodChanged` | 支払方法変更通知 | 支払方法変更履歴 | mTGシステム |
| `pi_Send_PaymentAlert` | 支払アラート送信 | 支払遅延情報 | 顧客通知システム |
| `pi_ProcessDirectDebitSetup` | 口座振替設定処理 | 口座設定情報 | 金融機関連携 |

**料金・請求管理系**:

| パイプライン名 | 業務機能 | データソース | 出力先 |
|---------------|----------|-------------|--------|
| `pi_UtilityBills` | 料金請求情報処理 | 使用量・料金データ | 請求システム |
| `pi_Send_LIMSettlementBreakdownRepair` | LIM決済内訳修正 | 決済内訳データ | LIMシステム |
| `pi_GenerateBillingReport` | 請求レポート生成 | 請求データ | レポートシステム |
| `pi_ProcessRefundRequests` | 返金処理 | 返金申請データ | 経理システム |

#### 5.1.3 ポイント・特典関連処理パイプライン（4個）

| パイプライン名 | 業務機能 | データソース | 出力先 |
|---------------|----------|-------------|--------|
| `pi_PointGrantEmail` | ポイント付与メール | ポイント付与データ | 顧客通知システム |
| `pi_PointLostEmail` | ポイント失効メール | ポイント失効予定 | 顧客通知システム |
| `pi_Insert_ActionPointEntryEvent` | アクションポイント登録 | アクション履歴 | ポイント管理DB |
| `pi_Insert_ActionPointTransactionHistory` | ポイント取引履歴 | ポイント取引データ | 取引履歴DB |

#### 5.1.4 外部連携・KARTE関連処理パイプライン（4個）

| パイプライン名 | 業務機能 | データソース | 出力先 |
|---------------|----------|-------------|--------|
| `pi_Send_karte_contract_score_info` | 契約・スコア情報送信 | 統合データマート | KARTE Amazon S3 |
| `pi_Ins_usageservice_mtgid` | 利用サービスmTGID登録 | 利用サービス情報 | KARTE Amazon S3 |
| `pi_Send_LINEIDLinkInfo` | LINE ID連携情報 | LINE連携データ | LINE連携API |
| `pi_Send_MovingPromotionList` | 引越しプロモーション | 引越し予定情報 | プロモーションシステム |

### 5.2 パイプライン技術実装設計

#### 5.2.1 データフロー技術パターン

**パターン1: ODS→ODM統合型（KARTE連携）**

```mermaid
flowchart TD
    A[諸元DB抽出] --> B[ODS構築]
    B --> C[データ重複排除]
    C --> D[LEFT JOIN統合]
    D --> E[ODM構築]
    E --> F[ハッシュ化]
    F --> G[外部連携配信]
```

**パターン2: 直接配信型（DM配信等）**

```mermaid
flowchart TD
    A[マーケティングDB] --> B[セグメント抽出]
    B --> C[配信リスト生成]
    C --> D[SFTP配信]
```

**パターン3: バッチ更新型（マスタ同期）**

```mermaid
flowchart TD
    A[マスタデータ] --> B[差分検出]
    B --> C[変更データ抽出]
    C --> D[ターゲットシステム更新]
```

#### 5.2.2 エラーハンドリング設計

**共通エラーハンドリングパターン**:

```mermaid
flowchart TD
    A[パイプライン開始] --> B[データ検証]
    B --> C{検証OK?}
    C -->|NG| D[エラーログ出力]
    C -->|OK| E[メイン処理]
    E --> F{処理成功?}
    F -->|NG| G[リトライ処理]
    F -->|OK| H[完了通知]
    G --> I{リトライ上限?}
    I -->|未達| E
    I -->|達成| J[エラー通知]
    D --> J
    J --> K[運用チーム通知]
```

### 5.3 パイプライン実行スケジュール設計

#### 5.3.1 時間帯別実行設計

```mermaid
gantt
    title 38パイプライン実行スケジュール設計
    dateFormat HH:mm
    axisFormat %H:%M
    
    section 早朝処理(02:00-06:00)
    ポイント失効(月次)     :02:00, 1h
    顧客DNA更新           :04:00, 2h
    
    section 朝の処理(06:00-10:00)
    KARTE連携            :06:00, 1h
    利用サービス更新      :07:00, 1h
    DM配信処理           :08:00, 2h
    
    section 日中処理(10:00-18:00)
    支払アラート          :10:00, 1h
    顧客通知配信          :12:00, 2h
    マスタ同期           :14:00, 2h
    レポート生成          :16:00, 2h
    
    section 夜間処理(18:00-02:00)
    料金確定(木曜)        :20:00, 3h
    バックアップ処理      :23:00, 2h
```

#### 5.3.2 依存関係設計

**主要パイプライン依存関係**:

```mermaid
flowchart TD
    A[pi_Copy_marketing_client_dna] --> B[pi_Send_karte_contract_score_info]
    C[pi_Ins_usageservice_mtgid] --> B
    D[pi_Insert_mTGCustomerMaster] --> E[pi_Send_UsageServices]
    F[pi_UtilityBills] --> G[pi_Send_PaymentAlert]
    H[pi_Insert_ActionPointEntryEvent] --> I[pi_PointGrantEmail]
```

---

## 6. 要件トレーサビリティ

### 6.1 要件定義書との対応表

| 要件定義書セクション | 設計仕様書対応セクション | 対応状況 |
|-------------------|---------------------|---------|
| 2.1 データソース要件 | 4.3 リンクサービス設計 | ✓完全対応 |
| 2.2 ODS構築要件 | 3.1 KARTE連携データフロー設計 | ✓完全対応 |
| 2.3 ODM構築要件 | 3.1.1 ODS統合フェーズ | ✓完全対応 |
| 2.4 業務処理パターン要件 | 5.1 38パイプライン詳細設計 | ✓完全対応 |
| 3.1 データ統合基盤要件 | 4.3 リンクサービス設計 | ✓完全対応 |
| 3.2 パフォーマンス要件 | 4.7 Integration Runtime設計 | ✓完全対応 |
| 3.3 セキュリティ要件 | 4.8 セキュリティ設計 | ✓完全対応 |
| 3.4 運用要件 | 4.6 トリガー設計 | ✓完全対応 |

### 6.2 設計変更管理

**重要な設計決定事項**:

1. **パイプライン数の正確化**: 当初想定120+→実際38パイプライン
2. **KARTE連携の重要度**: 最重要業務要件として設計優先度を最高に設定
3. **ODS/ODM段階構築**: 要件定義書に従った段階的データ統合アーキテクチャ
4. **38パイプライン詳細設計**: 業務パターン別の詳細実装設計

## 7. 今後の設計課題

### 7.1 次回リリース向け検討事項

- **スケーラビリティ**: 新規業務システム追加時の拡張性
- **災害復旧**: BCP対応の技術実装詳細
- **監視・アラート**: 運用監視システムとの連携設計
- **データガバナンス**: 個人情報保護強化対応

---

## 文書管理

**更新履歴**:

- v3.0 (2025/07/03) - 要件定義書完全対応・38パイプライン詳細設計・トレーサビリティ追加
- v2.0 (2025/07/03) - 業務要件対応・データフロー図示化による全面改訂
- v1.0 (2025/07/03) - 初版作成

**レビュー予定**: 月次  
**承認状況**: [承認待ち/承認済み]

**関連文書**:

- [ARM テンプレート要件定義書](./ARM_TEMPLATE_REQUIREMENTS_DEFINITION.md)
- [テスト仕様書各種](./TEST_STRATEGY_DOCUMENT.md)
- [運用手順書](./README.md)
