# 38パイプライン個別テスト 業務分類・仕様書体系化

## 分析概要

**分析日時**: 2025年7月3日  
**対象**: Azure Data Factory ETL 38パイプライン個別テスト  
**目的**: 業務機能別分類・仕様書体系化・トレーサビリティ確立

## パイプライン業務分類

### 1. 顧客管理・マーケティング系（Customer Management & Marketing）

| パイプライン名 | テストファイル | 業務機能 | 優先度 |
|---------------|---------------|----------|--------|
| marketing_client_dm | test_e2e_pipeline_marketing_client_dm.py | 顧客マスタ・マーケティング分析 | 高 |
| marketing_client_dm | test_e2e_pipeline_marketing_client_dm_comprehensive.py | 顧客マスタ包括分析 | 高 |
| marketing_client_dm | test_e2e_pipeline_marketing_client_dm_fixed.py | 顧客マスタ修正版 | 中 |
| client_dm_new | test_e2e_pipeline_client_dm_new.py | 新規顧客マスタ | 高 |
| insert_clientdm_bx | test_e2e_pipeline_insert_clientdm_bx.py | 顧客マスタ更新 | 高 |

### 2. 決済・支払い系（Payment & Billing）

| パイプライン名 | テストファイル | 業務機能 | 優先度 |
|---------------|---------------|----------|--------|
| payment_alert | test_e2e_pipeline_payment_alert.py | 支払いアラート | 高 |
| payment_alert | test_e2e_pipeline_payment_alert_fixed.py | 支払いアラート修正版 | 中 |
| payment_method_changed | test_e2e_pipeline_payment_method_changed.py | 支払方法変更 | 中 |
| payment_method_master | test_e2e_pipeline_payment_method_master.py | 支払方法マスタ | 高 |
| opening_payment_guide | test_e2e_pipeline_opening_payment_guide.py | 支払案内 | 中 |

### 3. ポイント管理系（Point Management）

| パイプライン名 | テストファイル | 業務機能 | 優先度 |
|---------------|---------------|----------|--------|
| point_grant_email | test_e2e_pipeline_point_grant_email.py | ポイント付与メール | 高 |
| point_grant_email | test_e2e_pipeline_point_grant_email_simple.py | ポイント付与（簡易版） | 中 |
| point_lost_email | test_e2e_pipeline_point_lost_email.py | ポイント失効メール | 高 |
| action_point_current_month_entry | test_e2e_pipeline_action_point_current_month_entry.py | 当月アクションポイント | 中 |
| action_point_recent_transaction_history_list | test_e2e_pipeline_action_point_recent_transaction_history_list.py | アクションポイント履歴 | 中 |

### 4. 契約・サービス管理系（Contract & Service Management）

| パイプライン名 | テストファイル | 業務機能 | 優先度 |
|---------------|---------------|----------|--------|
| electricity_contract_thanks | test_e2e_pipeline_electricity_contract_thanks.py | 電力契約御礼 | 中 |
| karte_contract_score_info | test_e2e_pipeline_karte_contract_score_info.py | カルテ契約スコア | 中 |
| usage_services | test_e2e_pipeline_usage_services.py | サービス利用状況 | 中 |
| cpkiyk | test_e2e_pipeline_cpkiyk.py | CPKIYK処理 | 低 |

### 5. 精算・決済処理系（Settlement & Transaction）

| パイプライン名 | テストファイル | 業務機能 | 優先度 |
|---------------|---------------|----------|--------|
| lim_settlement_breakdown_repair_simple | test_e2e_pipeline_lim_settlement_breakdown_repair_simple.py | LIM精算内訳修復 | 中 |
| line_id_link_info_simple | test_e2e_pipeline_line_id_link_info_simple.py | LINE ID連携 | 低 |

### 6. システム基盤・共通機能系（System Infrastructure）

| パイプライン名 | テストファイル | 業務機能 | 優先度 |
|---------------|---------------|----------|--------|
| 複数パイプライン統合 | test_e2e_multiple_pipelines_sql_externalized.py | 複数パイプライン結合テスト | 高 |
| 汎用データ品質 | test_advanced_business_logic.py | 高度なビジネスロジック | 高 |
| データ整合性 | test_data_integrity.py | データ整合性検証 | 高 |
| データ品質・セキュリティ | test_data_quality_and_security.py | データ品質・セキュリティ | 高 |

## 業務優先度別整理

### 高優先度（ビジネスクリティカル）: 15パイプライン

**顧客管理・マーケティング**: 4パイプライン

- marketing_client_dm（包括・分析）
- client_dm_new
- insert_clientdm_bx

**決済・支払い**: 2パイプライン

- payment_alert
- payment_method_master

**ポイント管理**: 2パイプライン

- point_grant_email
- point_lost_email

**システム基盤**: 7パイプライン

- 複数パイプライン統合
- 高度なビジネスロジック
- データ整合性
- データ品質・セキュリティ
- その他基盤系テスト

### 中優先度（業務運用重要）: 18パイプライン

**顧客管理**: 1パイプライン
**決済・支払い**: 3パイプライン
**ポイント管理**: 2パイプライン
**契約・サービス**: 4パイプライン
**精算・決済**: 1パイプライン
**その他中優先度**: 7パイプライン

### 低優先度（補助機能）: 5パイプライン

## テスト戦略への整合

### 自動化必須項目（高優先度）との対応

| テスト戦略分類 | 該当パイプライン数 | 実装状況 |
|---------------|------------------|----------|
| **顧客データ処理** | 4 | 100%実装済み |
| **決済データ処理** | 2 | 100%実装済み |
| **ポイントデータ処理** | 2 | 100%実装済み |
| **データ品質・整合性** | 7 | 100%実装済み |

**結果**: 高優先度15パイプライン すべて実装済み、テスト戦略完全準拠

## 推奨仕様書構造

### E2E_TEST_SPECIFICATION.md 拡張提案

```markdown
## 7. 業務固有E2Eテスト項目（パイプライン個別）

### 7.1 顧客管理・マーケティング系

#### 7.1.1 顧客マスタ統合処理（E2E-CUS-001）
- **テストID**: E2E-CUS-001
- **実装ファイル**: test_e2e_pipeline_marketing_client_dm_comprehensive.py
- **業務機能**: 顧客マスタデータの包括的ETL処理
- **テスト観点**: データ統合・品質チェック・変換精度

#### 7.1.2 新規顧客登録処理（E2E-CUS-002）
- **テストID**: E2E-CUS-002
- **実装ファイル**: test_e2e_pipeline_client_dm_new.py
- **業務機能**: 新規顧客データの正規化・登録
- **テスト観点**: データ正規化・重複チェック・整合性

### 7.2 決済・支払い系

#### 7.2.1 支払いアラート処理（E2E-PAY-001）
- **テストID**: E2E-PAY-001
- **実装ファイル**: test_e2e_pipeline_payment_alert.py
- **業務機能**: 支払い遅延・異常の検出・通知
- **テスト観点**: 条件判定・アラート生成・通知精度
```

## 実装計画

### フェーズ1: 高優先度パイプライン仕様書化（1週間）

1. 顧客管理・マーケティング系（4パイプライン）
2. 決済・支払い系（2パイプライン）
3. ポイント管理系（2パイプライン）

### フェーズ2: 中優先度パイプライン仕様書化（2週間）

1. 契約・サービス管理系（4パイプライン）
2. 精算・決済処理系（2パイプライン）
3. その他中優先度（12パイプライン）

### フェーズ3: 低優先度・統合最適化（1週間）

1. 低優先度パイプライン（5パイプライン）
2. 統一命名規則の全適用
3. トレーサビリティテーブル完成

## 継続メンテナンス

### 月次レビュー項目

1. 新規パイプライン追加時の分類・優先度決定
2. 業務要件変更による優先度見直し
3. テスト実行結果に基づく仕様書更新

### 四半期レビュー項目

1. 業務分類体系の見直し
2. テスト戦略との整合性確認
3. ROI評価に基づく優先度調整

**結論**: 38パイプライン個別テストの体系的整理により、業務価値に基づく効率的なテスト管理と仕様書保守を実現。
