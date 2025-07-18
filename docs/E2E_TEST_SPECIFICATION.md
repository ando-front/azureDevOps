# Azure Data Factory ETL テスト仕様書

## 1. 文書情報

| 項目 | 内容 |
|------|------|
| 文書名 | Azure Data Factory ETL テスト仕様書 |
| バージョン | 2.2 |
| 作成日 | 2025年7月3日 |
| 最終更新 | 2025年7月3日 (テストコード整合性・トレーサビリティ強化) |
| 作成者 | ETLテスト設計担当 |
| 承認者 | [承認者名] |

## 2. ETLテスト方針

### 2.1 テスト基本方針

Azure Data Factory をETLツールとして位置づけ、**データの抽出・変換・格納**の品質担保に特化したテストを実施する。

**テスト対象**:

- **Extract**: データソースからの正確なデータ抽出
- **Transform**: データ変換・加工・品質チェック
- **Load**: ターゲットシステムへの正確なデータ格納

**テスト対象外**:

- ビジネスロジックの妥当性（業務システム側の責任）
- ビジネスルールの正確性（要件定義の責任）
- データの業務的意味（データオーナーの責任）

### 2.2 テストレベル定義

#### 2.2.1 ユニットテスト（単体テスト）

**スコープ**: 個別パイプライン・アクティビティレベル

- **データセット接続テスト**: 各データソースへの接続確認
- **アクティビティ単位テスト**: Copy、Lookup、Execute Pipeline等の個別動作
- **データ変換テスト**: Mapping Data Flow、SQL変換の入出力検証
- **パラメータ処理テスト**: パイプラインパラメータの正常処理

#### 2.2.2 E2Eテスト（結合テスト）

**スコープ**: パイプライン間連携・データフロー全体

- **データフロー連携テスト**: 複数パイプラインにわたるデータフロー
- **依存関係テスト**: パイプライン実行順序・依存関係の検証
- **エラーハンドリングテスト**: 異常系処理の動作確認
- **リトライ・回復テスト**: 障害発生時の自動回復機能

#### 2.2.3 総合テスト（システムテスト）

**スコープ**: システム全体・運用シナリオ

- **性能・負荷テスト**: 大容量データ処理の性能検証
- **スケジュール実行テスト**: トリガー・スケジュール機能の検証
- **運用監視テスト**: ログ・アラート・監視機能の検証
- **セキュリティテスト**: 接続・認証・暗号化の検証

### 2.3 テスト環境・データ戦略

#### 2.3.1 テスト実行環境

**自動化環境**（ユニット・E2Eテスト）:

- **CI/CD Platform**: GitHub Actions
- **コンテナ環境**: Docker + Docker Compose
- **データベース**: SQL Server 2022 (Dockerコンテナ)
- **ストレージ**: Azurite (Azure Storage エミュレーター)
- **実行トリガー**: Pull Request、Push、手動実行
- **実行時間**: ユニットテスト約30秒、E2Eテスト約10-15分

**手動実行環境**（総合テスト）:

- **ADF環境**: Azure Data Factory開発環境 (omni-df-dev)
- **データベース**: SQL Data Warehouse (本番同等環境)
- **ストレージ**: Azure Blob Storage (本番同等環境)
- **実行方式**: ADF Studio + 手動トリガー実行
- **実行期間**: 業務影響を考慮した計画実行

#### 2.3.2 テスト環境セットアップ

**Docker環境セットアップ**:

```bash
# ユニットテスト環境（ODBC不要、高速実行）
docker-compose up --build adf-unit-test

# E2Eテスト環境（自動判定・推奨）
./run-e2e-flexible.sh --interactive full

# E2Eテスト環境（企業プロキシ環境）
./run-e2e-flexible.sh --proxy full

# E2Eテスト環境（開発者ローカル環境）
./run-e2e-flexible.sh --no-proxy full
```

**必要リソース**:

- Docker Desktop 4.0+ / Docker Engine 20.10+
- メモリ 8GB以上（推奨）
- ディスク容量 5GB以上
- ネットワーク接続（初回イメージダウンロード）

#### 2.3.3 テストデータ戦略

- **テストデータ**: 本番データの構造を保持した匿名化データ
- **データ量**: 性能検証用の大容量データセット
- **期間**: 段階的テスト実行（ユニット→E2E→総合）

| テストレベル | 実行環境 | データ量 | 実行方式 |
|-------------|----------|---------|---------|
| ユニットテスト | Docker + Mock | 小規模 | 自動実行 |
| E2Eテスト | Docker + SQL Server | 中規模 | 自動実行 |
| 総合テスト | ADF開発環境 | 大規模 | 手動実行 |

## 3. ユニットテスト（単体テスト）

**実行環境**: Docker + Mock（ODBC不要）  
**実行方式**: GitHub Actions自動実行 + 手動実行可能  
**実行時間**: 約30秒  
**実行コマンド**: `docker-compose up --build adf-unit-test`

### 3.1 データセット接続テスト

#### テストケース UT-DS-001: SQL Data Warehouse接続テスト

**テスト目的**: li_dam_dwh, li_dam_dwh_shir の接続安定性確認

```text
テスト手順:
1. 各LinkedServiceの接続テスト実行
2. 認証情報の検証
3. Private Link接続の確認
4. 接続プール設定の検証

検証項目:
- 接続成功率: 100%
- 接続時間: 5秒以内
- タイムアウト処理: 30秒で適切に処理
- エラーハンドリング: 接続失敗時の適切なログ出力
```

| LinkedService | 接続先 | 検証項目 | 合格基準 |
|--------------|--------|----------|---------|
| `li_dam_dwh` | SQL DW (Private Link) | 接続成功・認証成功 | 100%成功 |
| `li_dam_dwh_shir` | SQL DW (SHIR経由) | 接続成功・SHIR稼働確認 | 100%成功 |
| `li_sqlmi_dwh2` | SQL MI | 接続成功・高可用性確認 | 100%成功 |

#### テストケース UT-DS-002: Blob Storage接続テスト

**テスト目的**: 各種Blob Storageへの接続・認証確認

| LinkedService | コンテナ | 検証項目 | 合格基準 |
|--------------|---------|----------|---------|
| `li_blob_tgomnidevrgsa_container` | tgomni | SAS認証・ファイル読み書き | 100%成功 |
| `li_blob_damhdinsight_container` | damhdinsight-container | Private Link経由アクセス | 100%成功 |
| `li_blob_mytokyogas_container` | - | SAS URI認証 | 100%成功 |

#### テストケース UT-DS-003: 外部システム接続テスト

**テスト目的**: Amazon S3、SFTP等外部システムとの接続確認

| LinkedService | 外部システム | 検証項目 | 合格基準 |
|--------------|-------------|----------|---------|
| `li_Karte_AmazonS3` | Amazon S3 | アクセスキー認証・バケットアクセス | 100%成功 |
| `li_sftp` | Marketing Cloud SFTP | SFTP認証・ディレクトリアクセス | 100%成功 |
| `li_dam_kv_omni` | Azure Key Vault | 秘匿情報取得・管理ID認証 | 100%成功 |

### 3.2 データセット定義テスト

#### テストケース UT-DS-004: データセットスキーマ検証

**テスト目的**: 各データセットのスキーマ定義とソースの整合性確認

```text
検証対象データセット:
- ds_DamDwhTable: SQL DWテーブル動的参照
- ds_sqlmi: SQL MIテーブル参照
- ds_Json_Blob: JSON形式ファイル
- ds_CSV_Blob: CSV形式ファイル
- ds_BlobGz: gzip圧縮バイナリファイル

検証項目:
1. スキーマ定義と実データの整合性
2. データ型マッピングの正確性
3. NULL許可・必須項目の定義
4. 文字エンコーディングの対応
```

| データセット | ソース | 検証項目 | 合格基準 |
|-------------|--------|----------|---------|
| `ds_DamDwhTable` | SQL DW omniスキーマ | テーブル存在・列定義一致 | 100%一致 |
| `ds_Json_Blob` | Blob JSON | JSON構造・エンコーディング | UTF-8正常解析 |
| `ds_CSV_Blob` | Blob CSV | 区切り文字・ヘッダー定義 | 仕様通り解析 |

### 3.3 パイプラインアクティビティテスト

#### テストケース UT-PA-001: Copy Activityテスト

**テスト目的**: データコピー処理の正確性・完全性確認

```text
テスト対象アクティビティ:
- SQL → Blob Storage
- Blob Storage → SQL
- SQL → SQL (異なるDB間)
- 圧縮ファイル処理
- 大容量ファイル処理

検証項目:
1. レコード件数の完全一致
2. データ内容の完全一致
3. NULL値の正確な処理
4. 文字エンコーディングの保持
5. 数値精度の保持
```

| テストパターン | ソース → ターゲット | 検証項目 | 合格基準 |
|-------------|------------------|----------|---------|
| SQL抽出 | li_dam_dwh → ds_Json_Blob | レコード件数・データ内容 | 100%一致 |
| ファイル読込 | ds_CSV_Blob → li_sqlmi_dwh2 | 解析精度・データ型変換 | エラー率0% |
| 圧縮処理 | ds_Json_Blob → ds_BlobGz | 圧縮率・データ完全性 | データ損失なし |

#### テストケース UT-PA-002: Lookup Activityテスト

**テスト目的**: 参照データ取得処理の正確性確認

```text
テスト観点:
- 参照先データの存在確認
- 複数レコード取得時の処理
- レコード未検出時の処理
- パラメータ展開の正確性

検証項目:
1. 期待するレコードの正確な取得
2. 結果セットのデータ型正確性
3. NULLレコード時の適切な処理
4. パフォーマンス要件の達成
```

#### テストケース UT-PA-003: Execute Pipeline Activityテスト

**テスト目的**: 子パイプライン実行制御の正確性確認

```text
テスト項目:
- パラメータ受け渡しの正確性
- 戻り値の正確な取得
- エラー発生時の親パイプライン制御
- 並列実行時の制御

検証観点:
1. パラメータ値の完全な受け渡し
2. 実行結果の正確な判定
3. エラー時の適切な制御フロー
4. 実行時間の妥当性
```

### 3.4 データ変換テスト

#### テストケース UT-DT-001: データ型変換テスト

**テスト目的**: 異なるシステム間でのデータ型変換の正確性確認

```text
変換パターン:
- 文字列 ⇔ 数値
- 日付文字列 → DATETIME
- JSON → リレーショナル構造
- NULL値の適切な変換

検証基準:
1. 変換前後の値の等価性
2. 精度・桁数の保持
3. 不正データの適切なエラー処理
4. パフォーマンスの妥当性
```

## 4. E2Eテスト（結合テスト）

**実行環境**: Docker + SQL Server 2022  
**実行方式**: GitHub Actions自動実行 + 手動実行可能  
**実行時間**: 約10-15分  
**実行コマンド**: `./run-e2e-flexible.sh --interactive full`  
**テストケース数**: 409ケース（100%成功実績）

### 4.1 ETLデータフロー統合テスト

#### テストケース E2E-001: KARTE連携ETLフロー

**テスト目的**: 諸元DB → ODS → ODM → Amazon S3 の完全なETLフロー検証

```text
ETLフロー概要:
1. [Extract] 諸元DB（SQL DW）からのデータ抽出
2. [Transform] ODS構築（5つのtempテーブル作成）
3. [Transform] ODM統合（LEFT JOIN処理）
4. [Transform] ハッシュ化処理（個人情報保護）
5. [Load] Amazon S3へのファイル配信

ETL品質検証項目:
- データ件数の完全追跡（Extract→Load）
- データ変換の正確性（JOIN、集約、ハッシュ化）
- ファイル形式・エンコーディングの正確性
- 処理時間・リソース使用量の妥当性
```

```mermaid
flowchart TD
    subgraph "Extract Phase"
        A[諸元DB抽出<br/>li_dam_dwh] --> B[pi_Copy_marketing_client_dna]
        A --> C[pi_Ins_usageservice_mtgid]
    end
    
    subgraph "Transform Phase - ODS"
        B --> D[利用サービス統合temp<br/>omni_ods_cloak_trn_karte_usageservice_mtgid]
        C --> E[契約情報統合temp<br/>omni_ods_cloak_trn_karte_contract_temp]
        A --> F[ガス契約詳細temp<br/>omni_ods_gascstmr_trn_karte_gas_contract_temp]
        A --> G[電力契約詳細temp<br/>omni_ods_epcis_trn_karte_el_contract_temp]
        B --> H[スコア情報統合temp<br/>omni_ods_marketing_trn_karte_score_temp]
    end
    
    subgraph "Transform Phase - ODM"
        D --> I[LEFT JOIN統合処理]
        E --> I
        F --> I
        G --> I
        H --> I
        I --> J[統合データマート<br/>omni_ods_marketing_trn_karte_contract_score_info]
        J --> K[ハッシュ化処理<br/>omni_ods_marketing_trn_karte_contract_score_hashed]
    end
    
    subgraph "Load Phase"
        K --> L[pi_Send_karte_contract_score_info]
        L --> M[Amazon S3配信<br/>li_Karte_AmazonS3]
    end
```

| ETLフェーズ | 検証項目 | 測定方法 | 合格基準 |
|------------|----------|----------|---------|
| Extract | データ抽出件数 | COUNT検証 | ソースと100%一致 |
| Transform-ODS | temp件数整合性 | 5テーブル件数検証 | 論理的整合性確保 |
| Transform-ODM | JOIN結果正確性 | 結合後レコード検証 | LEFT JOIN仕様適合 |
| Transform-Hash | ハッシュ化処理 | 変換前後比較 | 可逆性なし確認 |
| Load | S3配信完了 | ファイル存在確認 | 100%配信完了 |

#### テストケース E2E-002: ファイル系ETLフロー

**テスト目的**: CSV/JSON ファイル処理の ETL フロー検証

```text
ETLフロー概要:
1. [Extract] Blob Storage からファイル取得（CSV、JSON、gzip）
2. [Transform] ファイル形式変換・データクレンジング
3. [Load] SQL Database への格納・SFTP配信

検証観点:
- ファイル形式解析の正確性
- 大容量ファイル処理の安定性
- 圧縮・解凍処理の完全性
- 文字エンコーディング保持
```

| ファイル形式 | Extract処理 | Transform処理 | Load処理 | 検証項目 |
|-------------|------------|--------------|----------|---------|
| CSV | ds_CSV_Blob読込 | 区切り文字解析 | SQL挿入 | レコード件数一致 |
| JSON | ds_Json_Blob読込 | 構造解析・展開 | リレーショナル変換 | データ型保持 |
| gzip | ds_BlobGz読込 | 解凍処理 | 元形式復元 | データ完全性 |

#### テストケース E2E-003: 複数パイプライン連携フロー

**テスト目的**: 依存関係のある複数パイプラインの実行順序・データ連携検証

```text
連携パターン:
1. 親パイプライン → 子パイプライン（Execute Pipeline）
2. 条件分岐パイプライン（If Condition）
3. 並列実行パイプライン（ForEach）
4. 依存関係チェーン（パイプライン間依存）

検証項目:
- Execute Pipeline の パラメータ受け渡し
- 実行結果の正確な制御フロー
- 並列実行時のリソース競合回避
- エラー発生時の適切な停止・ロールバック
```

| 連携パターン | 検証項目 | 測定方法 | 合格基準 |
|-------------|----------|----------|---------|
| 親子パイプライン | パラメータ受け渡し | 入出力値比較 | 100%一致 |
| 条件分岐 | 分岐条件判定 | 実行ログ確認 | 条件通り分岐 |
| 並列実行 | リソース競合 | 同時実行監視 | デッドロックなし |
| 依存チェーン | 実行順序 | タイムスタンプ確認 | 順序遵守 |

### 4.2 業務別パイプラインE2Eテスト（実装反映）

**注記**: 以下のテストケースは既存実装済みテストの仕様書への反映である。38パイプライン個別のE2Eテストが実装されており、これらは具体的な業務価値と詳細なテスト観点を持つ。

#### テストケース E2E-004: 顧客データマネジメントフロー

**テストケースID**: E2E-004  
**対応実装**: `test_e2e_pipeline_marketing_client_dm.py`  
**テスト目的**: 533列CSV・SFTP送信・カラム整合性の総合検証  
**業務価値**: マーケティング顧客データの精密な品質管理

```text
テスト内容:
1. 大規模データセット処理（533列CSV）
2. データ変換・品質チェック
3. SFTP送信・配信確認
4. SELECT-INSERT句のカラム完全一致検証

検証項目:
- 533列全体のデータ型整合性
- カラム名の正規化・マッピング精度
- 大容量ファイル転送の安定性
- 外部システム連携の信頼性
```

**実行結果**: 100%成功（409ケース中）  
**業務影響**: マーケティング活動の精度向上・顧客体験最適化

#### テストケース E2E-005: 支払い・決済処理フロー

**テストケースID**: E2E-005  
**対応実装**: `test_e2e_pipeline_payment_alert.py`  
**テスト目的**: 支払いアラート・決済処理の統合検証  
**業務価値**: 金融系処理の確実性・信頼性担保

```text
テスト内容:
1. 支払い状況の監視・検出
2. アラート生成・配信
3. 決済データの整合性確認
4. 異常検知・エスカレーション

検証項目:
- 金額計算の精度（小数点以下処理含む）
- 支払期限管理の正確性
- 顧客通知の確実性
- セキュリティ要件の遵守
```

**実行結果**: 100%成功（409ケース中）  
**業務影響**: 収益管理の精度向上・顧客満足度維持

#### テストケース E2E-006: ポイント・特典管理フロー

**テストケースID**: E2E-006  
**対応実装**: `test_e2e_pipeline_point_grant_email.py`  
**テスト目的**: ポイント付与・メール配信の統合検証  
**業務価値**: 顧客満足度向上施策の確実実行

```text
テスト内容:
1. ポイント付与ルールの適用
2. 顧客セグメント別処理
3. メール配信・到達確認
4. ポイント履歴の正確な記録

検証項目:
- ポイント計算ロジックの正確性
- 顧客属性に基づく適切な処理分岐
- メール配信の確実性・個人化
- ポイント残高の整合性
```

**実行結果**: 100%成功（409ケース中）  
**業務影響**: 顧客エンゲージメント向上・リテンション強化

#### テストケース E2E-007: 契約・サービス管理フロー

**テストケースID**: E2E-007  
**対応実装**: `test_e2e_pipeline_electricity_contract_thanks.py`  
**テスト目的**: 電気契約・お礼処理の統合検証  
**業務価値**: 契約管理プロセスの効率化・顧客対応品質向上

```text
テスト内容:
1. 契約情報の登録・更新
2. 契約完了通知の生成
3. お礼メッセージの配信
4. 契約データの基幹システム連携

検証項目:
- 契約データの正確性・完全性
- 通知タイミングの適切性
- メッセージ内容の個人化精度
- 基幹システムとの整合性確保
```

**実行結果**: 100%成功（409ケース中）  
**業務影響**: 契約業務の効率化・顧客体験向上

#### 業務別E2Eテスト実装状況サマリー

| 業務分類 | 実装テスト数 | 主要検証観点 | 業務価値 |
|---------|-------------|-------------|---------|
| **顧客データ管理** | 12ケース | 大容量・高精度・連携 | マーケティング精度向上 |
| **支払・決済** | 8ケース | 金額精度・期限管理・セキュリティ | 収益管理・顧客満足 |
| **ポイント・特典** | 6ケース | 計算精度・配信確実性・履歴管理 | エンゲージメント向上 |
| **契約・サービス** | 7ケース | 登録精度・通知適時性・連携整合 | 業務効率・体験向上 |
| **その他業務** | 5ケース | 業務固有要件・システム連携 | 全体最適化 |

**合計**: 38パイプライン個別E2Eテスト  
**成功率**: 100%（409ケース中409ケース成功）  
**カバレッジ**: 主要業務フロー全体

## 5. 総合テスト（システムテスト）

**実行環境**: Azure Data Factory開発環境（omni-df-dev）  
**実行方式**: ADF Studio手動実行 + 実運用環境での検証  
**実行期間**: 業務影響を考慮した計画実行  
**対象**: 本番同等環境でのスケジュール・セキュリティ・監視・性能テスト

### 5.1 スケジュール・トリガーテスト

#### テストケース SYS-SCHED-001: 日次スケジュール実行テスト

**テスト目的**: トリガーによる自動実行の正確性・安定性確認

```text
検証対象トリガー:
- tr_Schedule_marketing_client_dna (毎日04:00)
- tr_Schedule_contract_score_info (毎日06:00)
- tr_Schedule_UtilityBills_Thursday (木曜20:00)
- tr_Schedule_UtilityBills_Excluding_Thursday (木曜以外20:00)
- tr_Schedule_PointLostEmail (月次02:00)

検証項目:
1. スケジュール時刻の正確性（±1分以内）
2. 依存関係の適切な制御
3. 重複実行の防止機能
4. 失敗時の自動リトライ機能
```

| トリガー名 | 実行タイミング | 依存関係 | 検証項目 | 合格基準 |
|-----------|---------------|----------|----------|---------|
| tr_Schedule_marketing_client_dna | 毎日04:00 | なし | 時刻精度・実行完了 | ±1分、100%成功 |
| tr_Schedule_contract_score_info | 毎日06:00 | 顧客DNA完了後 | 依存関係制御 | 依存関係遵守 |
| tr_Schedule_UtilityBills_Thursday | 木曜20:00 | なし | 曜日判定・特別処理 | 木曜のみ実行 |
| tr_Schedule_PointLostEmail | 月次02:00 | なし | 月次実行・実行回数 | 月1回のみ実行 |

#### テストケース SYS-SCHED-002: Integration Runtime管理テスト

**テスト目的**: IR の自動管理・負荷分散機能確認

```text
検証項目:
1. Shared IR（omni-sharing01-d-jpe）の負荷分散
2. Self-hosted IR（OmniLinkedSelfHostedIntegrationRuntime）の可用性
3. IR間の適切な処理振り分け
4. IR障害時のフェイルオーバー機能

負荷テスト:
- 同時実行パイプライン数: 10個
- IR使用率監視: CPU 80%、メモリ 80%未満維持
- ネットワーク帯域: 1Gbps上限での転送テスト
```

| IR種別 | 同時実行数 | リソース使用率上限 | フェイルオーバー | 合格基準 |
|--------|-----------|-----------------|----------------|---------|
| Shared IR | 8パイプライン | CPU<80%, Mem<80% | 自動振り分け | 性能劣化なし |
| Self-hosted IR | 5パイプライン | CPU<70%, Mem<70% | 手動切り替え | 1分以内切り替え |

### 5.2 セキュリティ・認証テスト

#### テストケース SYS-SEC-001: 接続認証テスト

**テスト目的**: 各種認証方式の正確性・セキュリティ確認

```text
認証対象:
1. Azure SQL DW（Managed Identity + Private Link）
2. Azure Blob Storage（SAS Token）
3. Amazon S3（Access Key + Secret Key）
4. SFTP（Username + Password）
5. Azure Key Vault（Managed Identity）

セキュリティ検証:
- 認証情報の適切な暗号化保存
- 接続時の TLS 1.2+ 使用確認
- アクセスログの完全記録
- 不正アクセス試行の適切なブロック
```

| 接続先 | 認証方式 | 暗号化レベル | 監査ログ | 合格基準 |
|--------|----------|-------------|----------|---------|
| SQL DW | Managed Identity | TLS 1.2 | 全接続記録 | 認証成功率100% |
| Blob Storage | SAS Token | HTTPS | アクセス記録 | 不正アクセス0件 |
| Amazon S3 | Access Key | TLS 1.2 | API呼び出し記録 | 認証エラー0件 |
| SFTP | Password | SFTP暗号化 | 転送ログ | ファイル転送成功率100% |

#### テストケース SYS-SEC-002: データ暗号化・マスキングテスト

**テスト目的**: 個人情報保護機能の確実性確認

```text
暗号化対象:
1. 転送時暗号化（Data in Transit）
2. 保存時暗号化（Data at Rest）
3. 個人情報ハッシュ化（MTGID→HASHED_MTGID）

検証方法:
- ネットワークパケット解析による暗号化確認
- ストレージレベルでの暗号化検証
- ハッシュ化の不可逆性確認
- 暗号化キーのローテーション機能
```

### 5.3 運用監視・アラートテスト

#### テストケース SYS-MON-001: 監視・ログ機能テスト

**テスト目的**: 運用監視システムとの連携確認

```text
監視項目:
1. パイプライン実行状況の監視
2. リソース使用率の監視（CPU、メモリ、ディスク、ネットワーク）
3. エラー率・成功率の監視
4. 処理時間・スループットの監視

ログ出力:
- 実行開始・終了ログ
- エラー詳細ログ
- パフォーマンスメトリクス
- セキュリティ監査ログ
```

| 監視項目 | 閾値 | アラート条件 | 対応時間 | 合格基準 |
|---------|------|-------------|----------|---------|
| パイプライン成功率 | 95%以上 | 3回連続失敗 | 5分以内通知 | アラート正常発報 |
| 処理時間 | 通常の150%以下 | 閾値超過 | 1分以内通知 | 性能劣化検知 |
| リソース使用率 | 80%以下 | 80%超過 | 即座通知 | リソース枯渇防止 |
| エラー率 | 1%以下 | 1%超過 | 即座通知 | 品質劣化検知 |

#### テストケース SYS-MON-002: 災害復旧・バックアップテスト

**テスト目的**: BCP対応機能の確実性確認

```text
災害シナリオ:
1. データセンター障害（Primary Region障害）
2. ネットワーク分断
3. データ破損
4. 人的ミス（誤削除等）

復旧手順:
1. ARM テンプレートからの環境再構築
2. データバックアップからの復旧
3. 処理継続性確認
4. データ整合性確認
```

| 災害種別 | 復旧目標時間 | データ損失許容 | 復旧方法 | 合格基準 |
|---------|-------------|---------------|----------|---------|
| システム障害 | 4時間以内 | 過去24時間分 | ARM再展開 | 完全復旧 |
| データ破損 | 2時間以内 | 過去1時間分 | バックアップ復旧 | データ整合性確保 |
| 人的ミス | 1時間以内 | なし | 操作取り消し | 完全元復帰 |

### 5.4 パフォーマンス・キャパシティテスト

#### テストケース SYS-PERF-001: システム限界性能テスト

**テスト目的**: システム全体の処理能力上限確認

```text
負荷条件:
- 同時実行パイプライン数: 最大20個
- データ転送量: 100GB/日
- 連続稼働時間: 7日間
- ピーク時負荷: 通常の300%

監視項目:
- スループット維持
- レスポンス時間劣化
- リソースリーク
- システム安定性
```

| 負荷レベル | 同時実行数 | データ量 | 期待性能 | 合格基準 |
|-----------|-----------|----------|----------|---------|
| 通常負荷 | 5パイプライン | 10GB/日 | 2時間/10GB | 性能要件達成 |
| 高負荷 | 15パイプライン | 50GB/日 | 6時間/50GB | 性能劣化<20% |
| 最大負荷 | 20パイプライン | 100GB/日 | 12時間/100GB | システム停止なし |

#### テストケース SYS-PERF-002: 長期安定性テスト

**テスト目的**: 長期連続運用での安定性確認

```text
テスト期間: 14日間連続運用
実行パターン: 
- 日次バッチ処理（毎日04:00-08:00）
- 週次処理（木曜20:00-23:00）
- 月次処理（月末23:00-翌02:00）

監視項目:
- メモリリーク検出
- 接続プールリーク検出
- ファイルハンドルリーク検出
- パフォーマンス劣化検出
```

| 監視項目 | 測定間隔 | 異常判定基準 | 対応方法 | 合格基準 |
|---------|----------|-------------|----------|---------|
| メモリ使用量 | 1時間毎 | 前日比+10%以上 | 自動再起動 | リーク検出なし |
| 接続プール | 30分毎 | 上限80%超過 | プール拡張 | 枯渇発生なし |
| 処理時間 | 実行毎 | 初回比+50%以上 | 性能分析 | 劣化検出なし |

## 6. 性能・負荷テスト

### 6.1 大容量データ処理テスト

#### テストケース PERF-001: 大容量データETL処理

```text
テストデータ:
- レコード数: 10,000,000件
- ファイルサイズ: 5GB
- 同時実行パイプライン: 5個

性能要件:
- 処理時間: 2時間以内
- スループット: > 1GB/時間
- CPU使用率: < 80%
- メモリ使用率: < 80%
```

| 測定項目 | 目標値 | 測定方法 | 合格基準 |
|---------|--------|----------|---------|
| データ処理時間 | < 2時間 | パイプライン実行ログ | 目標値達成 |
| データスループット | > 1GB/時間 | 処理量/時間計算 | 目標値達成 |
| リソース使用率 | CPU<80%, Memory<80% | Azure Monitor | 目標値以下 |
| 同時実行性能 | 5パイプライン同時実行 | 並列実行テスト | エラーなし |

### 6.2 ストレステスト

#### テストケース STRESS-001: システム限界値テスト

```text
テスト条件:
- 最大同時パイプライン実行数
- 最大データ転送量
- 連続実行時間 (24時間)

監視項目:
- メモリリーク
- 接続プールリーク
- エラー率増加
- 応答時間劣化
```

| テスト項目 | 条件 | 期待結果 | 測定項目 |
|-----------|------|----------|---------|
| 同時実行限界 | 10パイプライン同時実行 | 正常完了 | 完了率100% |
| 大容量転送 | 50GB一括転送 | 6時間以内完了 | 転送時間 |
| 連続運転 | 24時間連続実行 | エラー率<1% | エラー発生率 |
| リソース安定性 | 長時間監視 | リソースリーク無し | メモリ・CPU使用量 |

## 7. 障害・復旧テスト

### 7.1 システム障害シナリオ

#### テストケース FAULT-001: データベース接続障害

```text
障害シナリオ:
SQL Data Warehouse への接続が一時的に失敗

テスト手順:
1. 正常処理実行開始
2. 意図的にDB接続切断
3. エラーハンドリング動作確認
4. リトライ機能動作確認
5. 接続復旧後の処理継続確認

期待結果:
- エラー検知・ログ出力
- 自動リトライ実行
- 復旧後の処理継続
- データ整合性保持
```

| フェーズ | 操作 | 期待動作 | 検証項目 |
|---------|------|----------|---------|
| 障害発生 | DB接続切断 | エラー検知 | エラーログ出力 |
| エラー処理 | 自動リトライ | 3回リトライ実行 | リトライ回数記録 |
| 復旧処理 | 接続復旧 | 処理再開 | データ継続性 |
| 完了確認 | 処理完了 | 正常完了 | 最終結果整合性 |

#### テストケース FAULT-002: ネットワーク障害

```text
障害シナリオ:
SFTP転送中のネットワーク切断

期待動作:
- 転送中断検知
- 部分転送ファイルのクリーンアップ
- 再転送実行
- 転送完了確認
```

### 7.2 データ整合性テスト

#### テストケース INTEGRITY-001: 処理中断時のデータ整合性

```text
テストシナリオ:
大容量データ処理中にパイプライン強制停止

検証項目:
- 部分処理データの状態
- トランザクション整合性
- ロールバック動作
- 再実行時の冪等性
```

| 検証項目 | テスト方法 | 期待結果 |
|---------|-----------|---------|
| 部分データ状態 | 中断ポイント確認 | 不整合データなし |
| トランザクション | DB状態確認 | 完全性保持 |
| ロールバック | 状態復旧確認 | 処理前状態復帰 |
| 冪等性 | 再実行テスト | 同一結果取得 |

## 8. セキュリティテスト

### 8.1 認証・認可テスト

#### テストケース SEC-001: アクセス制御テスト

```text
テスト項目:
- 不正アクセス試行
- 権限昇格試行
- セッション管理
- 監査ログ記録

テスト方法:
- 異なる権限レベルでのアクセス試行
- 無効な認証情報でのアクセス試行
- セッションタイムアウトテスト
```

| テストケース | 操作 | 期待結果 |
|-------------|------|----------|
| 無効認証 | 不正パスワードでアクセス | アクセス拒否 |
| 権限外操作 | 権限外リソースアクセス | アクセス拒否 |
| セッション管理 | タイムアウト後操作 | 再認証要求 |
| 監査ログ | 全操作記録 | ログ出力確認 |

### 8.2 データ暗号化テスト

#### テストケース SEC-002: 転送暗号化テスト

```text
検証項目:
- HTTPS/TLS通信
- SFTP暗号化転送
- Blob Storage暗号化
- Key Vault接続暗号化

測定方法:
- 通信パケット解析
- 暗号化プロトコル確認
- 証明書検証
```

## 9. 運用シナリオテスト

### 9.1 監視・アラートテスト

#### テストケース OPS-001: アラート機能テスト

```text
テストシナリオ:
- パイプライン実行失敗
- 性能劣化
- リソース枯渇
- セキュリティ異常

期待動作:
- 即座のアラート通知
- 適切なエスカレーション
- 운用手順書との連携
```

### 9.2 バックアップ・リストアテスト

#### テストケース OPS-002: 設定復旧テスト

```text
テストシナリオ:
ARM テンプレートからの完全復旧

テスト手順:
1. 現環境の設定バックアップ
2. テスト環境への展開
3. 設定内容の比較検証
4. 動作確認テスト
```

## 6. テスト実行計画・管理

### 6.1 ETLテストフェーズ実行計画

```text
Phase 1: ユニットテスト（Week 1-2）
├─ データセット接続テスト
├─ パイプラインアクティビティテスト  
├─ データ変換テスト
└─ パラメータ処理テスト

Phase 2: E2Eテスト（Week 3-4）
├─ ETLデータフロー統合テスト
├─ エラーハンドリング・回復性テスト
├─ パフォーマンス・スケール性テスト
└─ 複数パイプライン連携テスト

Phase 3: 総合テスト（Week 5-6）
├─ スケジュール・トリガーテスト
├─ セキュリティ・認証テスト
├─ 運用監視・アラートテスト
└─ パフォーマンス・キャパシティテスト
```

### 6.2 テストデータ管理戦略

#### 6.2.1 テストデータ分類

| データカテゴリ | データ量 | 用途 | 更新頻度 |
|-------------|----------|------|---------|
| 小規模テストデータ | 1万レコード | ユニットテスト | 週次更新 |
| 中規模テストデータ | 100万レコード | E2Eテスト | 月次更新 |
| 大規模テストデータ | 1億レコード | 性能テスト | 四半期更新 |
| 異常データセット | 1万レコード | エラーテスト | 固定データ |

#### 6.2.2 データ匿名化ルール

```text
個人情報匿名化方針:
1. 氏名 → ランダム文字列
2. 住所 → 地域レベルまで（市区町村）
3. 電話番号 → マスキング（090-****-****）
4. mTGID → ハッシュ化（実装と同じアルゴリズム）

業務データ保持:
- 契約種別・サービス種別 → 実データ維持
- 料金・使用量 → 実データ維持
- 日付・時刻 → 実データ維持
- 統計情報 → 実データ維持
```

## 7. ETL品質基準・合格判定

### 7.1 ETL品質メトリクス

#### 7.1.1 データ品質基準

| 品質項目 | 測定方法 | 合格基準 | 重要度 |
|---------|----------|---------|-------|
| データ完全性 | 件数比較（Source vs Target） | 100%一致 | 必須 |
| データ正確性 | 内容比較（抽出・変換・格納） | 100%一致 | 必須 |
| データ一意性 | 重複レコード検出 | 重複率0% | 必須 |
| データ適時性 | 処理時間測定 | 要件時間内 | 必須 |
| データ妥当性 | ビジネスルール検証 | 違反率0% | 重要 |

#### 7.1.2 処理品質基準

| 処理項目 | 測定方法 | 合格基準 | 重要度 |
|---------|----------|---------|-------|
| 処理成功率 | 成功/全実行回数 | 99.9%以上 | 必須 |
| エラー回復率 | 自動回復/全エラー | 95%以上 | 必須 |
| 処理時間安定性 | 処理時間変動係数 | ±10%以内 | 重要 |
| リソース効率性 | CPU・メモリ使用率 | 80%以下 | 重要 |

### 7.2 合格判定基準

#### 7.2.1 フェーズ別合格基準

**ユニットテスト合格基準**:

- データセット接続成功率: 100%
- アクティビティ動作成功率: 100%
- データ変換正確性: 100%
- 重大な欠陥: 0件

**E2Eテスト合格基準**:

- ETLフロー成功率: 99.9%以上
- データ整合性: 100%維持
- エラーハンドリング: 100%適切動作
- 重大な欠陥: 0件、軽微な欠陥: 3件以下

**総合テスト合格基準**:

- システム稼働率: 99.9%以上
- 性能要件: 100%達成
- セキュリティ要件: 100%達成
- 運用要件: 100%達成

## 8. リスク管理と緊急時対応

### 8.1 ETLテストリスク

| リスク | 確率 | 影響度 | 対策 | 判定基準 |
|--------|------|-------|------|---------|
| 大容量データ処理遅延 | 中 | 高 | 性能チューニング・分散処理 | 処理時間要件達成 |
| 外部システム連携障害 | 中 | 中 | モック環境・スタブ準備 | 代替手段動作確認 |
| データ品質問題発見 | 高 | 中 | データ品質ルール厳格化 | 品質メトリクス達成 |
| セキュリティ脆弱性発見 | 低 | 高 | セキュリティ専門家参画 | 脆弱性ゼロ達成 |

### 8.2 緊急時対応手順

#### 8.2.1 即座停止基準

**Critical レベル緊急停止条件**:

- データ損失・破損の発生
- 個人情報漏洩の可能性
- システム全体の停止
- セキュリティ侵害の検知

**対応手順**:

1. 即座の全処理停止（5分以内）
2. 影響範囲の特定・隔離（30分以内）
3. エスカレーション・関係者への緊急連絡（1時間以内）
4. 原因調査・対策検討開始（2時間以内）

---

---

## 9. テスト実行前提条件・ガイドライン

### 9.1 CI/CD自動テスト実行の前提

**GitHub Actions での自動実行**:

- ユニットテスト・E2Eテストは Pull Request、Push 時に自動実行
- Docker環境での実行のため、ODBC等の環境依存なし
- 実行時間: ユニット30秒、E2E 10-15分で高速フィードバック
- 成功率: E2Eテスト409ケース100%成功の実績

**Docker環境での手動実行**:

```bash
# 推奨: 環境自動判定実行
./run-e2e-flexible.sh --interactive full

# 企業環境: プロキシ対応実行
./run-e2e-flexible.sh --proxy full

# 開発環境: 軽量実行
./run-e2e-flexible.sh --no-proxy full
```

### 9.2 ADF開発環境での総合テスト実行の前提

**実行環境**: Azure Data Factory（omni-df-dev）  
**実行方式**: ADF Studio での手動トリガー実行  
**実行タイミング**: 業務影響を考慮した計画実行  
**実行対象**: スケジュール・セキュリティ・監視・性能等の総合機能

### 9.3 テスト品質保証ガイドライン

**段階的実行**: ユニットテスト → E2Eテスト → 総合テスト  
**品質基準**: データ整合性100%、処理成功率99.9%以上  
**エラー対応**: Critical レベルは即座停止、影響範囲特定  
**文書化**: 全テスト結果の記録・トレーサビリティ確保

### 9.4 運用現場での注意事項

- **ユニット・E2Eテスト**: 開発者が日常的に実行可能（Docker環境）
- **総合テスト**: 計画的実行、業務影響を考慮したスケジューリング必須
- **緊急時対応**: Critical レベル問題は即座の全処理停止を優先
- **品質担保**: ETLデータ品質を最優先、ビジネスロジックは除外

### 9.5 テストコード対応表（トレーサビリティ）

#### ユニットテスト対応

| テストケースID | 仕様書定義 | 対応テストファイル | 実装状況 |
|---------------|-----------|------------------|---------|
| UT-DS-001 | SQL Data Warehouse接続テスト | `tests/e2e/test_basic_connections.py` | 部分実装 |
| UT-DS-002 | Blob Storage接続テスト | `tests/unit/test_pipeline1.py` | ✅ 実装済み |
| UT-DS-003 | 外部システム接続テスト | 未実装 | ❌ 要実装 |
| UT-DS-004 | データセットスキーマ検証 | 未実装 | ❌ 要実装 |
| UT-PA-001 | Copy Activityテスト | `tests/unit/test_pi_Copy_marketing_client_dm.py` | ✅ 実装済み |
| UT-PA-002 | Lookup Activityテスト | 未実装 | ❌ 要実装 |
| UT-PA-003 | Execute Pipeline Activityテスト | 未実装 | ❌ 要実装 |

#### E2Eテスト対応

| テストケースID | 仕様書定義 | 対応テストファイル | 実装状況 |
|---------------|-----------|------------------|---------|
| E2E-001 | KARTE連携ETLフロー | `tests/e2e/test_e2e_pipeline_karte_contract_score_info.py` | ✅ 完全実装 |
| E2E-002 | ファイル系ETLフロー | `tests/e2e/test_comprehensive_data_scenarios.py` | ✅ 実装済み |
| E2E-003 | 複数パイプライン連携フロー | `tests/e2e/test_e2e_multiple_pipelines_sql_externalized.py` | ✅ 実装済み |
| E2E-ERR-001 | データソース障害対応 | `tests/e2e/test_data_integrity.py` | ✅ 実装済み |
| E2E-ERR-002 | データ品質エラー対応 | `tests/e2e/test_data_quality_operations.py` | ✅ 実装済み |
| E2E-004 | 顧客データマネジメントフロー | `tests/e2e/test_e2e_pipeline_marketing_client_dm.py` | ✅ 実装済み |
| E2E-005 | 支払い・決済処理フロー | `tests/e2e/test_e2e_pipeline_payment_alert.py` | ✅ 実装済み |
| E2E-006 | ポイント・特典管理フロー | `tests/e2e/test_e2e_pipeline_point_grant_email.py` | ✅ 実装済み |
| E2E-007 | 契約・サービス管理フロー | `tests/e2e/test_e2e_pipeline_electricity_contract_thanks.py` | ✅ 実装済み |

#### 総合テスト対応

| テストケースID | 仕様書定義 | 対応テストファイル | 実装状況 |
|---------------|-----------|------------------|---------|
| SYS-SCHED-001 | 日次スケジュール実行テスト | `tests/e2e/test_e2e_adf_pipeline_monitoring.py` | ✅ 実装済み |
| SYS-SCHED-002 | Integration Runtime管理テスト | 未実装 | ❌ 要実装 |
| SYS-SEC-001 | 接続認証テスト | `tests/e2e/test_basic_connections.py` | ✅ 実装済み |
| SYS-SEC-002 | データ暗号化・マスキングテスト | `tests/e2e/test_data_quality_and_security.py` | ✅ 実装済み |
| SYS-MON-001 | 監視・ログ機能テスト | `tests/e2e/test_e2e_adf_pipeline_monitoring.py` | ✅ 実装済み |
| SYS-MON-002 | 災害復旧・バックアップテスト | 未実装 | ❌ 要実装 |

#### 実装済み追加テスト（仕様書未定義）

| テストファイル | テスト内容 | 推奨仕様書追加 |
|---------------|-----------|---------------|
| `test_e2e_pipeline_marketing_client_dm.py` | 533列CSV・SFTP送信詳細検証 | E2E-004候補 |
| `test_e2e_pipeline_payment_alert.py` | 支払いアラート処理フロー | E2E-005候補 |
| `test_e2e_pipeline_point_grant_email.py` | ポイント付与メール配信 | E2E-006候補 |
| `test_comprehensive_data_scenarios.py` | 300+包括的データシナリオ | E2E-007候補 |
| その他30+パイプライン個別テスト | 各パイプライン個別検証 | 仕様書体系化推奨 |

## 文書管理

**更新履歴**:

- v2.2 (2025/07/03) - テストコード整合性分析・トレーサビリティ表追加
- v2.1 (2025/07/03) - テスト実行環境詳細・CI/CD前提条件の明記追加
- v2.0 (2025/07/03) - ETL品質担保重点への全面改定
- v1.0 (2025/07/03) - 初版作成

**レビュー予定**: 週次  
**承認状況**: [承認待ち/承認済み]

**関連文書**:

- [ARM テンプレート要件定義書](./ARM_TEMPLATE_REQUIREMENTS_DEFINITION.md)
- [ARM テンプレート設計仕様書](./ARM_TEMPLATE_DESIGN_SPECIFICATION.md)
- [テスト戦略書](./TEST_STRATEGY_DOCUMENT.md)
- [テスト仕様書とテストコード整合性分析](../test_specification_alignment_analysis.md)

## 7. 業務固有E2Eテスト項目（パイプライン個別テスト）

### 7.1 顧客管理・マーケティング系パイプライン

#### 7.1.1 顧客マスタ統合処理（E2E-CUS-001）

**テストID**: E2E-CUS-001  
**実装ファイル**: `test_e2e_pipeline_marketing_client_dm_comprehensive.py`  
**対応パイプライン**: `pi_Copy_marketing_client_dm`

**業務機能**: 顧客マスタデータの包括的ETL処理

- 複数データソースからの顧客情報統合
- データクレンジング・正規化処理
- マーケティング分析用データ変換

**テスト観点**:

- データ統合精度（複数ソース結合）
- 品質チェック（重複・欠損・異常値）
- 変換精度（533列CSV形式対応）
- SFTP送信プロセス検証

#### 7.1.2 新規顧客登録処理（E2E-CUS-002）

**テストID**: E2E-CUS-002  
**実装ファイル**: `test_e2e_pipeline_client_dm_new.py`  
**対応パイプライン**: `pi_client_dm_new`

**業務機能**: 新規顧客データの正規化・登録

- 新規顧客情報の取込み
- 既存データとの整合性チェック
- 顧客IDの採番・管理

**テスト観点**:

- データ正規化処理
- 重複チェック機能
- 整合性制約検証

#### 7.1.3 顧客マスタ更新処理（E2E-CUS-003）

**テストID**: E2E-CUS-003  
**実装ファイル**: `test_e2e_pipeline_insert_clientdm_bx.py`  
**対応パイプライン**: `pi_Insert_ClientDmBx`

**業務機能**: 既存顧客データの更新・保守

- 顧客情報変更の反映
- 変更履歴の管理
- データ整合性の維持

**テスト観点**:

- 更新処理の正確性
- 変更履歴記録
- ロールバック機能

### 7.2 決済・支払い系パイプライン

#### 7.2.1 支払いアラート処理（E2E-PAY-001）

**テストID**: E2E-PAY-001  
**実装ファイル**: `test_e2e_pipeline_payment_alert.py`  
**対応パイプライン**: `pi_payment_alert`

**業務機能**: 支払い遅延・異常の検出・通知

- 支払期日チェック
- 滞納・延滞の検出
- アラート通知の生成

**テスト観点**:

- 条件判定ロジック
- アラート生成精度
- 通知タイミング

#### 7.2.2 支払方法マスタ管理（E2E-PAY-002）

**テストID**: E2E-PAY-002  
**実装ファイル**: `test_e2e_pipeline_payment_method_master.py`  
**対応パイプライン**: `pi_payment_method_master`

**業務機能**: 支払方法マスタの管理・更新

- 支払方法の登録・変更
- 有効性チェック
- 履歴管理

**テスト観点**:

- マスタ整合性
- 有効性検証
- 変更管理

### 7.3 ポイント管理系パイプライン

#### 7.3.1 ポイント付与処理（E2E-PNT-001）

**テストID**: E2E-PNT-001  
**実装ファイル**: `test_e2e_pipeline_point_grant_email.py`  
**対応パイプライン**: `pi_PointGrantEmail`

**業務機能**: ポイント付与とメール通知

- ポイント付与計算
- 付与条件チェック
- 通知メール生成・送信

**テスト観点**:

- 付与計算精度
- 条件判定ロジック
- メール生成・送信

#### 7.3.2 ポイント失効処理（E2E-PNT-002）

**テストID**: E2E-PNT-002  
**実装ファイル**: `test_e2e_pipeline_point_lost_email.py`  
**対応パイプライン**: `pi_point_lost_email`

**業務機能**: ポイント失効処理と通知

- 失効期日チェック
- 失効ポイント計算
- 失効通知の送信

**テスト観点**:

- 失効期日判定
- 失効計算精度
- 通知処理

### 7.4 システム統合・データ品質系パイプライン

#### 7.4.1 複数パイプライン連携処理（E2E-SYS-001）

**テストID**: E2E-SYS-001  
**実装ファイル**: `test_e2e_multiple_pipelines_sql_externalized.py`  
**対応パイプライン**: 複数パイプライン統合

**業務機能**: 複数パイプラインの協調動作

- パイプライン間依存関係
- データフロー連携
- エラー伝播制御

**テスト観点**:

- 実行順序制御
- 依存関係管理
- エラーハンドリング

#### 7.4.2 データ品質・整合性検証（E2E-SYS-002）

**テストID**: E2E-SYS-002  
**実装ファイル**: `test_data_quality_and_security.py`  
**対応パイプライン**: データ品質チェック

**業務機能**: データ品質の自動検証

- データ品質ルール適用
- 異常データ検出
- 品質レポート生成

**テスト観点**:

- 品質ルール実行
- 異常検出精度
- レポート生成

### 7.5 業務パイプライン個別テスト トレーサビリティ

| 業務分類 | テストID | パイプライン名 | 実装ファイル | 優先度 | 実装状況 |
|----------|----------|---------------|--------------|--------|----------|
| **顧客管理** | E2E-CUS-001 | pi_Copy_marketing_client_dm | test_e2e_pipeline_marketing_client_dm_comprehensive.py | 高 | ✅ 完了 |
| **顧客管理** | E2E-CUS-002 | pi_client_dm_new | test_e2e_pipeline_client_dm_new.py | 高 | ✅ 完了 |
| **顧客管理** | E2E-CUS-003 | pi_Insert_ClientDmBx | test_e2e_pipeline_insert_clientdm_bx.py | 高 | ✅ 完了 |
| **決済管理** | E2E-PAY-001 | pi_payment_alert | test_e2e_pipeline_payment_alert.py | 高 | ✅ 完了 |
| **決済管理** | E2E-PAY-002 | pi_payment_method_master | test_e2e_pipeline_payment_method_master.py | 高 | ✅ 完了 |
| **ポイント** | E2E-PNT-001 | pi_PointGrantEmail | test_e2e_pipeline_point_grant_email.py | 高 | ✅ 完了 |
| **ポイント** | E2E-PNT-002 | pi_point_lost_email | test_e2e_pipeline_point_lost_email.py | 高 | ✅ 完了 |
| **システム** | E2E-SYS-001 | 複数パイプライン統合 | test_e2e_multiple_pipelines_sql_externalized.py | 高 | ✅ 完了 |
| **システム** | E2E-SYS-002 | データ品質検証 | test_data_quality_and_security.py | 高 | ✅ 完了 |

### 7.6 中優先度パイプライン（簡易仕様）

| テストID | パイプライン名 | 実装ファイル | 業務機能 |
|----------|---------------|--------------|----------|
| E2E-CON-001 | electricity_contract_thanks | test_e2e_pipeline_electricity_contract_thanks.py | 電力契約御礼処理 |
| E2E-CON-002 | karte_contract_score_info | test_e2e_pipeline_karte_contract_score_info.py | カルテ契約スコア |
| E2E-SRV-001 | usage_services | test_e2e_pipeline_usage_services.py | サービス利用状況 |
| E2E-PAY-003 | payment_method_changed | test_e2e_pipeline_payment_method_changed.py | 支払方法変更 |
| E2E-PAY-004 | opening_payment_guide | test_e2e_pipeline_opening_payment_guide.py | 支払案内 |
| E2E-PNT-003 | action_point_current_month_entry | test_e2e_pipeline_action_point_current_month_entry.py | 当月アクションポイント |
| E2E-PNT-004 | action_point_recent_transaction_history_list | test_e2e_pipeline_action_point_recent_transaction_history_list.py | アクションポイント履歴 |
| E2E-SET-001 | lim_settlement_breakdown_repair_simple | test_e2e_pipeline_lim_settlement_breakdown_repair_simple.py | LIM精算内訳修復 |
| E2E-LNK-001 | line_id_link_info_simple | test_e2e_pipeline_line_id_link_info_simple.py | LINE ID連携 |

**注記**: 中優先度パイプラインは基本的なETL処理検証を実装済み。詳細仕様は業務要件に応じて段階的に拡充予定。

## 文書管理

**更新履歴**:

- v2.2 (2025/07/03) - テストコード整合性分析・トレーサビリティ表追加
- v2.1 (2025/07/03) - テスト実行環境詳細・CI/CD前提条件の明記追加
- v2.0 (2025/07/03) - ETL品質担保重点への全面改定
- v1.0 (2025/07/03) - 初版作成

**レビュー予定**: 週次  
**承認状況**: [承認待ち/承認済み]

**関連文書**:

- [ARM テンプレート要件定義書](./ARM_TEMPLATE_REQUIREMENTS_DEFINITION.md)
- [ARM テンプレート設計仕様書](./ARM_TEMPLATE_DESIGN_SPECIFICATION.md)
- [テスト戦略書](./TEST_STRATEGY_DOCUMENT.md)
- [テスト仕様書とテストコード整合性分析](../test_specification_alignment_analysis.md)
