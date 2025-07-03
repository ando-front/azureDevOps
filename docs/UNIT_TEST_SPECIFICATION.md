# Azure Data Factory 単体テスト仕様書

## 1. 文書情報

| 項目 | 内容 |
|------|------|
| 文書名 | Azure Data Factory 単体テスト仕様書 |
| バージョン | 1.0 |
| 作成日 | 2025年7月3日 |
| 作成者 | テスト設計担当 |
| 承認者 | [承認者名] |

## 2. 単体テスト概要

### 2.1 テスト目的

Azure Data Factory の各コンポーネントが個別に正常に動作することを検証する。

### 2.2 テスト対象

- ARM テンプレート構文
- リンクサービス (14個)
- データセット (14個)
- パイプライン (主要25個)
- トリガー (30個)
- Integration Runtime (2個)

### 2.3 テスト環境

- **環境**: 開発環境 (omni-df-dev)
- **実行者**: 開発者・テストエンジニア
- **ツール**: Azure Data Factory Studio、Azure CLI、PowerShell

## 3. ARM テンプレート テスト

### 3.1 テンプレート構文テスト

| テストID | テスト項目 | 期待結果 | 実行方法 |
|---------|-----------|----------|---------|
| ARM-001 | JSON構文チェック | エラーなし | `az deployment group validate` |
| ARM-002 | スキーマ準拠性 | エラーなし | ARM Template Toolkit |
| ARM-003 | パラメータ型チェック | 型一致 | Azure CLI |
| ARM-004 | リソース依存関係 | 循環参照なし | 依存関係分析 |
| ARM-005 | デプロイ可能性 | デプロイ成功 | `az deployment group create` |

#### テストケース ARM-001: JSON構文チェック

```powershell
# テスト実行コマンド
az deployment group validate 
  --resource-group "test-rg" 
  --template-file "ARMTemplateForFactory.json" 
  --parameters @parameters.json

# 期待結果
{
  "error": null,
  "properties": {
    "validatedResources": [...],
    "status": "Succeeded"
  }
}
```

### 3.2 パラメータテスト

| テストID | パラメータ名 | テスト値 | 期待結果 |
|---------|-------------|----------|---------|
| PRM-001 | factoryName | "test-df-001" | 受入 |
| PRM-002 | factoryName | "" | 拒否 |
| PRM-003 | factoryName | "invalid-name!" | 拒否 |
| PRM-004 | li_dam_dwh_connectionString | "Server=test;..." | 受入 |
| PRM-005 | li_dam_dwh_connectionString | null | 拒否 |

## 4. リンクサービス テスト

### 4.1 データベース接続テスト

#### テストケース LS-001: li_dam_dwh 接続テスト

```json
{
  "testId": "LS-001",
  "target": "li_dam_dwh",
  "type": "AzureSqlDW",
  "testSteps": [
    {
      "step": 1,
      "action": "接続テスト実行",
      "command": "Test Connection",
      "expected": "接続成功"
    },
    {
      "step": 2,
      "action": "認証テスト",
      "command": "Authentication Test",
      "expected": "認証成功"
    },
    {
      "step": 3,
      "action": "スキーマ一覧取得",
      "command": "Get Schema List",
      "expected": "omni スキーマ確認"
    }
  ]
}
```

| テストID | サービス名 | 種類 | 期待結果 | テスト方法 |
|---------|-----------|------|----------|-----------|
| LS-001 | li_dam_dwh | AzureSqlDW | 接続成功 | Test Connection |
| LS-002 | li_dam_dwh_shir | AzureSqlDW | 接続成功 | Test Connection |
| LS-003 | li_sqlmi_dwh2 | AzureSqlMI | 接続成功 | Test Connection |
| LS-004 | AzureSqlDatabase1 | AzureSqlDatabase | 接続成功 | Test Connection |

### 4.2 ストレージ接続テスト

| テストID | サービス名 | 種類 | 期待結果 | テスト方法 |
|---------|-----------|------|----------|-----------|
| LS-005 | li_blob_tgomnidevrgsa_container | AzureBlobStorage | 接続成功 | Test Connection |
| LS-006 | li_blob_damhdinsight_container | AzureBlobStorage | 接続成功 | Test Connection |
| LS-007 | li_blob_mytokyogas_container | AzureBlobStorage | 接続成功 | Test Connection |
| LS-008 | li_blob_tgomni_container | AzureBlobStorage | 接続成功 | Test Connection |
| LS-009 | li_blob_tgomnidevrgsa_container_shir | AzureBlobStorage | 接続成功 | Test Connection |

### 4.3 外部システム接続テスト

| テストID | サービス名 | 種類 | 期待結果 | テスト方法 |
|---------|-----------|------|----------|-----------|
| LS-010 | li_Karte_AmazonS3 | AmazonS3 | 接続成功 | Test Connection |
| LS-011 | li_sftp | Sftp | 接続成功 | Test Connection |
| LS-012 | li_ftp_test | Ftp | 接続成功 | Test Connection |
| LS-013 | li_dam_kv_omni | AzureKeyVault | 接続成功 | Test Connection |
| LS-014 | li_blob_damhdinsight_container_akv | AzureBlobStorage | 接続成功 | Test Connection |

## 5. データセット テスト

### 5.1 構造化データセットテスト

#### テストケース DS-001: ds_DamDwhTable スキーマテスト

```json
{
  "testId": "DS-001",
  "target": "ds_DamDwhTable",
  "type": "AzureSqlDWTable",
  "parameters": {
    "table": "test_table"
  },
  "testSteps": [
    {
      "step": 1,
      "action": "データプレビュー",
      "expected": "スキーマ表示"
    },
    {
      "step": 2,
      "action": "パラメータ検証",
      "expected": "table パラメータ適用"
    }
  ]
}
```

| テストID | データセット名 | 種類 | テスト項目 | 期待結果 |
|---------|---------------|------|-----------|---------|
| DS-001 | ds_DamDwhTable | AzureSqlDWTable | スキーマ取得 | 成功 |
| DS-002 | ds_DamDwhTable | AzureSqlDWTable | パラメータ適用 | table名動的設定 |
| DS-003 | ds_DamDwhTable_shir | AzureSqlDWTable | SHIR経由接続 | 成功 |
| DS-004 | ds_sqlmi | AzureSqlMITable | スキーマ取得 | marketing.顧客DNA_推定DM |
| DS-005 | ds_contract_score | AzureSqlDWTable | 詳細スキーマ | 全カラム定義確認 |

### 5.2 半構造化データセットテスト

| テストID | データセット名 | 種類 | テスト項目 | 期待結果 |
|---------|---------------|------|-----------|---------|
| DS-006 | ds_Json_Blob | Json | データプレビュー | JSON構造表示 |
| DS-007 | ds_Json_Blob | Json | パラメータ検証 | directory/filename設定 |
| DS-008 | ds_CSV_Blob | DelimitedText | 区切り文字設定 | カンマ区切り適用 |
| DS-009 | ds_CSV_BlobGz | DelimitedText | 圧縮設定 | gzip圧縮適用 |
| DS-010 | ds_KarteS3 | Json | S3接続 | バケット接続確認 |

### 5.3 バイナリデータセットテスト

| テストID | データセット名 | 種類 | テスト項目 | 期待結果 |
|---------|---------------|------|-----------|---------|
| DS-011 | ds_BlobGz | Binary | ファイル読取 | バイナリデータ取得 |
| DS-012 | ds_Gz_Sftp | Binary | SFTP転送設定 | 転送設定確認 |
| DS-013 | ds_BlobFiles_mTG | Json | ファイル一覧 | JSON形式一覧取得 |
| DS-014 | Json1 | Json | 基本機能 | JSON読み込み確認 |

## 6. パイプライン テスト

### 6.1 主要パイプライン単体テスト

#### テストケース PL-001: pi_Send_karte_contract_score_info

```json
{
  "testId": "PL-001",
  "target": "pi_Send_karte_contract_score_info",
  "description": "Karte契約スコア情報送信パイプライン",
  "testData": {
    "inputRecords": 100,
    "expectedOutput": "CSV file with 100 records"
  },
  "testSteps": [
    {
      "step": 1,
      "activity": "データ取得",
      "expected": "契約スコアデータ100件取得"
    },
    {
      "step": 2,
      "activity": "データ変換",
      "expected": "Karte形式への変換完了"
    },
    {
      "step": 3,
      "activity": "ファイル出力",
      "expected": "CSVファイル生成"
    }
  ]
}
```

| テストID | パイプライン名 | 分類 | テスト項目 | 期待結果 |
|---------|---------------|------|-----------|---------|
| PL-001 | pi_Send_karte_contract_score_info | データ連携 | 実行可能性 | 正常完了 |
| PL-002 | pi_PointLostEmail | マーケティング | 実行可能性 | 正常完了 |
| PL-003 | pi_Send_PaymentAlert | 料金 | 実行可能性 | 正常完了 |
| PL-004 | pi_alert_test2 | 監視 | 実行可能性 | 正常完了 |
| PL-005 | DoUntilPipeline | 制御 | ループ制御 | 条件満足まで実行 |

### 6.2 パイプライン構成要素テスト

| テストID | テスト項目 | 対象パイプライン | 期待結果 |
|---------|-----------|-----------------|---------|
| PL-006 | Copy Activity | 全データ移動パイプライン | データコピー成功 |
| PL-007 | Lookup Activity | 制御パイプライン | メタデータ取得成功 |
| PL-008 | ForEach Activity | バッチ処理パイプライン | 繰り返し処理成功 |
| PL-009 | If Condition | 条件分岐パイプライン | 分岐処理成功 |
| PL-010 | Wait Activity | 遅延制御パイプライン | 指定時間待機 |

### 6.3 エラーハンドリングテスト

| テストID | テスト項目 | エラー条件 | 期待結果 |
|---------|-----------|-----------|---------|
| PL-011 | 接続エラー処理 | DB接続失敗 | エラーログ出力 |
| PL-012 | データエラー処理 | 不正データ | エラー分離処理 |
| PL-013 | タイムアウト処理 | 実行時間超過 | タイムアウトエラー |
| PL-014 | 容量エラー処理 | ディスク容量不足 | 容量エラー通知 |
| PL-015 | リトライ機能 | 一時的エラー | 自動リトライ実行 |

## 7. トリガー テスト

### 7.1 スケジュールトリガーテスト

#### テストケース TR-001: tr_Schedule_contract_score_info

```json
{
  "testId": "TR-001",
  "target": "tr_Schedule_contract_score_info",
  "type": "ScheduleTrigger",
  "schedule": {
    "frequency": "Day",
    "interval": 1,
    "startTime": "2025-07-03T02:00:00Z"
  },
  "testSteps": [
    {
      "step": 1,
      "action": "トリガー有効化",
      "expected": "状態がStartedに変更"
    },
    {
      "step": 2,
      "action": "スケジュール確認",
      "expected": "日次02:00実行設定確認"
    },
    {
      "step": 3,
      "action": "手動実行",
      "expected": "パイプライン実行開始"
    }
  ]
}
```

| テストID | トリガー名 | 頻度 | テスト項目 | 期待結果 |
|---------|-----------|------|-----------|---------|
| TR-001 | tr_Schedule_contract_score_info | 日次 | スケジュール設定 | 正常設定 |
| TR-002 | tr_Schedule_karteS3 | 日次 | スケジュール設定 | 正常設定 |
| TR-003 | tr_Schedule_marketing_client_dna | 日次 | スケジュール設定 | 正常設定 |
| TR-004 | tr_Schedule_UtilityBills_Thursday | 週次(木) | 木曜日設定 | 木曜日のみ実行 |
| TR-005 | tr_Schedule_UtilityBills_Excluding_Thursday | 週次(木以外) | 木曜以外設定 | 木曜以外実行 |

### 7.2 トリガー状態テスト

| テストID | テスト項目 | 操作 | 期待結果 |
|---------|-----------|------|---------|
| TR-006 | トリガー開始 | Start Trigger | runtimeState: Started |
| TR-007 | トリガー停止 | Stop Trigger | runtimeState: Stopped |
| TR-008 | 手動実行 | Trigger Now | パイプライン即座実行 |
| TR-009 | 依存関係確認 | Pipeline Assignment | 正しいパイプライン紐付け |
| TR-010 | スケジュール変更 | Recurrence Update | スケジュール更新適用 |

## 8. Integration Runtime テスト

### 8.1 共有Integration Runtimeテスト

| テストID | テスト項目 | 対象 | 期待結果 |
|---------|-----------|------|---------|
| IR-001 | 接続性確認 | omni-sharing01-d-jpe | 利用可能状態 |
| IR-002 | 実行テスト | Copy Activity | データコピー成功 |
| IR-003 | 性能測定 | 大容量データ処理 | 許容範囲内 |
| IR-004 | 同時実行 | 複数パイプライン | 並列実行成功 |

### 8.2 Self-hosted Integration Runtimeテスト

| テストID | テスト項目 | 対象 | 期待結果 |
|---------|-----------|------|---------|
| IR-005 | SHIR接続確認 | OmniLinkedSelfHostedIntegrationRuntime | 接続成功 |
| IR-006 | Private Link経由アクセス | SHIR経由データアクセス | アクセス成功 |
| IR-007 | セキュリティ確認 | 暗号化通信 | TLS暗号化確認 |
| IR-008 | フェイルオーバー | SHIR障害時動作 | 代替経路利用 |

## 9. テスト実行手順

### 9.1 事前準備

1. **環境準備**
   - 開発環境 (omni-df-dev) のアクセス確認
   - テストデータの準備
   - 必要権限の確認

2. **ツール準備**
   - Azure Data Factory Studio
   - Azure CLI
   - PowerShell
   - テスト結果記録用スプレッドシート

### 9.2 テスト実行順序

```text
1. ARM テンプレートテスト
   ├─ 構文チェック
   ├─ パラメータ検証
   └─ デプロイテスト

2. リンクサービステスト
   ├─ データベース接続
   ├─ ストレージ接続
   └─ 外部システム接続

3. データセットテスト
   ├─ 構造化データセット
   ├─ 半構造化データセット
   └─ バイナリデータセット

4. パイプラインテスト
   ├─ 個別実行テスト
   ├─ アクティビティテスト
   └─ エラーハンドリングテスト

5. トリガーテスト
   ├─ スケジュール設定
   ├─ 状態管理
   └─ 手動実行

6. Integration Runtimeテスト
   ├─ 共有IR
   └─ Self-hosted IR
```

### 9.3 テスト結果記録

各テストケースについて以下を記録：

- **実行日時**
- **実行者**
- **テスト結果** (成功/失敗)
- **実行時間**
- **エラー内容** (失敗時)
- **スクリーンショット** (必要時)

## 10. 合格基準

### 10.1 定量的基準

- **テスト実行率**: 100%
- **成功率**: 95%以上
- **クリティカル欠陥**: 0件
- **メジャー欠陥**: 5件以下

### 10.2 定性的基準

- 全リンクサービスの接続成功
- 全データセットのスキーマ取得成功
- 主要パイプラインの実行成功
- トリガーの正常動作確認
- Integration Runtimeの安定動作

## 11. 缺陥管理

### 11.1 缺陥分類

| 重要度 | 定義 | 対応 |
|--------|------|------|
| Critical | システム停止、データ損失 | 即座修正 |
| Major | 機能不全、性能問題 | 24時間以内修正 |
| Minor | 軽微な問題 | 計画的修正 |
| Cosmetic | 表示問題等 | 必要に応じて修正 |

### 11.2 缺陥記録フォーマット

```json
{
  "defectId": "DEF-001",
  "testId": "LS-001",
  "component": "li_dam_dwh",
  "severity": "Major",
  "description": "接続タイムアウトエラー",
  "reproduction": "Test Connection実行時に30秒でタイムアウト",
  "expected": "接続成功",
  "actual": "ConnectionTimeoutException",
  "environment": "omni-df-dev",
  "foundBy": "テストエンジニア",
  "assignee": "開発者",
  "status": "Open"
}
```

---

**文書管理**

- 更新履歴: v1.0 (2025/07/03) - 初版作成
- レビュー予定: 週次
- 承認状況: [承認待ち/承認済み]
