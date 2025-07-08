# E2E V2 テスト実行ガイド

## 目次
1. [クイックスタート](#クイックスタート)
2. [実行コマンド集](#実行コマンド集)
3. [テスト結果の見方](#テスト結果の見方)
4. [トラブルシューティング](#トラブルシューティング)
5. [継続的インテグレーション](#継続的インテグレーション)

## クイックスタート

### 前提条件
- Python 3.10以上
- 作業ディレクトリ: `/mnt/c/Users/angie/git/azureDevOps`

### 全テスト実行（推奨）
```bash
# プロジェクトルートから実行
python3 e2e_v2/scripts/run_all_tests.py
```

**期待結果**:
```
成功率: 100.0% (52/52テスト)
実行時間: 約12秒
終了コード: 0
```

## 実行コマンド集

### 1. 全体実行

#### 基本実行
```bash
python3 e2e_v2/scripts/run_all_tests.py
```

#### 詳細ログ付き実行
```bash
python3 e2e_v2/scripts/run_all_tests.py 2>&1 | tee test_execution.log
```

### 2. ドメイン別実行

#### KENDENKIドメイン（ポイント管理）
```bash
# ポイント付与メール
python3 -c "
import sys
sys.path.insert(0, '.')
from e2e_v2.domains.kendenki.test_point_grant_email import TestPointGrantEmailPipeline
test = TestPointGrantEmailPipeline()
print('=== ポイント付与メールテスト ===')
test.test_functional_with_file_exists()
test.test_data_quality_validation()
test.test_performance_large_dataset()
print('✓ 全テスト完了')
"

# ポイント失効メール
python3 -c "
import sys
sys.path.insert(0, '.')
from e2e_v2.domains.kendenki.test_point_lost_email import TestPointLostEmailPipeline
test = TestPointLostEmailPipeline()
print('=== ポイント失効メールテスト ===')
test.test_functional_with_file_exists()
test.test_integration_sftp_connectivity()
print('✓ 全テスト完了')
"

# 使用サービスmTGID
python3 -c "
import sys
sys.path.insert(0, '.')
from e2e_v2.domains.kendenki.test_usage_service_mtgid import TestUsageServiceMtgIdPipeline
test = TestUsageServiceMtgIdPipeline()
print('=== 使用サービスmTGIDテスト ===')
test.test_functional_with_file_exists()
test.test_integration_database_operations()
print('✓ 全テスト完了')
"
```

#### SMCドメイン（支払い・請求）
```bash
# 支払いアラート
python3 -c "
import sys
sys.path.insert(0, '.')
from e2e_v2.domains.smc.test_payment_alert import TestPaymentAlertPipeline
test = TestPaymentAlertPipeline()
print('=== 支払いアラートテスト ===')
test.test_functional_payment_alerts()
test.test_functional_overdue_processing()
print('✓ 全テスト完了')
"

# 公共料金処理
python3 -c "
import sys
sys.path.insert(0, '.')
from e2e_v2.domains.smc.test_utility_bills import TestUtilityBillsPipeline
test = TestUtilityBillsPipeline()
print('=== 公共料金処理テスト ===')
test.test_functional_utility_bills_processing()
test.test_functional_seasonal_adjustment()
print('✓ 全テスト完了')
"
```

#### ACTIONPOINTドメイン（ポイントイベント）
```bash
# アクションポイント登録イベント
python3 -c "
import sys
sys.path.insert(0, '.')
from e2e_v2.domains.actionpoint.test_actionpoint_entry_event import TestActionPointEntryEventPipeline
test = TestActionPointEntryEventPipeline()
print('=== アクションポイント登録イベントテスト ===')
test.test_functional_entry_event_processing()
test.test_functional_campaign_event_processing()
print('✓ 全テスト完了')
"

# アクションポイント取引履歴
python3 -c "
import sys
sys.path.insert(0, '.')
from e2e_v2.domains.actionpoint.test_actionpoint_transaction_history import TestActionPointTransactionHistoryPipeline
test = TestActionPointTransactionHistoryPipeline()
print('=== アクションポイント取引履歴テスト ===')
test.test_functional_transaction_event_processing()
test.test_performance_bulk_transaction_processing()
print('✓ 全テスト完了')
"
```

#### MARKETINGドメイン（顧客マーケティング）
```bash
# 顧客ダイレクトメール
python3 -c "
import sys
sys.path.insert(0, '.')
from e2e_v2.domains.marketing.test_client_dm import TestClientDMPipeline
test = TestClientDMPipeline()
print('=== 顧客ダイレクトメールテスト ===')
test.test_functional_client_dm_processing()
test.test_functional_customer_segmentation()
test.test_functional_opt_out_filtering()
print('✓ 全テスト完了')
"
```

#### TGCONTRACTドメイン（契約スコア）
```bash
# 契約スコア情報
python3 -c "
import sys
sys.path.insert(0, '.')
from e2e_v2.domains.tgcontract.test_contract_score_info import TestContractScoreInfoPipeline
test = TestContractScoreInfoPipeline()
print('=== 契約スコア情報テスト ===')
test.test_functional_score_calculation()
test.test_functional_risk_assessment()
test.test_functional_premium_customer_identification()
print('✓ 全テスト完了')
"
```

#### INFRASTRUCTUREドメイン（データ複製）
```bash
# マーケティング顧客DM複製
python3 -c "
import sys
sys.path.insert(0, '.')
from e2e_v2.domains.infrastructure.test_copy_marketing_client_dm import TestCopyMarketingClientDmPipeline
test = TestCopyMarketingClientDmPipeline()
print('=== マーケティング顧客DM複製テスト ===')
test.test_functional_data_copy_processing()
test.test_functional_data_quality_improvement()
test.test_functional_incremental_processing()
print('✓ 全テスト完了')
"
```

### 3. カテゴリ別実行

#### 機能テストのみ
```bash
python3 -c "
import sys
sys.path.insert(0, '.')
from e2e_v2.scripts.run_all_tests import E2ETestRunner

runner = E2ETestRunner()
# 機能テストメソッドのみ抽出して実行
functional_methods = [
    ('functional_file_exists', '機能テスト(ファイル有り)'),
    ('functional_no_file', '機能テスト(ファイル無し)'),
    ('functional_with_file_exists', '機能テスト(ファイル有り)'),
    ('functional_without_file', '機能テスト(ファイル無し)')
]
runner.test_methods = functional_methods
runner.run_all_tests()
"
```

#### パフォーマンステストのみ
```bash
python3 -c "
import sys
sys.path.insert(0, '.')
from e2e_v2.scripts.run_all_tests import E2ETestRunner

runner = E2ETestRunner()
performance_methods = [
    ('performance_large_dataset', 'パフォーマンステスト'),
    ('performance_large_customer_base', 'パフォーマンステスト(大量顧客)'),
    ('performance_bulk_processing', 'パフォーマンステスト(大量)')
]
runner.test_methods = performance_methods
runner.run_all_tests()
"
```

### 4. 個別テストメソッド実行

#### 特定メソッドのみ実行
```bash
# データ品質テストのみ
python3 -c "
import sys
sys.path.insert(0, '.')
from e2e_v2.domains.kendenki.test_point_grant_email import TestPointGrantEmailPipeline
test = TestPointGrantEmailPipeline()
test.test_data_quality_validation()
print('データ品質テスト完了')
"

# 統合テストのみ
python3 -c "
import sys
sys.path.insert(0, '.')
from e2e_v2.domains.kendenki.test_point_lost_email import TestPointLostEmailPipeline
test = TestPointLostEmailPipeline()
test.test_integration_sftp_connectivity()
print('統合テスト完了')
"
```

## テスト結果の見方

### 成功例
```
INFO:__main__:=== E2E V2 全パイプラインテスト開始 ===
INFO:__main__:パイプラインテスト開始: kendenki/pi_PointGrantEmail
INFO:__main__:    ✓ 機能テスト(ファイル有り) - 0.00秒
INFO:__main__:    ✓ 機能テスト(ファイル無し) - 0.00秒
INFO:__main__:    ✓ データ品質テスト - 0.00秒
INFO:__main__:    ✓ パフォーマンステスト - 0.47秒
INFO:__main__:    ✓ 統合テスト(SFTP) - 0.00秒
INFO:__main__:パイプラインテスト完了: pi_PointGrantEmail - 成功率: 100.0%
```

### 警告例
```
Warning: ビジネスルール違反が検出されました: ['行3: 請求月形式不正 (999999)']
Warning: 一部キャンペーンが生成されませんでした: ['BUSINESS_CAMPAIGN']
```

### エラー例
```
ERROR:__main__:    ✗ 機能テスト(ファイル無し): ファイルが存在する（テスト条件不正）
AssertionError: パイプライン実行失敗: ['データ形式エラー']
```

### 最終レポート例
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

## KENDENKIドメイン
### pi_PointGrantEmail
- 成功率: 100.0% (5/5)
- 実行時間: 0.47秒
- テスト詳細:
  ✓ 機能テスト(ファイル有り)
  ✓ 機能テスト(ファイル無し)
  ✓ データ品質テスト
  ✓ パフォーマンステスト
  ✓ 統合テスト(SFTP)
```

### 終了コード
- **0**: 全テスト成功
- **1**: 1つ以上のテスト失敗

## トラブルシューティング

### よくある問題と解決方法

#### 1. ImportError: No module named 'e2e_v2'
**原因**: Pythonパスが正しく設定されていない

**解決方法**:
```bash
# プロジェクトルートディレクトリにいることを確認
pwd
# /mnt/c/Users/angie/git/azureDevOps であることを確認

# 正しいディレクトリから実行
python3 e2e_v2/scripts/run_all_tests.py
```

#### 2. 成功率が100%未満
**原因**: テストデータやロジックの問題

**診断方法**:
```bash
# 失敗したテストのみ個別実行
python3 -c "
import sys
sys.path.insert(0, '.')
from e2e_v2.domains.{domain}.test_{pipeline} import Test{Pipeline}Pipeline
try:
    test = Test{Pipeline}Pipeline()
    test.test_{failed_method}()
    print('テスト成功')
except Exception as e:
    print(f'エラー詳細: {e}')
    import traceback
    traceback.print_exc()
"
```

#### 3. パフォーマンステスト失敗
**原因**: 処理時間やスループットが基準未達

**確認項目**:
- システムリソース使用状況
- データサイズ設定
- 処理ロジックの効率性

**調整方法**:
```python
# テスト基準値の調整（必要に応じて）
# e2e_v2/base/pipeline_test_base.py内
assert throughput > 5000  # 基準値を下げる
```

#### 4. メモリ不足エラー
**原因**: 大量データ処理時のメモリ不足

**解決方法**:
```bash
# 大量データテストのデータサイズ削減
# または十分なメモリ環境での実行
free -h  # メモリ使用量確認
```

#### 5. ファイルパスエラー
**原因**: Windows/Linux パス区切り文字の違い

**確認方法**:
```python
import os
print(f"現在のディレクトリ: {os.getcwd()}")
print(f"パス区切り文字: {os.sep}")
```

### デバッグ用コマンド

#### ログレベル調整
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

#### データ内容確認
```python
# テストデータ生成内容確認
test = TestPipelinePipeline()
test_data = test._generate_test_data()
print(f"Generated data:\n{test_data[:500]}...")  # 最初の500文字表示
```

#### 処理時間測定
```python
import time
start_time = time.time()
# テスト実行
execution_time = time.time() - start_time
print(f"実行時間: {execution_time:.2f}秒")
```

## 継続的インテグレーション

### GitHub Actions設定例

#### .github/workflows/e2e-tests.yml
```yaml
name: E2E V2 Tests

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

jobs:
  e2e-tests:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python
      uses: actions/setup-python@v3
      with:
        python-version: '3.10'
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r requirements.txt
    
    - name: Run E2E Tests
      run: |
        python3 e2e_v2/scripts/run_all_tests.py
    
    - name: Upload test reports
      uses: actions/upload-artifact@v3
      if: always()
      with:
        name: test-reports
        path: e2e_v2/reports/
```

### Azure DevOps Pipeline設定例

#### azure-pipelines.yml
```yaml
trigger:
- main
- develop

pool:
  vmImage: 'ubuntu-latest'

steps:
- task: UsePythonVersion@0
  inputs:
    versionSpec: '3.10'
  displayName: 'Use Python 3.10'

- script: |
    python -m pip install --upgrade pip
    pip install -r requirements.txt
  displayName: 'Install dependencies'

- script: |
    python3 e2e_v2/scripts/run_all_tests.py
  displayName: 'Run E2E Tests'

- task: PublishTestResults@2
  condition: always()
  inputs:
    testResultsFiles: 'e2e_v2/reports/*.json'
    testRunTitle: 'E2E V2 Test Results'

- task: PublishBuildArtifacts@1
  condition: always()
  inputs:
    pathToPublish: 'e2e_v2/reports'
    artifactName: 'test-reports'
```

### 定期実行設定

#### crontab設定例
```bash
# 毎日午前2時に実行
0 2 * * * cd /path/to/azureDevOps && python3 e2e_v2/scripts/run_all_tests.py >> /var/log/e2e_tests.log 2>&1

# 毎週月曜日午前6時に実行
0 6 * * 1 cd /path/to/azureDevOps && python3 e2e_v2/scripts/run_all_tests.py
```

### テスト結果通知

#### Slack通知例
```bash
#!/bin/bash
# run_e2e_with_notification.sh

cd /path/to/azureDevOps
python3 e2e_v2/scripts/run_all_tests.py > test_result.log 2>&1

if [ $? -eq 0 ]; then
    # 成功時
    curl -X POST -H 'Content-type: application/json' \
        --data '{"text":"✅ E2E V2 Tests: All tests passed!"}' \
        YOUR_SLACK_WEBHOOK_URL
else
    # 失敗時
    curl -X POST -H 'Content-type: application/json' \
        --data '{"text":"❌ E2E V2 Tests: Some tests failed. Check logs for details."}' \
        YOUR_SLACK_WEBHOOK_URL
fi
```

### メトリクス収集

#### テスト実行履歴の記録
```python
import json
import datetime

def save_test_metrics(report_data):
    metrics = {
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "success_rate": report_data["success_rate"],
        "execution_time": report_data["execution_time"],
        "total_tests": report_data["total_tests"],
        "failed_tests": report_data["failed_tests"]
    }
    
    with open("test_metrics_history.jsonl", "a") as f:
        f.write(json.dumps(metrics) + "\n")
```

---

**Document Version**: 1.0  
**Last Updated**: 2025-07-08  
**Author**: Claude Code Assistant  
**Status**: Production Ready