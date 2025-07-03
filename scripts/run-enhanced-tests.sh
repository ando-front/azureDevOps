#!/bin/bash
# scripts/run-enhanced-tests.sh
# GitHub Actions用の拡張テスト実行スクリプト
# 目的: YMLファイルから詳細なテストロジックを分離し、保守性を向上

set -e

echo "🚀 Azure Data Factory 拡張テスト実行開始"

# 環境変数確認
CONTAINER_NAME="pytest-test"
RESULTS_DIR="/app/test-results"

# テスト実行関数
run_test_with_reporting() {
    local test_file="$1"
    local test_id="$2"
    local test_description="$3"
    local output_prefix="$4"
    
    echo "📝 Running ${test_description} (${test_id})"
    
    if docker exec $CONTAINER_NAME python -m pytest "$test_file" \
        -v --tb=short --junit-xml="${RESULTS_DIR}/${output_prefix}-results.xml" \
        --html="${RESULTS_DIR}/${output_prefix}-report.html" --self-contained-html; then
        echo "✅ ${test_description} (${test_id}) 成功"
        return 0
    else
        echo "❌ ${test_description} (${test_id}) 失敗"
        docker exec $CONTAINER_NAME cat "${RESULTS_DIR}/${output_prefix}-results.xml" 2>/dev/null || echo "結果ファイルなし"
        return 1
    fi
}

# 新規実装テスト（自動化必須項目）
echo "🔗 新規実装テスト実行中..."

run_test_with_reporting \
    "tests/unit/test_ut_ds_001_linkedservice_connections_complete.py" \
    "UT-DS-001" \
    "LinkedService接続テスト" \
    "ut-ds-001"

run_test_with_reporting \
    "tests/unit/test_ut_ds_004_dataset_schema_validation.py" \
    "UT-DS-004" \
    "データセットスキーマ検証テスト" \
    "ut-ds-004"

# 統一命名規則適用済みテスト
echo "🔄 統一命名規則適用済みテスト実行中..."

run_test_with_reporting \
    "tests/unit/test_pi_Insert_ClientDmBx.py" \
    "UT-PI-003" \
    "pi_Insert_ClientDmBx テスト" \
    "ut-pi-003"

run_test_with_reporting \
    "tests/unit/test_pi_Send_ActionPointCurrentMonthEntryList.py" \
    "UT-PI-004" \
    "pi_Send_ActionPointCurrentMonthEntryList テスト" \
    "ut-pi-004"

# 段階的実装テスト（フェーズ2）
echo "🔧 Integration Runtime管理テスト（段階2実装）実行中..."

run_test_with_reporting \
    "tests/e2e/test_sys_sched_002_integration_runtime_management.py" \
    "SYS-SCHED-002" \
    "Integration Runtime管理（災害復旧含む）" \
    "sys-sched-002"

echo "✅ 全拡張テスト実行完了"
