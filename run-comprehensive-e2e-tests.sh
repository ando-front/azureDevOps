#!/bin/bash

# 包括的E2Eテスト実行スクリプト
# 300+テストケースを効率的に実行し、詳細なレポートを生成

set -e

# カラー出力の設定
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# ログファイルの設定
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_DIR="test_results"
LOG_FILE="${LOG_DIR}/comprehensive_e2e_${TIMESTAMP}.log"
SUMMARY_FILE="${LOG_DIR}/test_summary_${TIMESTAMP}.json"

mkdir -p "${LOG_DIR}"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}   包括的E2Eテスト実行開始${NC}"
echo -e "${BLUE}========================================${NC}"
echo "実行時刻: $(date)"
echo "ログファイル: ${LOG_FILE}"
echo ""

# 実行モードの選択
if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    echo "使用方法:"
    echo "  $0 [オプション]"
    echo ""
    echo "オプション:"
    echo "  --all           すべてのテストを実行 (デフォルト)"
    echo "  --client        クライアント関連テストのみ"
    echo "  --security      セキュリティ関連テストのみ"
    echo "  --performance   パフォーマンステストのみ"
    echo "  --data-quality  データ品質テストのみ"
    echo "  --quick         高速テスト (基本テストのみ)"
    echo "  --parallel      並列実行モード"
    echo "  --verbose       詳細ログ出力"
    echo "  --dry-run       テスト内容のみ表示"
    exit 0
fi

# 実行設定
VERBOSE=false
DRY_RUN=false
PARALLEL=false
TEST_MODE="all"

# 引数解析
while [[ $# -gt 0 ]]; do
    case $1 in
        --verbose)
            VERBOSE=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --parallel)
            PARALLEL=true
            shift
            ;;
        --client)
            TEST_MODE="client"
            shift
            ;;
        --security)
            TEST_MODE="security"
            shift
            ;;
        --performance)
            TEST_MODE="performance"
            shift
            ;;
        --data-quality)
            TEST_MODE="data-quality"
            shift
            ;;
        --quick)
            TEST_MODE="quick"
            shift
            ;;
        --all)
            TEST_MODE="all"
            shift
            ;;
        *)
            echo "不明なオプション: $1"
            exit 1
            ;;
    esac
done

# Python環境の確認
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}エラー: Python3が見つかりません${NC}"
    exit 1
fi

# 必要なパッケージの確認
echo -e "${YELLOW}Python環境を確認中...${NC}"
python3 -c "import pytest, unittest, time, threading, tempfile, sys, re, random, hashlib, base64, json" 2>/dev/null
if [ $? -ne 0 ]; then
    echo -e "${RED}エラー: 必要なPythonパッケージが不足しています${NC}"
    echo "以下のコマンドでインストールしてください:"
    echo "pip install pytest"
    exit 1
fi

# テスト実行関数
run_tests() {
    local test_file="tests/e2e/test_comprehensive_data_scenarios.py"
    local test_args=""
    
    # Pythonパスを設定
    export PYTHONPATH="${PWD}:${PYTHONPATH}"
    
    case $TEST_MODE in
        "client")
            test_args="--client-only"
            echo -e "${BLUE}クライアント関連テストを実行中...${NC}"
            ;;
        "security")
            test_args="--security-only"
            echo -e "${BLUE}セキュリティ関連テストを実行中...${NC}"
            ;;
        "performance")
            test_args="-k performance"
            echo -e "${BLUE}パフォーマンステストを実行中...${NC}"
            ;;
        "data-quality")
            test_args="-k 'data_validation or data_cleansing or data_transformation'"
            echo -e "${BLUE}データ品質テストを実行中...${NC}"
            ;;
        "quick")
            test_args="-k 'scenario_001 or scenario_002'"
            echo -e "${BLUE}高速テスト（基本テストのみ）を実行中...${NC}"
            ;;
        "all")
            echo -e "${BLUE}すべてのテストを実行中...${NC}"
            ;;
    esac
    
    if [ "$DRY_RUN" = true ]; then
        echo -e "${YELLOW}ドライランモード: 以下のコマンドが実行されます${NC}"
        if [ "$TEST_MODE" = "client" ] || [ "$TEST_MODE" = "security" ]; then
            echo "PYTHONPATH=${PWD} python3 ${test_file} ${test_args}"
        else
            echo "PYTHONPATH=${PWD} python3 -m pytest ${test_file} ${test_args} --verbose"
        fi
        return 0
    fi
    
    # テスト実行時間の測定開始
    START_TIME=$(date +%s)
    
    if [ "$VERBOSE" = true ]; then
        if [ "$TEST_MODE" = "client" ] || [ "$TEST_MODE" = "security" ]; then
            PYTHONPATH="${PWD}" python3 "${test_file}" ${test_args} --verbose 2>&1 | tee "${LOG_FILE}"
        else
            PYTHONPATH="${PWD}" python3 -m pytest "${test_file}" ${test_args} --verbose -s 2>&1 | tee "${LOG_FILE}"
        fi
    else
        if [ "$TEST_MODE" = "client" ] || [ "$TEST_MODE" = "security" ]; then
            PYTHONPATH="${PWD}" python3 "${test_file}" ${test_args} > "${LOG_FILE}" 2>&1
        else
            PYTHONPATH="${PWD}" python3 -m pytest "${test_file}" ${test_args} > "${LOG_FILE}" 2>&1
        fi
    fi
    
    TEST_EXIT_CODE=$?
    END_TIME=$(date +%s)
    EXECUTION_TIME=$((END_TIME - START_TIME))
    
    return $TEST_EXIT_CODE
}

# 並列実行関数
run_tests_parallel() {
    echo -e "${BLUE}並列実行モードでテストを実行中...${NC}"
    
    # テストカテゴリを並列実行
    categories=("client" "security" "performance" "data-quality")
    pids=()
    
    for category in "${categories[@]}"; do
        if [ "$VERBOSE" = true ]; then
            echo "カテゴリ ${category} を並列実行中..."
        fi
        
        # 各カテゴリを背景で実行
        (
            CATEGORY_LOG="${LOG_DIR}/parallel_${category}_${TIMESTAMP}.log"
            TEST_MODE="$category" run_tests > "${CATEGORY_LOG}" 2>&1
            echo $? > "${LOG_DIR}/exit_code_${category}.tmp"
        ) &
        
        pids+=($!)
    done
    
    # すべての並列プロセスの完了を待機
    exit_codes=()
    for i in "${!pids[@]}"; do
        wait ${pids[$i]}
        category=${categories[$i]}
        if [ -f "${LOG_DIR}/exit_code_${category}.tmp" ]; then
            exit_code=$(cat "${LOG_DIR}/exit_code_${category}.tmp")
            exit_codes+=($exit_code)
            rm -f "${LOG_DIR}/exit_code_${category}.tmp"
        else
            exit_codes+=(1)
        fi
    done
    
    # 並列実行結果の統合
    cat "${LOG_DIR}"/parallel_*_${TIMESTAMP}.log > "${LOG_FILE}"
    
    # 全体の成功判定
    for code in "${exit_codes[@]}"; do
        if [ $code -ne 0 ]; then
            return 1
        fi
    done
    
    return 0
}

# メインテスト実行
if [ "$PARALLEL" = true ]; then
    run_tests_parallel
    OVERALL_EXIT_CODE=$?
else
    run_tests
    OVERALL_EXIT_CODE=$?
fi

# 結果サマリーの生成
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}   テスト実行結果サマリー${NC}"
echo -e "${BLUE}========================================${NC}"

if [ $OVERALL_EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✓ すべてのテストが成功しました${NC}"
    STATUS="SUCCESS"
else
    echo -e "${RED}✗ 一部のテストが失敗しました${NC}"
    STATUS="FAILURE"
fi

echo "実行モード: ${TEST_MODE}"
echo "実行時間: ${EXECUTION_TIME:-0}秒"
echo "ログファイル: ${LOG_FILE}"

# テスト結果の統計情報を抽出
if [ -f "${LOG_FILE}" ]; then
    TOTAL_TESTS=$(grep -c "test_.*\.py::" "${LOG_FILE}" 2>/dev/null || grep -c "test_" "${LOG_FILE}" 2>/dev/null || echo "N/A")
    PASSED_TESTS=$(grep -c "PASSED\|OK" "${LOG_FILE}" 2>/dev/null || echo "N/A")
    FAILED_TESTS=$(grep -c "FAILED\|FAIL\|ERROR" "${LOG_FILE}" 2>/dev/null || echo "0")
    
    echo "総テスト数: ${TOTAL_TESTS}"
    echo "成功: ${PASSED_TESTS}"
    echo "失敗: ${FAILED_TESTS}"
fi

# JSON形式でのサマリー出力
cat > "${SUMMARY_FILE}" << EOF
{
    "timestamp": "$(date -Iseconds)",
    "test_mode": "${TEST_MODE}",
    "execution_time_seconds": ${EXECUTION_TIME:-0},
    "overall_status": "${STATUS}",
    "total_tests": "${TOTAL_TESTS}",
    "passed_tests": "${PASSED_TESTS}",
    "failed_tests": "${FAILED_TESTS}",
    "log_file": "${LOG_FILE}",
    "parallel_execution": ${PARALLEL}
}
EOF

echo "詳細サマリー: ${SUMMARY_FILE}"

# 失敗した場合の詳細表示
if [ $OVERALL_EXIT_CODE -ne 0 ] && [ "$VERBOSE" = false ]; then
    echo ""
    echo -e "${YELLOW}失敗したテストの詳細:${NC}"
    grep -A 3 -B 1 "FAILED\|ERROR" "${LOG_FILE}" | head -20
    echo ""
    echo "完全なログを確認するには: cat ${LOG_FILE}"
fi

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}   テスト実行完了${NC}"
echo -e "${BLUE}========================================${NC}"

exit $OVERALL_EXIT_CODE
