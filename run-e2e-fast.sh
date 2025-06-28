#!/bin/bash

# =================================================================
# 高速E2Eテスト実行スクリプト（最適化版）
# テスト実行時間を大幅に短縮し、効率的にテストを実行
# =================================================================

set -euo pipefail

# カラー定義
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# ロギング関数
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
    echo -e "${PURPLE}[STEP]${NC} $1"
}

# 使用方法の表示
usage() {
    cat << EOF
E2Eテスト高速実行スクリプト（最適化版）

使用方法:
  $0 [オプション] [テストモード]

テストモード:
  quick       - 基本接続テストのみ（1-2分）
  standard    - 主要パイプラインテスト（5-10分）
  full        - 全E2Eテスト（15-20分）
  single      - 単一テストファイルを指定実行

オプション:
  -h, --help          この使用方法を表示
  -c, --clean         実行前にDockerボリュームをクリーンアップ
  -p, --parallel      並列実行を有効化
  -v, --verbose       詳細ログを表示
  -t, --timeout SEC   テストタイムアウト時間（秒）
  -f, --file FILE     単一テストファイルを指定（singleモード用）

例:
  $0 quick                     # クイックテスト
  $0 standard --parallel       # 標準テスト（並列実行）
  $0 full --clean --verbose    # 全テスト（クリーンアップ＋詳細ログ）
  $0 single -f test_docker_e2e_client_dm.py  # 単一ファイルテスト

EOF
}

# デフォルト設定
TEST_MODE="standard"
CLEAN_VOLUMES=false
PARALLEL_EXECUTION=false
VERBOSE=false
TIMEOUT=180
SINGLE_TEST_FILE=""
COMPOSE_FILE="docker-compose.e2e.optimized.yml"

# 引数解析
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage
            exit 0
            ;;
        -c|--clean)
            CLEAN_VOLUMES=true
            shift
            ;;
        -p|--parallel)
            PARALLEL_EXECUTION=true
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -t|--timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        -f|--file)
            SINGLE_TEST_FILE="$2"
            shift 2
            ;;
        quick|standard|full|single)
            TEST_MODE="$1"
            shift
            ;;
        *)
            log_error "不明なオプション: $1"
            usage
            exit 1
            ;;
    esac
done

# singleモード用の検証
if [[ "$TEST_MODE" == "single" && -z "$SINGLE_TEST_FILE" ]]; then
    log_error "singleモードではテストファイルを指定してください（-f オプション）"
    exit 1
fi

# 設定確認
log_info "E2Eテスト設定:"
log_info "  テストモード: $TEST_MODE"
log_info "  クリーンアップ: $CLEAN_VOLUMES"
log_info "  並列実行: $PARALLEL_EXECUTION"
log_info "  詳細ログ: $VERBOSE"
log_info "  タイムアウト: ${TIMEOUT}秒"
[[ -n "$SINGLE_TEST_FILE" ]] && log_info "  単一テストファイル: $SINGLE_TEST_FILE"

# Dockerが実行中かチェック
if ! docker info >/dev/null 2>&1; then
    log_error "Dockerが実行されていません。Dockerを起動してから再実行してください。"
    exit 1
fi

# 既存のE2Eコンテナを停止・削除
cleanup_containers() {
    log_step "既存のE2Eコンテナをクリーンアップ中..."
    
    # E2E関連コンテナを停止
    docker-compose -f "$COMPOSE_FILE" down --remove-orphans 2>/dev/null || true
    
    # 孤立したコンテナを削除
    docker container prune -f >/dev/null 2>&1 || true
    
    if [[ "$CLEAN_VOLUMES" == true ]]; then
        log_step "Dockerボリュームをクリーンアップ中..."
        docker-compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
        docker volume prune -f >/dev/null 2>&1 || true
    fi
    
    log_success "クリーンアップ完了"
}

# テスト環境の起動
start_test_environment() {
    log_step "E2Eテスト環境を起動中..."
    
    # バックグラウンドサービスを起動
    if [[ "$VERBOSE" == true ]]; then
        docker-compose -f "$COMPOSE_FILE" up -d sqlserver-test azurite-test ir-simulator
    else
        docker-compose -f "$COMPOSE_FILE" up -d sqlserver-test azurite-test ir-simulator >/dev/null 2>&1
    fi
    
    # サービスの健全性チェック
    log_step "サービスの起動を待機中..."
    
    # Azuriteの起動を確認
    if curl -s http://localhost:10000/ >/dev/null 2>&1; then
        log_success "Azuriteが準備完了"
    else
        log_warning "Azuriteに接続できません（一部テストに影響する可能性があります）"
    fi
    
    log_success "テスト環境の起動完了"
}

# テスト実行
run_tests() {
    log_step "E2Eテストを実行中..."
    
    local pytest_options="-v --tb=short --junitxml=/app/test_results/results.xml"
    local test_files=""
    
    # 並列実行の設定
    if [[ "$PARALLEL_EXECUTION" == true ]]; then
        pytest_options="$pytest_options -n 2"
    fi
    
    # タイムアウトの設定
    pytest_options="$pytest_options --timeout=$TIMEOUT"
    
    # ログレベルの設定
    if [[ "$VERBOSE" == true ]]; then
        pytest_options="$pytest_options --capture=no"
    else
        pytest_options="$pytest_options --quiet"
    fi
    
    # テストモードに応じたテストファイルの選択
    case "$TEST_MODE" in
        quick)
            test_files="/app/tests/e2e/test_basic_connections.py"
            log_info "クイックテスト実行中（約1-2分）..."
            ;;
        standard)
            test_files="/app/tests/e2e/test_basic_connections.py /app/tests/e2e/test_docker_e2e_integration.py /app/tests/e2e/test_e2e_pipeline_client_dm_new.py"
            log_info "標準テスト実行中（約5-10分）..."
            ;;
        full)
            test_files="/app/tests/e2e/"
            pytest_options="$pytest_options --maxfail=10"
            log_info "全テスト実行中（約15-20分）..."
            ;;
        single)
            test_files="/app/tests/e2e/$SINGLE_TEST_FILE"
            log_info "単一テストファイル実行中: $SINGLE_TEST_FILE"
            ;;
    esac
    
    # テスト結果ディレクトリの作成
    docker-compose -f "$COMPOSE_FILE" exec -T e2e-test-runner mkdir -p /app/test_results 2>/dev/null || true
    
    # テストの実行
    if docker-compose -f "$COMPOSE_FILE" up --abort-on-container-exit --exit-code-from e2e-test-runner e2e-test-runner; then
        log_success "E2Eテストが正常に完了しました"
        return 0
    else
        log_error "E2Eテストが失敗しました"
        return 1
    fi
}

# テスト結果の表示
show_test_results() {
    log_step "テスト結果を取得中..."
    
    # テスト結果ファイルをホストにコピー
    local results_dir="./test_results"
    mkdir -p "$results_dir"
    
    # XML結果ファイルを取得
    if docker-compose -f "$COMPOSE_FILE" exec -T e2e-test-runner test -f /app/test_results/results.xml 2>/dev/null; then
        docker cp "$(docker-compose -f "$COMPOSE_FILE" ps -q e2e-test-runner):/app/test_results/results.xml" "$results_dir/" 2>/dev/null || true
        log_success "テスト結果を $results_dir/results.xml に保存しました"
    fi
    
    # 簡易テスト結果サマリー
    log_info "テスト実行サマリー:"
    docker-compose -f "$COMPOSE_FILE" exec -T e2e-test-runner find /app/test_results -name '*.xml' -exec echo '  結果ファイル: {}' \; 2>/dev/null || true
}

# クリーンアップ（終了時）
cleanup_on_exit() {
    log_step "終了時クリーンアップ中..."
    docker-compose -f "$COMPOSE_FILE" down --remove-orphans >/dev/null 2>&1 || true
}

# メイン実行フロー
main() {
    local start_time=$(date +%s)
    
    echo "🚀 E2Eテスト高速実行スクリプト（最適化版）"
    echo "=========================================="
    
    # 終了時のクリーンアップを設定
    trap cleanup_on_exit EXIT
    
    # 実行ステップ
    cleanup_containers
    start_test_environment
    
    # テスト環境が準備できたらテストを実行
    if run_tests; then
        show_test_results
        
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        
        log_success "🎉 E2Eテストが完了しました！"
        log_info "実行時間: ${duration}秒"
        
        exit 0
    else
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        
        log_error "❌ E2Eテストが失敗しました"
        log_info "実行時間: ${duration}秒"
        
        show_test_results
        exit 1
    fi
}

# スクリプトの実行
main "$@"
