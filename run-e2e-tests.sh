#!/bin/bash

# =================================================================
# E2Eテスト実行自動化スクリプト
# Docker Composeを使用したADF E2Eテストの完全実行
# =================================================================

set -euo pipefail

# カラー出力設定
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# ログ出力関数
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

# 使用法表示
show_usage() {
    cat << EOF
使用法: $0 [オプション]

オプション:
    --build-only        Dockerイメージのビルドのみ実行
    --test-only         テストのみ実行（ビルドスキップ）
    --cleanup           テスト後にコンテナとボリュームを削除
    --parallel          並列テスト実行
    --timeout SECONDS   テストタイムアウト設定（デフォルト: 600秒）
    --help              このヘルプを表示

例:
    $0                  # 完全なE2Eテスト実行
    $0 --build-only     # イメージビルドのみ
    $0 --test-only      # テスト実行のみ
    $0 --cleanup        # テスト後クリーンアップ
EOF
}

# デフォルト設定
BUILD_ONLY=false
TEST_ONLY=false
CLEANUP=false
PARALLEL=false
TIMEOUT=600
COMPOSE_FILE="docker-compose.e2e.yml"

# 引数解析
while [[ $# -gt 0 ]]; do
    case $1 in
        --build-only)
            BUILD_ONLY=true
            shift
            ;;
        --test-only)
            TEST_ONLY=true
            shift
            ;;
        --cleanup)
            CLEANUP=true
            shift
            ;;
        --parallel)
            PARALLEL=true
            shift
            ;;
        --timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        --help)
            show_usage
            exit 0
            ;;
        *)
            log_error "不明なオプション: $1"
            show_usage
            exit 1
            ;;
    esac
done

# 前提条件チェック
check_prerequisites() {
    log_info "前提条件をチェック中..."
    
    # Docker Composeコマンドの確認
    if command -v docker-compose >/dev/null 2>&1; then
        DOCKER_COMPOSE_CMD="docker-compose"
    elif docker compose version >/dev/null 2>&1; then
        DOCKER_COMPOSE_CMD="docker compose"
    else
        log_error "Docker Composeが見つかりません"
        exit 1
    fi
    
    # 必要ファイルの確認
    if [[ ! -f "$COMPOSE_FILE" ]]; then
        log_error "Docker Composeファイルが見つかりません: $COMPOSE_FILE"
        exit 1
    fi
    
    if [[ ! -f "Dockerfile.e2e.complete-light" ]]; then
        log_error "E2E用Dockerfileが見つかりません: Dockerfile.e2e.complete-light"
        exit 1
    fi
    
    # テストディレクトリの確認
    if [[ ! -d "tests/e2e" ]]; then
        log_error "E2Eテストディレクトリが見つかりません: tests/e2e"
        exit 1
    fi
    
    log_success "前提条件チェック完了"
}

# Docker環境のクリーンアップ
cleanup_docker() {
    log_info "Docker環境をクリーンアップ中..."
    
    # コンテナ停止と削除
    $DOCKER_COMPOSE_CMD -f "$COMPOSE_FILE" down --remove-orphans || true
    
    if [[ "$CLEANUP" == "true" ]]; then
        # ボリューム削除
        $DOCKER_COMPOSE_CMD -f "$COMPOSE_FILE" down --volumes || true
        
        # 未使用イメージ削除
        docker image prune -f || true
        
        log_success "完全クリーンアップ完了"
    else
        log_success "基本クリーンアップ完了"
    fi
}

# Dockerイメージビルド
build_images() {
    log_info "Dockerイメージをビルド中..."
    
    # キャッシュなしビルド（常に最新状態を保証）
    $DOCKER_COMPOSE_CMD -f "$COMPOSE_FILE" build --no-cache --parallel
    
    log_success "Dockerイメージビルド完了"
}

# E2Eテスト実行
run_e2e_tests() {
    log_info "E2Eテスト環境を起動中..."
    
    # 環境変数ファイルの確認
    if [[ -f ".env.e2e" ]]; then
        log_info ".env.e2eファイルを使用"
        ENV_FILE_OPTION="--env-file .env.e2e"
    else
        ENV_FILE_OPTION=""
    fi
    
    # テスト結果ディレクトリ作成
    mkdir -p test_results
    
    # タイムアウト設定
    export TEST_TIMEOUT="$TIMEOUT"
    
    # E2Eテスト実行
    log_info "E2Eテストを実行中（タイムアウト: ${TIMEOUT}秒）..."
    
    if [[ "$PARALLEL" == "true" ]]; then
        log_info "並列テスト実行モード"
        timeout "$TIMEOUT" $DOCKER_COMPOSE_CMD -f "$COMPOSE_FILE" up $ENV_FILE_OPTION --abort-on-container-exit --exit-code-from e2e-test-runner
    else
        log_info "順次テスト実行モード"
        timeout "$TIMEOUT" $DOCKER_COMPOSE_CMD -f "$COMPOSE_FILE" up $ENV_FILE_OPTION --abort-on-container-exit --exit-code-from e2e-test-runner
    fi
    
    # テスト結果の収集
    collect_test_results
}

# テスト結果収集
collect_test_results() {
    log_info "テスト結果を収集中..."
    
    # コンテナからテスト結果をコピー
    if docker container ls -a --format '{{.Names}}' | grep -q "adf-e2e-test-runner"; then
        docker cp adf-e2e-test-runner:/app/test_results/. ./test_results/ 2>/dev/null || log_warning "テスト結果のコピーに失敗"
    fi
    
    # テスト結果の確認
    if [[ -d "./test_results" ]]; then
        log_info "テスト結果:"
        find ./test_results -name "*.xml" -exec echo "  - JUnit XML: {}" \;
        find ./test_results -name "*.html" -exec echo "  - HTML Report: {}" \;
        find ./test_results -name "coverage.xml" -exec echo "  - Coverage XML: {}" \;
        find ./test_results -type d -name "coverage_html" -exec echo "  - Coverage HTML: {}" \;
    fi
    
    log_success "テスト結果収集完了"
}

# メイン実行
main() {
    log_info "ADF E2Eテスト自動実行スクリプト開始"
    log_info "実行モード: BUILD_ONLY=$BUILD_ONLY, TEST_ONLY=$TEST_ONLY, CLEANUP=$CLEANUP"
    
    # トラップ設定（エラー時のクリーンアップ）
    trap cleanup_docker EXIT
    
    # 前提条件チェック
    check_prerequisites
    
    # 既存環境のクリーンアップ
    cleanup_docker
    
    if [[ "$TEST_ONLY" == "false" ]]; then
        # Dockerイメージビルド
        build_images
    fi
    
    if [[ "$BUILD_ONLY" == "false" ]]; then
        # E2Eテスト実行
        run_e2e_tests
        
        # 結果確認
        if [[ $? -eq 0 ]]; then
            log_success "🎉 E2Eテストが正常に完了しました！"
        else
            log_warning "⚠️ E2Eテストが一部失敗しましたが、実行は完了しました"
        fi
    fi
    
    log_success "ADF E2Eテスト自動実行スクリプト完了"
}

# スクリプト実行
main "$@"
