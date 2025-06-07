#!/bin/bash

# プロキシなし環境でのE2Eテスト実行スクリプト
# 作成日: 2025-06-07

set -e

# カラー出力用の設定
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# ログ関数
log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# ヘルプ表示
show_help() {
    cat << EOF
プロキシなし環境でのE2Eテスト実行スクリプト

使用方法:
    $0 [オプション]

オプション:
    --build-only    Dockerイメージのビルドのみ実行
    --test-only     テストのみ実行（ビルドスキップ）
    --cleanup       テスト後にコンテナとボリュームを削除
    --basic-only    基本テストのみ実行（IR Simulatorなし）
    --help         このヘルプを表示

例:
    $0                    # 完全なE2Eテスト実行
    $0 --basic-only       # 基本テストのみ
    $0 --cleanup          # テスト後クリーンアップ付き
EOF
}

# コマンドライン引数の解析
BUILD_ONLY=false
TEST_ONLY=false
CLEANUP=false
BASIC_ONLY=false

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
        --basic-only)
            BASIC_ONLY=true
            shift
            ;;
        --help)
            show_help
            exit 0
            ;;
        *)
            log_error "不明なオプション: $1"
            show_help
            exit 1
            ;;
    esac
done

# 作業ディレクトリの確認
if [[ ! -f "docker-compose.e2e.no-proxy.yml" ]]; then
    log_error "docker-compose.e2e.no-proxy.yml が見つかりません"
    log_error "プロジェクトルートディレクトリで実行してください"
    exit 1
fi

# プロキシ環境変数のクリア
log_info "プロキシ環境変数をクリア中..."
unset HTTP_PROXY
unset HTTPS_PROXY
unset http_proxy
unset https_proxy
export NO_PROXY="*"
export no_proxy="*"

# テスト結果ディレクトリの作成
log_info "テスト結果ディレクトリを準備中..."
mkdir -p test_results
chmod 777 test_results

# 既存のコンテナとボリュームのクリーンアップ
log_info "既存のコンテナとボリュームをクリーンアップ中..."
docker-compose -f docker-compose.e2e.no-proxy.yml down --volumes --remove-orphans 2>/dev/null || true

# Dockerイメージのビルド
if [[ "$TEST_ONLY" != "true" ]]; then
    log_info "Dockerイメージをビルド中..."
    if ! docker-compose -f docker-compose.e2e.no-proxy.yml build --no-cache; then
        log_error "Dockerイメージのビルドに失敗しました"
        exit 1
    fi
    log_success "Dockerイメージのビルドが完了しました"
    
    if [[ "$BUILD_ONLY" == "true" ]]; then
        log_success "ビルドのみ完了しました"
        exit 0
    fi
fi

# E2Eテストの実行
log_info "プロキシなし環境でのE2Eテストを開始中..."

# テスト実行の開始時刻
START_TIME=$(date +%s)

# Docker Composeでテスト実行
if [[ "$BASIC_ONLY" == "true" ]]; then
    log_info "基本テストのみを実行中（IR Simulatorなし）..."
    TEST_COMMAND="pytest tests/e2e/test_e2e_working.py -v --tb=short --junitxml=test_results/e2e_basic_no_proxy.xml --html=test_results/e2e_basic_no_proxy.html --self-contained-html -k 'not ir_simulator'"
else
    log_info "完全なE2Eテストを実行中..."
    TEST_COMMAND="pytest tests/e2e/test_e2e_working.py -v --tb=short --junitxml=test_results/e2e_complete_no_proxy.xml --html=test_results/e2e_complete_no_proxy.html --self-contained-html"
fi

# Docker Composeを使用してテスト実行
if docker-compose -f docker-compose.e2e.no-proxy.yml up --abort-on-container-exit --exit-code-from e2e-test-runner; then
    log_success "E2Eテストが完了しました"
    TEST_RESULT=0
else
    log_warning "E2Eテストで一部問題が発生しましたが、結果を確認してください"
    TEST_RESULT=1
fi

# 実行時間の計算
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
MINUTES=$((DURATION / 60))
SECONDS=$((DURATION % 60))

log_info "テスト実行時間: ${MINUTES}分${SECONDS}秒"

# テスト結果の表示
log_info "テスト結果ファイルを確認中..."
if [[ -f "test_results/e2e_basic_no_proxy.xml" ]] || [[ -f "test_results/e2e_complete_no_proxy.xml" ]]; then
    log_success "テスト結果ファイルが生成されました:"
    ls -la test_results/e2e_*_no_proxy.*
else
    log_warning "テスト結果ファイルが見つかりません"
fi

# コンテナログの表示（デバッグ用）
log_info "コンテナログを表示中..."
docker-compose -f docker-compose.e2e.no-proxy.yml logs --tail=50 e2e-test-runner

# クリーンアップ
if [[ "$CLEANUP" == "true" ]]; then
    log_info "コンテナとボリュームをクリーンアップ中..."
    docker-compose -f docker-compose.e2e.no-proxy.yml down --volumes --remove-orphans
    log_success "クリーンアップが完了しました"
else
    log_info "サービスを停止中（ボリュームは保持）..."
    docker-compose -f docker-compose.e2e.no-proxy.yml down
fi

# 結果サマリー
echo
log_info "=== E2Eテスト実行結果サマリー ==="
log_info "実行環境: プロキシなし"
log_info "実行時間: ${MINUTES}分${SECONDS}秒"
log_info "結果ディレクトリ: test_results/"

if [[ $TEST_RESULT -eq 0 ]]; then
    log_success "✅ E2Eテストが正常に完了しました"
else
    log_warning "⚠️ E2Eテストで一部問題が発生しました。詳細は結果ファイルを確認してください。"
fi

echo
log_info "次のステップ:"
log_info "1. テスト結果を確認: cat test_results/e2e_*_no_proxy.xml"
log_info "2. HTMLレポートを確認: open test_results/e2e_*_no_proxy.html"
log_info "3. 再実行の場合: $0 --test-only"

exit $TEST_RESULT
