#!/bin/bash

# =============================================================================
# Enhanced E2E テスト実行スクリプト（テスト結果確認機能強化版）
# =============================================================================

set -e

# スクリプトのディレクトリを取得
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 色付きログ関数
log_info() {
    echo -e "\033[34m[INFO]\033[0m $1"
}

log_success() {
    echo -e "\033[32m[SUCCESS]\033[0m $1"
}

log_warning() {
    echo -e "\033[33m[WARNING]\033[0m $1"
}

log_error() {
    echo -e "\033[31m[ERROR]\033[0m $1"
}

# テスト結果ディレクトリの準備
prepare_test_results_dir() {
    local test_results_dir="test_results"
    
    if [[ ! -d "$test_results_dir" ]]; then
        mkdir -p "$test_results_dir"
        log_info "テスト結果ディレクトリを作成しました: $test_results_dir"
    fi
    
    # 過去のテスト結果をアーカイブ
    if [[ -f "$test_results_dir/e2e_no_proxy_results.xml" ]]; then
        local timestamp=$(date +"%Y%m%d_%H%M%S")
        mkdir -p "$test_results_dir/archive"
        mv "$test_results_dir"/e2e_no_proxy_results.* "$test_results_dir/archive/" 2>/dev/null || true
        log_info "過去のテスト結果をアーカイブしました"
    fi
}

# テスト結果の表示
show_test_results() {
    local test_results_dir="test_results"
    
    echo ""
    echo "========================================================"
    echo "🧪 E2E テスト結果サマリー"
    echo "========================================================"
    
    if [[ -f "$test_results_dir/e2e_no_proxy_results.xml" ]]; then
        log_success "JUnit XMLレポートが生成されました"
        
        # XMLから結果を抽出
        local xml_file="$test_results_dir/e2e_no_proxy_results.xml"
        if command -v xmllint >/dev/null 2>&1; then
            local tests=$(xmllint --xpath "string(/testsuites/@tests)" "$xml_file" 2>/dev/null || echo "N/A")
            local failures=$(xmllint --xpath "string(/testsuites/@failures)" "$xml_file" 2>/dev/null || echo "N/A")
            local errors=$(xmllint --xpath "string(/testsuites/@errors)" "$xml_file" 2>/dev/null || echo "N/A")
            local time=$(xmllint --xpath "string(/testsuites/@time)" "$xml_file" 2>/dev/null || echo "N/A")
            
            echo "📊 総テスト数: $tests"
            echo "✅ 成功: $((tests - failures - errors))"
            echo "❌ 失敗: $failures"
            echo "⚠️ エラー: $errors"
            echo "⏱️ 実行時間: ${time}秒"
        else
            # XMLlintが利用できない場合の代替手段
            echo "📄 XMLレポート: $xml_file"
            if grep -q "failures=\"0\"" "$xml_file" && grep -q "errors=\"0\"" "$xml_file"; then
                echo "✅ 全テストが成功しました"
            else
                echo "⚠️ 一部のテストで問題が発生しました"
            fi
        fi
    else
        log_warning "JUnit XMLレポートが見つかりません"
    fi
    
    if [[ -f "$test_results_dir/e2e_no_proxy_report.html" ]]; then
        log_success "HTMLレポートが生成されました: $test_results_dir/e2e_no_proxy_report.html"
    else
        log_warning "HTMLレポートが見つかりません"
    fi
    
    # ログファイルの確認
    if [[ -d "logs" ]]; then
        echo ""
        echo "📝 ログファイル:"
        ls -la logs/ 2>/dev/null || echo "ログファイルなし"
    fi
    
    echo "========================================================"
}

# Docker コンテナのログを表示
show_container_logs() {
    local container_name="adf-e2e-test-runner"
    
    echo ""
    echo "========================================================"
    echo "📋 E2E テストコンテナログ (最新20行)"
    echo "========================================================"
    
    if docker ps -a --format "{{.Names}}" | grep -q "^${container_name}$"; then
        docker logs --tail 20 "$container_name"
    else
        log_warning "テストコンテナ ($container_name) が見つかりません"
    fi
    
    echo "========================================================"
}

# 詳細なテスト結果の表示
show_detailed_test_results() {
    local test_results_dir="test_results"
    
    prepare_test_results_dir
    show_test_results
    show_container_logs
    
    # HTMLレポートの場所を案内
    if [[ -f "$test_results_dir/e2e_no_proxy_report.html" ]]; then
        echo ""
        log_info "詳細なテスト結果を確認するには、以下のHTMLファイルをブラウザで開いてください:"
        echo "   file://$(pwd)/$test_results_dir/e2e_no_proxy_report.html"
    fi
}

# テスト実行前の準備
pre_test_setup() {
    log_info "テスト実行前の準備を開始..."
    prepare_test_results_dir
    
    # 必要なディレクトリを作成
    mkdir -p logs
    
    log_success "テスト実行準備が完了しました"
}

# テスト実行後の処理
post_test_cleanup() {
    log_info "テスト実行後の処理を開始..."
    
    # テスト結果を表示
    show_detailed_test_results
    
    # テスト結果ファイルの権限を調整
    if [[ -d "test_results" ]]; then
        chmod -R 755 test_results/ 2>/dev/null || true
    fi
    
    log_success "テスト実行後の処理が完了しました"
}

# メイン実行関数（拡張版）
run_e2e_tests_enhanced() {
    local proxy_mode="${1:-no-proxy}"
    local operation="${2:-full}"
    
    echo "============================================="
    echo "  Enhanced E2E テスト実行"
    echo "============================================="
    echo "プロキシモード: $proxy_mode"
    echo "実行操作: $operation"
    echo ""
    
    # テスト実行前の準備
    pre_test_setup
    
    # 既存のrun-e2e-flexible.shを使用してテスト実行
    log_info "E2Eテストを実行中..."
    if bash run-e2e-flexible.sh --${proxy_mode} ${operation}; then
        log_success "E2Eテスト実行が完了しました"
    else
        log_error "E2Eテスト実行中にエラーが発生しました"
    fi
    
    # テスト実行後の処理
    post_test_cleanup
}

# コマンドライン引数の処理
case "${1:-help}" in
    "run")
        run_e2e_tests_enhanced "${2:-no-proxy}" "${3:-full}"
        ;;
    "results")
        show_detailed_test_results
        ;;
    "logs")
        show_container_logs
        ;;
    "help"|*)
        cat << EOF
Enhanced E2E テスト実行スクリプト

使用方法:
    $0 run [proxy-mode] [operation]    # E2Eテスト実行（結果表示付き）
    $0 results                         # 最新のテスト結果を表示
    $0 logs                           # コンテナログを表示
    $0 help                           # このヘルプを表示

例:
    $0 run no-proxy full              # プロキシなしでフル実行
    $0 run proxy test                 # プロキシありでテストのみ
    $0 results                        # テスト結果確認
    $0 logs                          # ログ確認

EOF
        ;;
esac
