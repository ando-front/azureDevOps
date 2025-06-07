#!/bin/bash

# =============================================================================
# プロキシ一時無効化 E2E テスト実行コマンド
# =============================================================================

# 現在のプロキシ設定を保存
BACKUP_HTTP_PROXY="$HTTP_PROXY"
BACKUP_HTTPS_PROXY="$HTTPS_PROXY"
BACKUP_http_proxy="$http_proxy"
BACKUP_https_proxy="$https_proxy"
BACKUP_NO_PROXY="$NO_PROXY"
BACKUP_no_proxy="$no_proxy"

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

# クリーンアップ関数
cleanup() {
    log_info "プロキシ設定を復元中..."
    export HTTP_PROXY="$BACKUP_HTTP_PROXY"
    export HTTPS_PROXY="$BACKUP_HTTPS_PROXY"
    export http_proxy="$BACKUP_http_proxy"
    export https_proxy="$BACKUP_https_proxy"
    export NO_PROXY="$BACKUP_NO_PROXY"
    export no_proxy="$BACKUP_no_proxy"
    log_success "プロキシ設定が復元されました"
}

# スクリプト終了時にプロキシ設定を復元
trap cleanup EXIT

echo "============================================="
echo "  プロキシ一時無効化 E2E テスト実行"
echo "============================================="
echo ""

# 現在のプロキシ設定を表示
if [[ -n "$HTTP_PROXY" || -n "$HTTPS_PROXY" || -n "$http_proxy" || -n "$https_proxy" ]]; then
    log_warning "現在のプロキシ設定:"
    [[ -n "$HTTP_PROXY" ]] && echo "  HTTP_PROXY: $HTTP_PROXY"
    [[ -n "$HTTPS_PROXY" ]] && echo "  HTTPS_PROXY: $HTTPS_PROXY"
    [[ -n "$http_proxy" ]] && echo "  http_proxy: $http_proxy"
    [[ -n "$https_proxy" ]] && echo "  https_proxy: $https_proxy"
    echo ""
fi

# プロキシ設定を一時的に無効化
log_info "プロキシ設定を一時的に無効化中..."
unset HTTP_PROXY
unset HTTPS_PROXY
unset http_proxy
unset https_proxy
export NO_PROXY="*"
export no_proxy="*"

log_success "プロキシが無効化されました"
echo ""

# E2E テスト実行
log_info "プロキシなしでE2Eテストを実行中..."

# スクリプトのディレクトリに移動
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# プロキシなし用 Docker Compose ファイルが存在しない場合は作成
if [[ ! -f "docker-compose.e2e.no-proxy.yml" ]]; then
    log_info "プロキシなし用設定ファイルを作成中..."
    ./run-e2e-flexible.sh --no-proxy build > /dev/null
fi

# 環境クリーンアップ
log_info "既存環境をクリーンアップ中..."
docker-compose -f docker-compose.e2e.no-proxy.yml down --remove-orphans --volumes 2>/dev/null || true

# テスト実行
log_info "E2Eテストを開始..."
docker-compose -f docker-compose.e2e.no-proxy.yml up --build --abort-on-container-exit

# 結果確認
if [[ -f "test_results/e2e_no_proxy_results.xml" ]]; then
    log_success "テスト結果が生成されました: test_results/e2e_no_proxy_results.xml"
fi

if [[ -f "test_results/e2e_no_proxy_report.html" ]]; then
    log_success "テストレポートが生成されました: test_results/e2e_no_proxy_report.html"
fi

echo ""
log_success "プロキシ一時無効化テストが完了しました"
