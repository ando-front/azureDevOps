#!/bin/bash

# =================================================================
# E2Eテスト環境バリデーションスクリプト
# 環境が正しく設定されているかをチェック
# =================================================================

set -euo pipefail

# カラー出力設定
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# ログ出力関数
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[✅ SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[⚠️  WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[❌ ERROR]${NC} $1"
}

# 検証関数
validate_docker() {
    log_info "Dockerインストール状況を確認中..."
    
    if command -v docker >/dev/null 2>&1; then
        DOCKER_VERSION=$(docker --version)
        log_success "Docker: $DOCKER_VERSION"
    else
        log_error "Dockerがインストールされていません"
        return 1
    fi
    
    if docker compose version >/dev/null 2>&1; then
        COMPOSE_VERSION=$(docker compose version)
        log_success "Docker Compose: $COMPOSE_VERSION"
    elif command -v docker-compose >/dev/null 2>&1; then
        COMPOSE_VERSION=$(docker-compose --version)
        log_success "Docker Compose: $COMPOSE_VERSION"
    else
        log_error "Docker Composeがインストールされていません"
        return 1
    fi
}

validate_files() {
    log_info "必要ファイルの存在確認中..."
    
    local files=(
        "Dockerfile.e2e.complete-light"
        "docker-compose.e2e.yml"
        "requirements.e2e.txt"
        "requirements.txt"
        "run-e2e-tests.sh"
    )
    
    for file in "${files[@]}"; do
        if [[ -f "$file" ]]; then
            log_success "ファイル存在: $file"
        else
            log_error "ファイル不存在: $file"
            return 1
        fi
    done
    
    # 実行権限チェック
    if [[ -x "run-e2e-tests.sh" ]]; then
        log_success "実行権限: run-e2e-tests.sh"
    else
        log_warning "実行権限なし: run-e2e-tests.sh"
        chmod +x run-e2e-tests.sh
        log_success "実行権限を付与: run-e2e-tests.sh"
    fi
}

validate_directories() {
    log_info "必要ディレクトリの存在確認中..."
    
    local dirs=(
        "tests/e2e"
        "src"
        "docker"
    )
    
    for dir in "${dirs[@]}"; do
        if [[ -d "$dir" ]]; then
            log_success "ディレクトリ存在: $dir"
        else
            log_warning "ディレクトリ不存在: $dir"
        fi
    done
    
    # テスト結果ディレクトリ作成
    if [[ ! -d "test_results" ]]; then
        mkdir -p test_results
        log_success "テスト結果ディレクトリを作成: test_results"
    else
        log_success "テスト結果ディレクトリ存在: test_results"
    fi
}

validate_compose_config() {
    log_info "Docker Compose設定ファイルをバリデーション中..."
    
    if docker compose -f docker-compose.e2e.yml config --quiet 2>/dev/null; then
        log_success "Docker Compose設定ファイル: 有効"
    else
        log_error "Docker Compose設定ファイル: 無効"
        return 1
    fi
}

validate_python_requirements() {
    log_info "Python依存関係ファイルをチェック中..."
    
    if [[ -f "requirements.e2e.txt" ]]; then
        local req_count=$(wc -l < requirements.e2e.txt)
        log_success "requirements.e2e.txt: ${req_count}個のパッケージ"
        
        # 重要なパッケージの確認
        local important_packages=("pytest" "pandas" "pyodbc" "azure-storage-blob")
        for package in "${important_packages[@]}"; do
            if grep -q "^$package" requirements.e2e.txt; then
                log_success "重要パッケージ存在: $package"
            else
                log_warning "重要パッケージ不存在: $package"
            fi
        done
    fi
    
    if [[ -f "requirements.txt" ]]; then
        local req_count=$(wc -l < requirements.txt)
        log_success "requirements.txt: ${req_count}個のパッケージ"
    fi
}

validate_test_files() {
    log_info "E2Eテストファイルをチェック中..."
    
    if [[ -d "tests/e2e" ]]; then
        local test_count=$(find tests/e2e -name "test_*.py" | wc -l)
        log_success "E2Eテストファイル: ${test_count}個"
        
        if [[ $test_count -eq 0 ]]; then
            log_warning "E2Eテストファイルが見つかりません"
        fi
    else
        log_warning "E2Eテストディレクトリが存在しません: tests/e2e"
    fi
}

validate_environment() {
    log_info "環境設定をチェック中..."
    
    if [[ -f ".env.e2e.template" ]]; then
        log_success "環境変数テンプレート存在: .env.e2e.template"
    else
        log_warning "環境変数テンプレート不存在: .env.e2e.template"
    fi
    
    if [[ -f ".env.e2e" ]]; then
        log_success "環境変数ファイル存在: .env.e2e"
    else
        log_warning "環境変数ファイル不存在: .env.e2e（テンプレートからコピーしてください）"
    fi
}

validate_system_resources() {
    log_info "システムリソースをチェック中..."
    
    # メモリチェック（macOS）
    if command -v sysctl >/dev/null 2>&1; then
        local memory_gb=$(sysctl -n hw.memsize | awk '{print int($1/1024/1024/1024)}')
        if [[ $memory_gb -ge 8 ]]; then
            log_success "メモリ: ${memory_gb}GB（推奨8GB以上）"
        else
            log_warning "メモリ: ${memory_gb}GB（推奨8GB以上、E2Eテストで問題が発生する可能性があります）"
        fi
    fi
    
    # ディスク容量チェック
    local disk_free=$(df -h . | tail -1 | awk '{print $4}')
    log_success "利用可能ディスク容量: $disk_free"
}

run_quick_test() {
    log_info "簡単な動作確認テストを実行中..."
    
    # Dockerが実行中か確認
    if docker info >/dev/null 2>&1; then
        log_success "Docker デーモン: 実行中"
    else
        log_error "Docker デーモン: 停止中（Dockerを起動してください）"
        return 1
    fi
    
    # 簡単なDockerコマンドテスト
    if docker run --rm hello-world >/dev/null 2>&1; then
        log_success "Docker基本動作: 正常"
    else
        log_warning "Docker基本動作: 問題あり"
    fi
}

# メイン実行
main() {
    echo "=============================================="
    echo "🔍 ADF E2Eテスト環境バリデーション"
    echo "=============================================="
    
    local validation_passed=true
    
    # 各種バリデーション実行
    validate_docker || validation_passed=false
    echo
    
    validate_files || validation_passed=false
    echo
    
    validate_directories || validation_passed=false
    echo
    
    validate_compose_config || validation_passed=false
    echo
    
    validate_python_requirements || validation_passed=false
    echo
    
    validate_test_files || validation_passed=false
    echo
    
    validate_environment || validation_passed=false
    echo
    
    validate_system_resources || validation_passed=false
    echo
    
    run_quick_test || validation_passed=false
    echo
    
    # 結果サマリー
    echo "=============================================="
    if [[ "$validation_passed" == "true" ]]; then
        log_success "🎉 バリデーション完了！E2Eテスト環境の準備ができています"
        echo
        echo "次のステップ:"
        echo "1. ./run-e2e-tests.sh を実行してE2Eテストを開始"
        echo "2. make test-e2e を実行してMakefile経由でテスト実行"
        echo "3. docker-compose -f docker-compose.e2e.yml up で手動起動"
    else
        log_error "❌ バリデーション失敗！上記のエラーと警告を確認してください"
        echo
        echo "トラブルシューティング:"
        echo "1. 不足ファイルを作成または修正"
        echo "2. Docker/Docker Composeを再インストール"
        echo "3. システムリソースを確認"
    fi
    echo "=============================================="
}

# スクリプト実行
main "$@"
