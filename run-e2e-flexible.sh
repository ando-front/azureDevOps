#!/bin/bash

# =============================================================================
# E2E テスト実行スクリプト（プロキシ設定選択可能版）
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

# ヘルプメッセージ
show_help() {
    cat << EOF
E2E テスト実行スクリプト（プロキシ設定選択可能版）

使用方法:
    $0 [オプション] [操作]

オプション:
    -p, --proxy         プロキシありの環境で実行
    -n, --no-proxy      プロキシなしの環境で実行
    -i, --interactive   対話的にプロキシ設定を選択
    -h, --help          このヘルプを表示

操作:
    build              Docker イメージのビルドのみ
    test               テストの実行のみ
    full               フルビルド + テスト実行（デフォルト）
    cleanup            テスト環境のクリーンアップ
    status             現在のサービス状況確認

例:
    $0 --no-proxy full          # プロキシなしでフル実行
    $0 -p test                  # プロキシありでテストのみ実行
    $0 -i                       # 対話的に設定選択してフル実行
    $0 cleanup                  # 環境クリーンアップ

EOF
}

# プロキシ設定の選択
select_proxy_mode() {
    if [[ "$PROXY_MODE" == "auto" ]]; then
        echo ""
        log_info "プロキシ設定を選択してください："
        echo "1) プロキシなし（推奨・高速）"
        echo "2) プロキシあり（企業環境）"
        echo "3) 自動検出"
        echo ""
        read -p "選択してください [1-3]: " choice
        
        case $choice in
            1) PROXY_MODE="no-proxy" ;;
            2) PROXY_MODE="proxy" ;;
            3) 
                log_info "プロキシ設定を自動検出中..."
                if [[ -n "$HTTP_PROXY" || -n "$HTTPS_PROXY" || -n "$http_proxy" || -n "$https_proxy" ]]; then
                    PROXY_MODE="proxy"
                    log_info "プロキシ環境変数が検出されました"
                else
                    PROXY_MODE="no-proxy"
                    log_info "プロキシ環境変数が検出されませんでした"
                fi
                ;;
            *) 
                log_warning "無効な選択です。プロキシなしで続行します。"
                PROXY_MODE="no-proxy"
                ;;
        esac
    fi
    
    log_success "選択されたモード: $PROXY_MODE"
}

# Docker Compose ファイルの選択
get_compose_file() {
    case "$PROXY_MODE" in
        "no-proxy")
            echo "docker-compose.e2e.no-proxy.yml"
            ;;
        "proxy")
            echo "docker-compose.e2e.yml"
            ;;
        *)
            log_error "無効なプロキシモード: $PROXY_MODE"
            exit 1
            ;;
    esac
}

# Docker Compose ファイルの存在確認と作成
ensure_compose_file() {
    local compose_file="$1"
    
    if [[ ! -f "$compose_file" ]]; then
        if [[ "$compose_file" == "docker-compose.e2e.no-proxy.yml" ]]; then
            log_info "プロキシなし用 Docker Compose ファイルを作成中..."
            create_no_proxy_compose_file
        else
            log_error "Docker Compose ファイルが見つかりません: $compose_file"
            exit 1
        fi
    fi
}

# プロキシなし用 Docker Compose ファイルの作成
create_no_proxy_compose_file() {
    cat > docker-compose.e2e.no-proxy.yml << 'EOF'
version: '3.8'

services:
  # SQL Server サービス
  sql-server:
    image: mcr.microsoft.com/mssql/server:2019-latest
    container_name: adf-sql-server-test
    environment:
      - ACCEPT_EULA=Y
      - SA_PASSWORD=YourStrong!Passw0rd123
      - MSSQL_PID=Express
    ports:
      - "1433:1433"
    volumes:
      - ./sql/init:/docker-entrypoint-initdb.d
      - sql_data:/var/opt/mssql
    networks:
      - adf-e2e-network
    healthcheck:
      test: ["CMD-SHELL", "/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P YourStrong!Passw0rd123 -Q 'SELECT 1' || exit 1"]
      interval: 10s
      retries: 10
      start_period: 10s
      timeout: 3s

  # Azurite (Azure Storage Emulator)
  azurite:
    image: mcr.microsoft.com/azure-storage/azurite:latest
    container_name: adf-azurite-test
    command: "azurite --blobHost 0.0.0.0 --queueHost 0.0.0.0 --tableHost 0.0.0.0"
    ports:
      - "10000:10000"  # Blob service
      - "10001:10001"  # Queue service
      - "10002:10002"  # Table service
    volumes:
      - azurite_data:/data
    networks:
      - adf-e2e-network

  # E2E テストランナー
  e2e-test-runner:
    build:
      context: .
      dockerfile: Dockerfile.e2e.complete-light
      args:
        - NO_PROXY=true
    container_name: adf-e2e-test-runner
    environment:
      # データベース接続設定
      - SQL_SERVER_HOST=sql-server
      - SQL_SERVER_PORT=1433
      - SQL_SERVER_DATABASE=SynapseTestDB
      - SQL_SERVER_USER=sa
      - SQL_SERVER_PASSWORD=YourStrong!Passw0rd123
      
      # Azure Storage 接続設定
      - AZURITE_HOST=azurite
      - AZURITE_BLOB_PORT=10000
      - AZURITE_CONNECTION_STRING=DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite:10000/devstoreaccount1;
      
      # テスト設定
      - E2E_TIMEOUT=300
      - E2E_RETRY_COUNT=3
      - PYTEST_ARGS=--tb=short --maxfail=5
      
      # プロキシ無効化
      - HTTP_PROXY=
      - HTTPS_PROXY=
      - http_proxy=
      - https_proxy=
      - NO_PROXY=*
      - no_proxy=*
      
    volumes:
      - .:/app
      - ./test_results:/app/test_results
      - ./logs:/app/logs
    working_dir: /app
    depends_on:
      sql-server:
        condition: service_healthy
      azurite:
        condition: service_started
    networks:
      - adf-e2e-network
    command: >
      bash -c "
        echo '🚀 E2E テスト環境の準備中...' &&
        sleep 10 &&
        echo '📊 テスト実行中...' &&
        pytest tests/e2e/test_e2e_working.py tests/e2e/test_basic_connections.py 
        --junitxml=test_results/e2e_no_proxy_results.xml 
        --html=test_results/e2e_no_proxy_report.html 
        --self-contained-html 
        --tb=short 
        --maxfail=10 
        -v &&
        echo '✅ E2E テスト完了'
      "

volumes:
  sql_data:
  azurite_data:

networks:
  adf-e2e-network:
    driver: bridge
EOF
    
    log_success "プロキシなし用 Docker Compose ファイルを作成しました"
}

# 環境のクリーンアップ
cleanup_environment() {
    log_info "E2E テスト環境をクリーンアップ中..."
    
    # 全ての関連するコンテナを停止・削除
    for compose_file in "docker-compose.e2e.yml" "docker-compose.e2e.no-proxy.yml"; do
        if [[ -f "$compose_file" ]]; then
            docker-compose -f "$compose_file" down --remove-orphans --volumes 2>/dev/null || true
        fi
    done
    
    # 関連するコンテナを強制削除
    docker rm -f adf-e2e-test-runner adf-sql-server-test adf-azurite-test 2>/dev/null || true
    
    # 関連するイメージをクリーンアップ
    docker image rm -f adf-e2e-test:latest 2>/dev/null || true
    
    # ネットワークをクリーンアップ
    docker network rm azuredevops_adf-e2e-network 2>/dev/null || true
    
    log_success "環境クリーンアップが完了しました"
}

# サービス状況の確認
check_status() {
    log_info "現在のサービス状況を確認中..."
    
    echo ""
    echo "=== Docker コンテナ状況 ==="
    docker ps -a --filter "name=adf-" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
    
    echo ""
    echo "=== Docker ネットワーク ==="
    docker network ls --filter "name=adf"
    
    echo ""
    echo "=== Docker ボリューム ==="
    docker volume ls --filter "name=adf"
    
    if [[ -d "test_results" ]]; then
        echo ""
        echo "=== テスト結果 ==="
        ls -la test_results/ 2>/dev/null || echo "テスト結果なし"
    fi
}

# メイン実行関数
main_execution() {
    local operation="$1"
    local compose_file=$(get_compose_file)
    
    ensure_compose_file "$compose_file"
    
    case "$operation" in
        "build")
            log_info "Docker イメージをビルド中..."
            docker-compose -f "$compose_file" build --no-cache
            log_success "ビルドが完了しました"
            ;;
            
        "test")
            log_info "テストを実行中..."
            docker-compose -f "$compose_file" up --abort-on-container-exit
            log_success "テストが完了しました"
            ;;
            
        "full")
            log_info "フルビルド + テスト実行中..."
            docker-compose -f "$compose_file" down --remove-orphans --volumes 2>/dev/null || true
            docker-compose -f "$compose_file" build --no-cache
            docker-compose -f "$compose_file" up --abort-on-container-exit
            log_success "フル実行が完了しました"
            ;;
            
        "cleanup")
            cleanup_environment
            ;;
            
        "status")
            check_status
            ;;
            
        *)
            log_error "無効な操作: $operation"
            show_help
            exit 1
            ;;
    esac
}

# 引数解析
PROXY_MODE="auto"
OPERATION="full"

while [[ $# -gt 0 ]]; do
    case $1 in
        -p|--proxy)
            PROXY_MODE="proxy"
            shift
            ;;
        -n|--no-proxy)
            PROXY_MODE="no-proxy"
            shift
            ;;
        -i|--interactive)
            PROXY_MODE="auto"
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        build|test|full|cleanup|status)
            OPERATION="$1"
            shift
            ;;
        *)
            log_error "不明なオプション: $1"
            show_help
            exit 1
            ;;
    esac
done

# メイン実行
echo "============================================="
echo "  E2E テスト実行（プロキシ設定選択可能版）"
echo "============================================="
echo ""

select_proxy_mode
echo ""

if [[ "$OPERATION" != "cleanup" && "$OPERATION" != "status" ]]; then
    log_info "使用する Docker Compose ファイル: $(get_compose_file)"
    log_info "実行する操作: $OPERATION"
    echo ""
fi

main_execution "$OPERATION"

echo ""
log_success "スクリプト実行が完了しました"
