#!/bin/bash

echo "Script started."

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
    results            最新のテスト結果表示

例:
    $0 --no-proxy full          # プロキシなしでフル実行
    $0 -p test                  # プロキシありでテストのみ実行
    $0 -i                       # 対話的に設定選択してフル実行
    $0 cleanup                  # 環境クリーンアップ
    $0 results                  # テスト結果のみ表示

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
  # SQL Server 初期化サービス
  sql-server-init:
    image: mcr.microsoft.com/mssql-tools
    container_name: adf-sql-server-init
    volumes:
      - ./docker/sql/init:/docker-sql-init
    entrypoint: /bin/bash
    command: >
      -c "
      echo 'Waiting for SQL Server to be healthy...'
      sleep 30
      echo 'Starting database initialization...'
      # The init-databases.sh script is executed here
      /opt/mssql-tools/bin/sqlcmd -S sql-server -U sa -P 'YourStrong!Passw0rd123' -i /docker-sql-init/init-databases.sh &&
      echo 'Database initialization complete.'
      "
    depends_on:
      sql-server:
        condition: service_healthy
    networks:
      - adf-e2e-network

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
      test: ["CMD-SHELL", "/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd123' -Q 'SELECT 1' || exit 1"]
      interval: 10s
      retries: 10
      start_period: 30s
      timeout: 10s

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
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:10000/devstoreaccount1"]
      interval: 5s
      timeout: 3s
      retries: 5
      start_period: 10s

  # E2E テストランナー
  e2e-test-runner:
    build:
      context: .
      dockerfile: Dockerfile
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
      sql-server-init:
        condition: service_completed_successfully
      azurite:
        condition: service_healthy
    networks:
      - adf-e2e-network
    # The command to run the tests inside the container
    command: "/app/docker/test-runner/run_e2e_tests_in_container.sh"

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

# テスト結果の詳細表示
show_test_results() {
    if [[ -d "test_results" ]]; then
        echo ""
        echo "=== 📊 E2E テスト結果詳細 ==="
        
        # JUnit XMLファイルの分析
        if [[ -f "test_results/e2e_no_proxy_results.xml" ]]; then
            echo "✅ JUnit XMLレポート: test_results/e2e_no_proxy_results.xml"
              # XMLからテスト統計を抽出（可能な場合）
            if command -v grep >/dev/null 2>&1; then
                local xml_content=$(cat test_results/e2e_no_proxy_results.xml)
                if [[ $xml_content =~ tests=\"([0-9]+)\" ]]; then
                    local total_tests="${BASH_REMATCH[1]}"
                    echo "📝 総テスト数: $total_tests"
                fi
                if [[ $xml_content =~ failures=\"([0-9]+)\" ]]; then
                    local failures="${BASH_REMATCH[1]}"
                    echo "❌ 失敗: $failures"
                fi
                if [[ $xml_content =~ errors=\"([0-9]+)\" ]]; then
                    local errors="${BASH_REMATCH[1]}"
                    echo "⚠️ エラー: $errors"
                fi
                if [[ $xml_content =~ time=\"([0-9.]+)\" ]]; then
                    local duration="${BASH_REMATCH[1]}"
                    echo "⏱️ 実行時間: ${duration}秒"
                fi
                
                # 成功率の計算
                if [[ -n "$total_tests" && -n "$failures" && -n "$errors" ]]; then
                    local passed=$((total_tests - failures - errors))
                    local success_rate=$(echo "scale=1; $passed * 100 / $total_tests" | bc 2>/dev/null || echo "N/A")
                    echo "✅ 成功: $passed"
                    echo "📈 成功率: ${success_rate}%"
                fi
            fi
        fi
        
        # HTMLレポートの確認
        if [[ -f "test_results/e2e_no_proxy_report.html" ]]; then
            echo "📄 HTMLレポート: test_results/e2e_no_proxy_report.html"
            echo "   ブラウザで確認: file://$(pwd)/test_results/e2e_no_proxy_report.html"
        fi
        
        # その他のテスト結果ファイル
        echo ""
        echo "📁 テスト結果ファイル一覧:"
        ls -la test_results/ 2>/dev/null
    else
        echo ""
        echo "=== テスト結果 ==="
        echo "❌ test_resultsディレクトリが見つかりません（テスト未実行）"
    fi
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
    
    # 強化されたテスト結果表示
    show_test_results
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
            log_success "ビルドが完了しました"            ;;
            
        "test")
            log_info "テストを実行中..."
            # テスト結果ディレクトリを準備
            mkdir -p test_results logs
            docker-compose -f "$compose_file" up --abort-on-container-exit
            log_success "テストが完了しました"
            echo ""
            show_test_results
            ;;
            
        "full")
            log_info "フルビルド + テスト実行中..."
            # テスト結果ディレクトリを準備
            mkdir -p test_results logs
            docker-compose -f "$compose_file" down --remove-orphans --volumes || true
            docker-compose -f "$compose_file" build --no-cache
            docker-compose -f "$compose_file" up -d

            log_info "Waiting for services to start..."
            sleep 5 # Add a short delay to allow containers to be created

            log_info "E2Eテストランナーコンテナの終了を待機中..."
            e2e_test_runner_cid=$(docker-compose -f "$compose_file" ps -q e2e-test-runner)
            if [ -z "$e2e_test_runner_cid" ]; then
                log_error "E2Eテストランナーコンテナが見つかりません。"
                docker-compose -f "$compose_file" logs
                exit 1
            fi

            docker wait "$e2e_test_runner_cid"
            test_exit_code=$?

            # テスト結果をコンテナからホストにコピー
            log_info "Copying test results from container..."
            if docker cp "$e2e_test_runner_cid":/app/test_results.tar.gz . 2>/dev/null; then
                tar -xzvf test_results.tar.gz -C test_results
                rm test_results.tar.gz
            else
                log_warning "Could not copy test results from container. It might have already been written to the volume."
            fi

            if [ "$test_exit_code" -ne 0 ]; then
                log_error "E2Eテストが終了コード $test_exit_code で失敗しました。"
                # コンテナのログを出力してデバッグ情報を提供する
                docker logs "$e2e_test_runner_cid"
                exit 1
            else
                log_success "E2Eテストが正常に完了しました。"
            fi

            log_success "フル実行が完了しました"
            echo ""
            show_test_results
            ;;
              "cleanup")
            cleanup_environment
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
            ;;        build|test|full|cleanup|status|results)
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

# 結果表示やステータス確認は非対話式実行をサポート
if [[ "$OPERATION" == "results" ]]; then
    show_test_results
    exit 0
elif [[ "$OPERATION" == "status" ]]; then
    check_status
    exit 0
fi

if [[ "$OPERATION" != "cleanup" ]]; then
    log_info "使用する Docker Compose ファイル: $(get_compose_file)"
    log_info "実行する操作: $OPERATION"
    echo ""
fi

main_execution "$OPERATION"

echo ""
log_success "スクリプト実行が完了しました"
