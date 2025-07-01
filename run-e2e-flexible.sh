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
    
    if [[ "$compose_file" == "docker-compose.e2e.no-proxy.yml" ]]; then
        # プロキシなし用ファイルは常に最新版で再作成
        log_info "プロキシなし用 Docker Compose ファイルを作成中..."
        create_no_proxy_compose_file
    elif [[ ! -f "$compose_file" ]]; then
        log_error "Docker Compose ファイルが見つかりません: $compose_file"
        exit 1
    fi
}

# プロキシなし用 Docker Compose ファイルの作成
create_no_proxy_compose_file() {
    cat > docker-compose.e2e.no-proxy.yml << 'EOF'
services:
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
      - sql_data:/var/opt/mssql
    networks:
      - adf-e2e-network
    healthcheck:
      test: ["CMD-SHELL", "/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd123' -Q 'SELECT 1'"]
      interval: 10s
      retries: 10
      start_period: 30s
      timeout: 5s

  sql-server-init:
    image: mcr.microsoft.com/mssql/server:2019-latest
    container_name: adf-sql-server-init
    volumes:
      - ./docker/sql/init:/docker-entrypoint-initdb.d
    networks:
      - adf-e2e-network
    environment:
      - SA_PASSWORD=YourStrong!Passw0rd123
    depends_on:
      sql-server:
        condition: service_healthy
    command: >
      /bin/bash -c "
      echo 'Starting database initialization...'
      
      # Wait for SQL Server to be ready
      for i in {1..30}; do
        if /opt/mssql-tools/bin/sqlcmd -S sql-server -U sa -P 'YourStrong!Passw0rd123' -Q 'SELECT 1' > /dev/null 2>&1; then
          echo 'SQL Server is ready'
          break
        fi
        echo 'Waiting for SQL Server... (\$i/30)'
        sleep 2
      done
      
      # Execute initialization script
      echo 'Executing database initialization script...'
      /opt/mssql-tools/bin/sqlcmd -S sql-server -U sa -P 'YourStrong!Passw0rd123' -i /docker-entrypoint-initdb.d/01-create-databases.sql
      
      echo 'Database initialization completed successfully'
      "

  azurite:
    image: mcr.microsoft.com/azure-storage/azurite:latest
    container_name: adf-azurite-test
    command: "azurite --blobHost 0.0.0.0 --queueHost 0.0.0.0 --tableHost 0.0.0.0"
    ports:
      - "10000:10000"
      - "10001:10001"
      - "10002:10002"
    volumes:
      - azurite_data:/data
    networks:
      - adf-e2e-network
    healthcheck:
      test: ["CMD-SHELL", "nc -z localhost 10000 && nc -z localhost 10001 && nc -z localhost 10002"]
      interval: 5s
      timeout: 3s
      retries: 5
      start_period: 15s

  e2e-test-runner:
    build:
      context: .
      dockerfile: docker/test-runner/Dockerfile
    image: azuredevops-e2e-test-runner:latest
    container_name: adf-e2e-test-runner
    volumes:
      - .:/app
      - ./test_results:/app/test_results
      - ./logs:/app/logs
    depends_on:
      sql-server:
        condition: service_healthy
      sql-server-init:
        condition: service_completed_successfully
      azurite:
        condition: service_healthy
    networks:
      - adf-e2e-network
    environment:
      - PYTHONPATH=/app
      - SQL_SERVER_HOST=sql-server
      - SQL_SERVER_DATABASE=SynapseTestDB
      - SQL_SERVER_USER=sa
      - SQL_SERVER_PASSWORD=YourStrong!Passw0rd123
      - SQL_SERVER_PORT=1433
      - AZURITE_HOST=azurite
      - E2E_TEST_MODE=flexible
      # Azurite接続文字列（多くのテストで必要）
      - AZURITE_CONNECTION_STRING=DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite:10000/devstoreaccount1;QueueEndpoint=http://azurite:10001/devstoreaccount1;TableEndpoint=http://azurite:10002/devstoreaccount1;
      # IRシミュレーター設定（必要に応じて）
      - IR_SIMULATOR_HOST=localhost
      - IR_SIMULATOR_PORT=8080
      # Azure Data Factory関連設定
      - ADF_RESOURCE_GROUP=test-rg
      - ADF_FACTORY_NAME=test-adf
      - ADF_SUBSCRIPTION_ID=test-subscription
      # テスト実行制御
      - PYTEST_MARKERS=e2e
      - PYTEST_VERBOSITY=2
    entrypoint: /usr/local/bin/run_e2e_tests_in_container.sh

networks:
  adf-e2e-network:
    driver: bridge

volumes:
  sql_data:
  azurite_data:

EOF
    log_success "docker-compose.e2e.no-proxy.yml を作成しました"
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
        if [[ -f "test_results/e2e_results.xml" ]]; then
            echo "✅ JUnit XMLレポート: test_results/e2e_results.xml"
            # XMLからテスト統計を抽出（可能な場合）
            if command -v grep >/dev/null 2>&1; then
                local xml_content=$(cat test_results/e2e_results.xml)
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
                if [[ $xml_content =~ skipped=\"([0-9]+)\" ]]; then
                    local skipped="${BASH_REMATCH[1]}"
                    echo "⏭️ スキップ: $skipped"
                fi
                if [[ $xml_content =~ time=\"([0-9.]+)\" ]]; then
                    local duration="${BASH_REMATCH[1]}"
                    echo "⏱️ 実行時間: ${duration}秒"
                fi
                
                # 成功率の計算
                if [[ -n "$total_tests" && -n "$failures" && -n "$errors" && -n "$skipped" ]]; then
                    local executed=$((total_tests - skipped))
                    local passed=$((executed - failures - errors))
                    if [[ $executed -gt 0 ]]; then
                        local success_rate=$(echo "scale=1; $passed * 100 / $executed" | bc 2>/dev/null || echo "N/A")
                        echo "✅ 成功: $passed"
                        echo "📈 成功率: ${success_rate}%"
                    else
                        echo "⚠️ 実際に実行されたテストはありません（全てスキップ）"
                    fi
                fi
            fi
        elif [[ -f "test_results/e2e_no_proxy_results.xml" ]]; then
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
            log_success "ビルドが完了しました"
            ;;
            
        "test" | "full")
            if [[ "$operation" == "full" ]]; then
                log_info "フルビルド + テスト実行を開始します..."
                cleanup_environment
                log_info "Docker イメージをビルドしています..."
                docker-compose -f "$compose_file" build --no-cache
            else
                log_info "テスト実行を開始します..."
            fi

            # テスト結果とログ用のディレクトリを準備
            mkdir -p test_results logs
            
            log_info "テスト環境をバックグラウンドで起動しています..."
            docker-compose -f "$compose_file" up -d

            log_info "テストランナーのログをストリーミングします... (Ctrl+Cで停止)"
            docker logs -f adf-e2e-test-runner &
            local log_pid=$!

            # Ctrl+Cでログのフォローを停止できるようにする
            trap "kill $log_pid 2>/dev/null" INT

            log_info "テストランナーの実行完了を待機しています..."
            local exit_code=$(docker wait adf-e2e-test-runner)
            
            # ログのフォローを停止
            kill $log_pid 2>/dev/null
            wait $log_pid 2>/dev/null
            trap - INT

            log_info "ログをファイルに収集しています..."
            docker logs adf-e2e-test-runner > logs/e2e-test-runner.log 2>&1
            docker logs adf-sql-server-test > logs/sql-server.log 2>&1
            docker logs adf-sql-server-init > logs/sql-server-init.log 2>&1
            docker logs adf-azurite-test > logs/azurite.log 2>&1

            if [[ "$exit_code" -eq 0 ]]; then
                log_success "テスト実行が正常に完了しました。"
            else
                log_error "テスト実行でエラーが検出されました (終了コード: $exit_code)。"
                log_error "詳細は logs/e2e-test-runner.log を確認してください。"
            fi

            log_info "テスト環境をクリーンアップしています..."
            cleanup_environment
            
            echo ""
            log_info "最終的なテスト結果:"
            show_test_results
            
            if [[ "$exit_code" -ne 0 ]]; then
                exit "$exit_code"
            fi
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
