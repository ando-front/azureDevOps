#!/bin/bash

# =================================================================
# Enhanced E2E Test Execution Script
# 改善されたE2Eテスト環境の実行スクリプト
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
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# プロジェクトディレクトリの確認
PROJECT_DIR="/Users/andokenji/Documents/書類 - 安藤賢二のMac mini/GitHub/azureDevOps"
cd "$PROJECT_DIR"

log_info "Enhanced E2E Test Environment Starting..."
log_info "Project Directory: $PROJECT_DIR"

# 1. 既存のコンテナを停止・削除
log_info "Stopping existing containers..."
docker-compose -f docker-compose.e2e.simple.yml down --remove-orphans || true

# 2. 改善されたE2E環境を起動
log_info "Starting enhanced E2E environment with improved schema and test data..."
docker-compose -f docker-compose.e2e.simple.yml up -d

# 3. サービスの起動を待機
log_info "Waiting for services to be ready..."
sleep 60

# 4. SQL Serverの接続確認
log_info "Checking SQL Server connectivity..."
docker exec sqlserver-e2e-simple /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P 'YourStrong!Passw0rd123' \
    -Q "SELECT 'SQL Server is ready!' as Status" -C || {
    log_error "SQL Server connection failed"
    exit 1
}

# 5. データベースとテーブルの存在確認
log_info "Verifying enhanced database schema..."
docker exec sqlserver-e2e-simple /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P 'YourStrong!Passw0rd123' \
    -Q "USE TGMATestDB; SELECT 
        TABLE_NAME, 
        (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = t.TABLE_NAME) as COLUMN_COUNT
        FROM INFORMATION_SCHEMA.TABLES t 
        WHERE TABLE_TYPE = 'BASE TABLE' 
        ORDER BY TABLE_NAME" -C

# 6. テストデータの確認
log_info "Verifying enhanced test data..."
docker exec sqlserver-e2e-simple /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P 'YourStrong!Passw0rd123' \
    -Q "USE TGMATestDB; 
        SELECT 'client_dm' as table_name, COUNT(*) as record_count FROM client_dm WHERE customer_cd LIKE 'E2E_%'
        UNION ALL
        SELECT 'ClientDmBx' as table_name, COUNT(*) as record_count FROM ClientDmBx WHERE customer_cd LIKE 'E2E_%'
        UNION ALL
        SELECT 'point_grant_email' as table_name, COUNT(*) as record_count FROM point_grant_email WHERE customer_cd LIKE 'E2E_%'
        UNION ALL
        SELECT 'marketing_client_dm' as table_name, COUNT(*) as record_count FROM marketing_client_dm WHERE customer_cd LIKE 'E2E_%'" -C

# 7. E2Eテストの実行（Python環境確認）
log_info "Verifying Python test environment..."
if command -v python3 &> /dev/null; then
    log_info "Running enhanced E2E tests locally..."
    export SQL_SERVER_HOST="localhost"
    export SQL_SERVER_PORT="1433"
    export SQL_SERVER_DATABASE="TGMATestDB"
    export SQL_SERVER_USER="sa"
    export SQL_SERVER_PASSWORD="YourStrong!Passw0rd123"
    
    # 必要なPythonパッケージの確認
    pip3 install -q pytest pyodbc pandas pytz azure-storage-blob || {
        log_warning "Some Python packages may need to be installed"
    }
    
    # スキーマ検証テストの実行
    log_info "Running database schema validation tests..."
    python3 -m pytest tests/e2e/test_database_schema.py -v -s || {
        log_warning "Schema validation tests encountered issues"
    }
    
    # データ整合性テストの実行
    log_info "Running data integrity validation tests..."
    python3 -m pytest tests/e2e/test_data_integrity.py -v -s || {
        log_warning "Data integrity tests encountered issues"
    }
else
    log_warning "Python3 not found - skipping local test execution"
fi

# 8. サービス状況の表示
log_info "E2E Environment Status:"
docker-compose -f docker-compose.e2e.simple.yml ps

log_success "Enhanced E2E Test Environment is ready!"
log_info "SQL Server: localhost:1433 (sa/YourStrong!Passw0rd123)"
log_info "Database: TGMATestDB with enhanced schema and comprehensive test data"
log_info "Azurite: localhost:10000/10001/10002"

echo ""
echo "To run manual tests:"
echo "  python3 -m pytest tests/e2e/ -v -s"
echo ""
echo "To stop the environment:"
echo "  docker-compose -f docker-compose.e2e.simple.yml down"
