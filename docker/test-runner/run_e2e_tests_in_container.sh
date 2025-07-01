#!/bin/bash

# E2E テストランナー用エントリーポイントスクリプト
# Docker コンテナ内でE2Eテストを実行します

set -e

echo "========================================"
echo "E2E Test Runner - Container Entrypoint"
echo "========================================"

# 環境変数の表示
echo "Environment Configuration:"
echo "  SQL_SERVER_HOST: ${SQL_SERVER_HOST:-localhost}"
echo "  SQL_SERVER_DATABASE: ${SQL_SERVER_DATABASE:-SynapseTestDB}"
echo "  SQL_SERVER_USER: ${SQL_SERVER_USER:-sa}"
echo "  SQL_SERVER_PORT: ${SQL_SERVER_PORT:-1433}"
echo "  AZURITE_HOST: ${AZURITE_HOST:-localhost}"
echo "  E2E_TEST_MODE: ${E2E_TEST_MODE:-default}"
echo "  PYTHONPATH: ${PYTHONPATH:-/app}"

# 作業ディレクトリの設定
cd /app

# 必要なディレクトリの作成
mkdir -p /app/test_results /app/logs

# Copy the Python health check script to a known location
if [ -f "/app/docker/test-runner/check_db_connection.py" ]; then
    cp /app/docker/test-runner/check_db_connection.py /usr/local/bin/check_db_connection.py
    chmod +x /usr/local/bin/check_db_connection.py
fi

# SQL Server 接続テスト
echo ""
echo "Testing SQL Server connection..."
if [ -f "/usr/local/bin/check_db_connection.py" ]; then
    python /usr/local/bin/check_db_connection.py
else
    python -c "
import pyodbc
import os
import sys

try:
    conn_str = f\"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={{os.getenv('SQL_SERVER_HOST', 'localhost')}};DATABASE={{os.getenv('SQL_SERVER_DATABASE', 'SynapseTestDB')}};UID={{os.getenv('SQL_SERVER_USER', 'sa')}};PWD={{os.getenv('SQL_SERVER_PASSWORD', 'YourStrong!Passw0rd123')}};TrustServerCertificate=yes;Encrypt=yes\"
    print(f'Connection string: {conn_str}')
    
    connection = pyodbc.connect(conn_str)
    cursor = connection.cursor()
    cursor.execute('SELECT 1 as test')
    result = cursor.fetchone()
    print(f'✅ SQL Server connection successful! Test result: {result[0]}')
    
    # テーブルの存在確認
    cursor.execute(\"SELECT table_name FROM information_schema.tables WHERE table_name = 'ClientDmBx'\")
    table_result = cursor.fetchone()
    if table_result:
        print(f'✅ ClientDmBx table found: {table_result[0]}')
    else:
        print('❌ ClientDmBx table not found')
    
    connection.close()
    sys.exit(0)
except Exception as e:
    print(f'❌ SQL Server connection failed: {e}')
    sys.exit(1)
"
fi

if [ $? -ne 0 ]; then
    echo "❌ SQL Server connection test failed. Exiting..."
    exit 1
fi

# pytest の利用可能なテストファイルを表示
echo ""
echo "Discovering test files..."
python -m pytest --collect-only tests/e2e/ -q 2>/dev/null | head -20

# E2E テストの実行
echo ""
echo "========================================"
echo "Running E2E Tests..."
echo "========================================"

# テストコマンド作成
TEST_CMD="python -m pytest tests/e2e/ -v --tb=short -m e2e --junitxml=/app/test_results/e2e_results.xml"

# 追加のオプション（環境変数により調整可能）
if [ "${E2E_TEST_MODE}" = "flexible" ]; then
    TEST_CMD="${TEST_CMD} --maxfail=5"
fi

if [ "${PYTEST_VERBOSE}" = "true" ]; then
    TEST_CMD="${TEST_CMD} -vv"
fi

# テスト実行
echo "Executing: ${TEST_CMD}"
eval ${TEST_CMD}

TEST_EXIT_CODE=$?

# 結果のサマリー
echo ""
echo "========================================"
echo "E2E Test Execution Completed"
echo "========================================"
echo "Exit code: ${TEST_EXIT_CODE}"

if [ ${TEST_EXIT_CODE} -eq 0 ]; then
    echo "✅ All tests passed!"
else
    echo "❌ Some tests failed. Check logs for details."
fi

# ログと結果ファイルの場所を表示
echo ""
echo "Results:"
echo "  Test results: /app/test_results/"
echo "  Logs: /app/logs/"

# 結果ファイルが存在する場合、簡単なサマリーを表示
if [ -f "/app/test_results/e2e_results.xml" ]; then
    echo ""
    echo "Test result file created successfully."
    ls -la /app/test_results/
fi

exit ${TEST_EXIT_CODE}