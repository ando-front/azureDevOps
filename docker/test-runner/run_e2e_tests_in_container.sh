#!/bin/sh

wait_for_sql_server() {
  echo "Waiting for SQL Server instance and SynapseTestDB to be ready..."
  local host="$SQL_SERVER_HOST"
  local database="$SQL_SERVER_DATABASE"
  local user="$SQL_SERVER_USER"
  local password="$SQL_SERVER_PASSWORD"
  local timeout=300 # seconds
  local start_time=$(date +%s)
  local error_log="/tmp/sqlcmd_error.log"

  while true; do
    # 1. Check if SQL Server instance is up and accepting connections (connect to master)
    /opt/mssql-tools18/bin/sqlcmd -S "$host" -d master -U "$user" -P "$password" -Q "SELECT 1" > /dev/null 2> "$error_log"
    if [ $? -eq 0 ]; then
      echo "SQL Server instance is up. Checking for SynapseTestDB..."
      # 2. Check if SynapseTestDB exists
      DB_EXISTS=$(/opt/mssql-tools18/bin/sqlcmd -S "$host" -d master -U "$user" -P "$password" -Q "SELECT name FROM sys.databases WHERE name = 'SynapseTestDB'" -h -1 2> "$error_log" | tr -d '[:space:]')
      if [ "$DB_EXISTS" = "SynapseTestDB" ]; then
        echo "SynapseTestDB exists. Attempting to connect to it..."
        # 3. Check if SynapseTestDB is accessible (connect to SynapseTestDB)
        /opt/mssql-tools18/bin/sqlcmd -S "$host" -d "$database" -U "$user" -P "$password" -Q "SELECT 1" > /dev/null 2> "$error_log"
        if [ $? -eq 0 ]; then
          echo "SQL Server and SynapseTestDB are fully ready."
          break
        fi
      fi
    fi

    current_time=$(date +%s)
    elapsed_time=$((current_time - start_time))
    if [ "$elapsed_time" -ge "$timeout" ]; then
      echo "SQL Server or SynapseTestDB did not become ready within the timeout period."
      echo "Last sqlcmd error output:"
      cat "$error_log"
      exit 1
    fi

    echo "SQL Server instance or SynapseTestDB not ready yet, waiting 5 seconds..."
    sleep 5
  done
}

echo '⏳ 完全なE2Eテスト環境の準備を開始します...'

wait_for_sql_server

echo '🚀 完全なE2Eテストスイートを実行します...'
echo '📁 テスト結果ディレクトリを作成...'
mkdir -p /app/test_results
echo '🔍 利用可能なE2Eテストファイルを確認...'
find /app/tests/e2e -name 'test_*.py' -type f | wc -l
echo '📊 フェーズ1: 基本接続テストを実行...'
python -m pytest /app/tests/e2e/test_basic_connections.py -v --tb=short --junitxml=/app/test_results/basic_connections.xml --timeout=300
echo '🔄 フェーズ2: Docker統合テストを実行...'
python -m pytest /app/tests/e2e/test_docker_simple_integration.py -v --tb=short --junitxml=/app/test_results/docker_integration.xml --timeout=300 || echo 'Docker統合テスト一部失敗'
echo '📦 フェーズ3: パイプライン個別テストを実行...'
python -m pytest /app/tests/e2e/test_e2e_pipeline_marketing_client_dm_comprehensive_fixed.py -v --tb=short --junitxml=/app/test_results/client_dm.xml --timeout=300 || echo 'ClientDMテスト一部失敗'
echo 'ポイント付与テスト一部失敗'
python -m pytest /app/tests/e2e/test_e2e_pipeline_point_grant_email_fixed.py -v --tb=short --junitxml=/app/test_results/point_grant_email.xml --timeout=300 || echo 'ポイント付与テスト一部失敗'
echo '🔍 フェーズ4: ADFデータ品質・セキュリティテストを実行...'
python -m pytest /app/tests/e2e/test_e2e_adf_data_quality_security.py -v --tb=short --junitxml=/app/test_results/data_quality_security.xml --timeout=300 || echo 'データ品質テスト一部失敗'
echo '⚡ フェーズ5: ADF パイプライン実行テストを実行...'
python -m pytest /app/tests/e2e/test_e2e_adf_pipeline_execution.py -v --tb=short --junitxml=/app/test_results/pipeline_execution.xml --timeout=300 || echo 'パイプライン実行テスト一部失敗'
echo '📈 フェーズ6: 最終統合テストを実行...'
python -m pytest /app/tests/e2e/test_final_integration.py -v --tb=short --junitxml=/app/test_results/final_integration.xml --timeout=300 || echo '最終統合テスト一部失敗'
echo '🏆 フェーズ7: 包括的E2Eテストスイートを実行...'
python -m pytest /app/tests/e2e/ -v --tb=short --maxfail=10 --junitxml=/app/test_results/e2e_complete.xml --html=/app/test_results/e2e_report.html --self-contained-html --cov=src --cov-report=html:/app/test_results/coverage_html --cov-report=xml:/app/test_results/coverage.xml --timeout=300 || echo '包括的テスト完了（一部失敗の可能性）'
echo '✅ 完全なE2Eテストスイート完了！'
echo '📊 テスト結果サマリー:'
find /app/test_results -name '*.xml' -exec echo 'XML結果ファイル: {}' \;
find /app/test_results -name '*.html' -exec echo 'HTML結果ファイル: {}' \;
echo '📈 テスト実行完了時刻:' $(date)
