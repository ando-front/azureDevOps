#!/bin/sh

set -e

# Copy the Python health check script to a known location
cp /app/docker/test-runner/check_db_connection.py /usr/local/bin/check_db_connection.py
chmod +x /usr/local/bin/check_db_connection.py

# Wait for SQL Server using the Python script
echo '⏳ 完全なE2Eテスト環境の準備を開始します...'
python /usr/local/bin/check_db_connection.py

echo '🚀 完全なE2Eテストスイートを実行します...'

# テスト結果ディレクトリを作成
mkdir -p /app/test_results /app/logs

# pytest を実行
# 環境変数 PYTEST_ARGS が設定されていればそれを使用
# 設定されていなければデフォルトの引数を使用
if [ -z "$PYTEST_ARGS" ]; then
  PYTEST_ARGS="-v --tb=long --maxfail=5"
fi

# テスト実行
echo "Running pytest..."
pytest tests/e2e $PYTEST_ARGS --junitxml=/app/test_results/e2e_no_proxy_results.xml --html=/app/test_results/e2e_no_proxy_report.html --self-contained-html
pytest_exit_code=$?

echo "Contents of /app/test_results after pytest:"
ls -la /app/test_results

exit $pytest_exit_code