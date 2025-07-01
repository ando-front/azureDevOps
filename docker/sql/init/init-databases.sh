#!/bin/bash

# SQL Server データベース初期化スクリプト
# このスクリプトはsql-server-initコンテナから実行されます

set -e

echo "Starting database initialization..."

# SQL Server の起動を待機
echo "Waiting for SQL Server to be ready..."
for i in {1..30}; do
    if /opt/mssql-tools18/bin/sqlcmd -S sql-server -U sa -P "$SA_PASSWORD" -Q "SELECT 1" -C > /dev/null 2>&1; then
        echo "SQL Server is ready"
        break
    fi
    echo "Waiting for SQL Server... ($i/30)"
    sleep 2
done

# 初期化スクリプトの実行
echo "Executing database initialization script..."
/opt/mssql-tools18/bin/sqlcmd -S sql-server -U sa -P "$SA_PASSWORD" -i /docker-entrypoint-initdb.d/01-create-databases.sql -C

echo "Database initialization completed successfully"