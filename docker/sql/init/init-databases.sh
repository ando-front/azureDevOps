#!/bin/bash
# filepath: c:\Users\0190402\git\tgma-MA-POC\docker\sql\init\init-databases.sh
# E2Eテスト用データベース初期化スクリプト（冪等性対応）

set -e

echo "Starting database initialization..."

# SQL Serverの起動完了を待機
echo "Waiting for SQL Server to be ready..."
sleep 30

# スクリプトの実行順序を定義
scripts=(
    "/opt/sql-scripts/00_create_synapse_db.sql"
    "/opt/sql-scripts/01_init_database.sql"
    "/opt/sql-scripts/02_create_test_tables.sql"
    "/opt/sql-scripts/04_enhanced_test_tables.sql"
    "/opt/sql-scripts/03_insert_test_data.sql"
    "/opt/sql-scripts/05_comprehensive_test_data.sql"
)

# 各スクリプトを順次実行
for script in "${scripts[@]}"; do
    if [ -f "$script" ]; then
        echo "Executing: $script"
        /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd123' -i "$script" -C
        if [ $? -eq 0 ]; then
            echo "Successfully executed: $script"
        else
            echo "Error executing: $script"
            exit 1
        fi
    else
        echo "Script not found: $script"
    fi
done

echo "Database initialization completed successfully!"
