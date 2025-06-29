#!/bin/bash
# filepath: c:\Users\0190402\git\tgma-MA-POC\docker\sql\init\init-databases.sh
# E2Eテスト用データベース初期化スクリプト（冪等性対応）

set -e

echo "Starting database initialization..."

# SQL Serverの起動完了はdocker-composeのhealthcheckに任せる
echo "Database is ready. Running initialization scripts..."

# スクリプトの実行順序を定義
# スクリプトは/docker-entrypoint-initdb.d/に配置される
scripts=(
    "00_create_synapse_db.sql"
    "01_init_database.sql"
    "02_create_test_tables.sql"
    "04_enhanced_test_tables.sql"
    "03_insert_test_data.sql"
    "05_comprehensive_test_data.sql"
    "06_additional_e2e_test_data.sql"
)

# 各スクリプトを順次実行
for script in "${scripts[@]}"; do
    full_path="/docker-entrypoint-initdb.d/$script"
    if [ -f "$full_path" ]; then
        echo "Executing: $full_path"
        /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -i "$full_path" -C
        if [ $? -eq 0 ]; then
            echo "Successfully executed: $full_path"
        else
            echo "Error executing: $full_path"
            exit 1
        fi
    else
        echo "Script not found: $full_path"
        # 必須ではないスクリプトもあるため、エラーで終了しない
    fi
done

echo "Database initialization scripts completed successfully!"

# 初期化完了を示すマーカーテーブルを作成
echo "Creating initialization marker table..."
/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -d master -Q "IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'InitializationComplete') CREATE TABLE InitializationComplete (id INT);"

echo "=============== MSSQL SERVER HAS BEEN INITIALIZED ================"
