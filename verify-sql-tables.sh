#!/bin/bash
# SQL Serverコンテナ内でテーブル作成確認スクリプト

echo "=== SQL Server テーブル作成確認 ==="

# Dockerコンテナが起動しているか確認
if ! docker ps | grep -q sqlserver-e2e-test; then
    echo "❌ SQL Serverコンテナが起動していません"
    echo "まず docker-compose を使ってコンテナを起動してください"
    exit 1
fi

echo "✅ SQL Serverコンテナが起動中"

# テーブル存在確認のSQLクエリ
echo "📊 テーブル存在確認..."

# テーブル一覧を取得
docker exec sqlserver-e2e-test /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P "YourStrong!Passw0rd123" \
    -d TGMATestDB \
    -Q "
SELECT 
    SCHEMA_NAME(schema_id) as SchemaName,
    name as TableName,
    create_date as Created
FROM sys.tables 
WHERE name IN ('client_dm', 'ClientDmBx', 'point_grant_email', 'marketing_client_dm', 'client_raw', 'pipeline_execution_log')
ORDER BY SchemaName, TableName;
" -C

echo ""
echo "📋 スキーマ存在確認..."

# スキーマ一覧を取得
docker exec sqlserver-e2e-test /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P "YourStrong!Passw0rd123" \
    -d TGMATestDB \
    -Q "
SELECT 
    name as SchemaName,
    schema_id
FROM sys.schemas 
WHERE name IN ('dbo', 'staging', 'etl')
ORDER BY name;
" -C

echo ""
echo "🔍 テーブル詳細情報..."

# テーブルとカラム情報を取得
docker exec sqlserver-e2e-test /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P "YourStrong!Passw0rd123" \
    -d TGMATestDB \
    -Q "
SELECT 
    t.TABLE_SCHEMA,
    t.TABLE_NAME,
    COUNT(c.COLUMN_NAME) as ColumnCount
FROM INFORMATION_SCHEMA.TABLES t
LEFT JOIN INFORMATION_SCHEMA.COLUMNS c ON t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
WHERE t.TABLE_TYPE = 'BASE TABLE'
GROUP BY t.TABLE_SCHEMA, t.TABLE_NAME
ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME;
" -C

echo ""
echo "=== 確認完了 ==="
