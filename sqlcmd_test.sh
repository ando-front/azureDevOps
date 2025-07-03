#!/usr/bin/env bash
# Docker コンテナ上の SQL Server に接続してクエリを実行
# adf-sql-server-test コンテナ内の sqlcmd を使用します
docker exec -i adf-sql-server-test /opt/mssql-tools/bin/sqlcmd \
  -S localhost -U sa -P 'YourStrong!Passw0rd123' \
  -Q "SELECT 1" -C