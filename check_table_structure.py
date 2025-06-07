#!/usr/bin/env python3
import pyodbc
import os

# データベース接続
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER=localhost,1433;DATABASE=master;UID=sa;PWD=YourStrong!Passw0rd123;TrustServerCertificate=yes;'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# client_dmテーブルの構造を取得
try:
    cursor.execute("""
    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = 'client_dm' AND TABLE_SCHEMA = 'dbo'
    ORDER BY ORDINAL_POSITION
    """)
    
    print('client_dm テーブルのカラム構造:')
    columns = cursor.fetchall()
    for col in columns:
        nullable = "NULL可" if col[2] == "YES" else "NOT NULL"
        print(f'  {col[0]} - {col[1]} ({nullable})')
        
except Exception as e:
    print(f'client_dmテーブルでエラー: {e}')
    
    # テーブルが存在しない場合、利用可能なテーブルを表示
    cursor.execute("""
    SELECT TABLE_SCHEMA, TABLE_NAME 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_SCHEMA, TABLE_NAME
    """)
    
    print('\n利用可能なテーブル:')
    tables = cursor.fetchall()
    for table in tables:
        print(f'  {table[0]}.{table[1]}')
    
conn.close()
