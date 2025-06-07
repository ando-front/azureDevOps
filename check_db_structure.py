#!/usr/bin/env python3
import pyodbc
import os

# データベース接続
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER=localhost,1433;DATABASE=master;UID=sa;PWD=YourStrong!Passw0rd123;TrustServerCertificate=yes;'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# 利用可能なデータベースを確認
try:
    cursor.execute("SELECT name FROM sys.databases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')")
    databases = cursor.fetchall()
    print('利用可能なデータベース:')
    for db in databases:
        print(f'  {db[0]}')
    
    if databases:
        # 最初のデータベースに接続してテーブルを確認
        db_name = databases[0][0]
        conn.close()
        
        # 新しいデータベースに接続
        conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER=localhost,1433;DATABASE={db_name};UID=sa;PWD=YourStrong!Passw0rd123;TrustServerCertificate=yes;'
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        print(f'\n{db_name} データベースのテーブル:')
        cursor.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
        """)
        
        tables = cursor.fetchall()
        for table in tables:
            print(f'  {table[0]}.{table[1]}')
            
        # client_dmテーブルがあるかチェック
        cursor.execute("""
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'client_dm'
        ORDER BY ORDINAL_POSITION
        """)
        
        print(f'\n{db_name} の client_dm テーブルのカラム構造:')
        columns = cursor.fetchall()
        if columns:
            for col in columns:
                nullable = "NULL可" if col[2] == "YES" else "NOT NULL"
                print(f'  {col[0]} - {col[1]} ({nullable})')
        else:
            print('  client_dmテーブルが見つかりません')
        
except Exception as e:
    print(f'エラー: {e}')
    
conn.close()
