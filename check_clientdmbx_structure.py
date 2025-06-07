#!/usr/bin/env python3
"""
テーブル構造確認スクリプト
"""

import pyodbc
import os

def check_table_structure():
    # SQL Server接続
    conn_str = f"""
    DRIVER={{ODBC Driver 17 for SQL Server}};
    SERVER=localhost,1433;
    DATABASE=SynapseTestDB;
    UID=sa;
    PWD=YourStrong!Passw0rd123;
    TrustServerCertificate=yes;
    """

    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # ClientDmBxテーブル構造確認
        print('=== ClientDmBx テーブル構造 ===')
        cursor.execute("""
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'ClientDmBx' AND TABLE_SCHEMA = 'dbo'
        ORDER BY ORDINAL_POSITION
        """)
        
        columns = cursor.fetchall()
        if not columns:
            print('ClientDmBxテーブルが見つかりません。利用可能なテーブルを確認します...')
            
            # 利用可能なテーブル一覧
            cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
            """)
            
            tables = cursor.fetchall()
            print('\n利用可能なテーブル:')
            for table in tables:
                print(f'  - {table[0]}')
        else:
            for row in columns:
                print(f'{row[0]} - {row[1]} - Nullable: {row[2]} - Default: {row[3]}')
        
        # client_dmテーブル構造も確認
        print('\n=== client_dm テーブル構造 ===')
        cursor.execute("""
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'client_dm' AND TABLE_SCHEMA = 'dbo'
        ORDER BY ORDINAL_POSITION
        """)
        
        for row in cursor.fetchall():
            print(f'{row[0]} - {row[1]} - Nullable: {row[2]} - Default: {row[3]}')
        
        conn.close()
        
    except Exception as e:
        print(f'エラー: {e}')

if __name__ == "__main__":
    check_table_structure()
