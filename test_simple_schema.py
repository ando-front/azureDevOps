#!/usr/bin/env python3
"""
Simple Schema Test - デバッグ用
"""

import pyodbc

def test_simple_connection():
    """シンプルな接続テスト"""
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=localhost,1433;"
        "DATABASE=TGMATestDB;"
        "UID=sa;"
        "PWD=YourStrong!Passw0rd123;"
        "TrustServerCertificate=yes;"
    )
    
    try:
        print("接続を試行中...")
        conn = pyodbc.connect(conn_str, timeout=5)
        print("接続成功!")
        
        cursor = conn.cursor()
        
        # 現在のデータベース確認
        cursor.execute("SELECT DB_NAME() as current_db")
        db_name = cursor.fetchone()[0]
        print(f"現在のデータベース: {db_name}")
        
        # テーブル一覧取得
        cursor.execute("""
            SELECT TABLE_SCHEMA, TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """)
        
        tables = cursor.fetchall()
        print(f"テーブル数: {len(tables)}")
        for table in tables:
            print(f"  {table[0]}.{table[1]}")
        
        # client_dmテーブル確認
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'client_dm' AND TABLE_SCHEMA = 'dbo'
        """)
        count = cursor.fetchone()[0]
        print(f"client_dmテーブル存在: {count}")
        
        conn.close()
        print("テスト完了")
        
    except Exception as e:
        print(f"エラー: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_simple_connection()
