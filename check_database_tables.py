#!/usr/bin/env python3
"""
データベースのテーブル一覧を確認するスクリプト
"""

import pyodbc

def check_database_tables():
    """現在のデータベースのテーブル一覧を表示"""
    try:
        # 接続文字列（履歴展開エラー回避のため、パスワードを直接指定）
        conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            "SERVER=localhost,1433;"
            "DATABASE=TGMATestDB;"
            "UID=sa;"
            "PWD=YourStrong!Passw0rd123;"
            "TrustServerCertificate=yes;"
            "Encrypt=no;"
        )
        
        print("🔗 データベースに接続中...")
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        print("📊 現在のデータベーステーブル一覧:")
        cursor.execute("""
            SELECT TABLE_SCHEMA, TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE' 
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """)
        
        tables = cursor.fetchall()
        for table in tables:
            print(f"  - {table.TABLE_SCHEMA}.{table.TABLE_NAME}")
        
        print(f"\n✅ 合計 {len(tables)} 個のテーブルが見つかりました")
        
        # ETLログテーブルの存在確認
        print("\n🔍 ETLログテーブルの存在確認:")
        cursor.execute("""
            SELECT COUNT(*) as table_count
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'pipeline_execution_log'
        """)
        log_table_count = cursor.fetchone()[0]
        
        if log_table_count > 0:
            print("  ✅ pipeline_execution_log テーブルが存在します")
            
            # テーブルの構造を確認
            cursor.execute("""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = 'pipeline_execution_log'
                ORDER BY ORDINAL_POSITION
            """)
            columns = cursor.fetchall()
            print("  📋 カラム構造:")
            for col in columns:
                print(f"    - {col.COLUMN_NAME} ({col.DATA_TYPE}) {'NULL' if col.IS_NULLABLE == 'YES' else 'NOT NULL'}")
        else:
            print("  ❌ pipeline_execution_log テーブルが存在しません")
        
        conn.close()
        print("\n🔚 データベース接続を閉じました")
        
    except Exception as e:
        print(f"❌ エラーが発生しました: {e}")

if __name__ == "__main__":
    check_database_tables()
