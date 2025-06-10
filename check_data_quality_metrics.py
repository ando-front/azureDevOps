#!/usr/bin/env python3
"""
data_quality_metricsテーブルの構造を確認するスクリプト
"""

import pyodbc

def check_data_quality_metrics():
    """data_quality_metricsテーブルの構造を確認"""
    try:
        # 接続文字列
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
        
        print("📊 data_quality_metrics テーブル構造:")
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'data_quality_metrics' 
            ORDER BY ORDINAL_POSITION
        """)
        
        columns = cursor.fetchall()
        if columns:
            for row in columns:
                col_name = row[0]
                data_type = row[1]
                max_length = row[2] if row[2] else ""
                nullable = row[3]
                
                length_str = f"({max_length})" if max_length else ""
                nullable_str = "NULL" if nullable == "YES" else "NOT NULL"
                
                print(f"  - {col_name}: {data_type}{length_str} {nullable_str}")
        else:
            print("  ❌ data_quality_metrics テーブルが見つかりません")
            
            # 全テーブル一覧を確認
            print("\n📋 現在のテーブル一覧:")
            cursor.execute("""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE' 
                ORDER BY TABLE_NAME
            """)
            tables = cursor.fetchall()
            for table in tables:
                print(f"  - {table[0]}")
        
        conn.close()
        print("\n🔚 データベース接続を閉じました")
        
    except Exception as e:
        print(f"❌ エラーが発生しました: {e}")

if __name__ == "__main__":
    check_data_quality_metrics()
