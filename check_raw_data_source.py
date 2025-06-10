#!/usr/bin/env python3
"""
raw_data_sourceテーブルの構造を確認
"""

import pyodbc

def check_raw_data_source_schema():
    """raw_data_sourceテーブルのスキーマを確認"""
    try:
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
        
        print("\n📊 raw_data_source テーブル構造:")
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE, COLUMN_DEFAULT
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'raw_data_source' 
            ORDER BY ORDINAL_POSITION
        """)
        
        results = cursor.fetchall()
        if results:
            for row in results:
                col_name = row[0]
                data_type = row[1]
                max_length = row[2] if row[2] else ""
                nullable = row[3]
                default_val = row[4] if row[4] else ""
                
                length_str = f"({max_length})" if max_length else ""
                nullable_str = "NULL" if nullable == "YES" else "NOT NULL"
                default_str = f" DEFAULT {default_val}" if default_val else ""
                
                print(f"  - {col_name}: {data_type}{length_str} {nullable_str}{default_str}")
                
            # 既存データも確認
            print(f"\n📊 raw_data_source データ:")
            cursor.execute("SELECT TOP 5 * FROM [dbo].[raw_data_source]")
            data_results = cursor.fetchall()
            for i, row in enumerate(data_results, 1):
                print(f"  Row {i}: {row}")
        else:
            print("  ❌ テーブル raw_data_source が見つかりません")
        
        conn.close()
        print("\n🔚 データベース接続を閉じました")
        
    except Exception as e:
        print(f"❌ エラーが発生しました: {e}")

if __name__ == "__main__":
    check_raw_data_source_schema()
