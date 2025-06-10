#!/usr/bin/env python3
"""
テーブルスキーマを詳細に確認するスクリプト
"""

import pyodbc

def check_table_schemas():
    """テーブルスキーマを詳細に確認"""
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
        
        tables_to_check = ['client_dm', 'ClientDmBx']
        
        for table_name in tables_to_check:
            print(f"\n📊 {table_name} テーブル構造:")
            cursor.execute("""
                SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = ? 
                ORDER BY ORDINAL_POSITION
            """, table_name)
            
            results = cursor.fetchall()
            if results:
                for row in results:
                    col_name = row[0]
                    data_type = row[1]
                    max_length = row[2] if row[2] else ""
                    nullable = row[3]
                    
                    length_str = f"({max_length})" if max_length else ""
                    nullable_str = "NULL" if nullable == "YES" else "NOT NULL"
                    
                    print(f"  - {col_name}: {data_type}{length_str} {nullable_str}")
            else:
                print(f"  ❌ テーブル {table_name} が見つかりません")
        
        conn.close()
        print("\n🔚 データベース接続を閉じました")
        
    except Exception as e:
        print(f"❌ エラーが発生しました: {e}")

if __name__ == "__main__":
    check_table_schemas()
