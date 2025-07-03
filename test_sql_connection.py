#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL Server接続テストスクリプト
"""

import os
import pyodbc
import socket
import sys
from dotenv import load_dotenv

# .envファイルから環境変数を読み込む
load_dotenv()

# 接続情報を環境変数から取得
server = os.getenv("SQL_SERVER")
database = os.getenv("SQL_DATABASE")
username = os.getenv("SQL_USERNAME")
password = os.getenv("SQL_PASSWORD")
driver = os.getenv("ODBC_DRIVER", "{ODBC Driver 18 for SQL Server}") # デフォルト値を設定

# 接続文字列を構築 (Azure SQL DB向けに暗号化を有効化)
conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"

def test_network_connection():
    """ネットワーク接続テスト"""
    print('🔍 SQL Server接続テストを開始...')
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex(('localhost', 1433))
        sock.close()
        if result == 0:
            print('✅ ポート1433への接続: 成功')
            return True
        else:
            print(f'❌ ポート1433への接続: 失敗 (エラーコード: {result})')
            return False
    except Exception as e:
        print(f'❌ ネットワークテスト失敗: {e}')
        return False

def test_sql_connection():
    """SQL Server接続テスト"""
    
    # 複数の接続文字列パターンを試す
    connection_patterns = [
        'DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost,1433;DATABASE=TGMATestDB;UID=sa;PWD=YourStrong!Passw0rd123;TrustServerCertificate=yes;Encrypt=no;',
        'DRIVER={ODBC Driver 18 for SQL Server};SERVER=127.0.0.1,1433;DATABASE=TGMATestDB;UID=sa;PWD=YourStrong!Passw0rd123;TrustServerCertificate=yes;Encrypt=no;',
        'DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost;DATABASE=TGMATestDB;UID=sa;PWD=YourStrong!Passw0rd123;TrustServerCertificate=yes;Encrypt=no;',
        'DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost,1433;DATABASE=master;UID=sa;PWD=YourStrong!Passw0rd123;TrustServerCertificate=yes;Encrypt=no;'
    ]
    
    for i, conn_str in enumerate(connection_patterns, 1):
        try:
            print(f'🔄 接続パターン {i} を試行中...')
            print('📝 接続文字列:', conn_str.replace('PWD=YourStrong!Passw0rd123', 'PWD=***'))
            
            conn = pyodbc.connect(conn_str, timeout=10)
            cursor = conn.cursor()
            cursor.execute('SELECT 1 as test')
            result = cursor.fetchone()
            print(f'✅ SQL Server接続成功: {result}')
            
            # データベース切り替えとテーブル確認
            if 'DATABASE=master' in conn_str:
                cursor.execute('USE TGMATestDB')
                print('📂 TGMATestDBに切り替え')
            
            # テーブル存在確認
            cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'client_dm'")
            table_count = cursor.fetchone()[0]
            print(f'📊 client_dmテーブル存在確認: {table_count}個のテーブル')
            
            if table_count > 0:
                # created_dateカラム確認
                cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'client_dm' AND COLUMN_NAME = 'created_date'")
                column_result = cursor.fetchone()
                if column_result:
                    print(f'✅ created_dateカラム存在確認: {column_result[0]}')
                else:
                    print('❌ created_dateカラムが見つかりません')
            
            cursor.close()
            conn.close()
            return True
            
        except Exception as e:
            print(f'❌ パターン {i} 接続失敗: {e}')
            continue
    
    print('❌ すべての接続パターンが失敗しました')
    return False

def main():
    """メイン実行"""
    print('🔧 SQL Server接続テストスクリプト開始')
    print('📦 利用可能なODBCドライバ:')
    try:
        drivers = [driver for driver in pyodbc.drivers()]
        for driver in drivers:
            print(f'  - {driver}')
    except Exception as e:
        print(f'  エラー: {e}')
    
    if not test_network_connection():
        print('❌ ネットワークテスト失敗のため終了')
        sys.exit(1)
    
    if not test_sql_connection():
        print('❌ SQL接続テスト失敗のため終了')
        sys.exit(1)
    
    print('🎉 すべてのテストが成功しました！')

if __name__ == '__main__':
    main()

print("以下の情報でデータベースに接続します...")
print(f"サーバー: {server}")
print(f"データベース: {database}")
print(f"ユーザー名: {username}")

try:
    # データベースに接続
    with pyodbc.connect(conn_str, timeout=30) as conn:
        print("\n\033[92m接続に成功しました！\033[0m")
        with conn.cursor() as cursor:
            cursor.execute("SELECT @@VERSION")
            row = cursor.fetchone()
            print("SQL Serverのバージョン:")
            print(row[0])

except pyodbc.Error as ex:
    sqlstate = ex.args[0]
    print(f"\n\033[91m接続に失敗しました。エラーコード: {sqlstate}\033[0m")
    print("エラー詳細:")
    print(ex)
    print("\n--- トラブルシューティング ---")
    print("1. .envファイル内のSQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORDが正しいか確認してください。")
    print("2. Azureポータルで、お使いのPCのIPアドレスがSQL Serverのファイアウォール規則で許可されているか確認してください。")
    print("3. 企業ネットワークの場合、ポート1433がプロキシやファイアウォールでブロックされていないか確認してください。")


except Exception as e:
    print("\n\033[91m予期せぬエラーが発生しました。\033[0m")
    print(e)
