#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL Server接続テストスクリプト
"""

import pyodbc
import socket
import sys

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
