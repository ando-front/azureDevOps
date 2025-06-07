#!/usr/bin/env python3
"""
SQL Server接続テスト用スクリプト
"""
import pyodbc
import sys
import time

def test_sql_connection():
    """SQL Server接続をテストする"""
    connection_string = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=localhost,1433;"
        "UID=sa;"
        "PWD=YourStrong!Passw0rd123;"
        "TrustServerCertificate=yes;"
        "Encrypt=yes;"
    )
    
    try:
        print("SQL Server接続を試行中...")
        conn = pyodbc.connect(connection_string, timeout=30)
        print("✅ SQL Server接続成功！")
        
        cursor = conn.cursor()
        
        # バージョン確認
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()[0]
        print(f"SQL Server Version: {version[:50]}...")
        
        # データベース一覧確認
        cursor.execute("SELECT name FROM sys.databases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')")
        databases = cursor.fetchall()
        print(f"既存のユーザーデータベース: {[db[0] for db in databases]}")
        
        # テスト用データベース作成
        try:
            cursor.execute("CREATE DATABASE TGMATestDB COLLATE Japanese_CI_AS")
            print("✅ TGMATestDB作成成功")
        except Exception as e:
            if "already exists" in str(e):
                print("📋 TGMATestDBは既に存在します")
            else:
                print(f"⚠️ TGMATestDB作成エラー: {e}")
        
        try:
            cursor.execute("CREATE DATABASE SynapseTestDB COLLATE Japanese_CI_AS")
            print("✅ SynapseTestDB作成成功")
        except Exception as e:
            if "already exists" in str(e):
                print("📋 SynapseTestDBは既に存在します")
            else:
                print(f"⚠️ SynapseTestDB作成エラー: {e}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"❌ SQL Server接続エラー: {e}")
        return False

def create_test_tables():
    """テスト用テーブルを作成する"""
    connection_string = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=localhost,1433;"
        "DATABASE=TGMATestDB;"
        "UID=sa;"
        "PWD=YourStrong!Passw0rd123;"
        "TrustServerCertificate=yes;"
        "Encrypt=yes;"
    )
    
    try:
        print("\nTGMATestDBでテーブル作成中...")
        conn = pyodbc.connect(connection_string, timeout=30)
        cursor = conn.cursor()
        
        # E2Eテスト用の基本テーブル作成
        tables_sql = [
            """
            CREATE TABLE client_dm (
                client_id NVARCHAR(50) PRIMARY KEY,
                client_name NVARCHAR(100),
                email NVARCHAR(100),
                created_date DATETIME DEFAULT GETDATE()
            )
            """,
            """
            CREATE TABLE ClientDmBx (
                client_id NVARCHAR(50) PRIMARY KEY,
                client_name NVARCHAR(100),
                status NVARCHAR(20),
                created_date DATETIME DEFAULT GETDATE()
            )
            """,
            """
            CREATE TABLE point_grant_email (
                email_id NVARCHAR(50) PRIMARY KEY,
                client_id NVARCHAR(50),
                point_amount INT,
                email_sent_date DATETIME DEFAULT GETDATE()
            )
            """,
            """
            CREATE TABLE e2e_test_execution_log (
                log_id INT IDENTITY(1,1) PRIMARY KEY,
                test_name NVARCHAR(200),
                execution_time DATETIME DEFAULT GETDATE(),
                status NVARCHAR(20),
                result_details NVARCHAR(MAX)
            )
            """
        ]
        
        for sql in tables_sql:
            try:
                cursor.execute(sql)
                table_name = sql.split('CREATE TABLE')[1].split('(')[0].strip()
                print(f"✅ テーブル {table_name} 作成成功")
            except Exception as e:
                if "already exists" in str(e):
                    table_name = sql.split('CREATE TABLE')[1].split('(')[0].strip()
                    print(f"📋 テーブル {table_name} は既に存在します")
                else:
                    print(f"⚠️ テーブル作成エラー: {e}")
        
        # テストデータ挿入
        test_data = [
            "INSERT INTO client_dm (client_id, client_name, email) VALUES ('E2E_001', 'Test Client 1', 'test1@example.com')",
            "INSERT INTO ClientDmBx (client_id, client_name, status) VALUES ('E2E_001', 'Test Client 1', 'active')",
            "INSERT INTO point_grant_email (email_id, client_id, point_amount) VALUES ('EMAIL_001', 'E2E_001', 100)"
        ]
        
        for data_sql in test_data:
            try:
                cursor.execute(data_sql)
                print(f"✅ テストデータ挿入成功")
            except Exception as e:
                if "duplicate" in str(e).lower() or "violation" in str(e).lower():
                    print(f"📋 テストデータは既に存在します")
                else:
                    print(f"⚠️ テストデータ挿入エラー: {e}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("✅ テーブル作成とデータ挿入完了")
        return True
        
    except Exception as e:
        print(f"❌ テーブル作成エラー: {e}")
        return False

if __name__ == "__main__":
    print("=== SQL Server E2E環境テスト ===")
    
    # 接続テスト
    if test_sql_connection():
        # テーブル作成
        create_test_tables()
        print("\n=== E2E環境準備完了 ===")
    else:
        print("\n=== E2E環境準備失敗 ===")
        sys.exit(1)
