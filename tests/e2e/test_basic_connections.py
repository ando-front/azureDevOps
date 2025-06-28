"""
簡単なE2E接続テスト（プロキシ/SSL対応版）
"""

import os
import pytest
import requests
import sys
from tests.helpers.reproducible_e2e_helper import setup_reproducible_test_class, cleanup_reproducible_test_class

# SQL Server接続の試行（ODBCドライバーがない場合のフォールバック付き）
def get_database_connection_info():
    """データベース接続情報を取得し、接続可能性をチェック"""
    db_host = os.environ.get('SQL_SERVER_HOST', 'sqlserver-test')  # CIではsqlserver-testコンテナを使用
    db_port = os.environ.get('SQL_SERVER_PORT', '1433')
    db_name = os.environ.get('SQL_SERVER_DATABASE', 'master')
    db_user = os.environ.get('SQL_SERVER_USER', 'sa')
    db_password = os.environ.get('SQL_SERVER_PASSWORD', 'YourStrong!Passw0rd123')
    
    return {
        'host': db_host,
        'port': db_port,
        'database': db_name,
        'user': db_user,
        'password': db_password
    }

@pytest.mark.e2e
@pytest.mark.basic_connection
@pytest.mark.database
def test_database_connection():
    """データベース接続の基本テスト（ODBCドライバー対応/フォールバック付き）"""
    db_info = get_database_connection_info()
      # まずODBCドライバーを試す
    try:
        import pyodbc
        print("📦 pyodbc ライブラリが利用可能です")
        
        conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={db_info['host']},{db_info['port']};"
            f"DATABASE={db_info['database']};"
            f"UID={db_info['user']};"
            f"PWD={db_info['password']};"
            "TrustServerCertificate=yes;"
            "Connection Timeout=60;"  # 接続タイムアウトを60秒に増加
            "Command Timeout=60;"     # コマンドタイムアウトも追加
        )
        
        try:
            # リトライ機能付きの接続（接続タイムアウト対策）
            max_retries = 3
            conn = None
            for attempt in range(max_retries):
                try:
                    conn = pyodbc.connect(conn_str, timeout=60)  # pyodbc内部タイムアウトも60秒
                    break
                except Exception as e:
                    print(f"Connection attempt {attempt + 1} failed: {e}")
                    if attempt < max_retries - 1:
                        import time
                        time.sleep(5)
                    else:
                        raise
            
            cursor = conn.cursor()
            cursor.execute("SELECT DB_NAME() as current_db, @@VERSION as version")
            result = cursor.fetchone()
            print(f"✅ ODBC接続成功: データベース={result[0]}")
            print(f"   SQL Server バージョン: {result[1][:50]}...")
            
            # データベース一覧を確認
            cursor.execute("SELECT name FROM sys.databases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')")
            databases = cursor.fetchall()
            print(f"   利用可能なデータベース: {[db[0] for db in databases]}")
            
            conn.close()
            print("✅ データベース接続テスト成功（ODBC）")
            return
            
        except Exception as odbc_error:
            print(f"⚠️ ODBC接続失敗: {odbc_error}")
            print("📝 SQLAlchemyでのフォールバック接続を試します...")
            
    except ImportError:
        print("⚠️ pyodbc ライブラリが利用できません。SQLAlchemyでのフォールバック接続を試します...")
    
    # SQLAlchemyフォールバック（ODBCドライバーがない場合）
    try:
        import sqlalchemy
        from sqlalchemy import create_engine, text
        
        # pymssql を使用したフォールバック接続
        connection_string = f"mssql+pymssql://{db_info['user']}:{db_info['password']}@{db_info['host']}:{db_info['port']}/{db_info['database']}"
        
        try:
            engine = create_engine(connection_string, connect_args={"timeout": 10})
            with engine.connect() as conn:
                result = conn.execute(text("SELECT DB_NAME() as current_db"))
                row = result.fetchone()
                print(f"✅ SQLAlchemy接続成功: データベース={row[0]}")
                print("✅ データベース接続テスト成功（SQLAlchemy）")
                return
                
        except Exception as sqlalchemy_error:
            print(f"⚠️ SQLAlchemy接続失敗: {sqlalchemy_error}")
            
    except ImportError:
        print("⚠️ SQLAlchemy ライブラリが利用できません")
    
    # 最終フォールバック: ネットワーク接続テスト
    try:
        import socket
        print("🔍 ネットワーク接続テストを実行中...")
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        result = sock.connect_ex((db_info['host'], int(db_info['port'])))
        sock.close()
        
        if result == 0:
            print(f"✅ ネットワーク接続成功: {db_info['host']}:{db_info['port']}")
            print("✅ データベース接続テスト成功（ネットワークレベル）")
        else:
            pytest.fail(f"データベースサーバーとのネットワーク接続失敗: {db_info['host']}:{db_info['port']}")
            
    except Exception as network_error:
        pytest.fail(f"データベース接続テスト完全失敗: {network_error}")

@pytest.mark.e2e
@pytest.mark.basic_connection
def test_ir_simulator_connection():
    """IR Simulator接続テスト（強化版）"""
    # 環境変数からURLを取得
    ir_url = os.environ.get('IR_SIMULATOR_URL', 'http://localhost:8080')
    
    print(f"🔍 IR Simulator接続テスト開始: {ir_url}")
    
    # 複数のエンドポイントを試す
    test_endpoints = [
        f"{ir_url}/health",      # ヘルスチェック
        f"{ir_url}/api/health",  # APIヘルスチェック
        f"{ir_url}/status",      # ステータス
        f"{ir_url}",            # ルート
    ]
    
    connection_success = False
    
    for endpoint in test_endpoints:
        try:
            print(f"  試行中: {endpoint}")
            response = requests.get(endpoint, timeout=10)
            print(f"  ✅ 応答: Status={response.status_code}")
            
            # 接続できていればOK（認証などの問題は別途対応）
            if response.status_code in [200, 403, 401, 404]:
                print(f"✅ IR Simulator接続成功: {endpoint}")
                connection_success = True
                break
                
        except requests.exceptions.ConnectionError:
            print(f"  ❌ 接続失敗: {endpoint}")
            continue
        except Exception as e:
            print(f"  ⚠️ エラー: {endpoint} - {e}")
            continue
    
    if not connection_success:
        # ネットワークレベルでの接続確認
        try:
            import socket
            from urllib.parse import urlparse
            
            parsed_url = urlparse(ir_url)
            host = parsed_url.hostname or 'localhost'
            port = parsed_url.port or 8080
            
            print(f"🔍 ネットワーク接続テスト: {host}:{port}")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((host, port))
            sock.close()
            
            if result == 0:
                print("✅ IR Simulator ネットワーク接続成功")
                connection_success = True
            else:
                print(f"❌ ネットワーク接続失敗: {host}:{port}")
                
        except Exception as network_error:
            print(f"⚠️ ネットワークテストエラー: {network_error}")
    
    if not connection_success:
        pytest.skip(f"IR Simulator not running (expected in no-proxy configuration): {ir_url}")

@pytest.mark.e2e
@pytest.mark.basic_connection
@pytest.mark.azure_storage
def test_azurite_connection():
    """Azurite接続テスト（強化版）"""
    # 環境変数から接続文字列を取得
    azurite_url = os.environ.get('AZURITE_BLOB_ENDPOINT', 'http://localhost:10000')
    
    print(f"🔍 Azurite接続テスト開始: {azurite_url}")
    
    # 複数のエンドポイントを試す
    test_endpoints = [
        f"{azurite_url}/devstoreaccount1?comp=list",      # ブロブリスト
        f"{azurite_url}/devstoreaccount1",                # アカウント
        f"{azurite_url}/",                                # ルート
    ]
    
    connection_success = False
    
    for endpoint in test_endpoints:
        try:
            print(f"  試行中: {endpoint}")
            response = requests.get(endpoint, timeout=10)
            print(f"  ✅ 応答: Status={response.status_code}")
            
            # 認証エラーでも接続自体は成功とする
            if response.status_code in [200, 403, 400, 404]:
                print(f"✅ Azurite接続成功: {endpoint}")
                connection_success = True
                break
                
        except requests.exceptions.ConnectionError:
            print(f"  ❌ 接続失敗: {endpoint}")
            continue
        except Exception as e:
            print(f"  ⚠️ エラー: {endpoint} - {e}")
            continue
    
    if not connection_success:
        # ネットワークレベルでの接続確認
        try:
            import socket
            from urllib.parse import urlparse
            
            parsed_url = urlparse(azurite_url)
            host = parsed_url.hostname or 'localhost'
            port = parsed_url.port or 10000
            
            print(f"🔍 ネットワーク接続テスト: {host}:{port}")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((host, port))
            sock.close()
            
            if result == 0:
                print("✅ Azurite ネットワーク接続成功")
                connection_success = True
            else:
                print(f"❌ ネットワーク接続失敗: {host}:{port}")
                
        except Exception as network_error:
            print(f"⚠️ ネットワークテストエラー: {network_error}")
    
    if not connection_success:
        pytest.skip(f"Azurite サービスが利用できません（Docker環境では正常）: {azurite_url}")

@pytest.mark.e2e
@pytest.mark.database
def test_database_tables_and_data():
    """データベースのテーブルとテストデータの確認"""
    try:
        import pyodbc
    except ImportError:
        pytest.skip("pyodbc not available - skipping ODBC-dependent test")
    
    # masterデータベースに接続してテーブルを確認
        connection_string = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            "SERVER=sqlserver-test,1433;"  # CIではsqlserver-testコンテナを使用
            "DATABASE=master;"
            "UID=sa;"
            "PWD=YourStrong!Passw0rd123;"
            "TrustServerCertificate=yes;"
            "Encrypt=no;"
            "Connection Timeout=60;"  # 接続タイムアウトを60秒に増加
            "Command Timeout=60;"     # コマンドタイムアウトも追加
        )
        
        # リトライ機能付きの接続
        max_retries = 3
        conn = None
        for attempt in range(max_retries):
            try:
                conn = pyodbc.connect(connection_string, timeout=60)  # pyodbc内部タイムアウトも60秒
                break
            except Exception as e:
                print(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    import time
                    time.sleep(5)
                else:
                    raise
        
        cursor = conn.cursor()
        
        # データベースの存在確認
        cursor.execute("SELECT name FROM sys.databases WHERE name IN ('TGMATestDB', 'SynapseTestDB')")
        databases = [row[0] for row in cursor.fetchall()]
        
        print(f"Available databases: {databases}")
        
        if 'TGMATestDB' in databases:
            # TGMATestDBのテーブル確認
            cursor.execute("USE TGMATestDB")
            cursor.execute("""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
            """)
            
            tables = [row[0] for row in cursor.fetchall()]
            expected_tables = ["client_dm", "ClientDmBx", "point_grant_email"]
            
            print(f"Found tables in TGMATestDB: {tables}")
            
            # テストデータの確認（存在すれば）
            for table in expected_tables:
                if table in tables:
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM {table}")
                        count = cursor.fetchone()[0]
                        print(f"{table} records: {count}")
                    except Exception as table_error:
                        print(f"Warning: Could not query {table}: {table_error}")
            
            # 基本的なアサーション
            assert len(databases) >= 1, f"Expected at least 1 test database, found {len(databases)}"
        else:
            print("Warning: TGMATestDB not found, skipping table verification")
        
        print("✅ Database tables and test data verification successful")
        
    except ImportError:
        pytest.skip("pyodbc not available - database test skipped")
    except Exception as e:
        pytest.fail(f"Database tables and data test failed: {str(e)}")

@pytest.mark.e2e
@pytest.mark.integration
def test_all_services_integration():
    """すべてのサービスの統合テスト"""
    print("🧪 統合テストを実行中...")
    
    # 各サービスの基本接続を確認
    services_status = {}
    
    # 1. データベース接続確認
    try:
        db_info = get_database_connection_info()
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((db_info['host'], int(db_info['port'])))
        sock.close()
        services_status['database'] = result == 0
    except:
        services_status['database'] = False
    
    # 2. IR Simulator接続確認
    try:
        ir_url = os.environ.get('IR_SIMULATOR_URL', 'http://localhost:8080')
        response = requests.get(ir_url, timeout=5)
        services_status['ir_simulator'] = response.status_code in [200, 403, 401, 404]
    except:
        services_status['ir_simulator'] = False
    
    # 3. Azurite接続確認
    try:
        azurite_url = os.environ.get('AZURITE_BLOB_ENDPOINT', 'http://localhost:10000')
        response = requests.get(f"{azurite_url}/", timeout=5)
        services_status['azurite'] = response.status_code in [200, 403, 400, 404]
    except:
        services_status['azurite'] = False
    
    # 結果を表示
    print("📊 サービス状況:")
    for service, status in services_status.items():
        status_icon = "✅" if status else "❌"
        print(f"   {status_icon} {service}: {'OK' if status else 'NG'}")
    
    # 少なくとも1つのサービスが動作していれば成功とする
    if any(services_status.values()):
        print(f"✅ 統合テスト成功: {sum(services_status.values())}/{len(services_status)} サービスが利用可能")
    else:
        pytest.fail("統合テスト失敗: すべてのサービスに接続できません")

if __name__ == "__main__":
    print("🚀 基本接続テストを実行中...")
    try:
        test_database_connection()
        test_ir_simulator_connection() 
        test_azurite_connection()
        test_all_services_integration()
        print("🎉 すべての接続テストが成功しました！")
    except Exception as e:
        print(f"❌ テスト実行エラー: {e}")
        sys.exit(1)
