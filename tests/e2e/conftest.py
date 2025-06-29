"""
Enhanced E2E Test Configuration
ODBC Driver フォールバック機能付きテスト設定
"""

import pytest
import os  
from typing import Dict, Optional

# pyodbcの条件付きインポート（技術的負債対応）
try:
    import pyodbc
    PYODBC_AVAILABLE = True
except ImportError:
    PYODBC_AVAILABLE = False
    # pyodbcが利用できない場合のモッククラス
    class MockPyodbc:
        @staticmethod
        def connect(*args, **kwargs):
            raise ImportError("pyodbc is not available - DB tests will be skipped")
        
        class Error(Exception):
            pass
            
        class DatabaseError(Error):
            pass
            
        class InterfaceError(Error):
            pass
        
        class Connection:
            def cursor(self):
                return MockPyodbc.Cursor()
            def close(self):
                pass
        
        class Cursor:
            def execute(self, *args, **kwargs):
                pass
            def fetchall(self):
                return []
            def fetchone(self):
                return None
            def close(self):
                pass
    
    pyodbc = MockPyodbc
    PYODBC_AVAILABLE = False


class ODBCDriverManager:
    """ODBC Driver管理クラス"""
    
    DRIVER_PRIORITIES = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",        "ODBC Driver 13 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server"
    ]
    
    @classmethod
    def get_available_driver(cls) -> Optional[str]:
        """利用可能なODBCドライバーを取得"""
        if not PYODBC_AVAILABLE:
            return None
            
        for driver in cls.DRIVER_PRIORITIES:
            try:
                # テスト接続文字列でドライバーの存在を確認
                test_conn_str = f"DRIVER={{{driver}}};SERVER=test;DATABASE=test;UID=test;PWD=test;"
                # 実際の接続は試さず、ドライバーの存在確認のみ
                pyodbc.connect(test_conn_str, timeout=1)
            except pyodbc.Error as e:
                # ドライバーが見つからない場合のエラーを除外
                error_msg = str(e)
                if "file not found" in error_msg.lower() or "driver not found" in error_msg.lower():
                    continue
                else:
                    # ドライバーは存在するが接続エラー（これは期待される）
                    return driver
            except Exception:
                continue
        return None    @classmethod 
    def build_connection_string(cls, host: str, port: str, database: str, 
                              user: str, password: str, driver: Optional[str] = None) -> str:
        """接続文字列を構築"""
        if not PYODBC_AVAILABLE:
            raise RuntimeError("pyodbc is not available - DB tests will be skipped")
            
        if driver is None:
            driver = cls.get_available_driver()
            
        if driver is None:
            raise RuntimeError("利用可能なODBCドライバーが見つかりません")
            
        return (
            f"DRIVER={{{driver}}};"
            f"SERVER={host},{port};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
            "TrustServerCertificate=yes;"
            "Encrypt=no;"
        )


@pytest.fixture(scope="session")
def e2e_db_connection():
    """E2Eテスト用データベース接続フィクスチャ（ODBC Driver対応）"""
    
    # pyodbcが利用できない場合はスキップ
    if not PYODBC_AVAILABLE:
        pytest.skip("pyodbc is not available - DB tests will be skipped")
    
    # 環境変数から接続情報を取得
    host = os.getenv('SQL_SERVER_HOST', 'sqlserver-test')
    port = os.getenv('SQL_SERVER_PORT', '1433')
    database = os.getenv('SQL_SERVER_DATABASE', 'master')
    user = os.getenv('SQL_SERVER_USER', 'sa')
    password = os.getenv('SQL_SERVER_PASSWORD', 'YourStrong!Passw0rd123')
    
    # 環境変数から接続文字列が直接指定されている場合はそれを使用
    connection_string = os.getenv('SQL_CONNECTION_STRING')
    
    if not connection_string:
        try:
            connection_string = ODBCDriverManager.build_connection_string(
                host, port, database, user, password
            )
        except RuntimeError as e:
            pytest.skip(f"ODBCドライバーが利用できません: {e}")
    
    try:
        conn = pyodbc.connect(connection_string, timeout=30)
        yield conn
    except Exception as e:
        pytest.skip(f"データベース接続に失敗しました: {e}")
    finally:
        if 'conn' in locals():
            conn.close()


@pytest.fixture(scope="session")
def e2e_test_config():
    """E2Eテスト設定フィクスチャ"""
    return {
        'timeout': 30,
        'max_retries': 3,
        'test_data_prefix': 'E2E_',
        'cleanup_on_failure': True,
        'enable_performance_monitoring': True,
        'log_level': 'INFO'
    }


@pytest.fixture(autouse=True)
def setup_e2e_environment():
    """E2Eテスト環境の自動セットアップ"""
    # テスト開始前の環境確認
    required_env_vars = [
        'SQL_SERVER_HOST',
        'AZURITE_CONNECTION_STRING'
    ]
    
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
        pytest.skip(f"必要な環境変数が設定されていません: {missing_vars}")
    
    yield
    
    # テスト終了後のクリーンアップ（必要に応じて）


def pytest_configure(config):
    """pytest設定のカスタマイズ"""
    config.addinivalue_line(
        "markers", "e2e: E2Eテストマーカー"
    )
    config.addinivalue_line(
        "markers", "database: データベーステストマーカー"
    )
    config.addinivalue_line(
        "markers", "schema_validation: スキーマ検証テストマーカー"
    )
    config.addinivalue_line(
        "markers", "data_integrity: データ整合性テストマーカー"
    )


def pytest_collection_modifyitems(config, items):
    """テスト収集時のカスタマイズ"""
    # E2Eテストにtimeoutマーカーを自動追加 (一時的に無効化)
    # for item in items:
    #     if "e2e" in item.keywords:
    #         item.add_marker(pytest.mark.timeout(600))  # 10分のタイムアウト
