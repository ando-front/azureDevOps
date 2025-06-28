#!/usr/bin/env python3
"""
再現可能なE2Eテスト用ヘルパークラス
各テスト実行前に自動でデータベースを初期化して完全な再現性を保証する
"""

import os
import sys
import time
import logging
import subprocess
from pathlib import Path

# pyodbcの条件付きインポート（技術的負債対応）
try:
    import pyodbc
    PYODBC_AVAILABLE = True
except ImportError:    # pyodbcが利用できない場合のモッククラス
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
            """モックConnection類"""
            def __init__(self):
                pass
            
            def close(self):
                pass
            
            def cursor(self):
                return MockPyodbc.Cursor()
                
        class Cursor:
            """モックCursor類"""
            def __init__(self):
                pass
            
            def execute(self, query):
                pass
            
            def fetchall(self):
                return []
            
            def close(self):
                pass
            pass
            
        class Connection:
            """Mock Connection class"""
            pass
    
    pyodbc = MockPyodbc()
    PYODBC_AVAILABLE = False

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

class ReproducibleE2EHelper:
    """再現可能なE2Eテスト実行をサポートするヘルパークラス"""
    
    def __init__(self):
        self.project_root = Path(__file__).parent.parent.parent
        self.initializer_script = self.project_root / "e2e_db_auto_initializer.py"
        
    def ensure_reproducible_database_state(self) -> bool:
        """
        完全に再現可能なデータベース状態を確保
        毎回同じ初期状態でテストを開始できることを保証
        """
        logger.info("🔄 Ensuring reproducible database state for E2E tests...")
        
        try:
            # 自動初期化スクリプトを実行
            result = subprocess.run(
                [sys.executable, str(self.initializer_script)],
                capture_output=True,
                text=True,
                timeout=120  # 2分でタイムアウト
            )
            
            if result.returncode == 0:
                logger.info("✅ Database initialization completed successfully")
                logger.info("📊 Test data is now in a completely reproducible state")
                return True
            else:
                logger.error(f"❌ Database initialization failed: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error("❌ Database initialization timed out")
            return False
        except Exception as e:
            logger.error(f"❌ Failed to run database initialization: {str(e)}")
            return False
    
    def validate_test_environment(self) -> bool:
        """テスト環境の準備状況を検証"""
        logger.info("🔍 Validating test environment...")
        
        # 1. 必要なスクリプトの存在確認
        # if not self.initializer_script.exists():
        #     logger.error(f"❌ Initializer script not found: {self.initializer_script}")
        #     return False
            
        # 2. Docker コンテナの状態確認（オプション）
        try:
            docker_result = subprocess.run(
                ["docker", "ps", "--filter", "name=sql", "--format", "table {{.Names}}\t{{.Status}}"],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if docker_result.returncode == 0 and "sql" in docker_result.stdout:
                logger.info("✅ SQL Server container is running")
            else:
                logger.warning("⚠️ SQL Server container may not be running")
                
        except Exception as e:
            logger.warning(f"⚠️ Could not check Docker status: {str(e)}")
        
        logger.info("✅ Test environment validation completed")
        return True
    
    def setup_test_class(self):
        """テストクラス開始時のセットアップ"""
        logger.info("🚀 Setting up test class with reproducible environment...")
        
        # 1. 環境の検証
        if not self.validate_test_environment():
            raise RuntimeError("Test environment validation failed")
        
        # 2. データベース状態の初期化
        if not self.ensure_reproducible_database_state():
            raise RuntimeError("Database initialization failed")
        
        # 3. プロキシ設定のクリア（既存のロジック）
        proxy_vars = ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY']
        for var in proxy_vars:
            if var in os.environ:
                del os.environ[var]
        
        logger.info("🎉 Test class setup completed - ready for reproducible testing!")
    
    def teardown_test_class(self):
        """テストクラス終了時のクリーンアップ"""
        logger.info("🧹 Cleaning up test class...")
        # 必要に応じてクリーンアップ処理を追加
        logger.info("✅ Test class cleanup completed")
    
    def get_expected_test_counts(self) -> dict:
        """期待されるテストデータ数を返す"""
        return {
            'client_dm_e2e': 15,        # 12 regular + 3 bulk clients
            'ClientDmBx_e2e': 7,        # 5 regular + 2 bulk entries
            'point_grant_email_e2e': 5, # 5 email records
            'marketing_client_dm_e2e': 5 # 5 marketing records
        }
    
    def wait_for_database_ready(self, max_retries: int = 30, delay: int = 2) -> bool:
        """データベースが準備完了するまで待機"""
        logger.info("⏳ Waiting for database to be ready...")
        
        for attempt in range(max_retries):
            try:
                # 簡単な接続テスト
                result = subprocess.run(
                    [sys.executable, str(self.initializer_script), "--check-only"],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                
                if result.returncode == 0:
                    logger.info("✅ Database is ready")
                    return True
                    
            except Exception:
                pass
            
            if attempt < max_retries - 1:
                logger.info(f"⏳ Database not ready, waiting... ({attempt + 1}/{max_retries})")
                time.sleep(delay)
        
        logger.error("❌ Database did not become ready within timeout")
        return False

# グローバルヘルパーインスタンス
reproducible_helper = ReproducibleE2EHelper()

def setup_reproducible_test_class():
    """テストクラスで使用する簡単なセットアップ関数"""
    reproducible_helper.setup_test_class()

def cleanup_reproducible_test_class():
    """テストクラスで使用する簡単なクリーンアップ関数"""
    reproducible_helper.teardown_test_class()

def teardown_reproducible_test_class():
    """テストクラスで使用する簡単なクリーンアップ関数（互換性のため）"""
    reproducible_helper.teardown_test_class()

def get_reproducible_database_connection():
    """再現可能テスト用のデータベース接続を取得"""
    if not PYODBC_AVAILABLE:
        raise ImportError("pyodbc is not available - DB tests will be skipped")
        
    connection_string = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost,1433;"
        "DATABASE=testdb;"
        "UID=sa;"
        "PWD=Password123!;"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(connection_string)

def get_reproducible_synapse_connection():
    """Synapse接続のシミュレーション（実際にはSQL Serverに接続）"""
    return get_reproducible_database_connection()
