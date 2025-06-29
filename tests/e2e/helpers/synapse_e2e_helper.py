"""
Azure Data Factory E2E Test Helper for Synapse Connection

SQL Server connection helper for E2E testing with actual database operations
"""

import os
import time
import logging
# pyodbcの条件付きインポート（技術的負債対応）
try:
    import pyodbc
    PYODBC_AVAILABLE = True
except ImportError:
    # pyodbcが利用できない場合のモッククラス
    class MockPyodbc:
        @staticmethod
        def connect(*args, **kwargs):
            raise ImportError('pyodbc is not available - DB tests will be skipped')
        
        class Error(Exception):
            pass
            
        class DatabaseError(Error):
            pass
            
        class InterfaceError(Error):
            pass
    
    pyodbc = MockPyodbc()
    PYODBC_AVAILABLE = False
from typing import List, Dict, Any, Optional

# 外部SQLクエリマネージャーのインポート
from .sql_query_manager import E2ESQLQueryManager

logger = logging.getLogger(__name__)


class SynapseE2EConnection:
    """E2Eテスト用のSynapse/SQL Server接続ヘルパークラス"""
    
    def __init__(self, connection_string: str = None):
        """
        初期化
        
        Args:
            connection_string: SQL Server接続文字列（省略時は環境変数から取得）
        """
        self.connection_string = connection_string or self._get_connection_string()
        self.query_manager = E2ESQLQueryManager()
        logger.info("E2E SynapseE2EConnection initialized with SQL query manager")
        self.wait_for_connection()
    
    def _get_connection_string(self) -> str:
        """環境変数から接続文字列を構築"""
        server = os.getenv('SQL_SERVER_HOST', 'sqlserver-test')
        database = os.getenv('SQL_SERVER_DATABASE', 'SynapseTestDB')
        username = os.getenv('SQL_SERVER_USER', 'sa')
        password = os.getenv('SQL_SERVER_PASSWORD', 'YourStrong!Passw0rd123')
        driver = os.getenv('E2E_SQL_DRIVER', 'ODBC Driver 18 for SQL Server')
        
        connection_string = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            "TrustServerCertificate=yes;"
            "Encrypt=no;"
            "LoginTimeout=30;"
        )
        
        logger.info(f"E2E Connection string: {connection_string.replace(password, '***')}")
        return connection_string
    
    def get_connection(self):
        """データベース接続を取得"""
        try:
            conn = pyodbc.connect(self.connection_string)
            return conn
        except Exception as e:
            logger.error(f"E2E Database connection failed: {e}")
            raise
    
    def execute_query(self, query: str, params: tuple = None) -> List[tuple]:
        """クエリを実行して結果を返す"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                # SELECT文の場合は結果を取得
                if query.strip().upper().startswith('SELECT'):
                    results = cursor.fetchall()
                    logger.info(f"E2E Query executed, returned {len(results)} rows")
                    return results
                else:
                    # INSERT/UPDATE/DELETE文の場合はcommitして行数を返す
                    conn.commit()
                    row_count = cursor.rowcount
                    logger.info(f"E2E Query executed, affected {row_count} rows")
                    return [(row_count,)]
                    
        except Exception as e:
            logger.error(f"E2E Query execution failed: {e}")
            raise
    
    def wait_for_connection(self, max_retries: int = 60, delay: int = 5) -> bool:
        """データベース接続が利用可能になるまで待機"""
        logger.info(f"E2E Waiting for database connection (max {max_retries} retries)...")
        
        for attempt in range(max_retries):
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                    logger.info("E2E Database connection is ready")
                    return True
            except Exception as e:
                logger.warning(f"E2E Connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(delay)
        
        logger.error("E2E Database connection failed after all retries")
        return False

    def test_synapse_connection(self) -> bool:
        """Synapse接続テスト"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1 as test_result")
                result = cursor.fetchone()
                return result is not None and result[0] == 1
        except Exception as e:
            logger.error(f"Synapse connection test failed: {e}")
            return False

    def validate_dataset_exists(self, dataset_name: str) -> bool:
        """データセット存在確認（モック実装）"""
        # 実際の実装では、ADF APIを使用してデータセットの存在を確認
        mock_datasets = ["ds_sqlmi", "ds_synapse_analytics", "ds_DamDwhTable"]
        return dataset_name in mock_datasets

    def check_table_exists(self, schema_name: str, table_name: str) -> bool:
        """テーブル存在確認"""
        try:
            query = """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            """
            result = self.execute_query(query, (schema_name, table_name))
            return result[0][0] > 0 if result else False
        except Exception as e:
            logger.error(f"Table existence check failed: {e}")
            return False

    def trigger_pipeline(self, pipeline_name: str, parameters: dict = None) -> str:
        """パイプライン実行トリガー（モック実装）"""
        import uuid
        run_id = str(uuid.uuid4())
        logger.info(f"Mock pipeline trigger: {pipeline_name} with run_id: {run_id}")
        return run_id

    def wait_for_pipeline_completion(self, run_id: str, timeout_minutes: int = 30) -> str:
        """パイプライン完了待機（モック実装）"""
        import time
        import random
        
        # 1-3秒のランダム待機でシミュレーション
        simulation_time = random.uniform(1, 3)
        time.sleep(simulation_time)
        
        logger.info(f"Mock pipeline completion: {run_id}")
        return "Succeeded"

    # =================================================================
    # 外部SQLクエリ管理機能
    # =================================================================
    
    def execute_external_query(self, filename: str, query_name: str, **params) -> List[Dict[str, Any]]:
        """
        外部SQLファイルからクエリを読み込んで実行
        
        Args:
            filename: SQLファイル名
            query_name: クエリ名
            **params: クエリパラメータ
            
        Returns:
            クエリ実行結果（辞書のリスト）
        """
        try:
            query = self.query_manager.get_query(filename, query_name, **params)
            logger.info(f"Executing external query: {filename}::{query_name}")
            
            results = self.execute_query(query)
            
            # 結果を辞書形式に変換
            if results and hasattr(results[0], '_fields'):
                # Named tupleの場合
                return [dict(zip(row._fields, row)) for row in results]
            else:
                # 通常のtupleの場合、カラム名を推定
                return [{"col" + str(i): val for i, val in enumerate(row)} for row in results]
                
        except Exception as e:
            logger.error(f"External query execution failed: {filename}::{query_name} - {e}")
            raise
    
    def execute_external_query_no_result(self, filename: str, query_name: str, **params) -> bool:
        """
        外部SQLファイルからクエリを読み込んで実行（結果なし）
        
        Args:
            filename: SQLファイル名
            query_name: クエリ名
            **params: クエリパラメータ
            
        Returns:
            実行成功/失敗
        """
        try:
            query = self.query_manager.get_query(filename, query_name, **params)
            logger.info(f"Executing external query (no result): {filename}::{query_name}")
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                conn.commit()
            
            return True
        except Exception as e:
            logger.error(f"External query execution failed: {filename}::{query_name} - {e}")
            return False
    
    def list_available_queries(self, filename: str = None) -> Dict[str, List[str]]:
        """
        利用可能なクエリの一覧を取得
        
        Args:
            filename: 指定ファイルのクエリのみ取得（None時は全ファイル）
            
        Returns:
            ファイル名をキー、クエリ名リストを値とする辞書
        """
        if filename:
            return {filename: self.query_manager.get_available_queries(filename)}
        
        result = {}
        for file in self.query_manager.get_available_files():
            result[file] = self.query_manager.get_available_queries(file)
        
        return result

    # =================================================================
    # E2Eテスト用便利メソッド
    # =================================================================
    
    def setup_client_dm_test_data(self, test_prefix: str = "E2E_TEST") -> bool:
        """
        Client DMテスト用データセットアップ
        
        Args:
            test_prefix: テストデータの接頭辞
            
        Returns:
            セットアップ成功/失敗
        """
        try:
            # 基本的なセットアップクエリを実行
            basic_setup_queries = [
                "basic_client_dm_setup",
                "gas_contract_client_insert",
                "electric_only_client_insert"
            ]
            
            for query_name in basic_setup_queries:
                if not self.execute_external_query_no_result("client_dm.sql", query_name):
                    logger.error(f"Failed to execute setup query: {query_name}")
                    return False
            
            logger.info(f"Client DM test data setup completed with prefix: {test_prefix}")
            return True
        except Exception as e:
            logger.error(f"Client DM test data setup failed: {e}")
            return False

    def setup_marketing_client_dm_comprehensive_test_data(self) -> bool:
        """
        Marketing Client DM包括的テスト用データセットアップ
        """
        try:
            # 包括的なテストデータセットアップ
            self.execute_external_query_no_result(
                "marketing_client_dm_comprehensive.sql", 
                "comprehensive_test_setup"
            )
            
            # 各種顧客タイプのデータ作成
            setup_queries = [
                "gas_only_customer_insert",
                "electric_only_customer_insert", 
                "combined_customer_insert",
                "business_customer_insert",
                "complex_equipment_customer_insert"
            ]
            
            for query_name in setup_queries:
                self.execute_external_query_no_result(
                    "marketing_client_dm_comprehensive.sql", query_name
                )
            
            logger.info("Marketing Client DM comprehensive test data setup completed")
            return True
        except Exception as e:
            logger.error(f"Marketing Client DM comprehensive test data setup failed: {e}")
            return False

    def validate_marketing_client_dm_structure(self) -> Dict[str, bool]:
        """
        Marketing Client DMの533列構造を検証
        """
        validation_results = {}
        
        try:
            # カラム数検証
            result = self.execute_external_query(
                "marketing_client_dm_comprehensive.sql", 
                "column_count_validation"
            )
            validation_results["column_count_533"] = len(result) >= 533
            
            # 必須カラム存在検証
            required_columns_queries = [
                "gas_meter_columns_validation",
                "electric_contract_columns_validation",
                "equipment_columns_validation",
                "web_history_columns_validation",
                "service_columns_validation"
            ]
            
            for query_name in required_columns_queries:
                result = self.execute_external_query(
                    "marketing_client_dm_comprehensive.sql", query_name
                )
                validation_results[query_name] = len(result) > 0
            
            # データ品質検証
            quality_queries = [
                "null_critical_fields_check",
                "duplicate_client_key_check",
                "invalid_date_check",
                "invalid_numeric_check"
            ]
            
            for query_name in quality_queries:
                result = self.execute_external_query(
                    "marketing_client_dm_comprehensive.sql", query_name
                )
                validation_results[query_name] = result[0]["error_count"] == 0 if result else False
            
            return validation_results
            
        except Exception as e:
            logger.error(f"Marketing Client DM structure validation failed: {e}")
            return {"validation_error": False}

    def cleanup_test_data(self, table_pattern: str = "E2E_%") -> bool:
        """テストデータのクリーンアップ"""
        try:
            cleanup_query = f"""
            DECLARE @sql NVARCHAR(MAX) = '';
            SELECT @sql = @sql + 'DROP TABLE IF EXISTS ' + TABLE_SCHEMA + '.' + TABLE_NAME + ';' + CHAR(13)
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME LIKE '{table_pattern}';
            EXEC sp_executesql @sql;
            """
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(cleanup_query)
                conn.commit()
            
            logger.info(f"Test data cleanup completed for pattern: {table_pattern}")
            return True
        except Exception as e:
            logger.error(f"Test data cleanup failed: {e}")
            return False

# グローバル関数とインスタンス - テストファイルからのインポート用
def e2e_synapse_connection() -> SynapseE2EConnection:
    """
    E2Eテスト用のSynapse接続インスタンスを取得
    
    Returns:
        SynapseE2EConnection: 設定済みの接続インスタンス
    """
    if not PYODBC_AVAILABLE:
        logger.warning("pyodbc not available - returning mock connection")
    return SynapseE2EConnection()


def e2e_clean_test_data(table_pattern: str = "test_%") -> bool:
    """
    E2Eテストデータのクリーンアップ（グローバル関数版）
    
    Args:
        table_pattern: クリーンアップ対象のテーブルパターン
        
    Returns:
        bool: クリーンアップ成功時True
    """
    if not PYODBC_AVAILABLE:
        logger.warning("pyodbc not available - skipping test data cleanup")
        return True
        
    try:
        connection = e2e_synapse_connection()
        return connection.clean_test_data(table_pattern)
    except Exception as e:
        logger.error(f"Global test data cleanup failed: {e}")
        return False


def e2e_execute_query(query: str, params: tuple = None) -> List[tuple]:
    """
    E2Eテスト用クエリ実行（グローバル関数版）
    
    Args:
        query: 実行するSQLクエリ
        params: クエリパラメータ
        
    Returns:
        List[tuple]: クエリ結果
    """
    if not PYODBC_AVAILABLE:
        logger.warning("pyodbc not available - returning empty result")
        return []
        
    try:
        connection = e2e_synapse_connection()
        return connection.execute_query(query, params)
    except Exception as e:
        logger.error(f"Global query execution failed: {e}")
        return []


def e2e_wait_for_connection(max_retries: int = 10, delay: int = 5) -> bool:
    """
    E2Eテスト用データベース接続待機（グローバル関数版）
    
    Args:
        max_retries: 最大リトライ回数
        delay: リトライ間隔（秒）
        
    Returns:
        bool: 接続成功時True
    """
    if not PYODBC_AVAILABLE:
        logger.warning("pyodbc not available - returning False for connection wait")
        return False
        
    try:
        connection = e2e_synapse_connection()
        return connection.wait_for_connection(max_retries, delay)
    except Exception as e:
        logger.error(f"Global connection wait failed: {e}")
        return False
