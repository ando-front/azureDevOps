"""
Azure Data Factory E2E Test Helper for Synapse Connection

SQL Server connection helper for E2E testing with actual database operations
"""

import os
import time
import logging
import pyodbc
from typing import List, Dict, Any, Optional

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
        logger.info("E2E SynapseE2EConnection initialized")
    
    def _get_connection_string(self) -> str:
        """環境変数から接続文字列を構築"""
        server = os.getenv('E2E_SQL_SERVER', 'localhost,1433')
        database = os.getenv('E2E_SQL_DATABASE', 'TGMATestDB')
        username = os.getenv('E2E_SQL_USERNAME', 'sa')
        password = os.getenv('E2E_SQL_PASSWORD', 'YourStrong!Passw0rd123')
        driver = os.getenv('E2E_SQL_DRIVER', 'ODBC Driver 18 for SQL Server')
        
        connection_string = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            "TrustServerCertificate=yes;"
            "Encrypt=yes;"
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
    
    def wait_for_connection(self, max_retries: int = 10, delay: int = 5) -> bool:
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
    
    def setup_test_data(self, table_name: str, data: List[dict]) -> bool:
        """テストデータのセットアップ（テーブル動的作成含む）"""
        logger.info(f"E2E Setting up test data for table: {table_name}")
        
        try:
            if not data:
                logger.warning("E2E No test data provided")
                return True
            
            # データの列名と型を推定
            columns = list(data[0].keys())
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # テーブルが存在しない場合は作成
                try:
                    cursor.execute(f"SELECT TOP 1 * FROM {table_name}")
                    logger.info(f"E2E Table {table_name} already exists")
                except:
                    # テーブル作成
                    column_definitions = []
                    for col in columns:
                        # サンプルデータから型を推定
                        sample_value = data[0][col]
                        if isinstance(sample_value, int):
                            col_type = "INT"
                        elif isinstance(sample_value, float):
                            col_type = "FLOAT"
                        elif isinstance(sample_value, bool):
                            col_type = "BIT"
                        else:
                            col_type = "NVARCHAR(255)"
                        column_definitions.append(f"{col} {col_type}")
                    
                    create_query = f"CREATE TABLE {table_name} ({', '.join(column_definitions)})"
                    cursor.execute(create_query)
                    logger.info(f"E2E Created table {table_name}")
                
                # データ挿入
                column_names = ', '.join(columns)
                placeholders = ', '.join(['?' for _ in columns])
                insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
                
                for row in data:
                    values = [row[col] for col in columns]
                    cursor.execute(insert_query, values)
                
                conn.commit()
                
            logger.info(f"E2E Successfully inserted {len(data)} rows into {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"E2E Failed to setup test data: {e}")
            return False
    
    def cleanup_test_data(self, table_name: str, drop_table: bool = True) -> bool:
        """テストデータのクリーンアップ（テーブル削除）"""
        logger.info(f"E2E Cleaning up test data from table: {table_name}")
        
        try:
            if drop_table:
                # テストテーブル全体を削除
                cleanup_query = f"DROP TABLE IF EXISTS {table_name}"
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(cleanup_query)
                    conn.commit()
                logger.info(f"E2E Dropped test table {table_name}")
            else:
                # データのみ削除
                cleanup_query = f"DELETE FROM {table_name}"
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(cleanup_query)
                    rows_affected = cursor.rowcount
                    conn.commit()
                logger.info(f"E2E Cleaned up {rows_affected} rows from {table_name}")
            return True
            
        except Exception as e:
            logger.warning(f"E2E Cleanup failed for {table_name}: {e}")
            return False
    
    def run_pipeline(self, pipeline_name: str, parameters: dict = None) -> dict:
        """パイプライン実行のシミュレーション（実際のADF連携は別途実装）"""
        logger.info(f"E2E Simulating pipeline execution: {pipeline_name}")
        
        # パイプライン実行のシミュレーション
        import uuid
        run_id = str(uuid.uuid4())
        
        # 基本的な実行結果を返す
        result = {
            "run_id": run_id,
            "pipeline_name": pipeline_name,
            "status": "Succeeded",
            "start_time": time.time(),
            "duration": 30,  # 30秒のシミュレーション
            "parameters": parameters or {}
        }
        
        logger.info(f"E2E Pipeline {pipeline_name} simulation completed with run_id: {run_id}")
        return result

    def execute_script(self, script_path: str) -> bool:
        """SQLスクリプトファイルを実行"""
        logger.info(f"E2E Executing SQL script: {script_path}")
        
        try:
            with open(script_path, 'r', encoding='utf-8') as file:
                script_content = file.read()
            
            # スクリプトをGOで分割して個別に実行
            commands = [cmd.strip() for cmd in script_content.split('GO') if cmd.strip()]
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                for command in commands:
                    if command:
                        cursor.execute(command)
                conn.commit()
            
            logger.info(f"E2E Script executed successfully: {script_path}")
            return True
        except Exception as e:
            logger.error(f"E2E Script execution failed: {e}")
            return False

    def get_test_data(self, table_name: str, limit: int = 10) -> List[tuple]:
        """テストデータを取得"""
        query = f"SELECT TOP {limit} * FROM {table_name}"
        return self.execute_query(query)

    def table_exists(self, table_name: str) -> bool:
        """テーブルの存在確認"""
        query = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?"
        result = self.execute_query(query, (table_name,))
        return result[0][0] > 0 if result else False

    def get_table_row_count(self, table_name: str) -> int:
        """テーブルの行数を取得"""
        query = f"SELECT COUNT(*) FROM {table_name}"
        result = self.execute_query(query)
        return result[0][0] if result else 0
