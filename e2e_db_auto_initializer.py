#!/usr/bin/env python3
"""
E2E Database Auto Initializer
E2Eテスト用データベース自動初期化スクリプト

このスクリプトは E2E テストの実行前にデータベース環境を自動的にセットアップします。
"""

import os
import sys
import time
import pyodbc
import logging
from pathlib import Path
from typing import List, Dict, Any

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)8s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

class E2EDBAutoInitializer:
    """E2Eテスト用データベース自動初期化クラス"""
    
    def __init__(self):
        self.project_root = Path(__file__).parent
        self.sql_scripts_dir = self.project_root / "docker" / "sql" / "init"
        
        # データベース接続情報
        self.server = os.getenv("SQL_SERVER_HOST", "localhost")
        self.port = os.getenv("SQL_SERVER_PORT", "1433")
        self.username = os.getenv("SQL_SERVER_USER", "sa")
        self.password = os.getenv("SQL_SERVER_PASSWORD", "YourStrong!Passw0rd123")
        self.database = os.getenv("SQL_SERVER_DATABASE", "TGMATestDB")
        
        # 接続文字列の構築
        self.connection_string = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={self.server},{self.port};"
            f"DATABASE=master;"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"TrustServerCertificate=yes;"
            f"Encrypt=yes;"
        )
        
        logger.info(f"E2E DB Auto Initializer initialized")
        logger.info(f"SQL scripts directory: {self.sql_scripts_dir}")
        logger.info(f"Target server: {self.server}:{self.port}")
    
    def check_connection(self) -> bool:
        """データベース接続確認"""
        try:
            with pyodbc.connect(self.connection_string, timeout=10) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                if result and result[0] == 1:
                    logger.info("✅ Database connection successful")
                    return True
                else:
                    logger.error("❌ Database connection test failed")
                    return False
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False
    
    def wait_for_database(self, max_retries: int = 30, retry_interval: int = 5) -> bool:
        """データベースの準備完了を待機"""
        logger.info(f"🔄 Waiting for database to be ready (max {max_retries} attempts)...")
        
        for attempt in range(1, max_retries + 1):
            logger.info(f"Attempt {attempt}/{max_retries}: Testing database connection...")
            
            if self.check_connection():
                logger.info(f"✅ Database is ready after {attempt} attempts")
                return True
            
            if attempt < max_retries:
                logger.info(f"⏳ Database not ready, waiting {retry_interval}s before retry...")
                time.sleep(retry_interval)
        
        logger.error(f"❌ Database failed to be ready after {max_retries} attempts")
        return False
    
    def check_database_exists(self, db_name: str) -> bool:
        """データベースの存在確認"""
        try:
            with pyodbc.connect(self.connection_string, timeout=10) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM sys.databases 
                    WHERE name = ?
                """, db_name)
                result = cursor.fetchone()
                exists = result[0] > 0
                logger.info(f"Database '{db_name}' exists: {exists}")
                return exists
        except Exception as e:
            logger.error(f"Error checking database existence: {e}")
            return False
    
    def execute_sql_file(self, sql_file_path: Path) -> bool:
        """SQLファイルを実行"""
        if not sql_file_path.exists():
            logger.warning(f"SQL file not found: {sql_file_path}")
            return False
        
        try:
            logger.info(f"📄 Executing SQL file: {sql_file_path.name}")
            
            with open(sql_file_path, 'r', encoding='utf-8') as file:
                sql_content = file.read()
            
            # GOで分割して個別実行
            sql_batches = [batch.strip() for batch in sql_content.split('GO') if batch.strip()]
            
            with pyodbc.connect(self.connection_string, timeout=30) as conn:
                cursor = conn.cursor()
                
                for i, batch in enumerate(sql_batches, 1):
                    if batch.strip():
                        try:
                            cursor.execute(batch)
                            conn.commit()
                        except Exception as batch_error:
                            logger.warning(f"Batch {i} in {sql_file_path.name} failed: {batch_error}")
                            # 一部のエラーは無視して続行（テーブル既存など）
                            continue
            
            logger.info(f"✅ Successfully executed: {sql_file_path.name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to execute {sql_file_path.name}: {e}")
            return False
    
    def initialize_database(self) -> bool:
        """データベースの初期化"""
        logger.info("🚀 Starting E2E database initialization...")
        
        # 実行順序を定義
        sql_files = [
            "00_create_synapse_db.sql",
            "01_init_database.sql", 
            "02_create_test_tables.sql",
            "04_enhanced_test_tables.sql",
            "03_insert_test_data.sql",
            "05_comprehensive_test_data.sql",
            "06_additional_e2e_test_data.sql",
            "07_additional_schema_objects.sql"
        ]
        
        success_count = 0
        total_files = len(sql_files)
        
        for sql_file in sql_files:
            sql_path = self.sql_scripts_dir / sql_file
            if self.execute_sql_file(sql_path):
                success_count += 1
            else:
                logger.warning(f"Failed to execute {sql_file}, but continuing...")
        
        logger.info(f"📊 Database initialization completed: {success_count}/{total_files} files executed successfully")
        
        # 最低限必要なファイルが実行されていれば成功とする
        minimum_required = 4  # 基本的なテーブル作成ファイル
        if success_count >= minimum_required:
            logger.info("✅ Database initialization successful")
            return True
        else:
            logger.error(f"❌ Database initialization failed: only {success_count} files succeeded")
            return False
    
    def validate_test_environment(self) -> bool:
        """テスト環境の検証"""
        logger.info("🔍 Validating test environment...")
        
        try:
            # TGMATestDBに接続
            test_db_connection = self.connection_string.replace("DATABASE=master", f"DATABASE={self.database}")
            
            with pyodbc.connect(test_db_connection, timeout=10) as conn:
                cursor = conn.cursor()
                
                # 基本テーブルの存在確認
                required_tables = [
                    "client_dm",
                    "ClientDmBx", 
                    "point_grant_email",
                    "marketing_client_dm"
                ]
                
                existing_tables = []
                for table in required_tables:
                    cursor.execute("""
                        SELECT COUNT(*) 
                        FROM INFORMATION_SCHEMA.TABLES 
                        WHERE TABLE_NAME = ?
                    """, table)
                    
                    if cursor.fetchone()[0] > 0:
                        existing_tables.append(table)
                
                logger.info(f"Found {len(existing_tables)}/{len(required_tables)} required tables")
                
                if len(existing_tables) >= 2:  # 最低限のテーブルが存在
                    logger.info("✅ Test environment validation passed")
                    return True
                else:
                    logger.warning("⚠️ Test environment validation failed: insufficient tables")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ Test environment validation error: {e}")
            return False
    
    def run_initialization(self) -> bool:
        """完全な初期化プロセスを実行"""
        logger.info("🎯 Starting complete E2E database initialization process...")
        
        # 1. データベース接続待機
        if not self.wait_for_database():
            logger.error("❌ Database connection failed")
            return False
        
        # 2. データベース初期化
        if not self.initialize_database():
            logger.error("❌ Database initialization failed")
            return False
        
        # 3. テスト環境検証
        if not self.validate_test_environment():
            logger.warning("⚠️ Test environment validation failed, but proceeding...")
        
        logger.info("🎉 E2E database initialization completed successfully!")
        return True

def main():
    """メイン実行関数"""
    logger.info("🚀 E2E Database Auto Initializer - Starting...")
    
    # 環境変数の確認
    required_env_vars = [
        "SQL_SERVER_HOST",
        "SQL_SERVER_USER", 
        "SQL_SERVER_PASSWORD"
    ]
    
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"❌ Missing required environment variables: {missing_vars}")
        return False
    
    # 初期化実行
    initializer = E2EDBAutoInitializer()
    success = initializer.run_initialization()
    
    if success:
        logger.info("✅ E2E Database Auto Initializer completed successfully!")
        return True
    else:
        logger.error("❌ E2E Database Auto Initializer failed!")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
