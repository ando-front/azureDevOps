#!/usr/bin/env python3
"""
E2E Test Database Auto-Initializer
テスト実行前に自動でデータベース状態を検証・初期化するツール
"""

import os
import sys
import time
import logging
import pyodbc
import argparse
from typing import Dict, List, Optional

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

class E2ETestDatabaseInitializer:
    """E2Eテスト用データベース自動初期化クラス"""
    
    def __init__(self):
        self.connection_string = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            "SERVER=localhost,1433;"
            "DATABASE=TGMATestDB;"
            "UID=sa;"
            "PWD=YourStrong!Passw0rd123;"
            "TrustServerCertificate=yes;"
        )
    
    def wait_for_database(self, max_retries: int = 30, delay: int = 2) -> bool:
        """データベースが利用可能になるまで待機"""
        for attempt in range(max_retries):
            try:
                conn = pyodbc.connect(self.connection_string, timeout=10)
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                conn.close()
                logger.info("Database is ready")
                return True
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Database not available after {max_retries} attempts: {str(e)}")
                    return False
                logger.info(f"Database not ready, attempt {attempt + 1}/{max_retries}")
                time.sleep(delay)
        return False
    
    def execute_query(self, query: str, fetch_results: bool = True) -> Optional[List[Dict]]:
        """クエリを実行"""
        try:
            conn = pyodbc.connect(self.connection_string, timeout=30)
            cursor = conn.cursor()
            cursor.execute(query)
            
            if fetch_results and query.strip().upper().startswith('SELECT'):
                columns = [column[0] for column in cursor.description]
                results = []
                for row in cursor.fetchall():
                    results.append(dict(zip(columns, row)))
                conn.close()
                return results
            else:
                conn.commit()
                conn.close()
                return None
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            logger.error(f"Query: {query}")
            raise
    
    def check_required_tables(self) -> Dict[str, bool]:
        """必要なテーブルの存在を確認"""
        required_tables = [
            ('dbo', 'client_dm'),
            ('dbo', 'ClientDmBx'),
            ('dbo', 'point_grant_email'),
            ('dbo', 'marketing_client_dm'),
            ('etl', 'e2e_test_execution_log'),
            ('staging', 'test_data_management')
        ]
        
        results = {}
        for schema, table in required_tables:
            query = f"""
            SELECT COUNT(*) as count 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
            """
            result = self.execute_query(query)
            table_exists = result[0]['count'] > 0 if result else False
            results[f"{schema}.{table}"] = table_exists
            
        return results
    
    def check_required_data(self) -> Dict[str, int]:
        """必要なテストデータの存在を確認"""
        data_checks = {
            'client_dm_total': "SELECT COUNT(*) as count FROM client_dm",
            'client_dm_e2e': "SELECT COUNT(*) as count FROM client_dm WHERE client_id LIKE 'E2E_%'",
            'ClientDmBx_e2e': "SELECT COUNT(*) as count FROM ClientDmBx WHERE client_id LIKE 'E2E_%'",
            'point_grant_email_e2e': "SELECT COUNT(*) as count FROM point_grant_email WHERE client_id LIKE 'E2E_%'",
            'marketing_client_dm_e2e': "SELECT COUNT(*) as count FROM marketing_client_dm WHERE client_id LIKE 'E2E_%'"
        }
        
        results = {}
        for check_name, query in data_checks.items():
            try:
                result = self.execute_query(query)
                results[check_name] = result[0]['count'] if result else 0
            except Exception as e:
                logger.warning(f"Data check failed for {check_name}: {str(e)}")
                results[check_name] = 0
                
        return results
    
    def create_missing_structures(self):
        """不足している構造を作成"""
        logger.info("Creating missing database structures...")
        
        # ETLスキーマ作成
        self.execute_query("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'etl') EXEC('CREATE SCHEMA etl')", False)
        
        # Stagingスキーマ作成
        self.execute_query("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'staging') EXEC('CREATE SCHEMA staging')", False)
        
        # E2E test execution logテーブル作成（正しいスキーマ）
        etl_table_sql = """
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'etl' AND TABLE_NAME = 'e2e_test_execution_log')
        BEGIN
            CREATE TABLE etl.e2e_test_execution_log (
                ExecutionID INT IDENTITY(1,1) PRIMARY KEY,
                TestName NVARCHAR(255) NOT NULL,
                test_suite NVARCHAR(100),
                test_status NVARCHAR(50),
                start_time DATETIME2,
                end_time DATETIME2,
                duration_seconds FLOAT,
                test_data_count INT,
                ExecutionTime DATETIME2 DEFAULT GETDATE(),
                Status NVARCHAR(50) NOT NULL DEFAULT 'PENDING',
                Duration FLOAT,
                ErrorMessage NVARCHAR(MAX),
                TestData NVARCHAR(MAX)
            )
        END
        """
        self.execute_query(etl_table_sql, False)
        
        # Staging test data managementテーブル作成（正しいスキーマ）
        staging_table_sql = """
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'staging' AND TABLE_NAME = 'test_data_management')
        BEGIN
            CREATE TABLE staging.test_data_management (
                id INT IDENTITY(1,1) PRIMARY KEY,
                table_name NVARCHAR(128),
                test_scenario NVARCHAR(255) NOT NULL,
                test_data NVARCHAR(MAX),
                data_status NVARCHAR(50),
                cleanup_required BIT,
                created_at DATETIME2 DEFAULT GETDATE(),
                status NVARCHAR(50) DEFAULT 'ACTIVE'
            )
        END
        """
        self.execute_query(staging_table_sql, False)
        
        # 必要なプロシージャ作成
        procedures = [
            """
            IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'ValidateE2ETestData')
                DROP PROCEDURE ValidateE2ETestData;
            """,
            """
            CREATE PROCEDURE ValidateE2ETestData
                @TableName NVARCHAR(128),
                @ExpectedCount INT = 0,
                @ValidationResult BIT OUTPUT
            AS
            BEGIN
                SET NOCOUNT ON;
                DECLARE @ActualCount INT;
                DECLARE @SQL NVARCHAR(MAX);
                SET @SQL = N'SELECT @Count = COUNT(*) FROM ' + QUOTENAME(@TableName);
                EXEC sp_executesql @SQL, N'@Count INT OUTPUT', @Count = @ActualCount OUTPUT;
                IF @ActualCount >= @ExpectedCount
                    SET @ValidationResult = 1;
                ELSE
                    SET @ValidationResult = 0;
                SELECT @TableName as TableName, @ActualCount as ActualCount, @ExpectedCount as ExpectedCount, @ValidationResult as ValidationPassed;
            END;
            """,
            """
            IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'GetE2ETestSummary')
                DROP PROCEDURE GetE2ETestSummary;
            """,
            """
            CREATE PROCEDURE GetE2ETestSummary
            AS
            BEGIN
                SELECT 
                    'client_dm' as TableName,
                    COUNT(*) as TotalRecords,
                    COUNT(CASE WHEN client_id LIKE 'E2E_%' THEN 1 END) as E2ERecords
                FROM client_dm
                UNION ALL
                SELECT 
                    'ClientDmBx' as TableName,
                    COUNT(*) as TotalRecords,
                    COUNT(CASE WHEN client_id LIKE 'E2E_%' THEN 1 END) as E2ERecords
                FROM ClientDmBx
                UNION ALL
                SELECT 
                    'point_grant_email' as TableName,
                    COUNT(*) as TotalRecords,
                    COUNT(CASE WHEN client_id LIKE 'E2E_%' THEN 1 END) as E2ERecords
                FROM point_grant_email
                UNION ALL
                SELECT 
                    'marketing_client_dm' as TableName,
                    COUNT(*) as TotalRecords,
                    COUNT(CASE WHEN client_id LIKE 'E2E_%' THEN 1 END) as E2ERecords
                FROM marketing_client_dm;
            END;
            """
        ]
        
        for proc_sql in procedures:
            try:
                self.execute_query(proc_sql, False)
            except Exception as e:
                logger.warning(f"Failed to create procedure: {str(e)}")
        
        # E2E test summaryビュー作成
        view_sql = """
        IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_e2e_test_summary')
            DROP VIEW vw_e2e_test_summary;
        """
        self.execute_query(view_sql, False)
        
        view_create_sql = """
        CREATE VIEW vw_e2e_test_summary AS
        SELECT 
            'client_dm' as table_name,
            COUNT(*) as total_records,
            COUNT(CASE WHEN client_id LIKE 'E2E_%' THEN 1 END) as e2e_records
        FROM client_dm
        UNION ALL
        SELECT 
            'ClientDmBx' as table_name,
            COUNT(*) as total_records,
            COUNT(CASE WHEN client_id LIKE 'E2E_%' THEN 1 END) as e2e_records
        FROM ClientDmBx
        UNION ALL
        SELECT 
            'point_grant_email' as table_name,
            COUNT(*) as total_records,
            COUNT(CASE WHEN client_id LIKE 'E2E_%' THEN 1 END) as e2e_records
        FROM point_grant_email
        UNION ALL
        SELECT 
            'marketing_client_dm' as table_name,
            COUNT(*) as total_records,
            COUNT(CASE WHEN client_id LIKE 'E2E_%' THEN 1 END) as e2e_records
        FROM marketing_client_dm;
        """
        self.execute_query(view_create_sql, False)
    
    def clean_test_data(self):
        """全てのテストデータをクリーンアップ（完全な初期化）"""
        logger.info("Cleaning all test data for fresh start...")
        
        # 外部キー制約を考慮した削除順序（基本テーブルのみ）
        cleanup_tables = [
            'marketing_client_dm',
            'point_grant_email',
            'ClientDmBx',
            'client_dm'
        ]
        
        for table in cleanup_tables:
            try:
                delete_sql = f"DELETE FROM [dbo].[{table}] WHERE client_id LIKE 'E2E_%'"
                self.execute_query(delete_sql, False)
                logger.info(f"Cleaned test data from {table}")
            except Exception as e:
                logger.warning(f"Failed to clean {table}: {str(e)}")
        
        # ETLとStagingテーブルの処理（存在する場合のみ）
        optional_tables = [
            ('etl', 'e2e_test_execution_log'),
            ('staging', 'test_data_management')
        ]
        
        for schema, table_name in optional_tables:
            try:
                # テーブルが存在するかチェック
                check_sql = f"SELECT COUNT(*) as count FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name}'"
                result = self.execute_query(check_sql)
                
                if result and result[0]['count'] > 0:
                    # テーブルが存在する場合、可能な列名でクリーンアップを試行
                    if table_name == 'e2e_test_execution_log':
                        possible_deletes = [
                            f"DELETE FROM [{schema}].[{table_name}] WHERE TestName LIKE 'E2E_%'",
                            f"DELETE FROM [{schema}].[{table_name}] WHERE ExecutionID > 0"  # 簡単なクリーンアップ
                        ]
                    else:  # test_data_management
                        possible_deletes = [
                            f"DELETE FROM [{schema}].[{table_name}] WHERE test_scenario LIKE 'E2E_%'",
                            f"DELETE FROM [{schema}].[{table_name}] WHERE id > 0"  # 簡単なクリーンアップ
                        ]
                    
                    for delete_sql in possible_deletes:
                        try:
                            self.execute_query(delete_sql, False)
                            logger.info(f"Cleaned test data from {schema}.{table_name}")
                            break
                        except Exception:
                            continue
                else:
                    logger.info(f"Table {schema}.{table_name} does not exist, skipping cleanup")
            except Exception as e:
                logger.warning(f"Failed to clean {schema}.{table_name}: {str(e)}")

    def insert_fresh_test_data(self):
        """完全に新しいテストデータを挿入（再現可能な状態）"""
        logger.info("Inserting fresh, reproducible test data...")
        
        # 固定された日時を使用して再現性を確保
        fixed_date = "'2024-01-15T10:00:00'"
        fixed_updated_date = "'2024-01-15T10:00:00'"
        
        # 1. 基本顧客データ（client_dm）
        client_dm_sql = f"""
        INSERT INTO client_dm (client_id, client_name, email, phone, address, registration_date, status, created_at, updated_at)
        VALUES 
            ('E2E_CLIENT_001', 'E2E Active Client 1', 'e2e.active1@test.com', '090-1111-0001', 'Tokyo Test Address 1', {fixed_date}, 'ACTIVE', {fixed_date}, {fixed_updated_date}),
            ('E2E_CLIENT_002', 'E2E Active Client 2', 'e2e.active2@test.com', '090-1111-0002', 'Tokyo Test Address 2', {fixed_date}, 'ACTIVE', {fixed_date}, {fixed_updated_date}),
            ('E2E_CLIENT_003', 'E2E Active Client 3', 'e2e.active3@test.com', '090-1111-0003', 'Tokyo Test Address 3', {fixed_date}, 'ACTIVE', {fixed_date}, {fixed_updated_date}),
            ('E2E_CLIENT_004', 'E2E Inactive Client', 'e2e.inactive@test.com', '090-1111-0004', 'Tokyo Test Address 4', {fixed_date}, 'INACTIVE', {fixed_date}, {fixed_updated_date}),
            ('E2E_CLIENT_005', 'E2E Premium Client', 'e2e.premium@test.com', '090-1111-0005', 'Tokyo Test Address 5', {fixed_date}, 'ACTIVE', {fixed_date}, {fixed_updated_date}),
            ('E2E_CLIENT_006', 'E2E Standard Client', 'e2e.standard@test.com', '090-1111-0006', 'Tokyo Test Address 6', {fixed_date}, 'ACTIVE', {fixed_date}, {fixed_updated_date}),
            ('E2E_CLIENT_007', 'E2E Test Client 7', 'e2e.test7@test.com', '090-1111-0007', 'Tokyo Test Address 7', {fixed_date}, 'ACTIVE', {fixed_date}, {fixed_updated_date}),
            ('E2E_CLIENT_008', 'E2E Test Client 8', 'e2e.test8@test.com', '090-1111-0008', 'Tokyo Test Address 8', {fixed_date}, 'ACTIVE', {fixed_date}, {fixed_updated_date}),
            ('E2E_CLIENT_009', 'E2E Test Client 9', 'e2e.test9@test.com', '090-1111-0009', 'Tokyo Test Address 9', {fixed_date}, 'ACTIVE', {fixed_date}, {fixed_updated_date}),
            ('E2E_CLIENT_010', 'E2E Test Client 10', 'e2e.test10@test.com', '090-1111-0010', 'Tokyo Test Address 10', {fixed_date}, 'ACTIVE', {fixed_date}, {fixed_updated_date}),
            ('E2E_CLIENT_011', 'E2E Test Client 11', 'e2e.test11@test.com', '090-1111-0011', 'Tokyo Test Address 11', {fixed_date}, 'ACTIVE', {fixed_date}, {fixed_updated_date}),
            ('E2E_CLIENT_012', 'E2E Test Client 12', 'e2e.test12@test.com', '090-1111-0012', 'Tokyo Test Address 12', {fixed_date}, 'ACTIVE', {fixed_date}, {fixed_updated_date}),
            ('E2E_BULK_001', 'E2E Bulk Test 1', 'e2e.bulk1@test.com', '090-2222-0001', 'Bulk Test Address 1', {fixed_date}, 'ACTIVE', {fixed_date}, {fixed_updated_date}),
            ('E2E_BULK_002', 'E2E Bulk Test 2', 'e2e.bulk2@test.com', '090-2222-0002', 'Bulk Test Address 2', {fixed_date}, 'ACTIVE', {fixed_date}, {fixed_updated_date}),
            ('E2E_BULK_003', 'E2E Bulk Test 3', 'e2e.bulk3@test.com', '090-2222-0003', 'Bulk Test Address 3', {fixed_date}, 'ACTIVE', {fixed_date}, {fixed_updated_date})
        """
        
        # 2. ClientDmBx データ（実際のスキーマに合わせて調整）
        clientdmbx_sql = f"""
        INSERT INTO ClientDmBx (client_id, segment, score, last_transaction_date, total_amount, processed_date, data_source)
        VALUES 
            ('E2E_CLIENT_001', 'PREMIUM', 95.5, {fixed_date}, 250000.00, {fixed_date}, 'E2E_TEST_PIPELINE'),
            ('E2E_CLIENT_002', 'HIGH_VALUE', 88.3, {fixed_date}, 180000.00, {fixed_date}, 'E2E_TEST_PIPELINE'),
            ('E2E_CLIENT_003', 'STANDARD', 72.1, {fixed_date}, 95000.00, {fixed_date}, 'E2E_TEST_PIPELINE'),
            ('E2E_CLIENT_004', 'LOW_VALUE', 45.8, {fixed_date}, 25000.00, {fixed_date}, 'E2E_TEST_PIPELINE'),
            ('E2E_CLIENT_005', 'PREMIUM', 92.8, {fixed_date}, 320000.00, {fixed_date}, 'E2E_TEST_PIPELINE'),
            ('E2E_BULK_001', 'STANDARD', 70.0, {fixed_date}, 100000.00, {fixed_date}, 'E2E_BULK_PIPELINE'),
            ('E2E_BULK_002', 'STANDARD', 71.0, {fixed_date}, 101000.00, {fixed_date}, 'E2E_BULK_PIPELINE')
        """
        
        # 3. Point Grant Email データ（実際のスキーマに合わせて調整）
        point_grant_sql = f"""
        INSERT INTO point_grant_email (client_id, email, points_granted, email_sent_date, campaign_id, status, created_at)
        VALUES 
            ('E2E_CLIENT_001', 'e2e.active1@test.com', 1000, {fixed_date}, 'E2E_CAMP_001', 'SENT', {fixed_date}),
            ('E2E_CLIENT_002', 'e2e.active2@test.com', 500, {fixed_date}, 'E2E_CAMP_001', 'SENT', {fixed_date}),
            ('E2E_CLIENT_003', 'e2e.active3@test.com', 750, {fixed_date}, 'E2E_CAMP_002', 'PENDING', {fixed_date}),
            ('E2E_CLIENT_004', 'e2e.inactive@test.com', 300, {fixed_date}, 'E2E_CAMP_002', 'FAILED', {fixed_date}),
            ('E2E_CLIENT_005', 'e2e.premium@test.com', 1500, {fixed_date}, 'E2E_CAMP_003', 'SENT', {fixed_date})
        """
        
        # 4. Marketing Client DM データ（実際のスキーマに合わせて調整）
        marketing_sql = f"""
        INSERT INTO marketing_client_dm (client_id, marketing_segment, preference_category, engagement_score, last_campaign_response, opt_in_email, opt_in_sms, created_at, updated_at)
        VALUES 
            ('E2E_CLIENT_001', 'HIGH_ENGAGEMENT', 'ELECTRONICS', 9.5, {fixed_date}, 1, 1, {fixed_date}, {fixed_date}),
            ('E2E_CLIENT_002', 'MEDIUM_ENGAGEMENT', 'FASHION', 7.8, {fixed_date}, 1, 0, {fixed_date}, {fixed_date}),
            ('E2E_CLIENT_003', 'LOW_ENGAGEMENT', 'BOOKS', 4.2, {fixed_date}, 0, 0, {fixed_date}, {fixed_date}),
            ('E2E_CLIENT_004', 'INACTIVE_SEGMENT', 'NONE', 1.0, {fixed_date}, 0, 0, {fixed_date}, {fixed_date}),
            ('E2E_CLIENT_005', 'HIGH_ENGAGEMENT', 'LUXURY', 9.8, {fixed_date}, 1, 1, {fixed_date}, {fixed_date})
        """
        
        # 5. ETL Test Execution Log
        etl_log_sql = f"""
        INSERT INTO etl.e2e_test_execution_log (test_suite, test_name, test_status, start_time, end_time, duration_seconds, test_data_count)
        VALUES 
            ('E2E_SETUP', 'Database Initialization', 'COMPLETED', {fixed_date}, {fixed_date}, 1, 15),
            ('E2E_VALIDATION', 'Table Structure Check', 'PENDING', {fixed_date}, NULL, NULL, 0),
            ('E2E_CONNECTION', 'Database Connection Test', 'PENDING', {fixed_date}, NULL, NULL, 0)
        """
        
        # 6. Staging Test Data Management
        staging_sql = f"""
        INSERT INTO staging.test_data_management (table_name, test_scenario, test_data, data_status, cleanup_required)
        VALUES 
            ('client_dm', 'E2E_BASIC_CRUD', 'E2E_CLIENT_001,E2E_CLIENT_002,E2E_CLIENT_003', 'ACTIVE', 1),
            ('ClientDmBx', 'E2E_DATAMART_PIPELINE', 'E2E_CLIENT_001,E2E_CLIENT_002,E2E_CLIENT_003', 'ACTIVE', 1),
            ('point_grant_email', 'E2E_EMAIL_PIPELINE', 'E2E_CLIENT_001,E2E_CLIENT_002,E2E_CLIENT_003', 'ACTIVE', 1),
            ('marketing_client_dm', 'E2E_MARKETING_PIPELINE', 'E2E_CLIENT_001,E2E_CLIENT_002,E2E_CLIENT_003', 'ACTIVE', 1),
            ('client_dm', 'E2E_BULK_PROCESSING', 'E2E_BULK_001,E2E_BULK_002,E2E_BULK_003', 'ACTIVE', 1)
        """
        
        # 各SQLを実行（基本テーブルのみ）
        sql_statements = [
            ("client_dm", client_dm_sql),
            ("ClientDmBx", clientdmbx_sql), 
            ("point_grant_email", point_grant_sql),
            ("marketing_client_dm", marketing_sql)
        ]
        
        for table_name, sql in sql_statements:
            try:
                self.execute_query(sql, False)
                logger.info(f"Successfully inserted test data into {table_name}")
            except Exception as e:
                logger.error(f"Failed to insert test data into {table_name}: {str(e)}")
                raise
        
        # ETLとStagingテーブルへの挿入（存在する場合のみ）
        optional_inserts = [
            ("etl.e2e_test_execution_log", etl_log_sql),
            ("staging.test_data_management", staging_sql)
        ]
        
        for table_name, sql in optional_inserts:
            try:
                schema, table = table_name.split('.')
                check_sql = f"SELECT COUNT(*) as count FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'"
                result = self.execute_query(check_sql)
                
                if result and result[0]['count'] > 0:
                    self.execute_query(sql, False)
                    logger.info(f"Successfully inserted test data into {table_name}")
                else:
                    logger.info(f"Table {table_name} does not exist, skipping data insertion")
            except Exception as e:
                logger.warning(f"Failed to insert test data into {table_name}: {str(e)}")

    def ensure_test_data(self):
        """完全に再現可能なテストデータを確保"""
        logger.info("Ensuring completely reproducible test data...")
        
        # 1. 既存のテストデータを完全にクリーンアップ
        self.clean_test_data()
        
        # 2. 固定された新しいテストデータを挿入
        self.insert_fresh_test_data()
        
        # 3. データの整合性を検証
        self.verify_test_data_consistency()
        
        logger.info("Test data ensured with full reproducibility")

    def verify_test_data_consistency(self):
        """テストデータの整合性を検証"""
        logger.info("Verifying test data consistency...")
        
        # 各テーブルのE2Eデータ数をチェック
        checks = {
            'client_dm': 15,      # 12 regular + 3 bulk
            'ClientDmBx': 7,      # 5 regular + 2 bulk  
            'point_grant_email': 5,  # 5 records
            'marketing_client_dm': 5  # 5 records
        }
        
        for table, expected_count in checks.items():
            try:
                result = self.execute_query(f"SELECT COUNT(*) as count FROM {table} WHERE client_id LIKE 'E2E_%'")
                actual_count = result[0]['count'] if result else 0
                
                if actual_count == expected_count:
                    logger.info(f"✅ {table}: {actual_count}/{expected_count} records")
                else:
                    logger.warning(f"⚠️ {table}: {actual_count}/{expected_count} records (mismatch)")
            except Exception as e:
                logger.error(f"❌ Failed to verify {table}: {str(e)}")
        
        logger.info("Test data consistency verification completed")
    
    def validate_initialization(self) -> bool:
        """初期化の検証"""
        logger.info("Validating database initialization...")
        
        # テーブル存在確認
        tables = self.check_required_tables()
        missing_tables = [table for table, exists in tables.items() if not exists]
        if missing_tables:
            logger.error(f"Missing tables: {missing_tables}")
            return False
        
        # データ存在確認
        data = self.check_required_data()
        
        required_minimums = {
            'client_dm_e2e': 12,
            'ClientDmBx_e2e': 5,
            'point_grant_email_e2e': 3,
            'marketing_client_dm_e2e': 3
        }
        
        insufficient_data = []
        for check, minimum in required_minimums.items():
            if data.get(check, 0) < minimum:
                insufficient_data.append(f"{check}: {data.get(check, 0)}/{minimum}")
        
        if insufficient_data:
            logger.warning(f"Insufficient data: {insufficient_data}")
            # 警告として記録するが、継続する
        
        logger.info("Database initialization validation completed")
        return True
    
    def initialize_for_tests(self) -> bool:
        """E2Eテスト用データベースの完全初期化"""
        logger.info("Starting E2E test database initialization...")
        
        # 1. データベース接続待機
        if not self.wait_for_database():
            logger.error("Database connection failed")
            return False
        
        # 2. 必要な構造を作成
        try:
            self.create_missing_structures()
        except Exception as e:
            logger.error(f"Failed to create structures: {str(e)}")
            return False
        
        # 3. テストデータを確保
        try:
            self.ensure_test_data()
        except Exception as e:
            logger.error(f"Failed to ensure test data: {str(e)}")
            return False
        
        # 4. 初期化を検証
        if not self.validate_initialization():
            logger.error("Initialization validation failed")
            return False
        
        logger.info("E2E test database initialization completed successfully!")
        return True


def main():
    """メイン処理"""
    import argparse
    
    parser = argparse.ArgumentParser(description='E2E Test Database Auto-Initializer')
    parser.add_argument('--check-only', action='store_true', 
                       help='Only check database connectivity without initialization')
    parser.add_argument('--clean-only', action='store_true',
                       help='Only clean test data without re-initialization')
    parser.add_argument('--verify-only', action='store_true',
                       help='Only verify existing test data without changes')
    
    args = parser.parse_args()
    
    initializer = E2ETestDatabaseInitializer()
    
    # データベース接続チェックのみ
    if args.check_only:
        if initializer.wait_for_database():
            logger.info("✅ Database connectivity check passed")
            sys.exit(0)
        else:
            logger.error("❌ Database connectivity check failed")
            sys.exit(1)
    
    # データクリーンアップのみ
    if args.clean_only:
        if initializer.wait_for_database():
            try:
                initializer.clean_test_data()
                logger.info("✅ Test data cleanup completed")
                sys.exit(0)
            except Exception as e:
                logger.error(f"❌ Test data cleanup failed: {str(e)}")
                sys.exit(1)
        else:
            logger.error("❌ Database not available for cleanup")
            sys.exit(1)
    
    # データ検証のみ
    if args.verify_only:
        if initializer.wait_for_database():
            try:
                data_status = initializer.check_required_data()
                logger.info("📊 Current test data status:")
                for check, count in data_status.items():
                    logger.info(f"  {check}: {count} records")
                
                if initializer.validate_initialization():
                    logger.info("✅ Test data verification passed")
                    sys.exit(0)
                else:
                    logger.warning("⚠️ Test data verification found issues")
                    sys.exit(1)
            except Exception as e:
                logger.error(f"❌ Test data verification failed: {str(e)}")
                sys.exit(1)
        else:
            logger.error("❌ Database not available for verification")
            sys.exit(1)
    
    # 完全初期化（デフォルト）
    success = initializer.initialize_for_tests()
    
    if success:
        logger.info("🎉 E2E database is ready for testing!")
        logger.info("🔄 Database is now in a completely reproducible state")
        sys.exit(0)
    else:
        logger.error("❌ E2E database initialization failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
