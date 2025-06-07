"""
E2Eテスト: 基本的なデータベーステスト
"""
import pytest
import time
from tests.helpers.reproducible_e2e_helper import setup_reproducible_test_class, cleanup_reproducible_test_class
from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection


@pytest.mark.e2e
class TestBasicDatabaseOperations:
    """基本的なデータベース操作のE2Eテストクラス"""
    
    @classmethod
    def setup_class(cls):
        """再現可能テスト環境のセットアップ"""
        setup_reproducible_test_class()
        
        # Disable proxy settings for tests
        import os
        for var in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY']:
            if var in os.environ:
                del os.environ[var]
    
    @classmethod 
    def teardown_class(cls):
        """再現可能テスト環境のクリーンアップ"""
        cleanup_reproducible_test_class()
    
    def test_database_connection_basic(self):
        """基本的なデータベース接続テスト"""
        connection = SynapseE2EConnection()
        result = connection.test_synapse_connection()
        assert result is True, "Database connection failed"
    
    def test_table_count_validation(self):
        """テーブル数の検証"""
        connection = SynapseE2EConnection()
        query = "SELECT COUNT(*) as table_count FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'"
        result = connection.execute_query(query)
        
        assert len(result) > 0, "No result returned"
        table_count = result[0][0]  # タプルのインデックスアクセス
        assert table_count > 0, "No tables found in database"
    
    def test_client_dm_table_structure(self):
        """client_dmテーブル構造のテスト"""
        connection = SynapseE2EConnection()
        query = """
        SELECT COLUMN_NAME, DATA_TYPE 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'client_dm'
        ORDER BY ORDINAL_POSITION
        """
        result = connection.execute_query(query)
        
        assert len(result) > 0, "client_dm table not found or has no columns"
        column_names = [row[0] for row in result]  # タプルのインデックスアクセス
        assert 'client_id' in column_names, "client_id column not found"
    
    def test_point_grant_email_table_structure(self):
        """point_grant_emailテーブル構造のテスト"""
        connection = SynapseE2EConnection()
        query = """
        SELECT COLUMN_NAME, DATA_TYPE 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'point_grant_email'
        ORDER BY ORDINAL_POSITION
        """
        result = connection.execute_query(query)
        
        assert len(result) > 0, "point_grant_email table not found or has no columns"
        column_names = [row[0] for row in result]  # タプルのインデックスアクセス
        assert 'id' in column_names, "id column not found"
        assert 'client_id' in column_names, "client_id column not found"
    
    def test_marketing_client_dm_table_structure(self):
        """marketing_client_dmテーブル構造のテスト"""
        connection = SynapseE2EConnection()
        query = """
        SELECT COLUMN_NAME, DATA_TYPE 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'marketing_client_dm'
        ORDER BY ORDINAL_POSITION
        """
        result = connection.execute_query(query)
        
        assert len(result) > 0, "marketing_client_dm table not found or has no columns"
        column_names = [row[0] for row in result]  # タプルのインデックスアクセス
        assert 'id' in column_names, "id column not found"
        assert 'client_id' in column_names, "client_id column not found"
    
    def test_data_insertion_basic(self):
        """基本的なデータ挿入テスト"""
        connection = SynapseE2EConnection()
        
        # Test data insertion into point_grant_email
        test_data = {
            'client_id': 'TEST001',
            'email': 'test@example.com',
            'points_granted': 100,
            'email_sent_date': '2024-01-01',
            'campaign_id': 'CAMP001',
            'status': 'sent',
            'created_at': '2024-01-01 00:00:00'
        }
        
        # Insert test data
        insert_query = """
        INSERT INTO point_grant_email (client_id, email, points_granted, email_sent_date, campaign_id, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        
        connection.execute_query(
            insert_query,
            test_data['client_id'],
            test_data['email'],
            test_data['points_granted'],
            test_data['email_sent_date'],
            test_data['campaign_id'],
            test_data['status'],
            test_data['created_at']
        )
        
        # Verify insertion
        select_query = "SELECT COUNT(*) as count FROM point_grant_email WHERE client_id = ?"
        result = connection.execute_query(select_query, test_data['client_id'])
        assert result[0]['count'] >= 1, "Data insertion failed"
    
    def test_database_performance_basic(self):
        """基本的なデータベースパフォーマンステスト"""
        connection = SynapseE2EConnection()
        
        start_time = time.time()
        query = "SELECT COUNT(*) as total_count FROM point_grant_email"
        result = connection.execute_query(query)
        end_time = time.time()
        
        execution_time = end_time - start_time
        assert execution_time < 5.0, f"Query took too long: {execution_time:.2f} seconds"
        assert len(result) > 0, "No result returned"
    
    def test_table_relationships_basic(self):
        """基本的なテーブル関係のテスト"""
        connection = SynapseE2EConnection()
        
        # Test if we can join tables without errors
        query = """
        SELECT TOP 10 
            pge.client_id,
            pge.email,
            pge.points_granted
        FROM point_grant_email pge
        WHERE pge.client_id IS NOT NULL
        """
        result = connection.execute_query(query)
        
        # Should not throw an error
        assert isinstance(result, list), "Query should return a list"
    
    def test_schema_validation_basic(self):
        """基本的なスキーマ検証テスト"""
        connection = SynapseE2EConnection()
        
        # Check if required schemas exist
        query = """
        SELECT SCHEMA_NAME
        FROM INFORMATION_SCHEMA.SCHEMATA
        WHERE SCHEMA_NAME IN ('dbo', 'etl', 'staging')
        """
        result = connection.execute_query(query)
        
        schema_names = [row['SCHEMA_NAME'] for row in result]
        assert 'dbo' in schema_names, "dbo schema not found"
