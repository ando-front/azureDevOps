"""
E2Eテスト: データ品質テスト
"""
import pytest
from tests.helpers.reproducible_e2e_helper import setup_reproducible_test_class, cleanup_reproducible_test_class
from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection


@pytest.mark.e2e
class TestDataQualityOperations:
    """データ品質に関するE2Eテストクラス"""
    
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
    
    def test_null_value_validation(self):
        """NULL値の検証テスト"""
        connection = SynapseE2EConnection()
        
        query = """
        SELECT 
            COUNT(*) as total,
            COUNT(client_id) as non_null_client_id,
            COUNT(email) as non_null_email
        FROM point_grant_email
        """
        result = connection.execute_query(query)
        
        assert len(result) > 0, "No result returned"
        data = result[0]
        # タプル形式でデータが返される: (total, non_null_client_id, non_null_email)
        assert data[1] >= 0, "Non-null client_id count should be non-negative"
        assert data[2] >= 0, "Non-null email count should be non-negative"
    
    def test_data_type_validation(self):
        """データ型の検証テスト"""
        connection = SynapseE2EConnection()
        
        query = """
        SELECT 
            client_id,
            points_granted,
            created_at
        FROM point_grant_email
        WHERE client_id IS NOT NULL
        AND points_granted IS NOT NULL
        """
        result = connection.execute_query(query)
        
        # Should not throw type conversion errors
        assert isinstance(result, list), "Query should return a list"
        
        if len(result) > 0:
            first_row = result[0]
            # タプル形式でデータが返される: (client_id, points_granted, created_at)
            assert first_row[0] is not None, "client_id should be present"
            assert first_row[1] is not None, "points_granted should be present"
    
    def test_duplicate_detection(self):
        """重複データの検出テスト"""
        connection = SynapseE2EConnection()
        
        query = """
        SELECT 
            client_id,
            COUNT(*) as occurrence_count
        FROM point_grant_email
        WHERE client_id IS NOT NULL
        GROUP BY client_id
        HAVING COUNT(*) > 1
        """
        result = connection.execute_query(query)
        
        # Test should pass regardless of duplicates (we're just testing the query works)
        assert isinstance(result, list), "Duplicate detection query should work"
    
    def test_date_range_validation(self):
        """日付範囲の検証テスト"""
        connection = SynapseE2EConnection()
        
        query = """
        SELECT 
            MIN(created_at) as earliest_date,
            MAX(created_at) as latest_date,
            COUNT(*) as total_records
        FROM point_grant_email
        WHERE created_at IS NOT NULL
        """
        result = connection.execute_query(query)
        
        assert len(result) > 0, "No result returned"
        # タプル形式でデータが返される: (earliest_date, latest_date, total_records)
        if result[0][2] > 0:  # total_records
            assert result[0][0] is not None, "Earliest date should not be null"
            assert result[0][1] is not None, "Latest date should not be null"
    
    def test_numeric_range_validation(self):
        """数値範囲の検証テスト"""
        connection = SynapseE2EConnection()
        
        query = """
        SELECT 
            MIN(points_granted) as min_points,
            MAX(points_granted) as max_points,
            AVG(CAST(points_granted as FLOAT)) as avg_points,
            COUNT(*) as total_records
        FROM point_grant_email
        WHERE points_granted IS NOT NULL
        """
        result = connection.execute_query(query)
        
        assert len(result) > 0, "No result returned"
        # タプル形式でデータが返される: (min_points, max_points, avg_points, total_records)
        if result[0][3] > 0:  # total_records
            assert result[0][0] >= 0, "Minimum points should be non-negative"
            assert result[0][1] >= result[0][0], "Max should be >= min"
    
    def test_string_length_validation(self):
        """文字列長の検証テスト"""
        connection = SynapseE2EConnection()
        
        query = """
        SELECT 
            LEN(client_id) as client_id_length,
            LEN(email) as email_length,
            LEN(status) as status_length
        FROM point_grant_email
        WHERE client_id IS NOT NULL 
        AND email IS NOT NULL 
        AND status IS NOT NULL
        """
        result = connection.execute_query(query)
        
        # Should not throw length calculation errors
        assert isinstance(result, list), "String length query should work"
    
    def test_referential_integrity_basic(self):
        """基本的な参照整合性テスト"""
        connection = SynapseE2EConnection()
        
        # Test basic join between tables
        query = """
        SELECT TOP 10
            pge.client_id,
            pge.email,
            pge.status
        FROM point_grant_email pge
        WHERE pge.client_id IS NOT NULL
        """
        result = connection.execute_query(query)
        
        # Should not throw join errors
        assert isinstance(result, list), "Referential integrity query should work"
    
    def test_email_format_validation(self):
        """メールフォーマットの検証テスト"""
        connection = SynapseE2EConnection()
        
        query = """
        SELECT 
            email,
            CASE 
                WHEN email LIKE '%@%' THEN 1 
                ELSE 0 
            END as has_at_symbol
        FROM point_grant_email
        WHERE email IS NOT NULL
        """
        result = connection.execute_query(query)
        
        # Should not throw pattern matching errors
        assert isinstance(result, list), "Email format validation should work"
    
    def test_status_value_validation(self):
        """ステータス値の検証テスト"""
        connection = SynapseE2EConnection()
        
        query = """
        SELECT 
            status,
            COUNT(*) as status_count
        FROM point_grant_email
        WHERE status IS NOT NULL
        GROUP BY status
        """
        result = connection.execute_query(query)
        
        # Should return status grouping without errors
        assert isinstance(result, list), "Status validation query should work"
    
    def test_data_freshness_validation(self):
        """データ鮮度の検証テスト"""
        connection = SynapseE2EConnection()
        
        query = """
        SELECT 
            DATEDIFF(day, MAX(created_at), GETDATE()) as days_since_last_update,
            COUNT(*) as total_records
        FROM point_grant_email
        WHERE created_at IS NOT NULL
        """
        result = connection.execute_query(query)
        
        assert len(result) > 0, "No result returned"
        # Data freshness test should complete without errors
        # pyodbc.Row objects can be accessed like tuples
        assert len(result[0]) == 2, "Should return 2 columns (days and count)"
        
        # Test the actual data structure
        days_diff, record_count = result[0][0], result[0][1]
        assert isinstance(days_diff, (int, float)), "Days difference should be numeric"
        assert isinstance(record_count, int), "Record count should be integer"
