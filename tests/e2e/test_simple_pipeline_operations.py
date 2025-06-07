"""
E2Eテスト: シンプルなパイプラインテスト
"""
import pytest
import time
from tests.helpers.reproducible_e2e_helper import setup_reproducible_test_class, cleanup_reproducible_test_class
from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection


@pytest.mark.e2e
class TestSimplePipelineOperations:
    """シンプルなパイプライン操作のE2Eテストクラス"""
    
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
    
    def test_simple_data_transfer(self):
        """シンプルなデータ転送テスト"""
        connection = SynapseE2EConnection()
        
        # Test basic data operation
        test_query = "SELECT 1 as test_value"
        result = connection.execute_query(test_query)
        
        assert len(result) == 1, "Expected one result"
        assert result[0]['test_value'] == 1, "Expected test_value to be 1"
    
    def test_pipeline_simulation_basic(self):
        """基本的なパイプラインシミュレーション"""
        connection = SynapseE2EConnection()
        
        # Simulate pipeline trigger
        pipeline_id = connection.trigger_pipeline_mock("test_pipeline")
        assert pipeline_id is not None, "Pipeline trigger failed"
        
        # Wait for completion simulation
        time.sleep(1)
        
        # Check status simulation
        status = connection.check_pipeline_status_mock(pipeline_id)
        assert status == 'Succeeded', f"Pipeline status is {status}, expected Succeeded"
    
    def test_data_validation_simple(self):
        """シンプルなデータ検証テスト"""
        connection = SynapseE2EConnection()
        
        # Check if test data exists
        query = "SELECT COUNT(*) as count FROM point_grant_email"
        result = connection.execute_query(query)
        
        assert len(result) > 0, "No result returned"
        count = result[0]['count']
        assert count >= 0, "Count should be non-negative"
    
    def test_table_accessibility(self):
        """テーブルアクセシビリティテスト"""
        connection = SynapseE2EConnection()
        
        tables_to_test = ['client_dm', 'point_grant_email', 'marketing_client_dm', 'ClientDmBx']
        
        for table in tables_to_test:
            try:
                query = f"SELECT TOP 1 * FROM {table}"
                result = connection.execute_query(query)
                # Table should be accessible (result can be empty, but no error)
                assert isinstance(result, list), f"Table {table} should be accessible"
            except Exception as e:
                pytest.fail(f"Table {table} is not accessible: {e}")
    
    def test_data_consistency_basic(self):
        """基本的なデータ整合性テスト"""
        connection = SynapseE2EConnection()
        
        # Test for basic data integrity
        query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT client_id) as unique_clients
        FROM point_grant_email
        WHERE client_id IS NOT NULL
        """
        result = connection.execute_query(query)
        
        assert len(result) > 0, "No result returned"
        total = result[0]['total_records']
        unique = result[0]['unique_clients']
        assert unique <= total, "Unique clients should not exceed total records"
    
    def test_timestamp_validation_basic(self):
        """基本的なタイムスタンプ検証テスト"""
        connection = SynapseE2EConnection()
        
        # Test timestamp fields
        query = """
        SELECT 
            created_at,
            email_sent_date
        FROM point_grant_email
        WHERE created_at IS NOT NULL
        """
        result = connection.execute_query(query)
        
        # Should not throw an error for timestamp operations
        assert isinstance(result, list), "Timestamp query should return a list"
    
    def test_error_handling_simulation(self):
        """エラーハンドリングシミュレーション"""
        connection = SynapseE2EConnection()
        
        try:
            # Intentionally use a non-existent table to test error handling
            query = "SELECT * FROM non_existent_table_12345"
            connection.execute_query(query)
            pytest.fail("Expected an error for non-existent table")
        except Exception as e:
            # Error is expected, test passes
            assert "Invalid object name" in str(e) or "non_existent_table" in str(e)
    
    def test_connection_resilience(self):
        """接続の復元力テスト"""
        connection = SynapseE2EConnection()
        
        # Test multiple consecutive queries
        for i in range(3):
            query = f"SELECT {i+1} as iteration"
            result = connection.execute_query(query)
            assert result[0]['iteration'] == i+1, f"Iteration {i+1} failed"
    
    def test_batch_operation_simulation(self):
        """バッチ操作のシミュレーション"""
        connection = SynapseE2EConnection()
        
        # Simulate batch processing
        batch_size = 5
        for batch in range(batch_size):
            query = f"SELECT {batch+1} as batch_number, GETDATE() as process_time"
            result = connection.execute_query(query)
            assert len(result) == 1, f"Batch {batch+1} failed"
            assert result[0]['batch_number'] == batch+1
    
    def test_concurrent_access_simulation(self):
        """同時アクセスのシミュレーション"""
        connection = SynapseE2EConnection()
        
        # Simulate concurrent queries
        results = []
        for i in range(3):
            query = f"SELECT {i+1} as concurrent_id, COUNT(*) as table_count FROM point_grant_email"
            result = connection.execute_query(query)
            results.append(result[0])
        
        assert len(results) == 3, "Expected 3 concurrent results"
        for i, result in enumerate(results):
            assert result['concurrent_id'] == i+1
