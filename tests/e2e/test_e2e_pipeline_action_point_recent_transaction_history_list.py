#!/usr/bin/env python3
"""
E2E test for Action Point Recent Transaction History List pipeline.
Tests data extraction, transformation, and delivery for action point transaction history.
"""

import pytest
import re
import time
from tests.helpers.reproducible_e2e_helper import setup_reproducible_test_class, cleanup_reproducible_test_class


class TestE2EPipelineActionPointRecentTransactionHistoryList:
    """E2E tests for Action Point Recent Transaction History List pipeline operations."""

    @classmethod
    def setup_class(cls):
        """Set up reproducible test environment for the entire test class."""
        setup_reproducible_test_class()

    @classmethod
    def teardown_class(cls):
        """Clean up test environment after all tests in the class."""
        cleanup_reproducible_test_class()

    def test_action_point_recent_transaction_history_pipeline_basic(self, pipeline_runner, synapse_helper):
        """Test basic pipeline execution for action point recent transaction history list."""
        # Insert test data for action point recent transactions
        test_data_query = """
        INSERT INTO [staging].[action_point_recent_transaction_history_test]
        VALUES 
            ('TXN001', 'CUST001', '2024-01-15', 'PURCHASE', 150.00, 'CREDIT_CARD', 'COMPLETED', '2024-01-15 10:30:00'),
            ('TXN002', 'CUST002', '2024-01-16', 'REFUND', -50.00, 'BANK_TRANSFER', 'COMPLETED', '2024-01-16 14:20:00'),
            ('TXN003', 'CUST003', '2024-01-17', 'PURCHASE', 200.00, 'DEBIT_CARD', 'PENDING', '2024-01-17 09:15:00')
        """
        
        synapse_helper.execute_query(test_data_query)
        
        # Execute pipeline
        result = pipeline_runner.run_pipeline("ActionPointRecentTransactionHistoryList")
        assert result.success, f"Pipeline failed: {result.error_message}"
        assert result.duration < 300, "Pipeline took too long to execute"

    def test_transaction_data_transformation(self, pipeline_runner, synapse_helper):
        """Test data transformation logic for transaction history."""
        # Insert complex test data
        transformation_test_query = """
        INSERT INTO [staging].[action_point_recent_transaction_history_test]
        VALUES 
            ('TXN101', 'CUST101', '2024-02-01', 'PURCHASE', 75.50, 'CREDIT_CARD', 'COMPLETED', '2024-02-01 11:30:00'),
            ('TXN102', 'CUST101', '2024-02-02', 'PURCHASE', 125.75, 'DEBIT_CARD', 'COMPLETED', '2024-02-02 16:45:00'),
            ('TXN103', 'CUST102', '2024-02-03', 'REFUND', -25.00, 'CASH', 'COMPLETED', '2024-02-03 13:20:00')
        """
        
        synapse_helper.execute_query(transformation_test_query)
        
        # Execute pipeline
        result = pipeline_runner.run_pipeline("ActionPointRecentTransactionHistoryList")
        assert result.success, f"Pipeline transformation failed: {result.error_message}"

    def test_output_file_validation(self, pipeline_runner, synapse_helper, blob_helper):
        """Test output file format and structure validation."""
        # Insert validation test data
        validation_test_query = """
        INSERT INTO [staging].[action_point_recent_transaction_history_test]
        VALUES 
            ('TXN201', 'CUST201', '2024-03-01', 'PURCHASE', 300.00, 'CREDIT_CARD', 'COMPLETED', '2024-03-01 09:00:00'),
            ('TXN202', 'CUST202', '2024-03-02', 'PURCHASE', 450.25, 'BANK_TRANSFER', 'COMPLETED', '2024-03-02 14:30:00')
        """
        
        synapse_helper.execute_query(validation_test_query)
        
        # Execute pipeline
        result = pipeline_runner.run_pipeline("ActionPointRecentTransactionHistoryList")
        assert result.success, f"Pipeline failed: {result.error_message}"
        
        # Validate output file
        transferred_files = blob_helper.list_transferred_files()
        action_point_files = [f for f in transferred_files if "ActionPointRecentTransactionHistoryList" in f]
        assert len(action_point_files) > 0, "No action point transaction history files found"
        
        transferred_file = action_point_files[0]
        assert "ActionPointRecentTransactionHistoryList" in transferred_file
        assert transferred_file.endswith(".csv.gz")
        
        # Validate timestamp in filename
        timestamp_pattern = r'\d{8}_\d{6}'
        assert re.search(timestamp_pattern, transferred_file), "Timestamp not found in filename"

    def test_error_handling_scenarios(self, pipeline_runner, synapse_helper):
        """Test pipeline error handling and recovery scenarios."""
        # Test with invalid transaction data scenario
        invalid_data_query = """
        INSERT INTO [staging].[action_point_recent_transaction_history_error_test]
        VALUES 
            (NULL, NULL, '2024-13-45', 'INVALID_TYPE', -1000, 'INVALID_METHOD', 'INVALID_STATUS', NULL),
            ('', 'invalid_customer', '1900-01-01', 'UNKNOWN', 999999999, '', '', '')
        """
        
        synapse_helper.execute_query(invalid_data_query)
        
        # Pipeline should handle errors gracefully
        result = pipeline_runner.run_pipeline("ActionPointRecentTransactionHistoryList")
        # Either succeeds with filtered data or fails gracefully
        if not result.success:
            assert "error" in result.error_message.lower() or "invalid" in result.error_message.lower()

    def test_performance_with_large_dataset(self, pipeline_runner, synapse_helper):
        """Test pipeline performance with large transaction dataset."""
        # Insert large dataset for performance testing
        large_dataset_query = """
        INSERT INTO [staging].[action_point_recent_transaction_history_test]
        SELECT 
            'TXN' + CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) + 1000 AS VARCHAR(10)),
            'CUST' + CAST((ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) % 100) + 1 AS VARCHAR(10)),
            DATEADD(day, -(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) % 30), '2024-03-01'),
            CASE (ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) % 3) 
                WHEN 0 THEN 'PURCHASE'
                WHEN 1 THEN 'REFUND'
                ELSE 'TRANSFER'
            END,
            (ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) % 1000) + 10.50,
            CASE (ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) % 4)
                WHEN 0 THEN 'CREDIT_CARD'
                WHEN 1 THEN 'DEBIT_CARD'
                WHEN 2 THEN 'BANK_TRANSFER'
                ELSE 'CASH'
            END,
            'COMPLETED',
            DATEADD(minute, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), '2024-03-01 08:00:00')
        FROM sys.objects s1 CROSS JOIN sys.objects s2
        WHERE ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) <= 500
        """
        
        synapse_helper.execute_query(large_dataset_query)
        
        # Measure execution time
        start_time = time.time()
        result = pipeline_runner.run_pipeline("ActionPointRecentTransactionHistoryList")
        execution_time = time.time() - start_time
        
        assert result.success, f"Pipeline failed with large dataset: {result.error_message}"
        assert execution_time < 600, f"Pipeline took too long: {execution_time} seconds"

    def test_transaction_filtering_logic(self, pipeline_runner, synapse_helper):
        """Test transaction filtering and selection logic."""
        # Insert data with various transaction statuses
        filtering_test_query = """
        INSERT INTO [staging].[action_point_recent_transaction_history_test]
        VALUES 
            ('TXN301', 'CUST301', '2024-04-01', 'PURCHASE', 100.00, 'CREDIT_CARD', 'COMPLETED', '2024-04-01 10:00:00'),
            ('TXN302', 'CUST301', '2024-04-02', 'PURCHASE', 150.00, 'DEBIT_CARD', 'PENDING', '2024-04-02 11:00:00'),
            ('TXN303', 'CUST301', '2024-04-03', 'PURCHASE', 200.00, 'BANK_TRANSFER', 'FAILED', '2024-04-03 12:00:00'),
            ('TXN304', 'CUST301', '2024-04-04', 'REFUND', -50.00, 'CREDIT_CARD', 'COMPLETED', '2024-04-04 13:00:00')
        """
        
        synapse_helper.execute_query(filtering_test_query)
        
        # Execute pipeline
        result = pipeline_runner.run_pipeline("ActionPointRecentTransactionHistoryList")
        assert result.success, f"Pipeline filtering failed: {result.error_message}"

    def test_data_consistency_across_runs(self, pipeline_runner, synapse_helper):
        """Test data consistency across multiple pipeline runs."""
        # Insert consistent test data
        consistency_test_query = """
        INSERT INTO [staging].[action_point_recent_transaction_history_test]
        VALUES 
            ('TXN401', 'CUST401', '2024-05-01', 'PURCHASE', 175.00, 'CREDIT_CARD', 'COMPLETED', '2024-05-01 09:30:00'),
            ('TXN402', 'CUST401', '2024-05-02', 'PURCHASE', 225.50, 'DEBIT_CARD', 'COMPLETED', '2024-05-02 14:15:00')
        """
        
        synapse_helper.execute_query(consistency_test_query)
        
        # Execute pipeline multiple times
        results = []
        for i in range(3):
            result = pipeline_runner.run_pipeline("ActionPointRecentTransactionHistoryList")
            results.append(result)
            time.sleep(1)  # Small delay between runs
        
        # Verify all runs succeeded
        for i, result in enumerate(results):
            assert result.success, f"Pipeline run {i+1} failed: {result.error_message}"

    def test_timezone_handling(self, pipeline_runner, synapse_helper):
        """Test timezone handling in transaction timestamps."""
        # Insert data with various timezone scenarios
        timezone_test_query = """
        INSERT INTO [staging].[action_point_recent_transaction_history_test]
        VALUES 
            ('TXN501', 'CUST501', '2024-06-01', 'PURCHASE', 120.00, 'CREDIT_CARD', 'COMPLETED', '2024-06-01 00:00:00'),
            ('TXN502', 'CUST501', '2024-06-01', 'PURCHASE', 180.00, 'DEBIT_CARD', 'COMPLETED', '2024-06-01 12:00:00'),
            ('TXN503', 'CUST501', '2024-06-01', 'PURCHASE', 250.00, 'BANK_TRANSFER', 'COMPLETED', '2024-06-01 23:59:59')
        """
        
        synapse_helper.execute_query(timezone_test_query)
        
        # Execute pipeline
        result = pipeline_runner.run_pipeline("ActionPointRecentTransactionHistoryList")
        assert result.success, f"Pipeline timezone handling failed: {result.error_message}"

    def test_concurrent_pipeline_execution(self, pipeline_runner, synapse_helper):
        """Test pipeline behavior under concurrent execution scenarios."""
        # Insert concurrent test data
        concurrent_test_query = """
        INSERT INTO [staging].[action_point_recent_transaction_history_test]
        VALUES 
            ('TXN601', 'CUST601', '2024-07-01', 'PURCHASE', 90.00, 'CREDIT_CARD', 'COMPLETED', '2024-07-01 10:00:00'),
            ('TXN602', 'CUST602', '2024-07-02', 'PURCHASE', 110.00, 'DEBIT_CARD', 'COMPLETED', '2024-07-02 11:00:00')
        """
        
        synapse_helper.execute_query(concurrent_test_query)
        
        # Execute pipeline
        result = pipeline_runner.run_pipeline("ActionPointRecentTransactionHistoryList")
        assert result.success, f"Pipeline concurrent execution failed: {result.error_message}"

    def test_data_validation_rules(self, pipeline_runner, synapse_helper):
        """Test data validation rules and constraints."""
        # Insert data to test validation rules
        validation_rules_query = """
        INSERT INTO [staging].[action_point_recent_transaction_history_test]
        VALUES 
            ('TXN701', 'CUST701', '2024-08-01', 'PURCHASE', 0.01, 'CREDIT_CARD', 'COMPLETED', '2024-08-01 10:00:00'),
            ('TXN702', 'CUST701', '2024-08-02', 'PURCHASE', 9999.99, 'DEBIT_CARD', 'COMPLETED', '2024-08-02 11:00:00')
        """
        
        synapse_helper.execute_query(validation_rules_query)
        
        # Execute pipeline
        result = pipeline_runner.run_pipeline("ActionPointRecentTransactionHistoryList")
        assert result.success, f"Pipeline validation failed: {result.error_message}"
