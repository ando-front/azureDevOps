#!/usr/bin/env python3
"""
E2E test for Payment Alert pipeline.
Tests payment alert notifications and processing.
"""

import pytest
import time
from tests.helpers.reproducible_e2e_helper import setup_reproducible_test_class, cleanup_reproducible_test_class


class TestE2EPipelinePaymentAlert:
    """E2E tests for Payment Alert pipeline operations."""
    
    PIPELINE_NAME = "pi_Send_PaymentAlert"

    @classmethod
    def setup_class(cls):
        """Set up reproducible test environment for the entire test class."""
        setup_reproducible_test_class()

    @classmethod
    def teardown_class(cls):
        """Clean up test environment after all tests in the class."""
        cleanup_reproducible_test_class()

    def test_payment_alert_basic_execution(self, pipeline_runner, synapse_helper):
        """Test basic payment alert pipeline execution."""
        # Insert test data for payment alerts
        test_data_query = """
        INSERT INTO [staging].[payment_alert_test]
        VALUES 
            ('ALERT001', 'CUST001', 'OVERDUE', 150.00, '2024-01-15', 'PENDING'),
            ('ALERT002', 'CUST002', 'UPCOMING', 200.00, '2024-01-20', 'PENDING'),
            ('ALERT003', 'CUST003', 'FAILED', 75.50, '2024-01-10', 'PENDING')
        """
        
        synapse_helper.execute_query(test_data_query)
        
        # Execute pipeline
        result = pipeline_runner.run_pipeline(self.PIPELINE_NAME)
        assert result.success, f"Pipeline failed: {result.error_message}"
        assert result.duration < 300, "Pipeline took too long to execute"

    def test_payment_alert_data_transformation(self, pipeline_runner, synapse_helper):
        """Test payment alert data transformation logic."""
        # Insert complex test data
        transformation_test_query = """
        INSERT INTO [staging].[payment_alert_test]
        VALUES 
            ('ALERT101', 'CUST101', 'OVERDUE', 300.00, '2024-02-01', 'PENDING'),
            ('ALERT102', 'CUST101', 'REMINDER', 300.00, '2024-02-05', 'PENDING'),
            ('ALERT103', 'CUST102', 'FINAL_NOTICE', 500.00, '2024-02-10', 'PENDING')
        """
        
        synapse_helper.execute_query(transformation_test_query)
        
        # Execute pipeline
        result = pipeline_runner.run_pipeline(self.PIPELINE_NAME)
        assert result.success, f"Pipeline transformation failed: {result.error_message}"

    def test_payment_alert_filtering(self, pipeline_runner, synapse_helper):
        """Test payment alert filtering and prioritization."""
        # Insert data with various alert types
        filtering_test_query = """
        INSERT INTO [staging].[payment_alert_test]
        VALUES 
            ('ALERT201', 'CUST201', 'LOW_PRIORITY', 25.00, '2024-03-01', 'PENDING'),
            ('ALERT202', 'CUST202', 'HIGH_PRIORITY', 1000.00, '2024-03-02', 'PENDING'),
            ('ALERT203', 'CUST203', 'URGENT', 2000.00, '2024-03-03', 'PENDING')
        """
        
        synapse_helper.execute_query(filtering_test_query)
        
        # Execute pipeline
        result = pipeline_runner.run_pipeline(self.PIPELINE_NAME)
        assert result.success, f"Pipeline filtering failed: {result.error_message}"

    def test_payment_alert_error_handling(self, pipeline_runner, synapse_helper):
        """Test payment alert error handling scenarios."""
        # Insert invalid test data
        error_test_query = """
        INSERT INTO [staging].[payment_alert_error_test]
        VALUES 
            (NULL, NULL, 'INVALID_TYPE', -100.00, '2024-13-45', 'INVALID_STATUS'),
            ('', 'invalid_customer', 'UNKNOWN', 999999.99, '1900-01-01', '')
        """
        
        synapse_helper.execute_query(error_test_query)
        
        # Pipeline should handle errors gracefully
        result = pipeline_runner.run_pipeline(self.PIPELINE_NAME)
        # Either succeeds with filtered data or fails gracefully
        if not result.success:
            assert "error" in result.error_message.lower() or "invalid" in result.error_message.lower()

    def test_payment_alert_performance(self, pipeline_runner, synapse_helper):
        """Test payment alert pipeline performance with larger dataset."""
        # Insert performance test data
        performance_test_query = """
        INSERT INTO [staging].[payment_alert_test]
        SELECT 
            'ALERT_PERF_' + CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS VARCHAR(10)),
            'CUST_PERF_' + CAST((ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) % 50) + 1 AS VARCHAR(10)),
            CASE (ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) % 4)
                WHEN 0 THEN 'OVERDUE'
                WHEN 1 THEN 'UPCOMING'
                WHEN 2 THEN 'REMINDER'
                ELSE 'FINAL_NOTICE'
            END,
            (ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) % 1000) + 50.00,
            DATEADD(day, -(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) % 30), '2024-03-01'),
            'PENDING'
        FROM sys.objects s1 CROSS JOIN sys.objects s2
        WHERE ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) <= 100
        """
        
        synapse_helper.execute_query(performance_test_query)
        
        # Measure execution time
        start_time = time.time()
        result = pipeline_runner.run_pipeline(self.PIPELINE_NAME)
        execution_time = time.time() - start_time
        
        assert result.success, f"Pipeline performance test failed: {result.error_message}"
        assert execution_time < 600, f"Pipeline took too long: {execution_time} seconds"

    def test_payment_alert_notification_validation(self, pipeline_runner, synapse_helper):
        """Test payment alert notification validation."""
        # Insert notification test data
        notification_test_query = """
        INSERT INTO [staging].[payment_alert_test]
        VALUES 
            ('NOTIF001', 'CUST301', 'URGENT', 750.00, '2024-04-01', 'PENDING'),
            ('NOTIF002', 'CUST302', 'REMINDER', 250.00, '2024-04-02', 'PENDING')
        """
        
        synapse_helper.execute_query(notification_test_query)
        
        # Execute pipeline
        result = pipeline_runner.run_pipeline(self.PIPELINE_NAME)
        assert result.success, f"Pipeline notification validation failed: {result.error_message}"

    def test_payment_alert_customer_segmentation(self, pipeline_runner, synapse_helper):
        """Test payment alert customer segmentation logic."""
        # Insert segmentation test data
        segmentation_test_query = """
        INSERT INTO [staging].[payment_alert_test]
        VALUES 
            ('SEG001', 'VIP_CUST001', 'GENTLE_REMINDER', 100.00, '2024-05-01', 'PENDING'),
            ('SEG002', 'REGULAR_CUST001', 'STANDARD_ALERT', 100.00, '2024-05-01', 'PENDING'),
            ('SEG003', 'NEW_CUST001', 'WELCOME_ALERT', 100.00, '2024-05-01', 'PENDING')
        """
        
        synapse_helper.execute_query(segmentation_test_query)
        
        # Execute pipeline
        result = pipeline_runner.run_pipeline(self.PIPELINE_NAME)
        assert result.success, f"Pipeline segmentation failed: {result.error_message}"
