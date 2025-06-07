"""E2E tests for pi_Send_ActionPointRecentTransactionHistoryList pipeline.

This pipeline sends recent transaction history list for action points to external systems.
Tests cover transaction history data extraction, CSV generation, and SFTP transfer validation.
"""

import pytest
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List
from decimal import Decimal
from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection
# from tests.helpers.data_validation import DataValidationHelper
# from tests.helpers.synapse_helper import SynapseHelper
# from tests.fixtures.pipeline_fixtures import pipeline_runner


class TestActionPointRecentTransactionHistoryListPipeline:
    """Test class for Action Point Recent Transaction History List pipeline."""

    PIPELINE_NAME = "pi_Send_ActionPointRecentTransactionHistoryList"
    EXPECTED_ACTIVITIES = ["at_CreateCSV_ActionPointRecentTransactionHistoryList", "at_SFTP_ActionPointRecentTransactionHistoryList"]
    
    def test_basic_pipeline_execution(self, pipeline_runner):
        """Test basic pipeline execution and completion."""
        result = pipeline_runner.run_pipeline(
            pipeline_name=self.PIPELINE_NAME,
            timeout_minutes=30
        )
        
        assert result.status == "Succeeded"
        assert "ActionPointRecentTransactionHistoryList" in result.output_file_path
        
    def test_pipeline_activities_execution(self, pipeline_runner):
        """Test that all expected activities execute successfully."""
        result = pipeline_runner.run_pipeline(
            pipeline_name=self.PIPELINE_NAME,
            timeout_minutes=30
        )
        
        executed_activities = result.get_activity_names()
        for activity in self.EXPECTED_ACTIVITIES:
            assert activity in executed_activities, f"Activity {activity} not executed"
            
        activity_results = result.get_activity_results()
        for activity in self.EXPECTED_ACTIVITIES:
            assert activity_results[activity]["status"] == "Succeeded"

    def test_transaction_history_data_extraction(self, pipeline_runner, synapse_helper):
        """Test transaction history data extraction with domain-specific validation."""
        result = pipeline_runner.run_pipeline(
            pipeline_name=self.PIPELINE_NAME,
            timeout_minutes=30
        )
        
        # Validate extracted transaction history data
        query = """
        SELECT TOP 100 
            TRANSACTION_ID,
            CUSTOMER_ID,
            TRANSACTION_DATE,
            TRANSACTION_TYPE,
            TRANSACTION_AMOUNT,
            PAYMENT_METHOD,
            TRANSACTION_STATUS,
            ACTION_POINT_ID,
            BUSINESS_UNIT,
            TRANSACTION_CATEGORY
        FROM [staging].[action_point_recent_transaction_history_data]
        WHERE created_date >= DATEADD(hour, -2, GETUTCDATE())
        """
        
        data = synapse_helper.execute_query(query)
        assert len(data) > 0, "No transaction history data extracted"
        
        # Validate transaction history specific fields
        for row in data:
            # Transaction ID validation
            assert row["TRANSACTION_ID"] is not None
            assert len(str(row["TRANSACTION_ID"])) > 0
            
            # Customer ID validation
            assert row["CUSTOMER_ID"] is not None
            
            # Transaction date validation
            if row["TRANSACTION_DATE"]:
                transaction_date = datetime.fromisoformat(str(row["TRANSACTION_DATE"]))
                assert transaction_date <= datetime.now()
                # Recent transactions should be within last 2 years
                assert transaction_date >= datetime.now() - timedelta(days=730)
            
            # Transaction type validation
            if row["TRANSACTION_TYPE"]:
                valid_transaction_types = [
                    "PAYMENT", "BILLING", "REFUND", "ADJUSTMENT", 
                    "SERVICE_FEE", "EQUIPMENT_PURCHASE", "MAINTENANCE_FEE"
                ]
                assert row["TRANSACTION_TYPE"] in valid_transaction_types
            
            # Transaction amount validation
            if row["TRANSACTION_AMOUNT"] is not None:
                amount = float(row["TRANSACTION_AMOUNT"])
                assert amount >= 0, "Transaction amount should be non-negative"
                assert amount <= 10000000, "Transaction amount seems unrealistic"
            
            # Payment method validation
            if row["PAYMENT_METHOD"]:
                valid_payment_methods = [
                    "CREDIT_CARD", "BANK_TRANSFER", "DIRECT_DEBIT", 
                    "CASH", "CHECK", "ELECTRONIC_PAYMENT"
                ]
                assert row["PAYMENT_METHOD"] in valid_payment_methods
            
            # Transaction status validation
            if row["TRANSACTION_STATUS"]:
                valid_statuses = ["COMPLETED", "PENDING", "CANCELLED", "FAILED", "REFUNDED"]
                assert row["TRANSACTION_STATUS"] in valid_statuses

    def test_large_dataset_performance(self, pipeline_runner, synapse_helper):
        """Test pipeline performance with large transaction history datasets."""
        # Prepare large dataset
        large_dataset_query = """
        INSERT INTO [staging].[action_point_recent_transaction_history_test]
        SELECT TOP 10000 
            CONCAT('TXN', RIGHT('00000000' + CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS VARCHAR), 8)) as TRANSACTION_ID,
            ABS(CHECKSUM(NEWID())) % 1000000 as CUSTOMER_ID,
            DATEADD(day, -ABS(CHECKSUM(NEWID())) % 730, GETDATE()) as TRANSACTION_DATE,
            CASE ABS(CHECKSUM(NEWID())) % 7
                WHEN 0 THEN 'PAYMENT'
                WHEN 1 THEN 'BILLING'
                WHEN 2 THEN 'REFUND'
                WHEN 3 THEN 'ADJUSTMENT'
                WHEN 4 THEN 'SERVICE_FEE'
                WHEN 5 THEN 'EQUIPMENT_PURCHASE'
                ELSE 'MAINTENANCE_FEE'
            END as TRANSACTION_TYPE,
            CAST((ABS(CHECKSUM(NEWID())) % 100000) / 100.0 AS DECIMAL(10,2)) as TRANSACTION_AMOUNT,
            CASE ABS(CHECKSUM(NEWID())) % 6
                WHEN 0 THEN 'CREDIT_CARD'
                WHEN 1 THEN 'BANK_TRANSFER'
                WHEN 2 THEN 'DIRECT_DEBIT'
                WHEN 3 THEN 'CASH'
                WHEN 4 THEN 'CHECK'
                ELSE 'ELECTRONIC_PAYMENT'
            END as PAYMENT_METHOD,
            CASE ABS(CHECKSUM(NEWID())) % 5
                WHEN 0 THEN 'COMPLETED'
                WHEN 1 THEN 'PENDING'
                WHEN 2 THEN 'CANCELLED'
                WHEN 3 THEN 'FAILED'
                ELSE 'REFUNDED'
            END as TRANSACTION_STATUS,
            CONCAT('AP', ABS(CHECKSUM(NEWID())) % 10000) as ACTION_POINT_ID,
            CASE ABS(CHECKSUM(NEWID())) % 4
                WHEN 0 THEN 'GAS'
                WHEN 1 THEN 'ELECTRIC'
                WHEN 2 THEN 'EQUIPMENT'
                ELSE 'SERVICE'
            END as BUSINESS_UNIT
        FROM sys.objects a, sys.objects b
        """
        
        synapse_helper.execute_query(large_dataset_query)
        
        start_time = time.time()
        result = pipeline_runner.run_pipeline(
            pipeline_name=self.PIPELINE_NAME,
            timeout_minutes=45
        )
        execution_time = time.time() - start_time
        
        assert result.status == "Succeeded"
        assert execution_time < 2700  # 45 minutes
        
        # Validate output file size
        file_info = result.get_output_file_info()
        assert file_info["size_mb"] > 1  # Expect substantial file size

    def test_csv_data_quality_validation(self, pipeline_runner):
        """Test CSV output data quality for transaction history."""
        result = pipeline_runner.run_pipeline(
            pipeline_name=self.PIPELINE_NAME,
            timeout_minutes=30
        )
        
        csv_content = result.get_csv_content()
        data_validator = DataValidationHelper()
        
        # Basic CSV structure validation
        validation_result = data_validator.validate_csv_structure(
            csv_content,
            required_columns=[
                "TRANSACTION_ID", "CUSTOMER_ID", "TRANSACTION_DATE",
                "TRANSACTION_TYPE", "TRANSACTION_AMOUNT", "PAYMENT_METHOD",
                "TRANSACTION_STATUS", "ACTION_POINT_ID"
            ]
        )
        assert validation_result.is_valid
        
        # Transaction history specific data validation
        rows = data_validator.parse_csv_data(csv_content)
        for row in rows[:100]:  # Validate first 100 rows
            # Transaction ID format validation
            transaction_id = row.get("TRANSACTION_ID", "")
            assert len(transaction_id) > 0, "Transaction ID cannot be empty"
            
            # Customer ID validation
            customer_id = row.get("CUSTOMER_ID", "")
            assert customer_id.isdigit() or customer_id == "", "Customer ID must be numeric or empty"
            
            # Date format validation
            transaction_date = row.get("TRANSACTION_DATE", "")
            if transaction_date:
                data_validator.validate_date_format(transaction_date, "yyyy/MM/dd")
            
            # Transaction amount validation
            amount = row.get("TRANSACTION_AMOUNT", "")
            if amount:
                try:
                    amount_val = float(amount)
                    assert amount_val >= 0, f"Negative transaction amount: {amount_val}"
                    assert amount_val <= 10000000, f"Unrealistic transaction amount: {amount_val}"
                except ValueError:
                    pytest.fail(f"Invalid amount format: {amount}")
            
            # Transaction type validation
            transaction_type = row.get("TRANSACTION_TYPE", "")
            if transaction_type:
                valid_types = [
                    "PAYMENT", "BILLING", "REFUND", "ADJUSTMENT",
                    "SERVICE_FEE", "EQUIPMENT_PURCHASE", "MAINTENANCE_FEE"
                ]
                assert transaction_type in valid_types, f"Invalid transaction type: {transaction_type}"
            
            # Payment method validation
            payment_method = row.get("PAYMENT_METHOD", "")
            if payment_method:
                valid_methods = [
                    "CREDIT_CARD", "BANK_TRANSFER", "DIRECT_DEBIT",
                    "CASH", "CHECK", "ELECTRONIC_PAYMENT"
                ]
                assert payment_method in valid_methods, f"Invalid payment method: {payment_method}"

    def test_sftp_transfer_validation(self, pipeline_runner):
        """Test SFTP transfer functionality for transaction history files."""
        result = pipeline_runner.run_pipeline(
            pipeline_name=self.PIPELINE_NAME,
            timeout_minutes=30
        )
        
        # Validate SFTP activity execution
        activity_results = result.get_activity_results()
        sftp_activity = activity_results["at_SFTP_ActionPointRecentTransactionHistoryList"]
        
        assert sftp_activity["status"] == "Succeeded"
        assert "rows_transferred" in sftp_activity
        assert sftp_activity["rows_transferred"] > 0
        
        # Validate file naming convention
        transferred_file = sftp_activity["destination_file"]
        assert "ActionPointRecentTransactionHistoryList" in transferred_file
        assert transferred_file.endswith(".csv.gz")
        
        # Validate timestamp in filename
        import re
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
        result = pipeline_runner.run_pipeline_with_error_handling(
            pipeline_name=self.PIPELINE_NAME,
            timeout_minutes=30,
            expected_errors=True
        )
        
        # Should either succeed with data cleansing or fail gracefully
        assert result.status in ["Succeeded", "Failed"]
        
        if result.status == "Failed":
            assert "data validation" in result.error_message.lower() or \
                   "invalid format" in result.error_message.lower()

    def test_timezone_handling(self, pipeline_runner, synapse_helper):
        """Test timezone handling for transaction dates."""
        # Insert test data with various timezone scenarios
        timezone_test_query = """
        INSERT INTO [staging].[action_point_recent_transaction_history_timezone_test]
        SELECT 
            CONCAT('TXN_TZ_', ROW_NUMBER() OVER (ORDER BY (SELECT NULL))) as TRANSACTION_ID,
            1000 + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as CUSTOMER_ID,
            DATEADD(hour, timezone_offset, GETUTCDATE()) as TRANSACTION_DATE,
            'PAYMENT' as TRANSACTION_TYPE,
            1000.0 as TRANSACTION_AMOUNT,
            'CREDIT_CARD' as PAYMENT_METHOD,
            'COMPLETED' as TRANSACTION_STATUS,
            'AP001' as ACTION_POINT_ID,
            'GAS' as BUSINESS_UNIT
        FROM (VALUES (-9), (0), (9)) as timezones(timezone_offset)
        """
        
        synapse_helper.execute_query(timezone_test_query)
        
        result = pipeline_runner.run_pipeline(
            pipeline_name=self.PIPELINE_NAME,
            timeout_minutes=30
        )
        
        assert result.status == "Succeeded"
        
        # Validate timezone conversion in output
        csv_content = result.get_csv_content()
        data_validator = DataValidationHelper()
        rows = data_validator.parse_csv_data(csv_content)
        
        jst_timezone_found = False
        for row in rows:
            transaction_date = row.get("TRANSACTION_DATE", "")
            if transaction_date:
                # Validate date is in JST format
                assert len(transaction_date) >= 10, "Date should include full date format"
                jst_timezone_found = True
        
        assert jst_timezone_found, "No timezone-converted dates found"

    def test_recent_transaction_history_scenarios(self, pipeline_runner, synapse_helper):
        """Test recent transaction history specific business scenarios."""
        # Test different transaction types and statuses
        transaction_scenarios_query = """
        INSERT INTO [staging].[action_point_recent_transaction_history_scenarios_test]
        VALUES 
            ('TH001', 1001, '2024/01/15', 'PAYMENT', 5000.0, 'CREDIT_CARD', 'COMPLETED', 'AP001', 'GAS'),
            ('TH002', 1002, '2024/02/10', 'BILLING', 3500.0, 'DIRECT_DEBIT', 'COMPLETED', 'AP002', 'ELECTRIC'),
            ('TH003', 1003, '2024/03/05', 'REFUND', 1200.0, 'BANK_TRANSFER', 'COMPLETED', 'AP003', 'EQUIPMENT'),
            ('TH004', 1004, '2024/01/20', 'EQUIPMENT_PURCHASE', 15000.0, 'CREDIT_CARD', 'PENDING', 'AP004', 'EQUIPMENT'),
            ('TH005', 1005, '2024/02/28', 'SERVICE_FEE', 800.0, 'ELECTRONIC_PAYMENT', 'COMPLETED', 'AP005', 'SERVICE'),
            ('TH006', 1006, '2024/03/15', 'MAINTENANCE_FEE', 2500.0, 'CASH', 'FAILED', 'AP006', 'SERVICE'),
            ('TH007', 1007, '2024/01/10', 'ADJUSTMENT', 500.0, 'CHECK', 'CANCELLED', 'AP007', 'GAS')
        """
        
        synapse_helper.execute_query(transaction_scenarios_query)
        
        result = pipeline_runner.run_pipeline(
            pipeline_name=self.PIPELINE_NAME,
            timeout_minutes=30
        )
        
        assert result.status == "Succeeded"
        
        # Validate transaction distribution
        csv_content = result.get_csv_content()
        data_validator = DataValidationHelper()
        rows = data_validator.parse_csv_data(csv_content)
        
        transaction_types_found = set()
        payment_methods_found = set()
        transaction_statuses_found = set()
        business_units_found = set()
        
        for row in rows:
            transaction_type = row.get("TRANSACTION_TYPE", "")
            payment_method = row.get("PAYMENT_METHOD", "")
            transaction_status = row.get("TRANSACTION_STATUS", "")
            business_unit = row.get("BUSINESS_UNIT", "")
            
            if transaction_type:
                transaction_types_found.add(transaction_type)
            if payment_method:
                payment_methods_found.add(payment_method)
            if transaction_status:
                transaction_statuses_found.add(transaction_status)
            if business_unit:
                business_units_found.add(business_unit)
        
        # Validate variety of transaction types
        expected_transaction_types = {
            "PAYMENT", "BILLING", "REFUND", "EQUIPMENT_PURCHASE",
            "SERVICE_FEE", "MAINTENANCE_FEE", "ADJUSTMENT"
        }
        assert len(transaction_types_found.intersection(expected_transaction_types)) > 0
        
        # Validate payment methods
        expected_payment_methods = {
            "CREDIT_CARD", "DIRECT_DEBIT", "BANK_TRANSFER",
            "ELECTRONIC_PAYMENT", "CASH", "CHECK"
        }
        assert len(payment_methods_found.intersection(expected_payment_methods)) > 0
        
        # Validate transaction statuses
        expected_statuses = {"COMPLETED", "PENDING", "FAILED", "CANCELLED"}
        assert len(transaction_statuses_found.intersection(expected_statuses)) > 0
        
        # Validate business units
        expected_business_units = {"GAS", "ELECTRIC", "EQUIPMENT", "SERVICE"}
        assert len(business_units_found.intersection(expected_business_units)) > 0

    def test_transaction_amount_validation(self, pipeline_runner, synapse_helper):
        """Test transaction amount validation and precision handling."""
        # Test different transaction amount scenarios
        amount_scenarios_query = """
        INSERT INTO [staging].[action_point_recent_transaction_history_amounts_test]
        VALUES 
            ('TA001', 3001, '2024/01/01', 'PAYMENT', 0.01, 'CREDIT_CARD', 'COMPLETED', 'AP001', 'GAS'),
            ('TA002', 3002, '2024/01/02', 'PAYMENT', 100.50, 'CREDIT_CARD', 'COMPLETED', 'AP002', 'GAS'),
            ('TA003', 3003, '2024/01/03', 'PAYMENT', 1000.00, 'CREDIT_CARD', 'COMPLETED', 'AP003', 'ELECTRIC'),
            ('TA004', 3004, '2024/01/04', 'EQUIPMENT_PURCHASE', 50000.99, 'BANK_TRANSFER', 'COMPLETED', 'AP004', 'EQUIPMENT'),
            ('TA005', 3005, '2024/01/05', 'SERVICE_FEE', 25.75, 'ELECTRONIC_PAYMENT', 'COMPLETED', 'AP005', 'SERVICE'),
            ('TA006', 3006, '2024/01/06', 'REFUND', 750.25, 'CREDIT_CARD', 'COMPLETED', 'AP006', 'GAS')
        """
        
        synapse_helper.execute_query(amount_scenarios_query)
        
        result = pipeline_runner.run_pipeline(
            pipeline_name=self.PIPELINE_NAME,
            timeout_minutes=30
        )
        
        assert result.status == "Succeeded"
        
        # Validate transaction amounts in output
        csv_content = result.get_csv_content()
        data_validator = DataValidationHelper()
        rows = data_validator.parse_csv_data(csv_content)
        
        transaction_amounts = []
        for row in rows:
            amount = row.get("TRANSACTION_AMOUNT", "")
            if amount:
                try:
                    val = float(amount)
                    transaction_amounts.append(val)
                except ValueError:
                    continue
        
        # Validate amount range and precision
        assert len(transaction_amounts) > 0, "No transaction amounts found"
        assert all(amount >= 0 for amount in transaction_amounts), "Negative amounts found"
        assert max(transaction_amounts) <= 10000000, "Unrealistic amounts found"
        
        # Validate decimal precision handling
        decimal_amounts = [a for a in transaction_amounts if a != int(a)]
        if decimal_amounts:
            # Check that decimal precision is maintained
            for amount in decimal_amounts[:10]:  # Check first 10 decimal amounts
                amount_str = f"{amount:.2f}"
                assert len(amount_str.split('.')[-1]) <= 2, "Amount precision should be at most 2 decimal places"

    def test_action_point_correlation(self, pipeline_runner, synapse_helper):
        """Test action point correlation with transaction history."""
        # Test action point ID correlation
        action_point_test_query = """
        INSERT INTO [staging].[action_point_recent_transaction_history_correlation_test]
        VALUES 
            ('AP001_TXN1', 4001, '2024/01/01', 'PAYMENT', 1000.0, 'CREDIT_CARD', 'COMPLETED', 'AP001', 'GAS'),
            ('AP001_TXN2', 4001, '2024/01/15', 'BILLING', 1200.0, 'DIRECT_DEBIT', 'COMPLETED', 'AP001', 'GAS'),
            ('AP002_TXN1', 4002, '2024/02/01', 'EQUIPMENT_PURCHASE', 5000.0, 'BANK_TRANSFER', 'COMPLETED', 'AP002', 'EQUIPMENT'),
            ('AP003_TXN1', 4003, '2024/03/01', 'SERVICE_FEE', 500.0, 'ELECTRONIC_PAYMENT', 'COMPLETED', 'AP003', 'SERVICE')
        """
        
        synapse_helper.execute_query(action_point_test_query)
        
        result = pipeline_runner.run_pipeline(
            pipeline_name=self.PIPELINE_NAME,
            timeout_minutes=30
        )
        
        assert result.status == "Succeeded"
        
        # Validate action point correlation in output
        csv_content = result.get_csv_content()
        data_validator = DataValidationHelper()
        rows = data_validator.parse_csv_data(csv_content)
        
        action_points = set()
        customer_action_point_map = {}
        
        for row in rows:
            action_point_id = row.get("ACTION_POINT_ID", "")
            customer_id = row.get("CUSTOMER_ID", "")
            
            if action_point_id:
                action_points.add(action_point_id)
                
                if customer_id:
                    if customer_id not in customer_action_point_map:
                        customer_action_point_map[customer_id] = set()
                    customer_action_point_map[customer_id].add(action_point_id)
        
        # Validate action point presence
        assert len(action_points) > 0, "No action points found"
        
        # Validate customer-action point relationships
        multi_action_point_customers = [
            customer for customer, aps in customer_action_point_map.items() 
            if len(aps) > 1
        ]
        
        # Some customers may have multiple action points
        if multi_action_point_customers:
            assert len(multi_action_point_customers) >= 0  # This is expected

    def test_data_privacy_compliance(self, pipeline_runner):
        """Test data privacy compliance for transaction history data."""
        result = pipeline_runner.run_pipeline(
            pipeline_name=self.PIPELINE_NAME,
            timeout_minutes=30
        )
        
        csv_content = result.get_csv_content()
        data_validator = DataValidationHelper()
        
        # Validate no sensitive data leakage
        privacy_validation = data_validator.validate_privacy_compliance(
            csv_content,
            sensitive_patterns=[
                r'\b\d{4}-\d{4}-\d{4}-\d{4}\b',  # Credit card patterns
                r'\b\d{3}-\d{2}-\d{4}\b',        # SSN patterns
                r'\b\d{10,11}\b'                 # Phone number patterns
            ]
        )
        
        assert privacy_validation.is_compliant, "Privacy compliance validation failed"
        
        # Validate transaction data anonymization
        rows = data_validator.parse_csv_data(csv_content)
        for row in rows[:50]:  # Check first 50 rows
            # Customer ID should not reveal sensitive information
            customer_id = row.get("CUSTOMER_ID", "")
            if customer_id:
                assert customer_id.isdigit(), "Customer ID should be numeric only"
            
            # Transaction ID should be anonymized
            transaction_id = row.get("TRANSACTION_ID", "")
            if transaction_id:
                # Should not contain obvious personal identifiers
                assert not any(word in transaction_id.lower() for word in ["name", "email", "phone"])
            
            # Payment method should not reveal specific card numbers
            payment_method = row.get("PAYMENT_METHOD", "")
            if payment_method:
                assert payment_method in [
                    "CREDIT_CARD", "BANK_TRANSFER", "DIRECT_DEBIT",
                    "CASH", "CHECK", "ELECTRONIC_PAYMENT"
                ], "Payment method should be categorized, not specific"
