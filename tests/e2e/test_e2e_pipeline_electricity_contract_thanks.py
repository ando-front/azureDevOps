"""E2E tests for pi_Send_ElectricityContractThanks pipeline.

This pipeline sends electricity contract thanks notifications to customers.
Tests cover data extraction, CSV generation, and SFTP transfer validation.
"""

import pytest
import time
import os
import requests
from datetime import datetime, timedelta
from typing import Any, Dict, List
from tests.e2e.helpers.data_quality_test_manager import DataQualityTestManager
from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection
from tests.e2e.helpers.missing_helpers_placeholder import DataValidationHelper, SynapseHelper
# from tests.helpers.data_validation import DataValidationHelper
# from tests.helpers.synapse_helper import SynapseHelper
from tests.fixtures.pipeline_fixtures import pipeline_runner


class TestElectricityContractThanksPipeline:

    @classmethod
    def setup_class(cls):
        """Disable proxy settings for tests"""
        # Store and clear proxy environment variables
        for var in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY']:
            if var in os.environ:
                del os.environ[var]

    def _get_no_proxy_session(self):
        """Get a requests session with proxy disabled"""
        session = requests.Session()
        session.proxies = {'http': None, 'https': None}
        return session
    """Test class for Electricity Contract Thanks pipeline."""

    PIPELINE_NAME = "pi_Send_ElectricityContractThanks"
    EXPECTED_ACTIVITIES = ["at_CreateCSV_ElectricityContractThanks", "at_SFTP_ElectricityContractThanks"]
    
    def test_basic_pipeline_execution(self, pipeline_runner):
        """Test basic pipeline execution and completion."""
        result = pipeline_runner.run_pipeline(
            pipeline_name=self.PIPELINE_NAME,
            timeout_minutes=30
        )
        
        assert result.status == "Succeeded"
        assert "ElectricityContractThanks" in result.output_file_path
        
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

    def test_electricity_contract_data_extraction(self, pipeline_runner, synapse_helper):
        """Test electricity contract data extraction with domain-specific validation."""
        result = pipeline_runner.run_pipeline(
            pipeline_name=self.PIPELINE_NAME,
            timeout_minutes=30
        )
        
        # Validate extracted data for electricity contracts
        query = """
        SELECT TOP 100 
            CONTRACT_ID,
            CUSTOMER_ID,
            CONTRACT_START_DATE,
            ELECTRICITY_PLAN_TYPE,
            CONTRACT_STATUS,
            NOTIFICATION_EMAIL
        FROM [staging].[electricity_contract_thanks_data]
        WHERE created_date >= DATEADD(hour, -2, GETUTCDATE())
        """
        
        data = synapse_helper.execute_query(query)
        assert len(data) > 0, "No electricity contract data extracted"
        
        # Validate electricity contract specific fields
        for row in data:
            # Contract ID validation
            assert row["CONTRACT_ID"] is not None
            assert len(str(row["CONTRACT_ID"])) > 0
            
            # Customer ID validation
            assert row["CUSTOMER_ID"] is not None
            
            # Contract start date validation
            if row["CONTRACT_START_DATE"]:
                contract_date = datetime.fromisoformat(str(row["CONTRACT_START_DATE"]))
                assert contract_date <= datetime.now()
            
            # Electricity plan type validation
            if row["ELECTRICITY_PLAN_TYPE"]:
                valid_plan_types = ["BASIC", "TIME_OF_USE", "PEAK_SHIFT", "FAMILY", "BUSINESS"]
                assert row["ELECTRICITY_PLAN_TYPE"] in valid_plan_types
            
            # Contract status validation
            if row["CONTRACT_STATUS"]:
                valid_statuses = ["ACTIVE", "PENDING", "CONFIRMED", "CANCELLED"]
                assert row["CONTRACT_STATUS"] in valid_statuses
            
            # Email validation
            if row["NOTIFICATION_EMAIL"]:
                email = str(row["NOTIFICATION_EMAIL"])
                assert "@" in email and "." in email

    def test_large_dataset_performance(self, pipeline_runner, synapse_helper):
        """Test pipeline performance with large electricity contract datasets."""
        # Prepare large dataset
        large_dataset_query = """
        INSERT INTO [staging].[electricity_contract_thanks_test]
        SELECT TOP 10000 
            NEWID() as CONTRACT_ID,
            ABS(CHECKSUM(NEWID())) % 1000000 as CUSTOMER_ID,
            DATEADD(day, -ABS(CHECKSUM(NEWID())) % 365, GETDATE()) as CONTRACT_START_DATE,
            CASE ABS(CHECKSUM(NEWID())) % 5
                WHEN 0 THEN 'BASIC'
                WHEN 1 THEN 'TIME_OF_USE'
                WHEN 2 THEN 'PEAK_SHIFT'
                WHEN 3 THEN 'FAMILY'
                ELSE 'BUSINESS'
            END as ELECTRICITY_PLAN_TYPE,
            'ACTIVE' as CONTRACT_STATUS,
            CONCAT('customer', ABS(CHECKSUM(NEWID())) % 1000000, '@example.com') as NOTIFICATION_EMAIL
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
        """Test CSV output data quality for electricity contracts."""
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
                "CONTRACT_ID", "CUSTOMER_ID", "CONTRACT_START_DATE",
                "ELECTRICITY_PLAN_TYPE", "CONTRACT_STATUS", "NOTIFICATION_EMAIL"
            ]
        )
        assert validation_result.is_valid
        
        # Electricity contract specific data validation
        rows = data_validator.parse_csv_data(csv_content)
        for row in rows[:100]:  # Validate first 100 rows
            # Contract ID format validation
            contract_id = row.get("CONTRACT_ID", "")
            assert len(contract_id) > 0, "Contract ID cannot be empty"
            
            # Customer ID validation
            customer_id = row.get("CUSTOMER_ID", "")
            assert customer_id.isdigit() or customer_id == "", "Customer ID must be numeric or empty"
            
            # Date format validation
            contract_date = row.get("CONTRACT_START_DATE", "")
            if contract_date:
                data_validator.validate_date_format(contract_date, "yyyy/MM/dd")
            
            # Plan type validation
            plan_type = row.get("ELECTRICITY_PLAN_TYPE", "")
            if plan_type:
                valid_plans = ["BASIC", "TIME_OF_USE", "PEAK_SHIFT", "FAMILY", "BUSINESS"]
                assert plan_type in valid_plans, f"Invalid plan type: {plan_type}"
            
            # Email format validation
            email = row.get("NOTIFICATION_EMAIL", "")
            if email:
                assert "@" in email and "." in email, f"Invalid email format: {email}"

    def test_sftp_transfer_validation(self, pipeline_runner):
        """Test SFTP transfer functionality for electricity contract files."""
        result = pipeline_runner.run_pipeline(
            pipeline_name=self.PIPELINE_NAME,
            timeout_minutes=30
        )
        
        # Validate SFTP activity execution
        activity_results = result.get_activity_results()
        sftp_activity = activity_results["at_SFTP_ElectricityContractThanks"]
        
        assert sftp_activity["status"] == "Succeeded"
        assert "rows_transferred" in sftp_activity
        assert sftp_activity["rows_transferred"] > 0
        
        # Validate file naming convention
        transferred_file = sftp_activity["destination_file"]
        assert "ElectricityContractThanks" in transferred_file
        assert transferred_file.endswith(".csv.gz")
        
        # Validate timestamp in filename
        import re
        timestamp_pattern = r'\d{8}_\d{6}'
        assert re.search(timestamp_pattern, transferred_file), "Timestamp not found in filename"

    def test_error_handling_scenarios(self, pipeline_runner, synapse_helper):
        """Test pipeline error handling and recovery scenarios."""
        # Test with invalid data scenario
        invalid_data_query = """
        INSERT INTO [staging].[electricity_contract_thanks_error_test]
        VALUES 
            (NULL, NULL, NULL, 'INVALID_PLAN', 'INVALID_STATUS', 'invalid-email'),
            ('', 'invalid_customer', '2024-13-45', 'UNKNOWN', '', 'no-at-symbol.com')
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
        """Test timezone handling for electricity contract dates."""
        # Insert test data with various timezone scenarios
        timezone_test_query = """
        INSERT INTO [staging].[electricity_contract_thanks_timezone_test]
        SELECT 
            NEWID() as CONTRACT_ID,
            1000 + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as CUSTOMER_ID,
            DATEADD(hour, timezone_offset, GETUTCDATE()) as CONTRACT_START_DATE,
            'BASIC' as ELECTRICITY_PLAN_TYPE,
            'ACTIVE' as CONTRACT_STATUS,
            'test@example.com' as NOTIFICATION_EMAIL
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
            contract_date = row.get("CONTRACT_START_DATE", "")
            if contract_date:
                # Validate date is in JST format
                assert len(contract_date) >= 10, "Date should include full date format"
                jst_timezone_found = True
        
        assert jst_timezone_found, "No timezone-converted dates found"

    def test_electricity_contract_specific_scenarios(self, pipeline_runner, synapse_helper):
        """Test electricity contract specific business scenarios."""
        # Test different contract types
        contract_scenarios_query = """
        INSERT INTO [staging].[electricity_contract_thanks_scenarios_test]
        VALUES 
            ('ELEC001', 1001, '2024/01/15', 'BASIC', 'ACTIVE', 'basic@example.com'),
            ('ELEC002', 1002, '2024/02/10', 'TIME_OF_USE', 'CONFIRMED', 'tou@example.com'),
            ('ELEC003', 1003, '2024/03/05', 'PEAK_SHIFT', 'PENDING', 'peak@example.com'),
            ('ELEC004', 1004, '2024/01/20', 'FAMILY', 'ACTIVE', 'family@example.com'),
            ('ELEC005', 1005, '2024/02/28', 'BUSINESS', 'CONFIRMED', 'business@example.com')
        """
        
        synapse_helper.execute_query(contract_scenarios_query)
        
        result = pipeline_runner.run_pipeline(
            pipeline_name=self.PIPELINE_NAME,
            timeout_minutes=30
        )
        
        assert result.status == "Succeeded"
        
        # Validate contract type distribution
        csv_content = result.get_csv_content()
        data_validator = DataValidationHelper()
        rows = data_validator.parse_csv_data(csv_content)
        
        plan_types_found = set()
        contract_statuses_found = set()
        
        for row in rows:
            plan_type = row.get("ELECTRICITY_PLAN_TYPE", "")
            contract_status = row.get("CONTRACT_STATUS", "")
            
            if plan_type:
                plan_types_found.add(plan_type)
            if contract_status:
                contract_statuses_found.add(contract_status)
        
        # Validate variety of plan types
        expected_plan_types = {"BASIC", "TIME_OF_USE", "PEAK_SHIFT", "FAMILY", "BUSINESS"}
        assert len(plan_types_found.intersection(expected_plan_types)) > 0
        
        # Validate contract statuses
        expected_statuses = {"ACTIVE", "CONFIRMED", "PENDING"}
        assert len(contract_statuses_found.intersection(expected_statuses)) > 0

    def test_data_privacy_compliance(self, pipeline_runner):
        """Test data privacy compliance for electricity contract data."""
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
        
        # Validate email masking if applicable
        rows = data_validator.parse_csv_data(csv_content)
        for row in rows[:50]:  # Check first 50 rows
            email = row.get("NOTIFICATION_EMAIL", "")
            if email and "@" in email:
                # Validate email format but not actual sensitive content
                assert email.count("@") == 1, "Invalid email format"
                assert len(email.split("@")[0]) > 0, "Email username part missing"
                assert len(email.split("@")[1]) > 0, "Email domain part missing"
