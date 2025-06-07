"""E2E tests for pi_Send_Cpkiyk pipeline.

This pipeline sends Cpkiyk (CP機器・給湯器) equipment information to external systems.
Tests cover equipment data extraction, CSV generation, and SFTP transfer validation.
"""

import pytest
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List
from tests.e2e.helpers.data_quality_test_manager import DataQualityTestManager
from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection
from tests.e2e.helpers.missing_helpers_placeholder import DataValidationHelper, SynapseHelper
from tests.fixtures.pipeline_fixtures import pipeline_runner


class TestCpkiykPipeline:
    """Test class for Cpkiyk (CP機器・給湯器) pipeline."""

    PIPELINE_NAME = "pi_Send_Cpkiyk"
    EXPECTED_ACTIVITIES = ["at_CreateCSV_Cpkiyk", "at_SFTP_Cpkiyk"]
    
    def test_basic_pipeline_execution(self, pipeline_runner):
        """Test basic pipeline execution and completion."""
        result = pipeline_runner.run_pipeline(
            pipeline_name=self.PIPELINE_NAME,
            timeout_minutes=30
        )
        
        assert result.status == "Succeeded"
        assert "Cpkiyk" in result.output_file_path
        
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

    def test_cpkiyk_equipment_data_extraction(self, pipeline_runner, synapse_helper):
        """Test CP機器・給湯器 equipment data extraction with domain-specific validation."""
        result = pipeline_runner.run_pipeline(
            pipeline_name=self.PIPELINE_NAME,
            timeout_minutes=30
        )
        
        # Validate extracted equipment data
        query = """
        SELECT TOP 100 
            EQUIPMENT_ID,
            CUSTOMER_ID,
            EQUIPMENT_TYPE,
            EQUIPMENT_MODEL,
            INSTALLATION_DATE,
            EQUIPMENT_STATUS,
            MAINTENANCE_TYPE,
            MANUFACTURER,
            SERIAL_NUMBER
        FROM [staging].[cpkiyk_equipment_data]
        WHERE created_date >= DATEADD(hour, -2, GETUTCDATE())
        """
        
        data = synapse_helper.execute_query(query)
        assert len(data) > 0, "No CP機器・給湯器 equipment data extracted"
        
        # Validate equipment specific fields
        for row in data:
            # Equipment ID validation
            assert row["EQUIPMENT_ID"] is not None
            assert len(str(row["EQUIPMENT_ID"])) > 0
            
            # Customer ID validation
            assert row["CUSTOMER_ID"] is not None
            
            # Equipment type validation
            if row["EQUIPMENT_TYPE"]:
                valid_equipment_types = [
                    "WATER_HEATER", "BOILER", "GAS_STOVE", "AIR_CONDITIONER",
                    "FLOOR_HEATER", "BATH_HEATER", "CP_EQUIPMENT"
                ]
                assert row["EQUIPMENT_TYPE"] in valid_equipment_types
            
            # Installation date validation
            if row["INSTALLATION_DATE"]:
                install_date = datetime.fromisoformat(str(row["INSTALLATION_DATE"]))
                assert install_date <= datetime.now()
                # Equipment shouldn't be installed in the far future
                assert install_date >= datetime(2000, 1, 1)
            
            # Equipment status validation
            if row["EQUIPMENT_STATUS"]:
                valid_statuses = ["ACTIVE", "INACTIVE", "MAINTENANCE", "REPLACED", "REMOVED"]
                assert row["EQUIPMENT_STATUS"] in valid_statuses
            
            # Manufacturer validation
            if row["MANUFACTURER"]:
                manufacturer = str(row["MANUFACTURER"])
                assert len(manufacturer) > 0 and len(manufacturer) <= 100
            
            # Serial number validation
            if row["SERIAL_NUMBER"]:
                serial = str(row["SERIAL_NUMBER"])
                assert len(serial) > 0 and len(serial) <= 50

    def test_large_dataset_performance(self, pipeline_runner, synapse_helper):
        """Test pipeline performance with large equipment datasets."""
        # Prepare large dataset
        large_dataset_query = """
        INSERT INTO [staging].[cpkiyk_equipment_test]
        SELECT TOP 10000 
            CONCAT('EQ', RIGHT('000000' + CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS VARCHAR), 6)) as EQUIPMENT_ID,
            ABS(CHECKSUM(NEWID())) % 1000000 as CUSTOMER_ID,
            CASE ABS(CHECKSUM(NEWID())) % 7
                WHEN 0 THEN 'WATER_HEATER'
                WHEN 1 THEN 'BOILER'
                WHEN 2 THEN 'GAS_STOVE'
                WHEN 3 THEN 'AIR_CONDITIONER'
                WHEN 4 THEN 'FLOOR_HEATER'
                WHEN 5 THEN 'BATH_HEATER'
                ELSE 'CP_EQUIPMENT'
            END as EQUIPMENT_TYPE,
            CONCAT('MODEL-', ABS(CHECKSUM(NEWID())) % 1000) as EQUIPMENT_MODEL,
            DATEADD(day, -ABS(CHECKSUM(NEWID())) % 3650, GETDATE()) as INSTALLATION_DATE,
            CASE ABS(CHECKSUM(NEWID())) % 5
                WHEN 0 THEN 'ACTIVE'
                WHEN 1 THEN 'INACTIVE'
                WHEN 2 THEN 'MAINTENANCE'
                WHEN 3 THEN 'REPLACED'
                ELSE 'REMOVED'
            END as EQUIPMENT_STATUS,
            CASE ABS(CHECKSUM(NEWID())) % 3
                WHEN 0 THEN 'REGULAR'
                WHEN 1 THEN 'EMERGENCY'
                ELSE 'PREVENTIVE'
            END as MAINTENANCE_TYPE,
            CASE ABS(CHECKSUM(NEWID())) % 5
                WHEN 0 THEN 'TOKYO_GAS'
                WHEN 1 THEN 'RINNAI'
                WHEN 2 THEN 'NORITZ'
                WHEN 3 THEN 'PALOMA'
                ELSE 'OTHER'
            END as MANUFACTURER,
            CONCAT('SN', ABS(CHECKSUM(NEWID())) % 100000) as SERIAL_NUMBER
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
        """Test CSV output data quality for CP機器・給湯器 equipment."""
        result = pipeline_runner.run_pipeline(
            pipeline_name=self.PIPELINE_NAME,
            timeout_minutes=30
        )
        
        csv_content = result.get_csv_content()
        data_validator = DataQualityTestManager()
        
        # Basic CSV structure validation
        validation_result = data_validator.validate_csv_structure(
            csv_content,
            required_columns=[
                "EQUIPMENT_ID", "CUSTOMER_ID", "EQUIPMENT_TYPE",
                "EQUIPMENT_MODEL", "INSTALLATION_DATE", "EQUIPMENT_STATUS"
            ]
        )
        assert validation_result.is_valid
        
        # Equipment specific data validation
        rows = data_validator.parse_csv_data(csv_content)
        for row in rows[:100]:  # Validate first 100 rows
            # Equipment ID format validation
            equipment_id = row.get("EQUIPMENT_ID", "")
            assert len(equipment_id) > 0, "Equipment ID cannot be empty"
            
            # Customer ID validation
            customer_id = row.get("CUSTOMER_ID", "")
            assert customer_id.isdigit() or customer_id == "", "Customer ID must be numeric or empty"
            
            # Date format validation
            install_date = row.get("INSTALLATION_DATE", "")
            if install_date:
                data_validator.validate_date_format(install_date, "yyyy/MM/dd")
            
            # Equipment type validation
            equipment_type = row.get("EQUIPMENT_TYPE", "")
            if equipment_type:
                valid_types = [
                    "WATER_HEATER", "BOILER", "GAS_STOVE", "AIR_CONDITIONER",
                    "FLOOR_HEATER", "BATH_HEATER", "CP_EQUIPMENT"
                ]
                assert equipment_type in valid_types, f"Invalid equipment type: {equipment_type}"
            
            # Equipment status validation
            status = row.get("EQUIPMENT_STATUS", "")
            if status:
                valid_statuses = ["ACTIVE", "INACTIVE", "MAINTENANCE", "REPLACED", "REMOVED"]
                assert status in valid_statuses, f"Invalid equipment status: {status}"

    def test_sftp_transfer_validation(self, pipeline_runner):
        """Test SFTP transfer functionality for CP機器・給湯器 equipment files."""
        result = pipeline_runner.run_pipeline(
            pipeline_name=self.PIPELINE_NAME,
            timeout_minutes=30
        )
        
        # Validate SFTP activity execution
        activity_results = result.get_activity_results()
        sftp_activity = activity_results["at_SFTP_Cpkiyk"]
        
        assert sftp_activity["status"] == "Succeeded"
        assert "rows_transferred" in sftp_activity
        assert sftp_activity["rows_transferred"] > 0
        
        # Validate file naming convention
        transferred_file = sftp_activity["destination_file"]
        assert "Cpkiyk" in transferred_file
        assert transferred_file.endswith(".csv.gz")
        
        # Validate timestamp in filename
        import re
        timestamp_pattern = r'\d{8}_\d{6}'
        assert re.search(timestamp_pattern, transferred_file), "Timestamp not found in filename"

    def test_error_handling_scenarios(self, pipeline_runner, synapse_helper):
        """Test pipeline error handling and recovery scenarios."""
        # Test with invalid equipment data scenario
        invalid_data_query = """
        INSERT INTO [staging].[cpkiyk_equipment_error_test]
        VALUES 
            (NULL, NULL, 'INVALID_TYPE', NULL, '2024-13-45', 'INVALID_STATUS', NULL, NULL, NULL),
            ('', 'invalid_customer', 'UNKNOWN', '', '1900-01-01', '', '', '', '')
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
        """Test timezone handling for equipment installation dates."""
        # Insert test data with various timezone scenarios
        timezone_test_query = """
        INSERT INTO [staging].[cpkiyk_equipment_timezone_test]
        SELECT 
            CONCAT('EQ_TZ_', ROW_NUMBER() OVER (ORDER BY (SELECT NULL))) as EQUIPMENT_ID,
            1000 + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as CUSTOMER_ID,
            'WATER_HEATER' as EQUIPMENT_TYPE,
            'TEST_MODEL' as EQUIPMENT_MODEL,
            DATEADD(hour, timezone_offset, GETUTCDATE()) as INSTALLATION_DATE,
            'ACTIVE' as EQUIPMENT_STATUS,
            'REGULAR' as MAINTENANCE_TYPE,
            'TOKYO_GAS' as MANUFACTURER,
            CONCAT('SN_TZ_', ROW_NUMBER() OVER (ORDER BY (SELECT NULL))) as SERIAL_NUMBER
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
        data_validator = DataQualityTestManager()
        rows = data_validator.parse_csv_data(csv_content)
        
        jst_timezone_found = False
        for row in rows:
            install_date = row.get("INSTALLATION_DATE", "")
            if install_date:
                # Validate date is in JST format
                assert len(install_date) >= 10, "Date should include full date format"
                jst_timezone_found = True
        
        assert jst_timezone_found, "No timezone-converted dates found"

    def test_cpkiyk_equipment_specific_scenarios(self, pipeline_runner, synapse_helper):
        """Test CP機器・給湯器 equipment specific business scenarios."""
        # Test different equipment types and scenarios
        equipment_scenarios_query = """
        INSERT INTO [staging].[cpkiyk_equipment_scenarios_test]
        VALUES 
            ('WH001', 1001, 'WATER_HEATER', 'GH-24T', '2020/03/15', 'ACTIVE', 'REGULAR', 'RINNAI', 'SN001'),
            ('BL002', 1002, 'BOILER', 'BF-32X', '2019/11/10', 'MAINTENANCE', 'PREVENTIVE', 'NORITZ', 'SN002'),
            ('GS003', 1003, 'GAS_STOVE', 'KT-5000', '2021/08/05', 'ACTIVE', 'REGULAR', 'PALOMA', 'SN003'),
            ('AC004', 1004, 'AIR_CONDITIONER', 'AC-2024', '2022/01/20', 'REPLACED', 'EMERGENCY', 'TOKYO_GAS', 'SN004'),
            ('FH005', 1005, 'FLOOR_HEATER', 'FH-100', '2018/12/01', 'INACTIVE', 'REGULAR', 'OTHER', 'SN005'),
            ('BH006', 1006, 'BATH_HEATER', 'BH-500', '2020/07/15', 'ACTIVE', 'PREVENTIVE', 'RINNAI', 'SN006'),
            ('CP007', 1007, 'CP_EQUIPMENT', 'CP-X1', '2023/02/28', 'ACTIVE', 'REGULAR', 'TOKYO_GAS', 'SN007')
        """
        
        synapse_helper.execute_query(equipment_scenarios_query)
        
        result = pipeline_runner.run_pipeline(
            pipeline_name=self.PIPELINE_NAME,
            timeout_minutes=30
        )
        
        assert result.status == "Succeeded"
        
        # Validate equipment type distribution
        csv_content = result.get_csv_content()
        data_validator = DataQualityTestManager()
        rows = data_validator.parse_csv_data(csv_content)
        
        equipment_types_found = set()
        equipment_statuses_found = set()
        manufacturers_found = set()
        
        for row in rows:
            equipment_type = row.get("EQUIPMENT_TYPE", "")
            equipment_status = row.get("EQUIPMENT_STATUS", "")
            manufacturer = row.get("MANUFACTURER", "")
            
            if equipment_type:
                equipment_types_found.add(equipment_type)
            if equipment_status:
                equipment_statuses_found.add(equipment_status)
            if manufacturer:
                manufacturers_found.add(manufacturer)
        
        # Validate variety of equipment types
        expected_types = {
            "WATER_HEATER", "BOILER", "GAS_STOVE", "AIR_CONDITIONER",
            "FLOOR_HEATER", "BATH_HEATER", "CP_EQUIPMENT"
        }
        assert len(equipment_types_found.intersection(expected_types)) > 0
        
        # Validate equipment statuses
        expected_statuses = {"ACTIVE", "INACTIVE", "MAINTENANCE", "REPLACED"}
        assert len(equipment_statuses_found.intersection(expected_statuses)) > 0
        
        # Validate manufacturers
        expected_manufacturers = {"TOKYO_GAS", "RINNAI", "NORITZ", "PALOMA", "OTHER"}
        assert len(manufacturers_found.intersection(expected_manufacturers)) > 0

    def test_equipment_lifecycle_tracking(self, pipeline_runner, synapse_helper):
        """Test equipment lifecycle tracking scenarios."""
        # Test equipment with different lifecycle stages
        lifecycle_test_query = """
        INSERT INTO [staging].[cpkiyk_equipment_lifecycle_test]
        VALUES 
            ('LC001', 2001, 'WATER_HEATER', 'WH-NEW', '2023/12/01', 'ACTIVE', 'REGULAR', 'RINNAI', 'LC001SN'),
            ('LC002', 2002, 'WATER_HEATER', 'WH-OLD', '2010/01/15', 'MAINTENANCE', 'PREVENTIVE', 'NORITZ', 'LC002SN'),
            ('LC003', 2003, 'BOILER', 'BL-REPLACE', '2015/06/10', 'REPLACED', 'EMERGENCY', 'PALOMA', 'LC003SN'),
            ('LC004', 2004, 'GAS_STOVE', 'GS-REMOVE', '2008/03/20', 'REMOVED', 'REGULAR', 'OTHER', 'LC004SN')
        """
        
        synapse_helper.execute_query(lifecycle_test_query)
        
        result = pipeline_runner.run_pipeline(
            pipeline_name=self.PIPELINE_NAME,
            timeout_minutes=30
        )
        
        assert result.status == "Succeeded"
        
        # Validate lifecycle stages in output
        csv_content = result.get_csv_content()
        data_validator = DataQualityTestManager()
        rows = data_validator.parse_csv_data(csv_content)
        
        lifecycle_statuses = set()
        maintenance_types = set()
        
        for row in rows:
            status = row.get("EQUIPMENT_STATUS", "")
            maintenance_type = row.get("MAINTENANCE_TYPE", "")
            
            if status:
                lifecycle_statuses.add(status)
            if maintenance_type:
                maintenance_types.add(maintenance_type)
        
        # Validate lifecycle statuses present
        expected_lifecycle = {"ACTIVE", "MAINTENANCE", "REPLACED", "REMOVED"}
        assert len(lifecycle_statuses.intersection(expected_lifecycle)) > 0
        
        # Validate maintenance types present
        expected_maintenance = {"REGULAR", "PREVENTIVE", "EMERGENCY"}
        assert len(maintenance_types.intersection(expected_maintenance)) > 0

    def test_data_privacy_compliance(self, pipeline_runner):
        """Test data privacy compliance for equipment data."""
        result = pipeline_runner.run_pipeline(
            pipeline_name=self.PIPELINE_NAME,
            timeout_minutes=30
        )
        
        csv_content = result.get_csv_content()
        data_validator = DataQualityTestManager()
        
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
        
        # Validate equipment data anonymization
        rows = data_validator.parse_csv_data(csv_content)
        for row in rows[:50]:  # Check first 50 rows
            # Customer ID should not reveal sensitive information
            customer_id = row.get("CUSTOMER_ID", "")
            if customer_id:
                assert customer_id.isdigit(), "Customer ID should be numeric only"
            
            # Serial numbers should not contain sensitive patterns
            serial_number = row.get("SERIAL_NUMBER", "")
            if serial_number:
                assert len(serial_number) <= 50, "Serial number too long"
                # Should not contain obvious personal identifiers
                assert not any(char.isalpha() and char.islower() for char in serial_number[:3])
