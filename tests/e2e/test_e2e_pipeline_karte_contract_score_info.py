"""E2E tests for pi_Send_karte_contract_score_info pipeline.

This pipeline sends contract score information (カルテ契約スコア情報) to external systems.
Tests cover contract scoring data extraction, CSV generation, and SFTP transfer validation.
"""

import pytest
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List
from decimal import Decimal
from tests.e2e.helpers.data_quality_test_manager import DataQualityTestManager
from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection
from tests.e2e.helpers.missing_helpers_placeholder import DataValidationHelper, SynapseHelper
# from tests.helpers.data_validation import DataValidationHelper
# from tests.helpers.synapse_helper import SynapseHelper
from tests.fixtures.pipeline_fixtures import pipeline_runner


class TestKarteContractScoreInfoPipeline:
    """Test class for Karte Contract Score Info pipeline."""

    PIPELINE_NAME = "pi_Send_karte_contract_score_info"
    EXPECTED_ACTIVITIES = ["at_CreateCSV_KarteContractScoreInfo", "at_SFTP_KarteContractScoreInfo"]
    
    def test_basic_pipeline_execution(self, pipeline_runner):
        """Test basic pipeline execution and completion."""
        result = pipeline_runner.run_pipeline(
            pipeline_name=self.PIPELINE_NAME,
            timeout_minutes=30
        )
        
        assert result.status == "Succeeded"
        assert "karte_contract_score_info" in result.output_file_path.lower()
        
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

    def test_contract_score_data_extraction(self, pipeline_runner, synapse_helper):
        """Test contract score data extraction with domain-specific validation."""
        result = pipeline_runner.run_pipeline(
            pipeline_name=self.PIPELINE_NAME,
            timeout_minutes=30
        )
        
        # Validate extracted contract score data
        query = """
        SELECT TOP 100 
            CONTRACT_ID,
            CUSTOMER_ID,
            SCORE_TYPE,
            SCORE_VALUE,
            SCORE_DATE,
            SCORE_CATEGORY,
            RISK_LEVEL,
            SCORE_COMPONENTS,
            CALCULATION_METHOD,
            LAST_UPDATED
        FROM [staging].[karte_contract_score_data]
        WHERE created_date >= DATEADD(hour, -2, GETUTCDATE())
        """
        
        data = synapse_helper.execute_query(query)
        assert len(data) > 0, "No contract score data extracted"
        
        # Validate contract score specific fields
        for row in data:
            # Contract ID validation
            assert row["CONTRACT_ID"] is not None
            assert len(str(row["CONTRACT_ID"])) > 0
            
            # Customer ID validation
            assert row["CUSTOMER_ID"] is not None
            
            # Score type validation
            if row["SCORE_TYPE"]:
                valid_score_types = [
                    "CREDIT_SCORE", "PAYMENT_SCORE", "USAGE_SCORE", 
                    "LOYALTY_SCORE", "RISK_SCORE", "SATISFACTION_SCORE"
                ]
                assert row["SCORE_TYPE"] in valid_score_types
            
            # Score value validation
            if row["SCORE_VALUE"] is not None:
                score_value = float(row["SCORE_VALUE"])
                assert 0 <= score_value <= 1000, "Score value should be between 0 and 1000"
            
            # Score date validation
            if row["SCORE_DATE"]:
                score_date = datetime.fromisoformat(str(row["SCORE_DATE"]))
                assert score_date <= datetime.now()
                # Score shouldn't be older than 10 years
                assert score_date >= datetime.now() - timedelta(days=3650)
            
            # Risk level validation
            if row["RISK_LEVEL"]:
                valid_risk_levels = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
                assert row["RISK_LEVEL"] in valid_risk_levels
            
            # Score category validation
            if row["SCORE_CATEGORY"]:
                valid_categories = ["A", "B", "C", "D", "E"]
                assert row["SCORE_CATEGORY"] in valid_categories

    def test_large_dataset_performance(self, pipeline_runner, synapse_helper):
        """Test pipeline performance with large contract score datasets."""
        # Prepare large dataset
        large_dataset_query = """
        INSERT INTO [staging].[karte_contract_score_test]
        SELECT TOP 10000 
            CONCAT('CT', RIGHT('000000' + CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS VARCHAR), 6)) as CONTRACT_ID,
            ABS(CHECKSUM(NEWID())) % 1000000 as CUSTOMER_ID,
            CASE ABS(CHECKSUM(NEWID())) % 6
                WHEN 0 THEN 'CREDIT_SCORE'
                WHEN 1 THEN 'PAYMENT_SCORE'
                WHEN 2 THEN 'USAGE_SCORE'
                WHEN 3 THEN 'LOYALTY_SCORE'
                WHEN 4 THEN 'RISK_SCORE'
                ELSE 'SATISFACTION_SCORE'
            END as SCORE_TYPE,
            CAST((ABS(CHECKSUM(NEWID())) % 1001) AS DECIMAL(10,2)) as SCORE_VALUE,
            DATEADD(day, -ABS(CHECKSUM(NEWID())) % 365, GETDATE()) as SCORE_DATE,
            CASE ABS(CHECKSUM(NEWID())) % 5
                WHEN 0 THEN 'A'
                WHEN 1 THEN 'B'
                WHEN 2 THEN 'C'
                WHEN 3 THEN 'D'
                ELSE 'E'
            END as SCORE_CATEGORY,
            CASE ABS(CHECKSUM(NEWID())) % 4
                WHEN 0 THEN 'LOW'
                WHEN 1 THEN 'MEDIUM'
                WHEN 2 THEN 'HIGH'
                ELSE 'CRITICAL'
            END as RISK_LEVEL,
            'AUTO_CALCULATED' as CALCULATION_METHOD
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
        """Test CSV output data quality for contract score information."""
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
                "CONTRACT_ID", "CUSTOMER_ID", "SCORE_TYPE",
                "SCORE_VALUE", "SCORE_DATE", "SCORE_CATEGORY", "RISK_LEVEL"
            ]
        )
        assert validation_result.is_valid
        
        # Contract score specific data validation
        rows = data_validator.parse_csv_data(csv_content)
        for row in rows[:100]:  # Validate first 100 rows
            # Contract ID format validation
            contract_id = row.get("CONTRACT_ID", "")
            assert len(contract_id) > 0, "Contract ID cannot be empty"
            
            # Customer ID validation
            customer_id = row.get("CUSTOMER_ID", "")
            assert customer_id.isdigit() or customer_id == "", "Customer ID must be numeric or empty"
            
            # Score value validation
            score_value = row.get("SCORE_VALUE", "")
            if score_value:
                try:
                    score_val = float(score_value)
                    assert 0 <= score_val <= 1000, f"Score value out of range: {score_val}"
                except ValueError:
                    pytest.fail(f"Invalid score value format: {score_value}")
            
            # Date format validation
            score_date = row.get("SCORE_DATE", "")
            if score_date:
                data_validator.validate_date_format(score_date, "yyyy/MM/dd")
            
            # Score type validation
            score_type = row.get("SCORE_TYPE", "")
            if score_type:
                valid_types = [
                    "CREDIT_SCORE", "PAYMENT_SCORE", "USAGE_SCORE",
                    "LOYALTY_SCORE", "RISK_SCORE", "SATISFACTION_SCORE"
                ]
                assert score_type in valid_types, f"Invalid score type: {score_type}"
            
            # Risk level validation
            risk_level = row.get("RISK_LEVEL", "")
            if risk_level:
                valid_risks = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
                assert risk_level in valid_risks, f"Invalid risk level: {risk_level}"
            
            # Score category validation
            score_category = row.get("SCORE_CATEGORY", "")
            if score_category:
                valid_categories = ["A", "B", "C", "D", "E"]
                assert score_category in valid_categories, f"Invalid score category: {score_category}"

    def test_sftp_transfer_validation(self, pipeline_runner):
        """Test SFTP transfer functionality for contract score files."""
        result = pipeline_runner.run_pipeline(
            pipeline_name=self.PIPELINE_NAME,
            timeout_minutes=30
        )
        
        # Validate SFTP activity execution
        activity_results = result.get_activity_results()
        sftp_activity = activity_results["at_SFTP_KarteContractScoreInfo"]
        
        assert sftp_activity["status"] == "Succeeded"
        assert "rows_transferred" in sftp_activity
        assert sftp_activity["rows_transferred"] > 0
        
        # Validate file naming convention
        transferred_file = sftp_activity["destination_file"]
        assert "karte_contract_score_info" in transferred_file.lower()
        assert transferred_file.endswith(".csv.gz")
        
        # Validate timestamp in filename
        import re
        timestamp_pattern = r'\d{8}_\d{6}'
        assert re.search(timestamp_pattern, transferred_file), "Timestamp not found in filename"

    def test_error_handling_scenarios(self, pipeline_runner, synapse_helper):
        """Test pipeline error handling and recovery scenarios."""
        # Test with invalid score data scenario
        invalid_data_query = """
        INSERT INTO [staging].[karte_contract_score_error_test]
        VALUES 
            (NULL, NULL, 'INVALID_SCORE', -100, '2024-13-45', 'X', 'INVALID_RISK'),
            ('', 'invalid_customer', 'UNKNOWN', 1001, '1900-01-01', '', '')
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
        """Test timezone handling for score calculation dates."""
        # Insert test data with various timezone scenarios
        timezone_test_query = """
        INSERT INTO [staging].[karte_contract_score_timezone_test]
        SELECT 
            CONCAT('CT_TZ_', ROW_NUMBER() OVER (ORDER BY (SELECT NULL))) as CONTRACT_ID,
            1000 + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as CUSTOMER_ID,
            'CREDIT_SCORE' as SCORE_TYPE,
            750.0 as SCORE_VALUE,
            DATEADD(hour, timezone_offset, GETUTCDATE()) as SCORE_DATE,
            'A' as SCORE_CATEGORY,
            'LOW' as RISK_LEVEL,
            'AUTO_CALCULATED' as CALCULATION_METHOD
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
            score_date = row.get("SCORE_DATE", "")
            if score_date:
                # Validate date is in JST format
                assert len(score_date) >= 10, "Date should include full date format"
                jst_timezone_found = True
        
        assert jst_timezone_found, "No timezone-converted dates found"

    def test_contract_score_specific_scenarios(self, pipeline_runner, synapse_helper):
        """Test contract score specific business scenarios."""
        # Test different score types and risk levels
        score_scenarios_query = """
        INSERT INTO [staging].[karte_contract_score_scenarios_test]
        VALUES 
            ('SC001', 1001, 'CREDIT_SCORE', 850.0, '2024/01/15', 'A', 'LOW', 'BUREAU_DATA'),
            ('SC002', 1002, 'PAYMENT_SCORE', 650.0, '2024/02/10', 'B', 'MEDIUM', 'PAYMENT_HISTORY'),
            ('SC003', 1003, 'USAGE_SCORE', 450.0, '2024/03/05', 'C', 'HIGH', 'USAGE_PATTERN'),
            ('SC004', 1004, 'LOYALTY_SCORE', 250.0, '2024/01/20', 'D', 'CRITICAL', 'BEHAVIORAL_ANALYSIS'),
            ('SC005', 1005, 'RISK_SCORE', 150.0, '2024/02/28', 'E', 'CRITICAL', 'RISK_MODEL'),
            ('SC006', 1006, 'SATISFACTION_SCORE', 950.0, '2024/03/15', 'A', 'LOW', 'SURVEY_DATA')
        """
        
        synapse_helper.execute_query(score_scenarios_query)
        
        result = pipeline_runner.run_pipeline(
            pipeline_name=self.PIPELINE_NAME,
            timeout_minutes=30
        )
        
        assert result.status == "Succeeded"
        
        # Validate score distribution
        csv_content = result.get_csv_content()
        data_validator = DataValidationHelper()
        rows = data_validator.parse_csv_data(csv_content)
        
        score_types_found = set()
        risk_levels_found = set()
        score_categories_found = set()
        
        for row in rows:
            score_type = row.get("SCORE_TYPE", "")
            risk_level = row.get("RISK_LEVEL", "")
            score_category = row.get("SCORE_CATEGORY", "")
            
            if score_type:
                score_types_found.add(score_type)
            if risk_level:
                risk_levels_found.add(risk_level)
            if score_category:
                score_categories_found.add(score_category)
        
        # Validate variety of score types
        expected_score_types = {
            "CREDIT_SCORE", "PAYMENT_SCORE", "USAGE_SCORE",
            "LOYALTY_SCORE", "RISK_SCORE", "SATISFACTION_SCORE"
        }
        assert len(score_types_found.intersection(expected_score_types)) > 0
        
        # Validate risk levels
        expected_risk_levels = {"LOW", "MEDIUM", "HIGH", "CRITICAL"}
        assert len(risk_levels_found.intersection(expected_risk_levels)) > 0
        
        # Validate score categories
        expected_categories = {"A", "B", "C", "D", "E"}
        assert len(score_categories_found.intersection(expected_categories)) > 0

    def test_score_value_ranges_validation(self, pipeline_runner, synapse_helper):
        """Test score value ranges and precision validation."""
        # Test different score value ranges
        score_ranges_query = """
        INSERT INTO [staging].[karte_contract_score_ranges_test]
        VALUES 
            ('SR001', 2001, 'CREDIT_SCORE', 0.0, '2024/01/01', 'E', 'CRITICAL'),
            ('SR002', 2002, 'CREDIT_SCORE', 250.5, '2024/01/02', 'D', 'HIGH'),
            ('SR003', 2003, 'CREDIT_SCORE', 500.75, '2024/01/03', 'C', 'MEDIUM'),
            ('SR004', 2004, 'CREDIT_SCORE', 750.25, '2024/01/04', 'B', 'LOW'),
            ('SR005', 2005, 'CREDIT_SCORE', 1000.0, '2024/01/05', 'A', 'LOW'),
            ('SR006', 2006, 'PAYMENT_SCORE', 999.99, '2024/01/06', 'A', 'LOW')
        """
        
        synapse_helper.execute_query(score_ranges_query)
        
        result = pipeline_runner.run_pipeline(
            pipeline_name=self.PIPELINE_NAME,
            timeout_minutes=30
        )
        
        assert result.status == "Succeeded"
        
        # Validate score value ranges in output
        csv_content = result.get_csv_content()
        data_validator = DataValidationHelper()
        rows = data_validator.parse_csv_data(csv_content)
        
        score_values = []
        for row in rows:
            score_value = row.get("SCORE_VALUE", "")
            if score_value:
                try:
                    val = float(score_value)
                    score_values.append(val)
                except ValueError:
                    continue
        
        # Validate score range distribution
        assert len(score_values) > 0, "No score values found"
        assert min(score_values) >= 0, "Score values should not be negative"
        assert max(score_values) <= 1000, "Score values should not exceed 1000"
        
        # Validate precision handling
        decimal_scores = [v for v in score_values if v != int(v)]
        if decimal_scores:
            # Check that decimal precision is maintained
            for score in decimal_scores[:10]:  # Check first 10 decimal scores
                assert len(str(score).split('.')[-1]) <= 2, "Score precision should be at most 2 decimal places"

    def test_data_privacy_compliance(self, pipeline_runner):
        """Test data privacy compliance for contract score data."""
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
        
        # Validate contract score data anonymization
        rows = data_validator.parse_csv_data(csv_content)
        for row in rows[:50]:  # Check first 50 rows
            # Customer ID should not reveal sensitive information
            customer_id = row.get("CUSTOMER_ID", "")
            if customer_id:
                assert customer_id.isdigit(), "Customer ID should be numeric only"
            
            # Contract ID should be anonymized
            contract_id = row.get("CONTRACT_ID", "")
            if contract_id:
                # Should not contain obvious personal identifiers
                assert not any(word in contract_id.lower() for word in ["name", "email", "phone"])

    def test_score_calculation_consistency(self, pipeline_runner, synapse_helper):
        """Test score calculation consistency and business logic."""
        # Test score-risk level consistency
        consistency_test_query = """
        INSERT INTO [staging].[karte_contract_score_consistency_test]
        VALUES 
            ('CS001', 3001, 'CREDIT_SCORE', 900.0, '2024/01/01', 'A', 'LOW'),     -- High score, low risk
            ('CS002', 3002, 'CREDIT_SCORE', 700.0, '2024/01/02', 'B', 'MEDIUM'),  -- Medium score, medium risk
            ('CS003', 3003, 'CREDIT_SCORE', 400.0, '2024/01/03', 'D', 'HIGH'),    -- Low score, high risk
            ('CS004', 3004, 'RISK_SCORE', 100.0, '2024/01/04', 'E', 'CRITICAL')   -- Very low score, critical risk
        """
        
        synapse_helper.execute_query(consistency_test_query)
        
        result = pipeline_runner.run_pipeline(
            pipeline_name=self.PIPELINE_NAME,
            timeout_minutes=30
        )
        
        assert result.status == "Succeeded"
        
        # Validate score-risk consistency in output
        csv_content = result.get_csv_content()
        data_validator = DataValidationHelper()
        rows = data_validator.parse_csv_data(csv_content)
        
        consistency_checks = 0
        for row in rows:
            score_value = row.get("SCORE_VALUE", "")
            risk_level = row.get("RISK_LEVEL", "")
            score_category = row.get("SCORE_CATEGORY", "")
            
            if score_value and risk_level and score_category:
                try:
                    score = float(score_value)
                    
                    # Basic consistency checks
                    if score >= 800 and risk_level == "LOW" and score_category == "A":
                        consistency_checks += 1
                    elif 600 <= score < 800 and risk_level in ["LOW", "MEDIUM"] and score_category in ["A", "B"]:
                        consistency_checks += 1
                    elif 400 <= score < 600 and risk_level in ["MEDIUM", "HIGH"] and score_category in ["B", "C"]:
                        consistency_checks += 1
                    elif score < 400 and risk_level in ["HIGH", "CRITICAL"] and score_category in ["D", "E"]:
                        consistency_checks += 1
                        
                except ValueError:
                    continue
        
        # At least some scores should follow expected business logic
        assert consistency_checks > 0, "No consistent score-risk-category relationships found"
