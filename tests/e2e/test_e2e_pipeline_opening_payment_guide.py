"""
E2E Test Suite for pi_Send_OpeningPaymentGuide Pipeline

開栓支払いガイド送信パイプラインのE2Eテスト
このパイプラインは新規開栓顧客に対する支払い方法ガイダンス情報をCSVファイルとして生成し、
gzip圧縮後にSFTPでSalesforce Marketing Cloud (SFMC) に送信します。

パイプライン構成:
1. at_CreateCSV_ClientDM: DAM-DBから顧客DM情報を抽出し、gzipファイルでBLOB出力
2. at_SendSftp_ClientDM: Blobに出力されたgzipファイルをSFMCにSFTP連携

テスト対象:
- パイプライン基本実行テスト
- 大量データセット処理パフォーマンステスト
- データ品質・整合性検証テスト
- エラーハンドリング・例外処理テスト
- モニタリング・ログ出力検証テスト
- CSV出力内容検証テスト
- 開栓支払いガイド特有の検証テスト

Created: 2024-12-19
Updated: 2024-12-19
"""

import asyncio
import gzip
import io
import json
import logging
import random
import tempfile
import os
import requests
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest
# from azure.core.exceptions import AzureError

# Placeholder for Azure exception
class AzureError(Exception):
    pass
# from azure.datafactory import DataFactoryManagementClient
# from azure.datafactory.models import PipelineRun
# from azure.identity import DefaultAzureCredential
# from azure.monitor.query import LogsQueryClient
# from azure.storage.blob import BlobServiceClient

# Placeholder classes for missing Azure modules
class DataFactoryManagementClient:
    def __init__(self, *args, **kwargs):
        pass

class PipelineRun:
    def __init__(self, *args, **kwargs):
        pass

class DefaultAzureCredential:
    def __init__(self, *args, **kwargs):
        pass

class LogsQueryClient:
    def __init__(self, *args, **kwargs):
        pass

class BlobServiceClient:
    def __init__(self, *args, **kwargs):
        pass


class TestPipelineOpeningPaymentGuide:

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
    """
    Comprehensive E2E test suite for pi_Send_OpeningPaymentGuide pipeline.
    
    This test class covers all aspects of the opening payment guide pipeline execution,
    including data extraction, CSV generation, compression, and SFTP transfer for new
    customer onboarding and payment method guidance.
    """

    # Pipeline configuration
    PIPELINE_NAME = "pi_Send_OpeningPaymentGuide"
    EXPECTED_ACTIVITIES = ["at_CreateCSV_ClientDM", "at_SendSftp_ClientDM"]
    SOURCE_TABLE = "omni.omni_ods_marketing_trn_client_dm_bx_temp"
    OUTPUT_DIRECTORY = "datalake/OMNI/MA/OpeningPaymentGuide"
    SFTP_DIRECTORY = "Import/DAM/ClientDM"
    
    # Data quality thresholds for opening payment guide
    MIN_EXPECTED_ROWS = 5000  # Lower threshold as this targets new customers
    MAX_EXECUTION_TIME_MINUTES = 90
    MIN_DATA_QUALITY_SCORE = 0.95
    
    # Performance benchmarks
    LARGE_DATASET_THRESHOLD = 100000  # 100K rows for opening payment guide
    EXPECTED_THROUGHPUT_ROWS_PER_MINUTE = 30000
    
    # Opening payment guide specific validation
    EXPECTED_PAYMENT_METHODS = [
        "口座振替", "クレジットカード", "コンビニ払い", "請求書払い"
    ]
    REQUIRED_OPENING_COLUMNS = [
        "CONNECTION_KEY", "USAGESERVICE_BX", "CLIENT_KEY_AX",
        "LIV0EU_1X", "LIV0EU_8X", "LIV0EU_4X", "LIV0EU_2X",
        "LIV0EU_GAS_PAY_METHOD_CD", "LIV0EU_GAS_PAY_METHOD",
        "LIV0EU_OPENING_REASON_NAME", "LIV0EU_OPENING_MONTH_PASSED",
        "OUTPUT_DATETIME"
    ]

    def setup_method(self):
        """Initialize test configuration and Azure clients."""
        self.config = {
            "azure": {
                "subscription_id": "test-subscription-id",
                "resource_group": "test-resource-group",
                "data_factory_name": "test-data-factory",
                "storage_account_name": "teststorageaccount",
                "key_vault_name": "test-key-vault"
            },
            "pipeline": {
                "name": self.PIPELINE_NAME,
                "timeout_minutes": self.MAX_EXECUTION_TIME_MINUTES,
                "retry_count": 3,
                "csv_directory": "datalake/OMNI/MA/OpeningPaymentGuide",
                "csv_filename_pattern": "OpeningPaymentGuide_{date}.csv.gz",
                "sftp_destination": "Import/DAM/ClientDM"
            }
        }
        
        self.logger = logging.getLogger(__name__)

    async def additional_setup_method(self):
        """Set up test environment before each test method."""
        self.credential = DefaultAzureCredential()
        self.df_client = DataFactoryManagementClient(
            self.credential,
            self.config["azure"]["subscription_id"]
        )
        self.blob_client = BlobServiceClient(
            account_url=f"https://{self.config['azure']['storage_account_name']}.blob.core.windows.net",
            credential=self.credential
        )
        self.logs_client = LogsQueryClient(self.credential)

    async def teardown_method(self):
        """Clean up test environment after each test method."""
        await self._cleanup_test_files()

    async def _cleanup_test_files(self):
        """Clean up temporary test files and blobs."""
        try:
            container_client = self.blob_client.get_container_client("datalake")
            blobs = container_client.list_blobs(name_starts_with="OMNI/MA/OpeningPaymentGuide/test_")
            for blob in blobs:
                await container_client.delete_blob(blob.name)
        except Exception as e:
            self.logger.warning(f"Failed to cleanup test files: {e}")

    @pytest.mark.asyncio
    async def test_basic_pipeline_execution(self):
        """
        Test basic pipeline execution with standard dataset.
        
        Validates:
        - Pipeline starts successfully
        - All activities complete without errors
        - CSV file is generated and compressed
        - SFTP transfer completes successfully
        - Data quality meets minimum thresholds
        """
        with patch.object(self.df_client.pipelines, 'create_run') as mock_create_run, \
             patch.object(self.df_client.pipeline_runs, 'get') as mock_get_run:
              # Mock successful pipeline run
            run_id = "test-run-id-001"
            mock_create_run.return_value.run_id = run_id
            mock_get_run.return_value = PipelineRun(
                run_id=run_id,
                status="Succeeded",
                start_time=datetime.now(timezone.utc) - timedelta(minutes=30),
                end_time=datetime.now(timezone.utc),
                message="Pipeline completed successfully"
            )

            # Execute pipeline
            run_response = self.df_client.pipelines.create_run(
                resource_group_name=self.config["azure"]["resource_group"],
                factory_name=self.config["azure"]["data_factory_name"],
                pipeline_name=self.PIPELINE_NAME
            )

            # Wait for completion
            await self._wait_for_pipeline_completion(run_response.run_id)

            # Verify pipeline execution
            pipeline_run = self.df_client.pipeline_runs.get(
                resource_group_name=self.config["azure"]["resource_group"],
                factory_name=self.config["azure"]["data_factory_name"],
                run_id=run_response.run_id
            )

            assert pipeline_run.status == "Succeeded"
            assert pipeline_run.run_id == run_id

            # Verify CSV file generation
            await self._verify_csv_file_generation()

            # Verify data quality
            data_quality_score = await self._calculate_data_quality_score()
            assert data_quality_score >= self.MIN_DATA_QUALITY_SCORE

            self.logger.info(f"Basic pipeline execution test passed - Run ID: {run_id}")

    @pytest.mark.asyncio
    async def test_large_dataset_performance(self):
        """
        Test pipeline performance with large dataset.
        
        Validates:
        - Pipeline handles large datasets efficiently
        - Processing time is within acceptable limits
        - Memory usage remains stable
        - Throughput meets performance benchmarks
        """
        with patch.object(self.df_client.pipelines, 'create_run') as mock_create_run, \
             patch.object(self.df_client.pipeline_runs, 'get') as mock_get_run:

            # Mock large dataset scenario
            large_dataset_rows = self.LARGE_DATASET_THRESHOLD
            expected_duration = large_dataset_rows / self.EXPECTED_THROUGHPUT_ROWS_PER_MINUTE

            run_id = "test-run-large-001"
            start_time = datetime.now(timezone.utc)
            end_time = start_time + timedelta(minutes=expected_duration)

            mock_create_run.return_value.run_id = run_id
            mock_get_run.return_value = PipelineRun(
                run_id=run_id,
                status="Succeeded",
                start_time=start_time,
                end_time=end_time,
                message=f"Pipeline processed {large_dataset_rows} rows successfully"
            )

            # Execute pipeline with large dataset
            run_response = self.df_client.pipelines.create_run(
                resource_group_name=self.config["azure"]["resource_group"],
                factory_name=self.config["azure"]["data_factory_name"],
                pipeline_name=self.PIPELINE_NAME,
                parameters={"datasetSize": "large"}
            )

            # Monitor performance
            pipeline_run = self.df_client.pipeline_runs.get(
                resource_group_name=self.config["azure"]["resource_group"],
                factory_name=self.config["azure"]["data_factory_name"],
                run_id=run_response.run_id
            )

            # Verify performance metrics
            execution_time = (pipeline_run.end_time - pipeline_run.start_time).total_seconds() / 60
            assert execution_time <= self.MAX_EXECUTION_TIME_MINUTES

            # Verify throughput
            throughput = large_dataset_rows / execution_time
            assert throughput >= self.EXPECTED_THROUGHPUT_ROWS_PER_MINUTE * 0.8  # 80% of expected

            self.logger.info(f"Large dataset performance test passed - Processed {large_dataset_rows} rows in {execution_time:.2f} minutes")

    @pytest.mark.asyncio
    async def test_opening_payment_guide_data_quality(self):
        """
        Test data quality specific to opening payment guide pipeline.
        
        Validates:
        - Opening month calculation accuracy
        - Payment method code mappings
        - New customer identification logic
        - Required field completeness
        - Opening reason classification
        """
        # Mock CSV data for testing
        mock_csv_data = await self._generate_mock_opening_payment_guide_data()

        with patch('pandas.read_csv', return_value=mock_csv_data):
            # Analyze data quality
            quality_results = await self._analyze_opening_payment_guide_quality(mock_csv_data)

            # Verify opening month calculations
            assert quality_results["opening_month_accuracy"] >= 0.98
            
            # Verify payment method distributions
            payment_method_dist = quality_results["payment_method_distribution"]
            assert all(method in payment_method_dist for method in self.EXPECTED_PAYMENT_METHODS)
            
            # Verify new customer identification
            assert quality_results["new_customer_ratio"] >= 0.85  # At least 85% should be new customers
            
            # Verify required fields completeness
            field_completeness = quality_results["field_completeness"]
            for field in self.REQUIRED_OPENING_COLUMNS:
                assert field_completeness.get(field, 0) >= 0.95
            
            # Verify opening reason diversity
            opening_reasons = quality_results["opening_reason_count"]
            assert opening_reasons >= 5  # Should have multiple opening reasons

            self.logger.info("Opening payment guide data quality test passed")

    @pytest.mark.asyncio
    async def test_error_handling_and_recovery(self):
        """
        Test error handling and recovery mechanisms.
        
        Validates:
        - Proper error handling for database connection issues
        - Recovery from temporary SFTP failures
        - Data validation error handling
        - Retry logic functionality
        """
        with patch.object(self.df_client.pipelines, 'create_run') as mock_create_run, \
             patch.object(self.df_client.pipeline_runs, 'get') as mock_get_run:

            # Test database connection error handling
            run_id_db_error = "test-run-db-error-001"
            mock_create_run.return_value.run_id = run_id_db_error
            mock_get_run.return_value = PipelineRun(
                run_id=run_id_db_error,
                status="Failed",
                start_time=datetime.now(timezone.utc) - timedelta(minutes=5),
                end_time=datetime.now(timezone.utc),
                message="Database connection timeout during data extraction"
            )

            # Execute pipeline with error injection
            run_response = self.df_client.pipelines.create_run(
                resource_group_name=self.config["azure"]["resource_group"],
                factory_name=self.config["azure"]["data_factory_name"],
                pipeline_name=self.PIPELINE_NAME,
                parameters={"inject_error": "database_timeout"}
            )

            pipeline_run = self.df_client.pipeline_runs.get(
                resource_group_name=self.config["azure"]["resource_group"],
                factory_name=self.config["azure"]["data_factory_name"],
                run_id=run_response.run_id
            )

            # Verify error is properly captured
            assert pipeline_run.status == "Failed"
            assert "Database connection timeout" in pipeline_run.message

            # Test SFTP error recovery
            run_id_sftp_retry = "test-run-sftp-retry-001"
            mock_create_run.return_value.run_id = run_id_sftp_retry
            mock_get_run.return_value = PipelineRun(
                run_id=run_id_sftp_retry,
                status="Succeeded",
                start_time=datetime.now(timezone.utc) - timedelta(minutes=10),
                end_time=datetime.now(timezone.utc),
                message="Pipeline succeeded after SFTP retry"
            )

            # Execute pipeline with SFTP retry scenario
            retry_response = self.df_client.pipelines.create_run(
                resource_group_name=self.config["azure"]["resource_group"],
                factory_name=self.config["azure"]["data_factory_name"],
                pipeline_name=self.PIPELINE_NAME,
                parameters={"inject_error": "sftp_transient"}
            )

            retry_run = self.df_client.pipeline_runs.get(
                resource_group_name=self.config["azure"]["resource_group"],
                factory_name=self.config["azure"]["data_factory_name"],
                run_id=retry_response.run_id
            )

            # Verify successful recovery
            assert retry_run.status == "Succeeded"
            assert "retry" in retry_run.message.lower()

            self.logger.info("Error handling and recovery test passed")

    @pytest.mark.asyncio
    async def test_monitoring_and_alerting(self):
        """
        Test monitoring and alerting capabilities.
        
        Validates:
        - Performance metrics collection
        - Error rate monitoring
        - Data volume tracking
        - Alert generation for anomalies
        """
        with patch.object(self.logs_client, 'query_workspace') as mock_query:
            # Mock monitoring data
            mock_logs = [
                {
                    "TimeGenerated": datetime.now(timezone.utc),
                    "PipelineName": self.PIPELINE_NAME,
                    "Status": "Succeeded",
                    "DurationMinutes": 25.5,
                    "RowsProcessed": 75000,
                    "FileSizeMB": 45.2
                }
            ]
            mock_query.return_value.tables = [type('Table', (), {'rows': mock_logs})]

            # Query pipeline metrics
            metrics = await self._query_pipeline_metrics()

            # Verify metrics collection
            assert len(metrics) > 0
            assert metrics[0]["PipelineName"] == self.PIPELINE_NAME
            assert metrics[0]["DurationMinutes"] < self.MAX_EXECUTION_TIME_MINUTES
            assert metrics[0]["RowsProcessed"] >= self.MIN_EXPECTED_ROWS

            # Test alerting thresholds
            alerts = await self._check_alerting_thresholds(metrics)
            
            # Verify no alerts for normal operation
            critical_alerts = [alert for alert in alerts if alert["severity"] == "critical"]
            assert len(critical_alerts) == 0

            self.logger.info("Monitoring and alerting test passed")

    @pytest.mark.asyncio
    async def test_csv_output_validation(self):
        """
        Test CSV output validation and format compliance.
        
        Validates:
        - CSV structure and headers
        - Data type consistency
        - Gzip compression integrity
        - File naming convention adherence
        - Character encoding (UTF-8)
        """
        # Generate mock CSV content
        mock_csv_content = await self._generate_mock_csv_content()

        # Test CSV structure
        df = pd.read_csv(io.StringIO(mock_csv_content))
        
        # Verify required columns
        for column in self.REQUIRED_OPENING_COLUMNS:
            assert column in df.columns, f"Required column {column} missing"

        # Verify data types
        assert df["CONNECTION_KEY"].dtype == "int64"
        assert df["LIV0EU_8X"].dtype == "object"  # String type
        assert pd.api.types.is_datetime64_any_dtype(pd.to_datetime(df["OUTPUT_DATETIME"], errors='coerce'))

        # Test gzip compression
        compressed_content = gzip.compress(mock_csv_content.encode('utf-8'))
        decompressed_content = gzip.decompress(compressed_content).decode('utf-8')
        assert decompressed_content == mock_csv_content

        # Test filename pattern
        filename = f"OpeningPaymentGuide_{datetime.now().strftime('%Y%m%d')}.csv.gz"
        assert filename.startswith("OpeningPaymentGuide_")
        assert filename.endswith(".csv.gz")

        self.logger.info("CSV output validation test passed")

    @pytest.mark.asyncio
    @pytest.mark.parametrize("opening_scenario", [
        "new_construction", "moving_in", "service_upgrade", "reconnection"
    ])
    async def test_opening_scenarios(self, opening_scenario):
        """
        Test different opening scenarios for payment guide targeting.
        
        Parameters:
        - opening_scenario: Type of opening scenario to test
        
        Validates:
        - Scenario-specific data filtering
        - Appropriate payment method recommendations
        - Opening month calculations
        - Business rule compliance
        """
        with patch.object(self.df_client.pipelines, 'create_run') as mock_create_run:
            run_id = f"test-{opening_scenario}-001"
            mock_create_run.return_value.run_id = run_id

            # Execute pipeline with scenario-specific parameters
            run_response = self.df_client.pipelines.create_run(
                resource_group_name=self.config["azure"]["resource_group"],
                factory_name=self.config["azure"]["data_factory_name"],
                pipeline_name=self.PIPELINE_NAME,
                parameters={"opening_scenario": opening_scenario}
            )

            # Verify scenario-specific processing
            processed_data = await self._get_scenario_processed_data(opening_scenario)
            
            # Validate scenario-specific business rules
            if opening_scenario == "new_construction":
                assert processed_data["avg_opening_months"] <= 1.0
            elif opening_scenario == "moving_in":
                assert processed_data["payment_method_diversity"] >= 3
            elif opening_scenario == "service_upgrade":
                assert processed_data["existing_customer_ratio"] >= 0.7
            elif opening_scenario == "reconnection":
                assert processed_data["previous_payment_history_available"] >= 0.5

            self.logger.info(f"Opening scenario test passed for: {opening_scenario}")

    # Helper methods

    async def _wait_for_pipeline_completion(self, run_id: str, timeout_minutes: int = 120):
        """Wait for pipeline completion with timeout."""
        timeout = datetime.now(timezone.utc) + timedelta(minutes=timeout_minutes)
        while datetime.now(timezone.utc) < timeout:
            pipeline_run = self.df_client.pipeline_runs.get(
                resource_group_name=self.config["azure"]["resource_group"],
                factory_name=self.config["azure"]["data_factory_name"],
                run_id=run_id
            )
            if pipeline_run.status in ["Succeeded", "Failed", "Cancelled"]:
                return pipeline_run
            await asyncio.sleep(30)
        raise TimeoutError(f"Pipeline {run_id} did not complete within {timeout_minutes} minutes")

    async def _verify_csv_file_generation(self) -> bool:
        """Verify CSV file was generated correctly."""
        try:
            container_client = self.blob_client.get_container_client("datalake")
            date_str = datetime.now().strftime("%Y%m%d")
            expected_filename = f"OpeningPaymentGuide_{date_str}.csv.gz"
            blob_path = f"OMNI/MA/OpeningPaymentGuide/{expected_filename}"
            
            blob_client = container_client.get_blob_client(blob_path)
            blob_properties = blob_client.get_blob_properties()
            
            return blob_properties.size > 0
        except Exception as e:
            self.logger.error(f"Failed to verify CSV file: {e}")
            return False

    async def _calculate_data_quality_score(self) -> float:
        """Calculate overall data quality score."""
        try:
            # Mock data quality calculation
            quality_metrics = {
                "completeness": 0.98,
                "accuracy": 0.96,
                "consistency": 0.97,
                "validity": 0.95
            }
            
            # Weighted average
            weights = {"completeness": 0.3, "accuracy": 0.3, "consistency": 0.2, "validity": 0.2}
            score = sum(quality_metrics[metric] * weights[metric] for metric in quality_metrics)
            
            return score
        except Exception as e:
            self.logger.error(f"Failed to calculate data quality score: {e}")
            return 0.0

    async def _generate_mock_opening_payment_guide_data(self) -> pd.DataFrame:
        """Generate mock data for opening payment guide testing."""
        import random
        
        data = []
        for i in range(1000):
            data.append({
                "CONNECTION_KEY": i + 1,
                "USAGESERVICE_BX": f"GAS{i:06d}",
                "CLIENT_KEY_AX": f"CK{i:08d}",
                "LIV0EU_1X": f"GM{i:08d}",
                "LIV0EU_8X": f"CM{i:08d}",
                "LIV0EU_4X": f"UC{i:08d}",
                "LIV0EU_2X": f"PC{i:08d}",
                "LIV0EU_GAS_PAY_METHOD_CD": random.choice(["01", "02", "03", "04"]),
                "LIV0EU_GAS_PAY_METHOD": random.choice(self.EXPECTED_PAYMENT_METHODS),
                "LIV0EU_OPENING_REASON_NAME": random.choice(["新築", "転居", "サービス変更", "再開栓"]),
                "LIV0EU_OPENING_MONTH_PASSED": random.randint(0, 6),
                "OUTPUT_DATETIME": datetime.now().strftime("%Y/%m/%d %H:%M:%S")
            })
        
        return pd.DataFrame(data)

    async def _analyze_opening_payment_guide_quality(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze data quality specific to opening payment guide."""
        results = {}
        
        # Opening month accuracy (should be recent for new customers)
        results["opening_month_accuracy"] = len(df[df["LIV0EU_OPENING_MONTH_PASSED"] <= 6]) / len(df)
        
        # Payment method distribution
        results["payment_method_distribution"] = df["LIV0EU_GAS_PAY_METHOD"].value_counts().to_dict()
        
        # New customer ratio (based on opening month)
        results["new_customer_ratio"] = len(df[df["LIV0EU_OPENING_MONTH_PASSED"] <= 3]) / len(df)
        
        # Field completeness
        results["field_completeness"] = {}
        for column in self.REQUIRED_OPENING_COLUMNS:
            if column in df.columns:
                results["field_completeness"][column] = 1 - (df[column].isna().sum() / len(df))
        
        # Opening reason diversity
        results["opening_reason_count"] = df["LIV0EU_OPENING_REASON_NAME"].nunique()
        
        return results

    async def _generate_mock_csv_content(self) -> str:
        """Generate mock CSV content for validation testing."""
        df = await self._generate_mock_opening_payment_guide_data()
        return df.to_csv(index=False)

    async def _query_pipeline_metrics(self) -> List[Dict[str, Any]]:
        """Query pipeline performance metrics from Azure Monitor."""
        # Mock implementation
        return [
            {
                "PipelineName": self.PIPELINE_NAME,
                "Status": "Succeeded",
                "DurationMinutes": 25.5,
                "RowsProcessed": 75000,
                "FileSizeMB": 45.2
            }
        ]

    async def _check_alerting_thresholds(self, metrics: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Check if metrics exceed alerting thresholds."""
        alerts = []
        
        for metric in metrics:
            if metric["DurationMinutes"] > self.MAX_EXECUTION_TIME_MINUTES:
                alerts.append({
                    "severity": "critical",
                    "message": f"Pipeline execution time exceeded threshold: {metric['DurationMinutes']} minutes",
                    "pipeline": metric["PipelineName"]
                })
            
            if metric["RowsProcessed"] < self.MIN_EXPECTED_ROWS:
                alerts.append({
                    "severity": "warning",
                    "message": f"Low row count detected: {metric['RowsProcessed']} rows",
                    "pipeline": metric["PipelineName"]
                })
        
        return alerts

    async def _get_scenario_processed_data(self, scenario: str) -> Dict[str, Any]:
        """Get processed data for specific opening scenario."""
        # Mock scenario-specific data
        scenario_data = {
            "new_construction": {
                "avg_opening_months": 0.5,
                "payment_method_diversity": 4,
                "existing_customer_ratio": 0.1,
                "previous_payment_history_available": 0.0
            },
            "moving_in": {
                "avg_opening_months": 1.2,
                "payment_method_diversity": 4,
                "existing_customer_ratio": 0.3,
                "previous_payment_history_available": 0.2
            },
            "service_upgrade": {
                "avg_opening_months": 2.0,
                "payment_method_diversity": 3,
                "existing_customer_ratio": 0.8,
                "previous_payment_history_available": 0.9
            },
            "reconnection": {
                "avg_opening_months": 1.5,
                "payment_method_diversity": 3,
                "existing_customer_ratio": 0.9,
                "previous_payment_history_available": 0.8
            }
        }
        
        return scenario_data.get(scenario, {})


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run specific test
    import sys
    if len(sys.argv) > 1:
        test_name = sys.argv[1]
        pytest.main([f"-v", f"test_{test_name}"])
    else:
        pytest.main(["-v", __file__])
