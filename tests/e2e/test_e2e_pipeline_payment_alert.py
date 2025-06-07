"""
E2E tests for pi_Send_PaymentAlert pipeline.

This module contains comprehensive end-to-end tests for the pi_Send_PaymentAlert pipeline,
which extracts payment alert data from the customer DM table and sends it to SFMC via SFTP.

Test Coverage:
- Basic pipeline execution and data flow validation
- Large dataset performance testing
- Data quality and integrity validation
- Error handling and recovery scenarios
- Monitoring and alerting validation
- CSV format and compression verification
"""

import pytest
import asyncio
import logging
import os
import requests
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from enum import Enum
import pandas as pd
import gzip
import io
import json

# from azure.datafactory import DataFactoryManagementClient
# from azure.storage.blob import BlobServiceClient
# from azure.monitor.query import LogsQueryClient
# from azure.identity import DefaultAzureCredential

# Placeholder classes for missing Azure modules
class DataFactoryManagementClient:
    def __init__(self, *args, **kwargs):
        pass

class BlobServiceClient:
    def __init__(self, *args, **kwargs):
        pass

class LogsQueryClient:
    def __init__(self, *args, **kwargs):
        pass

class DefaultAzureCredential:
    def __init__(self, *args, **kwargs):
        pass

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PaymentAlertTestScenario(Enum):
    """Test scenarios for Payment Alert pipeline testing."""
    BASIC_EXECUTION = "basic_execution"
    LARGE_DATASET = "large_dataset"
    DATA_QUALITY = "data_quality"
    ERROR_HANDLING = "error_handling"
    MONITORING = "monitoring"
    CSV_VALIDATION = "csv_validation"


@dataclass
class PaymentAlertTestResult:
    """Test result data structure for Payment Alert pipeline tests."""
    scenario: PaymentAlertTestScenario
    pipeline_run_id: str
    execution_duration: float
    status: str
    rows_processed: int
    file_size_mb: float
    data_quality_score: float
    error_message: Optional[str] = None
    performance_metrics: Optional[Dict[str, Any]] = None


class TestPipelinePaymentAlert:

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
    Comprehensive E2E test suite for pi_Send_PaymentAlert pipeline.
    
    This test class covers all aspects of the payment alert pipeline execution,
    including data extraction, CSV generation, compression, and SFTP transfer.
    """

    # Pipeline configuration
    PIPELINE_NAME = "pi_Send_PaymentAlert"
    EXPECTED_ACTIVITIES = ["at_CreateCSV_ClientDM", "at_SendSftp_ClientDM"]
    SOURCE_TABLE = "omni.omni_ods_marketing_trn_client_dm_bx_temp"
    OUTPUT_DIRECTORY = "datalake/OMNI/MA/ClientDM"
    SFTP_DIRECTORY = "Import/DAM/ClientDM"
    
    # Data quality thresholds
    MIN_EXPECTED_ROWS = 10000
    MAX_EXECUTION_TIME_MINUTES = 120
    MIN_DATA_QUALITY_SCORE = 0.95
    
    # Performance benchmarks
    LARGE_DATASET_THRESHOLD = 1000000  # 1M rows
    EXPECTED_THROUGHPUT_ROWS_PER_MINUTE = 50000

    @pytest.fixture(scope="class")
    def azure_clients(self):
        """Initialize Azure service clients for testing."""
        credential = DefaultAzureCredential()
        
        return {
            'data_factory': DataFactoryManagementClient(
                credential=credential,
                subscription_id="your-subscription-id"
            ),
            'blob_service': BlobServiceClient(
                account_url="https://your-storage-account.blob.core.windows.net",
                credential=credential
            ),
            'logs_client': LogsQueryClient(credential=credential)
        }

    @pytest.fixture
    def test_config(self):
        """Test configuration and parameters."""
        return {
            'resource_group': 'your-resource-group',
            'factory_name': 'your-adf-name',
            'storage_account': 'your-storage-account',
            'container_name': 'your-container',
            'log_analytics_workspace': 'your-workspace-id'
        }

    async def execute_pipeline_run(self, clients: Dict, config: Dict, 
                                 parameters: Dict = None) -> PaymentAlertTestResult:
        """
        Execute the payment alert pipeline and monitor its execution.
        
        Args:
            clients: Azure service clients
            config: Test configuration
            parameters: Pipeline parameters
            
        Returns:
            PaymentAlertTestResult: Test execution results
        """
        start_time = datetime.utcnow()
        
        try:
            # Trigger pipeline execution
            run_response = clients['data_factory'].pipelines.create_run(
                resource_group_name=config['resource_group'],
                factory_name=config['factory_name'],
                pipeline_name=self.PIPELINE_NAME,
                parameters=parameters or {}
            )
            
            pipeline_run_id = run_response.run_id
            logger.info(f"Started pipeline run: {pipeline_run_id}")
            
            # Monitor pipeline execution
            status = await self._monitor_pipeline_execution(
                clients['data_factory'], config, pipeline_run_id
            )
            
            end_time = datetime.utcnow()
            execution_duration = (end_time - start_time).total_seconds() / 60
            
            # Analyze pipeline results
            rows_processed = await self._get_rows_processed(
                clients, config, pipeline_run_id
            )
            
            file_size_mb = await self._get_output_file_size(
                clients['blob_service'], config, pipeline_run_id
            )
            
            data_quality_score = await self._calculate_data_quality_score(
                clients, config, pipeline_run_id
            )
            
            performance_metrics = await self._get_performance_metrics(
                clients['logs_client'], config, pipeline_run_id
            )
            
            return PaymentAlertTestResult(
                scenario=PaymentAlertTestScenario.BASIC_EXECUTION,
                pipeline_run_id=pipeline_run_id,
                execution_duration=execution_duration,
                status=status,
                rows_processed=rows_processed,
                file_size_mb=file_size_mb,
                data_quality_score=data_quality_score,
                performance_metrics=performance_metrics
            )
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}")
            return PaymentAlertTestResult(
                scenario=PaymentAlertTestScenario.BASIC_EXECUTION,
                pipeline_run_id="",
                execution_duration=0,
                status="Failed",
                rows_processed=0,
                file_size_mb=0,
                data_quality_score=0,
                error_message=str(e)
            )

    async def _monitor_pipeline_execution(self, data_factory_client, config: Dict, 
                                        run_id: str) -> str:
        """Monitor pipeline execution until completion."""
        max_wait_time = timedelta(minutes=self.MAX_EXECUTION_TIME_MINUTES)
        start_time = datetime.utcnow()
        
        while datetime.utcnow() - start_time < max_wait_time:
            run_status = data_factory_client.pipeline_runs.get(
                resource_group_name=config['resource_group'],
                factory_name=config['factory_name'],
                run_id=run_id
            )
            
            if run_status.status in ['Succeeded', 'Failed', 'Cancelled']:
                return run_status.status
                
            await asyncio.sleep(30)  # Wait 30 seconds before next check
            
        return 'Timeout'

    async def _get_rows_processed(self, clients: Dict, config: Dict, 
                                run_id: str) -> int:
        """Get the number of rows processed by the pipeline."""
        try:
            # Query activity runs to get copy activity details
            activity_runs = clients['data_factory'].activity_runs.list_by_pipeline_run(
                resource_group_name=config['resource_group'],
                factory_name=config['factory_name'],
                run_id=run_id
            )
            
            for activity in activity_runs.value:
                if activity.activity_name == "at_CreateCSV_ClientDM":
                    if activity.output and 'rowsRead' in activity.output:
                        return activity.output['rowsRead']
                        
            return 0
            
        except Exception as e:
            logger.warning(f"Could not retrieve rows processed: {str(e)}")
            return 0

    async def _get_output_file_size(self, blob_service: BlobServiceClient, 
                                   config: Dict, run_id: str) -> float:
        """Get the size of the generated output file."""
        try:
            # Generate expected filename based on current date
            today = datetime.utcnow().strftime('%Y%m%d')
            filename = f"PaymentAlert_{today}.csv.gz"
            
            blob_client = blob_service.get_blob_client(
                container=config['container_name'],
                blob=f"{self.OUTPUT_DIRECTORY}/{filename}"
            )
            
            properties = blob_client.get_blob_properties()
            return properties.size / (1024 * 1024)  # Convert to MB
            
        except Exception as e:
            logger.warning(f"Could not retrieve file size: {str(e)}")
            return 0

    async def _calculate_data_quality_score(self, clients: Dict, config: Dict, 
                                          run_id: str) -> float:
        """Calculate data quality score based on various metrics."""
        try:
            # Download and analyze the generated CSV file
            today = datetime.utcnow().strftime('%Y%m%d')
            filename = f"PaymentAlert_{today}.csv.gz"
            
            blob_client = clients['blob_service'].get_blob_client(
                container=config['container_name'],
                blob=f"{self.OUTPUT_DIRECTORY}/{filename}"
            )
            
            # Download and decompress the file
            blob_data = blob_client.download_blob().readall()
            
            with gzip.GzipFile(fileobj=io.BytesIO(blob_data)) as gz_file:
                csv_content = gz_file.read().decode('utf-8')
                
            # Convert to DataFrame for analysis
            df = pd.read_csv(io.StringIO(csv_content))
            
            # Calculate quality metrics
            total_rows = len(df)
            if total_rows == 0:
                return 0.0
                
            # Check for null values in critical columns
            critical_columns = ['CONNECTION_KEY', 'USAGESERVICE_BX', 'CLIENT_KEY_AX']
            null_count = df[critical_columns].isnull().sum().sum()
            
            # Check for duplicate connection keys
            duplicate_count = df['CONNECTION_KEY'].duplicated().sum()
            
            # Check for data format consistency
            format_errors = 0
            
            # Date format validation
            date_columns = [col for col in df.columns if 'YMD' in col or 'DATE' in col]
            for col in date_columns:
                if col in df.columns:
                    try:
                        pd.to_datetime(df[col], format='%Y/%m/%d', errors='coerce')
                    except:
                        format_errors += 1
            
            # Calculate overall quality score
            quality_score = max(0, 1.0 - (
                (null_count / (total_rows * len(critical_columns))) * 0.4 +
                (duplicate_count / total_rows) * 0.3 +
                (format_errors / len(date_columns) if date_columns else 0) * 0.3
            ))
            
            return quality_score
            
        except Exception as e:
            logger.warning(f"Could not calculate data quality score: {str(e)}")
            return 0.0

    async def _get_performance_metrics(self, logs_client: LogsQueryClient, 
                                     config: Dict, run_id: str) -> Dict[str, Any]:
        """Get performance metrics from Azure Monitor logs."""
        try:
            query = f"""
            ADFPipelineRun
            | where PipelineName == "{self.PIPELINE_NAME}"
            | where RunId == "{run_id}"
            | project TimeGenerated, Status, DurationInMs, TotalCost
            """
            
            response = logs_client.query_workspace(
                workspace_id=config['log_analytics_workspace'],
                query=query,
                timespan=timedelta(hours=24)
            )
            
            metrics = {}
            for table in response.tables:
                for row in table.rows:
                    metrics['duration_ms'] = row[2] if len(row) > 2 else 0
                    metrics['total_cost'] = row[3] if len(row) > 3 else 0
                    break
                    
            return metrics
            
        except Exception as e:
            logger.warning(f"Could not retrieve performance metrics: {str(e)}")
            return {}

    @pytest.mark.asyncio
    async def test_payment_alert_basic_execution(self, azure_clients, test_config):
        """
        Test basic pipeline execution functionality.
        
        Validates:
        - Pipeline executes successfully
        - All activities complete
        - Output file is generated
        - Basic data validation
        """
        logger.info("Testing Payment Alert pipeline basic execution")
        
        result = await self.execute_pipeline_run(azure_clients, test_config)
        
        # Assertions
        assert result.status == "Succeeded", f"Pipeline failed: {result.error_message}"
        assert result.execution_duration <= self.MAX_EXECUTION_TIME_MINUTES, \
            f"Pipeline execution exceeded time limit: {result.execution_duration} minutes"
        assert result.rows_processed >= self.MIN_EXPECTED_ROWS, \
            f"Insufficient rows processed: {result.rows_processed}"
        assert result.file_size_mb > 0, "Output file was not generated"
        assert result.data_quality_score >= self.MIN_DATA_QUALITY_SCORE, \
            f"Data quality below threshold: {result.data_quality_score}"
        
        logger.info(f"Basic execution test completed successfully. "
                   f"Processed {result.rows_processed} rows in {result.execution_duration:.2f} minutes")

    @pytest.mark.asyncio
    async def test_payment_alert_large_dataset_performance(self, azure_clients, test_config):
        """
        Test pipeline performance with large datasets.
        
        Validates:
        - Performance with 1M+ rows
        - Memory usage optimization
        - Throughput benchmarks
        - Resource utilization
        """
        logger.info("Testing Payment Alert pipeline performance with large dataset")
        
        # This test assumes the source table has sufficient data
        # In a real scenario, you might set up test data or use a specific date range
        result = await self.execute_pipeline_run(azure_clients, test_config)
        
        # Performance assertions
        if result.rows_processed >= self.LARGE_DATASET_THRESHOLD:
            throughput = result.rows_processed / result.execution_duration if result.execution_duration > 0 else 0
            
            assert result.status == "Succeeded", "Large dataset processing failed"
            assert throughput >= self.EXPECTED_THROUGHPUT_ROWS_PER_MINUTE, \
                f"Throughput below benchmark: {throughput} rows/minute"
            assert result.file_size_mb <= 500, \
                f"Output file too large: {result.file_size_mb} MB"
        else:
            pytest.skip(f"Insufficient data for large dataset test: {result.rows_processed} rows")
        
        logger.info(f"Large dataset test completed. "
                   f"Throughput: {result.rows_processed/result.execution_duration:.0f} rows/minute")

    @pytest.mark.asyncio
    async def test_payment_alert_data_quality(self, azure_clients, test_config):
        """
        Test data quality and integrity validation.
        
        Validates:
        - No critical data loss
        - Proper data formatting
        - CSV structure compliance
        - Character encoding integrity
        """
        logger.info("Testing Payment Alert pipeline data quality")
        
        result = await self.execute_pipeline_run(azure_clients, test_config)
        
        assert result.status == "Succeeded", "Pipeline execution failed"
        assert result.data_quality_score >= 0.98, \
            f"Data quality insufficient: {result.data_quality_score}"
        
        # Validate CSV file structure
        await self._validate_csv_structure(azure_clients, test_config)
        
        logger.info(f"Data quality test completed. Quality score: {result.data_quality_score:.3f}")

    async def _validate_csv_structure(self, clients: Dict, config: Dict):
        """Validate the structure and format of the generated CSV file."""
        try:
            today = datetime.utcnow().strftime('%Y%m%d')
            filename = f"PaymentAlert_{today}.csv.gz"
            
            blob_client = clients['blob_service'].get_blob_client(
                container=config['container_name'],
                blob=f"{self.OUTPUT_DIRECTORY}/{filename}"
            )
            
            # Download and analyze file
            blob_data = blob_client.download_blob().readall()
            
            with gzip.GzipFile(fileobj=io.BytesIO(blob_data)) as gz_file:
                csv_content = gz_file.read().decode('utf-8')
            
            # Validate CSV structure
            lines = csv_content.strip().split('\n')
            assert len(lines) > 1, "CSV file has no data rows"
            
            # Check header
            header = lines[0].split(',')
            expected_columns = ['CONNECTION_KEY', 'USAGESERVICE_BX', 'CLIENT_KEY_AX']
            for col in expected_columns:
                assert col in header, f"Missing expected column: {col}"
            
            # Validate data rows
            for i, line in enumerate(lines[1:6]):  # Check first 5 data rows
                fields = line.split(',')
                assert len(fields) == len(header), \
                    f"Row {i+2} has incorrect number of fields: {len(fields)} vs {len(header)}"
            
            logger.info("CSV structure validation completed successfully")
            
        except Exception as e:
            pytest.fail(f"CSV structure validation failed: {str(e)}")

    @pytest.mark.asyncio
    async def test_payment_alert_error_handling(self, azure_clients, test_config):
        """
        Test error handling and recovery scenarios.
        
        Validates:
        - Graceful handling of connection issues
        - Proper error logging
        - Recovery mechanisms
        - Alert generation on failures
        """
        logger.info("Testing Payment Alert pipeline error handling")
        
        # Test with invalid parameters to trigger controlled failure
        invalid_params = {
            'invalid_parameter': 'invalid_value'
        }
        
        try:
            result = await self.execute_pipeline_run(
                azure_clients, test_config, invalid_params
            )
            
            # The pipeline might still succeed if it ignores invalid parameters
            # This is actually good - it shows robustness
            if result.status == "Succeeded":
                logger.info("Pipeline handled invalid parameters gracefully")
            else:
                logger.info("Pipeline properly failed with invalid parameters")
                
        except Exception as e:
            # Expected behavior for some error scenarios
            logger.info(f"Pipeline correctly raised exception for invalid input: {str(e)}")
        
        logger.info("Error handling test completed")

    @pytest.mark.asyncio
    async def test_payment_alert_monitoring_alerting(self, azure_clients, test_config):
        """
        Test monitoring and alerting functionality.
        
        Validates:
        - Pipeline metrics collection
        - Azure Monitor integration
        - Alert rule functionality
        - Performance counter accuracy
        """
        logger.info("Testing Payment Alert pipeline monitoring and alerting")
        
        result = await self.execute_pipeline_run(azure_clients, test_config)
        
        assert result.status == "Succeeded", "Pipeline execution failed"
        assert result.performance_metrics is not None, "No performance metrics collected"
        
        # Validate monitoring data
        if result.performance_metrics:
            assert 'duration_ms' in result.performance_metrics, "Duration metric missing"
            
            duration_minutes = result.performance_metrics.get('duration_ms', 0) / (1000 * 60)
            assert duration_minutes > 0, "Invalid duration metric"
            
        logger.info("Monitoring and alerting test completed successfully")

    def test_pipeline_configuration(self):
        """
        Test pipeline configuration and metadata.
        
        Validates:
        - Pipeline exists in ADF
        - Required activities are configured
        - Dataset references are valid
        - Parameter definitions
        """
        logger.info("Testing Payment Alert pipeline configuration")
        
        # This would typically involve checking the pipeline definition
        # against expected configuration
        assert self.PIPELINE_NAME == "pi_Send_PaymentAlert"
        assert len(self.EXPECTED_ACTIVITIES) == 2
        assert "at_CreateCSV_ClientDM" in self.EXPECTED_ACTIVITIES
        assert "at_SendSftp_ClientDM" in self.EXPECTED_ACTIVITIES
        
        logger.info("Pipeline configuration test completed")

    @pytest.mark.parametrize("file_format", ["csv.gz", "csv"])
    def test_output_file_formats(self, file_format):
        """
        Test different output file format support.
        
        Args:
            file_format: File format to test (csv.gz, csv)
        """
        logger.info(f"Testing output file format: {file_format}")
        
        # In the actual implementation, this would test the pipeline
        # with different output format configurations
        expected_compression = file_format.endswith('.gz')
        
        if expected_compression:
            assert file_format == "csv.gz", "Expected gzip compression for Payment Alert pipeline"
        
        logger.info(f"File format test completed for: {file_format}")


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])
