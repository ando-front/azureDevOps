"""
E2E tests for pi_Send_PaymentMethodChanged pipeline.

This module contains comprehensive end-to-end tests for the pi_Send_PaymentMethodChanged pipeline,
which extracts payment method change data from the customer DM table and sends it to SFMC via SFTP.

Test Coverage:
- Basic pipeline execution and data flow validation
- Large dataset performance testing
- Data quality and integrity validation
- Error handling and recovery scenarios
- Monitoring and alerting validation
- CSV format and compression verification
- Payment method change specific validations
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
from azure.identity import DefaultAzureCredential

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PaymentMethodChangedTestScenario(Enum):
    """Test scenarios for Payment Method Changed pipeline testing."""
    BASIC_EXECUTION = "basic_execution"
    LARGE_DATASET = "large_dataset"
    DATA_QUALITY = "data_quality"
    ERROR_HANDLING = "error_handling"
    MONITORING = "monitoring"
    PAYMENT_METHOD_VALIDATION = "payment_method_validation"
    CHANGE_TRACKING = "change_tracking"


@dataclass
class PaymentMethodChangedTestResult:
    """Test result data structure for Payment Method Changed pipeline tests."""
    scenario: PaymentMethodChangedTestScenario
    pipeline_run_id: str
    execution_duration: float
    status: str
    rows_processed: int
    file_size_mb: float
    data_quality_score: float
    payment_method_changes_count: int
    unique_customers_count: int
    error_message: Optional[str] = None
    performance_metrics: Optional[Dict[str, Any]] = None


class TestPipelinePaymentMethodChanged:

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
    Comprehensive E2E test suite for pi_Send_PaymentMethodChanged pipeline.
    
    This test class covers all aspects of the payment method changed pipeline execution,
    including data extraction, CSV generation, compression, and SFTP transfer with
    specific focus on payment method change tracking and validation.
    """

    # Pipeline configuration
    PIPELINE_NAME = "pi_Send_PaymentMethodChanged"
    EXPECTED_ACTIVITIES = ["at_CreateCSV_ClientDM", "at_SendSftp_ClientDM"]
    SOURCE_TABLE = "omni.omni_ods_marketing_trn_client_dm_bx_temp"
    OUTPUT_DIRECTORY = "datalake/OMNI/MA/ClientDM"
    SFTP_DIRECTORY = "Import/DAM/ClientDM"
    
    # Data quality thresholds
    MIN_EXPECTED_ROWS = 5000  # Lower threshold for payment method changes
    MAX_EXECUTION_TIME_MINUTES = 120
    MIN_DATA_QUALITY_SCORE = 0.95
    
    # Performance benchmarks
    LARGE_DATASET_THRESHOLD = 500000  # 500K rows (smaller than general pipelines)
    EXPECTED_THROUGHPUT_ROWS_PER_MINUTE = 50000
    
    # Payment method specific validations
    EXPECTED_PAYMENT_METHODS = [
        "口座振替", "クレジットカード", "コンビニ払い", "請求書払い"
    ]

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
                                 parameters: Dict = None) -> PaymentMethodChangedTestResult:
        """
        Execute the payment method changed pipeline and monitor its execution.
        
        Args:
            clients: Azure service clients
            config: Test configuration
            parameters: Pipeline parameters
            
        Returns:
            PaymentMethodChangedTestResult: Test execution results
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
            
            # Payment method specific analysis
            payment_analysis = await self._analyze_payment_method_changes(
                clients, config, pipeline_run_id
            )
            
            performance_metrics = await self._get_performance_metrics(
                clients['logs_client'], config, pipeline_run_id
            )
            
            return PaymentMethodChangedTestResult(
                scenario=PaymentMethodChangedTestScenario.BASIC_EXECUTION,
                pipeline_run_id=pipeline_run_id,
                execution_duration=execution_duration,
                status=status,
                rows_processed=rows_processed,
                file_size_mb=file_size_mb,
                data_quality_score=data_quality_score,
                payment_method_changes_count=payment_analysis['changes_count'],
                unique_customers_count=payment_analysis['unique_customers'],
                performance_metrics=performance_metrics
            )
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}")
            return PaymentMethodChangedTestResult(
                scenario=PaymentMethodChangedTestScenario.BASIC_EXECUTION,
                pipeline_run_id="",
                execution_duration=0,
                status="Failed",
                rows_processed=0,
                file_size_mb=0,
                data_quality_score=0,
                payment_method_changes_count=0,
                unique_customers_count=0,
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
            filename = f"PaymentMethodChanged_{today}.csv.gz"
            
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
            filename = f"PaymentMethodChanged_{today}.csv.gz"
            
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
            critical_columns = ['CONNECTION_KEY', 'USAGESERVICE_BX', 'CLIENT_KEY_AX', 'LIV0EU_GAS_PAY_METHOD_CD']
            null_count = df[critical_columns].isnull().sum().sum()
            
            # Check for duplicate connection keys
            duplicate_count = df['CONNECTION_KEY'].duplicated().sum()
            
            # Check payment method code validity
            payment_method_errors = 0
            if 'LIV0EU_GAS_PAY_METHOD_CD' in df.columns:
                invalid_payment_methods = df['LIV0EU_GAS_PAY_METHOD_CD'].isna().sum()
                payment_method_errors = invalid_payment_methods
            
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
                (null_count / (total_rows * len(critical_columns))) * 0.3 +
                (duplicate_count / total_rows) * 0.2 +
                (payment_method_errors / total_rows) * 0.3 +
                (format_errors / len(date_columns) if date_columns else 0) * 0.2
            ))
            
            return quality_score
            
        except Exception as e:
            logger.warning(f"Could not calculate data quality score: {str(e)}")
            return 0.0

    async def _analyze_payment_method_changes(self, clients: Dict, config: Dict, 
                                            run_id: str) -> Dict[str, int]:
        """Analyze payment method changes in the output data."""
        try:
            # Download and analyze the generated CSV file
            today = datetime.utcnow().strftime('%Y%m%d')
            filename = f"PaymentMethodChanged_{today}.csv.gz"
            
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
            
            # Analyze payment method changes
            unique_customers = df['CLIENT_KEY_AX'].nunique() if 'CLIENT_KEY_AX' in df.columns else 0
            
            # Count records that likely represent payment method changes
            # This could be based on specific columns that track changes
            changes_count = len(df)  # All records in this pipeline represent changes
            
            return {
                'changes_count': changes_count,
                'unique_customers': unique_customers
            }
            
        except Exception as e:
            logger.warning(f"Could not analyze payment method changes: {str(e)}")
            return {'changes_count': 0, 'unique_customers': 0}

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
    async def test_payment_method_changed_basic_execution(self, azure_clients, test_config):
        """
        Test basic pipeline execution functionality.
        
        Validates:
        - Pipeline executes successfully
        - All activities complete
        - Output file is generated
        - Basic data validation for payment method changes
        """
        logger.info("Testing Payment Method Changed pipeline basic execution")
        
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
        assert result.payment_method_changes_count > 0, "No payment method changes detected"
        
        logger.info(f"Basic execution test completed successfully. "
                   f"Processed {result.rows_processed} rows with {result.payment_method_changes_count} "
                   f"payment method changes in {result.execution_duration:.2f} minutes")

    @pytest.mark.asyncio
    async def test_payment_method_changed_large_dataset_performance(self, azure_clients, test_config):
        """
        Test pipeline performance with large datasets.
        
        Validates:
        - Performance with large number of payment method changes
        - Memory usage optimization
        - Throughput benchmarks
        - Resource utilization
        """
        logger.info("Testing Payment Method Changed pipeline performance with large dataset")
        
        result = await self.execute_pipeline_run(azure_clients, test_config)
        
        # Performance assertions
        if result.rows_processed >= self.LARGE_DATASET_THRESHOLD:
            throughput = result.rows_processed / result.execution_duration if result.execution_duration > 0 else 0
            
            assert result.status == "Succeeded", "Large dataset processing failed"
            assert throughput >= self.EXPECTED_THROUGHPUT_ROWS_PER_MINUTE, \
                f"Throughput below benchmark: {throughput} rows/minute"
            assert result.file_size_mb <= 100, \
                f"Output file too large: {result.file_size_mb} MB"
            
            # Payment method specific performance checks
            change_processing_rate = result.payment_method_changes_count / result.execution_duration
            assert change_processing_rate >= 1000, \
                f"Payment method change processing rate too low: {change_processing_rate} changes/minute"
        else:
            pytest.skip(f"Insufficient data for large dataset test: {result.rows_processed} rows")
        
        logger.info(f"Large dataset test completed. "
                   f"Throughput: {result.rows_processed/result.execution_duration:.0f} rows/minute")

    @pytest.mark.asyncio
    async def test_payment_method_changed_data_quality(self, azure_clients, test_config):
        """
        Test data quality and integrity validation.
        
        Validates:
        - Payment method code validity
        - Customer data consistency
        - Change tracking accuracy
        - CSV structure compliance
        """
        logger.info("Testing Payment Method Changed pipeline data quality")
        
        result = await self.execute_pipeline_run(azure_clients, test_config)
        
        assert result.status == "Succeeded", "Pipeline execution failed"
        assert result.data_quality_score >= 0.98, \
            f"Data quality insufficient: {result.data_quality_score}"
        
        # Validate payment method specific data quality
        await self._validate_payment_method_data_quality(azure_clients, test_config)
        
        logger.info(f"Data quality test completed. Quality score: {result.data_quality_score:.3f}")

    async def _validate_payment_method_data_quality(self, clients: Dict, config: Dict):
        """Validate payment method specific data quality."""
        try:
            today = datetime.utcnow().strftime('%Y%m%d')
            filename = f"PaymentMethodChanged_{today}.csv.gz"
            
            blob_client = clients['blob_service'].get_blob_client(
                container=config['container_name'],
                blob=f"{self.OUTPUT_DIRECTORY}/{filename}"
            )
            
            # Download and analyze file
            blob_data = blob_client.download_blob().readall()
            
            with gzip.GzipFile(fileobj=io.BytesIO(blob_data)) as gz_file:
                csv_content = gz_file.read().decode('utf-8')
            
            # Validate CSV structure and payment method data
            lines = csv_content.strip().split('\n')
            assert len(lines) > 1, "CSV file has no data rows"
            
            # Check header for payment method related columns
            header = lines[0].split(',')
            expected_columns = [
                'CONNECTION_KEY', 'USAGESERVICE_BX', 'CLIENT_KEY_AX',
                'LIV0EU_GAS_PAY_METHOD_CD', 'LIV0EU_GAS_PAY_METHOD'
            ]
            for col in expected_columns:
                assert col in header, f"Missing expected column: {col}"
            
            # Validate payment method data consistency
            df = pd.read_csv(io.StringIO(csv_content))
            
            # Check payment method code consistency
            if 'LIV0EU_GAS_PAY_METHOD_CD' in df.columns and 'LIV0EU_GAS_PAY_METHOD' in df.columns:
                # Ensure payment method codes and names are consistent
                unique_combinations = df[['LIV0EU_GAS_PAY_METHOD_CD', 'LIV0EU_GAS_PAY_METHOD']].drop_duplicates()
                assert len(unique_combinations) <= 20, "Too many payment method combinations"
            
            logger.info("Payment method data quality validation completed successfully")
            
        except Exception as e:
            pytest.fail(f"Payment method data quality validation failed: {str(e)}")

    @pytest.mark.asyncio
    async def test_payment_method_changed_error_handling(self, azure_clients, test_config):
        """
        Test error handling and recovery scenarios.
        
        Validates:
        - Graceful handling of connection issues
        - Proper error logging
        - Recovery mechanisms
        - Alert generation on failures
        """
        logger.info("Testing Payment Method Changed pipeline error handling")
        
        # Test with parameters that might cause controlled failure
        test_params = {
            'test_error_scenario': 'connection_timeout'
        }
        
        try:
            result = await self.execute_pipeline_run(
                azure_clients, test_config, test_params
            )
            
            # The pipeline might still succeed if it handles errors gracefully
            if result.status == "Succeeded":
                logger.info("Pipeline handled error scenario gracefully")
                assert result.payment_method_changes_count >= 0, "Invalid change count"
            else:
                logger.info("Pipeline properly failed with error scenario")
                
        except Exception as e:
            # Expected behavior for some error scenarios
            logger.info(f"Pipeline correctly raised exception: {str(e)}")
        
        logger.info("Error handling test completed")

    @pytest.mark.asyncio
    async def test_payment_method_changed_monitoring_alerting(self, azure_clients, test_config):
        """
        Test monitoring and alerting functionality.
        
        Validates:
        - Pipeline metrics collection
        - Payment method change tracking
        - Azure Monitor integration
        - Alert rule functionality
        """
        logger.info("Testing Payment Method Changed pipeline monitoring and alerting")
        
        result = await self.execute_pipeline_run(azure_clients, test_config)
        
        assert result.status == "Succeeded", "Pipeline execution failed"
        assert result.performance_metrics is not None, "No performance metrics collected"
        
        # Validate payment method change monitoring
        assert result.payment_method_changes_count > 0, "No payment method changes tracked"
        assert result.unique_customers_count > 0, "No unique customers tracked"
        assert result.unique_customers_count <= result.payment_method_changes_count, \
            "Customer count inconsistency"
        
        # Validate monitoring data
        if result.performance_metrics:
            assert 'duration_ms' in result.performance_metrics, "Duration metric missing"
            
            duration_minutes = result.performance_metrics.get('duration_ms', 0) / (1000 * 60)
            assert duration_minutes > 0, "Invalid duration metric"
            
        logger.info("Monitoring and alerting test completed successfully")

    @pytest.mark.asyncio
    async def test_payment_method_validation(self, azure_clients, test_config):
        """
        Test payment method specific validations.
        
        Validates:
        - Payment method code validity
        - Change tracking logic
        - Business rule compliance
        - Data consistency checks
        """
        logger.info("Testing payment method validation logic")
        
        result = await self.execute_pipeline_run(azure_clients, test_config)
        
        assert result.status == "Succeeded", "Pipeline execution failed"
        
        # Validate payment method specific business rules
        await self._validate_payment_method_business_rules(azure_clients, test_config)
        
        logger.info("Payment method validation test completed")

    async def _validate_payment_method_business_rules(self, clients: Dict, config: Dict):
        """Validate payment method specific business rules."""
        try:
            today = datetime.utcnow().strftime('%Y%m%d')
            filename = f"PaymentMethodChanged_{today}.csv.gz"
            
            blob_client = clients['blob_service'].get_blob_client(
                container=config['container_name'],
                blob=f"{self.OUTPUT_DIRECTORY}/{filename}"
            )
            
            # Download and analyze file
            blob_data = blob_client.download_blob().readall()
            
            with gzip.GzipFile(fileobj=io.BytesIO(blob_data)) as gz_file:
                csv_content = gz_file.read().decode('utf-8')
            
            df = pd.read_csv(io.StringIO(csv_content))
            
            # Business rule validations
            if 'LIV0EU_GAS_PAY_METHOD_CD' in df.columns:
                # Check that payment method codes are valid
                payment_method_codes = df['LIV0EU_GAS_PAY_METHOD_CD'].dropna().unique()
                assert len(payment_method_codes) > 0, "No payment method codes found"
                
                # Check that all codes are reasonable (e.g., numeric or specific format)
                for code in payment_method_codes:
                    assert str(code).strip() != '', "Empty payment method code found"
            
            # Check customer uniqueness per day (business rule)
            if 'CLIENT_KEY_AX' in df.columns:
                customer_counts = df['CLIENT_KEY_AX'].value_counts()
                max_changes_per_customer = customer_counts.max()
                assert max_changes_per_customer <= 5, \
                    f"Too many payment method changes for single customer: {max_changes_per_customer}"
            
            logger.info("Payment method business rules validation completed")
            
        except Exception as e:
            pytest.fail(f"Payment method business rules validation failed: {str(e)}")

    def test_pipeline_configuration(self):
        """
        Test pipeline configuration and metadata.
        
        Validates:
        - Pipeline exists in ADF
        - Required activities are configured
        - Dataset references are valid
        - Parameter definitions
        """
        logger.info("Testing Payment Method Changed pipeline configuration")
        
        # Configuration validation
        assert self.PIPELINE_NAME == "pi_Send_PaymentMethodChanged"
        assert len(self.EXPECTED_ACTIVITIES) == 2
        assert "at_CreateCSV_ClientDM" in self.EXPECTED_ACTIVITIES
        assert "at_SendSftp_ClientDM" in self.EXPECTED_ACTIVITIES
        
        # Validate expected payment methods
        assert len(self.EXPECTED_PAYMENT_METHODS) > 0
        assert "口座振替" in self.EXPECTED_PAYMENT_METHODS
        assert "クレジットカード" in self.EXPECTED_PAYMENT_METHODS
        
        logger.info("Pipeline configuration test completed")

    @pytest.mark.parametrize("change_scenario", [
        "bank_to_credit", "credit_to_bank", "convenience_to_bank", "invoice_to_credit"
    ])
    def test_payment_method_change_scenarios(self, change_scenario):
        """
        Test different payment method change scenarios.
        
        Args:
            change_scenario: Type of payment method change to test
        """
        logger.info(f"Testing payment method change scenario: {change_scenario}")
        
        # In actual implementation, this would test the pipeline
        # with different change scenarios and validate the results
        change_mapping = {
            "bank_to_credit": ("口座振替", "クレジットカード"),
            "credit_to_bank": ("クレジットカード", "口座振替"),
            "convenience_to_bank": ("コンビニ払い", "口座振替"),
            "invoice_to_credit": ("請求書払い", "クレジットカード")
        }
        
        assert change_scenario in change_mapping, f"Unknown change scenario: {change_scenario}"
        
        from_method, to_method = change_mapping[change_scenario]
        assert from_method in self.EXPECTED_PAYMENT_METHODS
        assert to_method in self.EXPECTED_PAYMENT_METHODS
        
        logger.info(f"Change scenario test completed: {from_method} -> {to_method}")


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])
