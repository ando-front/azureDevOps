"""
Pipeline fixtures placeholder for E2E tests
"""
import pytest
from typing import Any, Dict, List
from unittest.mock import Mock


class PipelineResult:
    """Pipeline execution result mock"""
    
    def __init__(self, status: str = "Succeeded", output_file_path: str = "/test/output.csv"):
        self.status = status
        self.output_file_path = output_file_path
        self.error_message = ""
    
    def get_activity_names(self) -> List[str]:
        """Get executed activity names"""
        return ["at_CreateCSV_MockActivity", "at_SFTP_MockActivity"]
    
    def get_activity_results(self) -> Dict[str, Any]:
        """Get activity execution results"""
        return {
            "at_CreateCSV_MockActivity": {"status": "Succeeded"},
            "at_SFTP_MockActivity": {"status": "Succeeded", "rows_transferred": 100, "destination_file": "mock_file_20241205_120000.csv.gz"}
        }
    
    def get_csv_content(self) -> str:
        """Get CSV content mock"""
        return "EQUIPMENT_ID,CUSTOMER_ID,EQUIPMENT_TYPE\nEQ001,1001,WATER_HEATER\nEQ002,1002,BOILER"
    
    def get_output_file_info(self) -> Dict[str, Any]:
        """Get output file information"""
        return {"size_mb": 2.5, "row_count": 1000}


class PipelineRunner:
    """Pipeline runner mock"""
    
    def run_pipeline(self, pipeline_name: str, timeout_minutes: int = 30) -> PipelineResult:
        """Run pipeline mock"""
        print(f"Mock running pipeline: {pipeline_name} (timeout: {timeout_minutes}min)")
        return PipelineResult()
    
    def run_pipeline_with_error_handling(self, pipeline_name: str, timeout_minutes: int = 30, expected_errors: bool = False) -> PipelineResult:
        """Run pipeline with error handling mock"""
        print(f"Mock running pipeline with error handling: {pipeline_name}")
        if expected_errors:
            return PipelineResult(status="Failed", output_file_path="")
        return PipelineResult()


@pytest.fixture
def pipeline_runner():
    """Pipeline runner fixture"""
    return PipelineRunner()


@pytest.fixture  
def synapse_helper():
    """Synapse helper fixture"""
    from tests.e2e.helpers.missing_helpers_placeholder import SynapseHelper
    return SynapseHelper()
