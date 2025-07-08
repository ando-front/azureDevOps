# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

E2E V2 Test Framework - A comprehensive end-to-end testing framework for Azure Data Factory pipelines across 38 pipelines in 7 domains (kendenki, smc, actionpoint, marketing, tgcontract, infrastructure, mtgmaster).

### Project Location
- **Working Directory**: `/mnt/c/Users/angie/git/azureDevOps`
- **Test Framework Root**: `/mnt/c/Users/angie/git/azureDevOps/e2e_v2/`
- **Git Repository**: Yes (branch: feat-テスト仕様書ベースのテストコード)

## Commands

### Run All Tests
```bash
# From the project root directory
cd /mnt/c/Users/angie/git/azureDevOps
python3 e2e_v2/scripts/run_all_tests.py
```

### Run Domain-Specific Tests
```bash
# Example for kendenki domain
cd /mnt/c/Users/angie/git/azureDevOps
python3 -c "
import sys
sys.path.insert(0, '.')
from e2e_v2.domains.kendenki.test_point_grant_email import TestPointGrantEmailPipeline
test = TestPointGrantEmailPipeline()
test.test_functional_with_file_exists()
"
```

### Run Single Test Method
```bash
# Pattern: Import test class and call specific test method
cd /mnt/c/Users/angie/git/azureDevOps
python3 -c "
import sys
sys.path.insert(0, '.')
from e2e_v2.domains.{domain}.test_{pipeline} import Test{Pipeline}Pipeline
test = Test{Pipeline}Pipeline()
test.test_{method_name}()
"
```

### Alternative: Direct Execution (if import paths are fixed)
```bash
# If the scripts are updated to use correct paths, you can run directly:
python3 /mnt/c/Users/angie/git/azureDevOps/e2e_v2/scripts/run_all_tests.py
```

### Important Note about Import Paths
The current test scripts contain hardcoded paths pointing to `/home/ando` (line 17 in run_all_tests.py):
```python
sys.path.insert(0, '/home/ando')
```

This needs to be updated to the actual project location or made relative. Until fixed, use the commands above that set the correct path via `sys.path.insert(0, '.')` after changing to the project directory.

### Lint and Type Check Commands
The framework expects these commands to be available. If they're not found, ask the user for the correct commands and suggest writing them to this file:
- `npm run lint` or `ruff check` (Python linting)
- `npm run typecheck` or `mypy` (Type checking)

## Architecture

### Test Framework Structure

The framework follows a hierarchical design with three key abstraction layers:

1. **Base Layer** (`e2e_v2/base/pipeline_test_base.py`)
   - `PipelineTestBase`: Core test functionality (mocks, test result creation)
   - `DomainTestBase`: Domain-specific extensions requiring `domain_specific_setup()` and `get_domain_test_data_template()` implementation
   - Provides mock services: `MockBlobStorage`, `MockSFTPServer`, `MockDatabase`

2. **Domain Layer** (`e2e_v2/domains/{domain}/`)
   - Each domain contains pipeline-specific test implementations
   - Tests inherit from `DomainTestBase`
   - Must implement domain-specific data generation and transformation logic

3. **Execution Layer** (`e2e_v2/scripts/`)
   - `run_all_tests.py`: Orchestrates all tests with comprehensive reporting
   - Generates both text and JSON reports in `e2e_v2/reports/`

### Test Categories

Each pipeline implements 4-6 tests across these categories:
- **FUNCTIONAL**: Core pipeline functionality (file processing, data transformation)
- **DATA_QUALITY**: Data validation, business rule compliance
- **PERFORMANCE**: Large dataset processing, throughput requirements
- **INTEGRATION**: External system connectivity (SFTP, Database)

### Key Patterns

1. **Data Transformation Flow**:
   ```python
   # Each test follows: Generate → Transform → Validate
   test_data = self._generate_{data_type}_data()
   transformed_data = self._transform_{data_type}_data(test_data)
   violations = self.validate_domain_business_rules(transformed_data)
   ```

2. **Mock Service Usage**:
   - `self.mock_storage`: Azure Blob Storage operations
   - `self.mock_sftp`: SFTP file transfers
   - `self.mock_database`: Database operations

3. **Test Result Creation**:
   ```python
   result = self.create_test_result(
       test_id=test_id,
       category=TestCategory.FUNCTIONAL,
       status=PipelineStatus.SUCCEEDED,
       records_extracted=count,
       records_transformed=count,
       records_loaded=count,
       data_quality_score=0.95,
       errors=[],
       warnings=violations
   )
   ```

## Domain-Specific Information

### Implemented Pipelines (10/38)
- **kendenki** (3/3): Point grant/lost emails, usage service
- **smc** (2/13): Payment alerts, utility bills
- **actionpoint** (2/2): Entry events, transaction history
- **marketing** (1/2): Client DM
- **tgcontract** (1/1): Contract score info
- **infrastructure** (1/4): Marketing client DM copy

### Common Test Methods
- `test_functional_with_file_exists`: Process when source file exists
- `test_functional_without_file`: Handle missing file scenarios
- `test_data_quality_validation`: Validate data quality metrics
- `test_performance_large_dataset`: Process 50K-200K records
- `test_integration_sftp_connectivity`: SFTP transfer validation

## Important Implementation Notes

1. **Date Handling**: Use `datetime.utcnow()` for consistency across all tests

2. **File Paths**: 
   - Source: `{domain}/{date}/{filename}.tsv`
   - Output: `{domain}/processed_{date}.csv`
   - SFTP: `/Import/{domain}/{filename}_{date}.csv`

3. **Data Quality Metrics**:
   - `completeness`: >= 0.95
   - `validity`: >= 0.80
   - `consistency`: >= 0.95
   - `accuracy`: >= 0.90

4. **Performance Thresholds**:
   - Small datasets (< 10K): > 5,000 records/sec
   - Large datasets (> 100K): > 1,500 records/sec

5. **Business Rule Validation**: Each domain implements `validate_domain_business_rules()` with specific checks:
   - ID format validation (CUST*, TXN*, SVC*, etc.)
   - Numeric field ranges
   - Date consistency
   - Required field presence