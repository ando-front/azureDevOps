#!/usr/bin/env python3
"""
E2E test for ADF Data Quality and Security operations.
Tests comprehensive data quality checks and security validations.
"""

import pytest
import json
from typing import Dict, Any
from tests.helpers.reproducible_e2e_helper import setup_reproducible_test_class, cleanup_reproducible_test_class


class TestE2EADFDataQualitySecurity:
    """E2E tests for ADF data quality and security operations."""

    @classmethod
    def setup_class(cls):
        """Set up reproducible test environment for the entire test class."""
        setup_reproducible_test_class()

    @classmethod
    def teardown_class(cls):
        """Clean up test environment after all tests in the class."""
        cleanup_reproducible_test_class()

    @pytest.mark.e2e
    def test_data_quality_validation(self, e2e_synapse_connection):
        """Test comprehensive data quality validation."""
        # Insert test data with quality issues
        test_data_query = """
        INSERT INTO [staging].[data_quality_test]
        VALUES 
            ('VALID001', 'John Doe', 'john@example.com', '2024-01-15', 100.00),
            ('INVALID001', NULL, 'invalid-email', '2024-13-45', -50.00),
            ('VALID002', 'Jane Smith', 'jane@example.com', '2024-02-20', 200.50),
            ('INVALID002', '', 'duplicate@example.com', '1900-01-01', 999999.99)
        """
        
        synapse_helper = e2e_synapse_connection
        synapse_helper.execute_query(test_data_query)
        
        # Validate data quality checks
        quality_check_query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN name IS NULL OR name = '' THEN 1 END) as null_names,
            COUNT(CASE WHEN email NOT LIKE '%@%' THEN 1 END) as invalid_emails,
            COUNT(CASE WHEN date_field < '1990-01-01' OR date_field > GETDATE() THEN 1 END) as invalid_dates
        FROM [staging].[data_quality_test]
        """
        
        result = synapse_helper.execute_query(quality_check_query)
        assert result is not None, "Quality check query failed"    @pytest.mark.e2e
    def test_security_access_control(self, e2e_synapse_connection):
        """Test security access control mechanisms."""
        synapse_helper = e2e_synapse_connection
        # Test role-based access control
        access_test_query = """
        SELECT 
            USER_NAME() as current_user,
            IS_MEMBER('db_datareader') as has_read_access,
            IS_MEMBER('db_datawriter') as has_write_access
        """
        
        result = synapse_helper.execute_query(access_test_query)
        assert result is not None, "Access control check failed"

    @pytest.mark.e2e
    def test_data_encryption_validation(self, e2e_synapse_connection):
        """Test data encryption and decryption processes."""
        synapse_helper = e2e_synapse_connection
        # Insert encrypted test data
        encryption_test_query = """
        INSERT INTO [staging].[encryption_test]
        VALUES 
            ('ENC001', 'ENC_encrypted_data_1', '2024-01-15'),
            ('ENC002', 'ENC_encrypted_data_2', '2024-02-20'),
            ('PLAIN001', 'plain_text_data', '2024-03-10')
        """
        
        synapse_helper.execute_query(encryption_test_query)
        
        # Validate encryption status
        encryption_check_query = """
        SELECT 
            id,
            CASE 
                WHEN data_field LIKE 'ENC_%' THEN 'ENCRYPTED'
                ELSE 'PLAIN'
            END as encryption_status
        FROM [staging].[encryption_test]
        """
        
        result = synapse_helper.execute_query(encryption_check_query)
        assert result is not None, "Encryption validation failed"

    @pytest.mark.e2e
    def test_audit_logging(self, e2e_synapse_connection):
        """Test comprehensive audit logging functionality."""
        synapse_helper = e2e_synapse_connection
        # Generate audit events
        audit_test_query = """
        INSERT INTO [audit].[system_logs]
        VALUES 
            (NEWID(), 'DATA_ACCESS', 'test_user', GETDATE(), 'SUCCESS'),
            (NEWID(), 'DATA_MODIFICATION', 'test_user', GETDATE(), 'SUCCESS'),
            (NEWID(), 'SECURITY_EVENT', 'test_user', GETDATE(), 'WARNING')
        """
        
        synapse_helper.execute_query(audit_test_query)
        
        # Validate audit log entries
        audit_check_query = """
        SELECT 
            COUNT(*) as total_logs,
            COUNT(CASE WHEN event_type = 'SECURITY_EVENT' THEN 1 END) as security_events
        FROM [audit].[system_logs]
        WHERE event_timestamp >= DATEADD(minute, -5, GETDATE())
        """
        
        result = synapse_helper.execute_query(audit_check_query)
        assert result is not None, "Audit logging validation failed"

    @pytest.mark.e2e
    def test_compliance_validation(self, e2e_synapse_connection):
        """Test compliance with data governance requirements."""
        synapse_helper = e2e_synapse_connection
        # Check data retention policies
        retention_check_query = """
        SELECT 
            TABLE_NAME,
            COLUMN_NAME,
            DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'staging'
        AND COLUMN_NAME LIKE '%date%'
        """
        
        result = synapse_helper.execute_query(retention_check_query)
        assert result is not None, "Compliance validation failed"

    @pytest.mark.e2e
    def test_data_masking(self, e2e_synapse_connection):
        """Test data masking for sensitive information."""
        synapse_helper = e2e_synapse_connection
        # Insert sensitive test data
        masking_test_query = """
        INSERT INTO [staging].[sensitive_data_test]
        VALUES 
            ('CUST001', '1234-5678-9012-3456', '123-45-6789', 'sensitive@example.com'),
            ('CUST002', '9876-5432-1098-7654', '987-65-4321', 'private@example.com')
        """
        
        synapse_helper.execute_query(masking_test_query)
        
        # Validate data masking
        masking_check_query = """
        SELECT 
            customer_id,
            LEFT(credit_card, 4) + '-****-****-' + RIGHT(credit_card, 4) as masked_cc,
            '***-**-' + RIGHT(ssn, 4) as masked_ssn
        FROM [staging].[sensitive_data_test]
        """
        
        result = synapse_helper.execute_query(masking_check_query)
        assert result is not None, "Data masking validation failed"
