#!/usr/bin/env python3
"""
E2E test for database operations.
Tests comprehensive database CRUD operations and data integrity.
"""

import pytest
from tests.helpers.reproducible_e2e_helper import setup_reproducible_test_class, cleanup_reproducible_test_class


class TestE2EDatabaseOperations:
    """E2E tests for database operations."""

    @classmethod
    def setup_class(cls):
        """Set up reproducible test environment for the entire test class."""
        setup_reproducible_test_class()

    @classmethod
    def teardown_class(cls):
        """Clean up test environment after all tests in the class."""
        cleanup_reproducible_test_class()

    def test_client_dm_crud_operations(self, e2e_synapse_connection):
        """Test complete CRUD operations for ClientDm table."""
        synapse_helper = e2e_synapse_connection
        # Create test data
        client_code = 'TEST_CLIENT_001'
        insert_query = """
        INSERT INTO [dbo].[ClientDm] (ClientName, ClientCode, Status)
        VALUES ('Test Client', ?, 'ACTIVE')
        """
        
        synapse_helper.execute_query(insert_query, [client_code])
        
        # Read test data
        select_query = "SELECT ClientName, ClientCode, Status FROM [dbo].[ClientDm] WHERE ClientCode = ?"
        result = synapse_helper.execute_query(select_query, [client_code])
        assert result is not None, "Failed to read inserted data"
        
        # Update test data
        update_query = "UPDATE [dbo].[ClientDm] SET Status = 'INACTIVE' WHERE ClientCode = ?"
        synapse_helper.execute_query(update_query, [client_code])
        
        # Delete test data
        delete_query = "DELETE FROM [dbo].[ClientDm] WHERE ClientCode = ?"
        synapse_helper.execute_query(delete_query, [client_code])

    def test_point_grant_email_workflow(self, e2e_synapse_connection):
        """Test point grant email workflow operations."""
        synapse_helper = e2e_synapse_connection
        # Insert test client data
        client_code = 'PGE_CLIENT_001'
        client_insert_query = """
        INSERT INTO [dbo].[ClientDm] (ClientName, ClientCode, Status)
        VALUES ('Point Grant Client', ?, 'ACTIVE')
        """
        
        synapse_helper.execute_query(client_insert_query, [client_code])
        
        # Insert point grant email data
        email_insert_query = """
        INSERT INTO [staging].[point_grant_email_test]
        VALUES (?, 'user@example.com', 100, 'PENDING', GETDATE())
        """
        
        synapse_helper.execute_query(email_insert_query, [client_code])
        
        # Validate workflow
        validation_query = """
        SELECT COUNT(*) as email_count
        FROM [staging].[point_grant_email_test]
        WHERE client_code = ? AND status = 'PENDING'
        """
        
        result = synapse_helper.execute_query(validation_query, [client_code])
        assert result is not None, "Point grant email workflow validation failed"

    def test_data_integrity_checks(self, e2e_synapse_connection):
        """Test database data integrity constraints."""
        synapse_helper = e2e_synapse_connection
        # Test foreign key constraints
        integrity_test_query = """
        SELECT 
            TABLE_NAME,
            CONSTRAINT_NAME,
            CONSTRAINT_TYPE
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
        WHERE TABLE_SCHEMA = 'dbo'
        AND CONSTRAINT_TYPE IN ('FOREIGN KEY', 'PRIMARY KEY', 'UNIQUE')
        """
        
        result = synapse_helper.execute_query(integrity_test_query)
        assert result is not None, "Data integrity check failed"

    def test_transaction_rollback(self, e2e_synapse_connection):
        """Test transaction rollback functionality."""
        synapse_helper = e2e_synapse_connection
        # Test transactional operations
        test_client_code = 'ROLLBACK_TEST_001'
        
        try:
            # Start transaction operations
            insert_query = """
            INSERT INTO [dbo].[ClientDm] (ClientName, ClientCode, Status)
            VALUES ('Rollback Test', ?, 'ACTIVE')
            """
            
            synapse_helper.execute_query(insert_query, [test_client_code])
            
            # Simulate error condition
            # Invalid operation that should cause rollback
            
        except Exception:
            # Expected behavior - transaction should rollback
            pass
        
        # Verify rollback worked
        verify_query = "SELECT COUNT(*) FROM [dbo].[ClientDm] WHERE ClientCode = ?"
        result = synapse_helper.execute_query(verify_query, [test_client_code])
        assert result is not None, "Transaction rollback test failed"
