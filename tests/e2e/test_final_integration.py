#!/usr/bin/env python3
"""
Final E2E Integration Test - Fully Reproducible Version
"""

import pytest
import pyodbc
import requests
import os
import sys
from datetime import datetime
from pathlib import Path

# Import test helpers
sys.path.append(str(Path(__file__).parent.parent))
from helpers.reproducible_e2e_helper import setup_reproducible_test_class, teardown_reproducible_test_class

class TestFinalE2EIntegration:
    """Final E2E Integration Test - Reproducible Version"""
    
    @classmethod
    def setup_class(cls):
        """Class setup - Initialize reproducible test environment"""
        print("🚀 Setting up reproducible E2E test environment...")
        setup_reproducible_test_class()
        print("✅ Reproducible E2E test environment ready!")
    
    @classmethod
    def teardown_class(cls):
        """Class teardown - Clean up test environment"""
        print("🧹 Cleaning up E2E test environment...")
        teardown_reproducible_test_class()
        print("✅ E2E test environment cleanup completed!")
    
    def _get_no_proxy_session(self):
        """Get requests session with proxy disabled"""
        session = requests.Session()
        session.proxies = {'http': None, 'https': None}
        return session
    
    def test_01_sql_server_connection(self):
        """SQL Server connection test"""
        conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            "SERVER=localhost,1433;"
            "DATABASE=TGMATestDB;"
            "UID=sa;"
            "PWD=YourStrong!Passw0rd123;"
            "TrustServerCertificate=yes;"
        )
        
        conn = pyodbc.connect(conn_str, timeout=10)
        cursor = conn.cursor()
        
        # Verify database name
        cursor.execute("SELECT DB_NAME() as current_db")
        result = cursor.fetchone()
        assert result[0] == "TGMATestDB"
        
        # Verify table existence
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME IN ('client_dm', 'ClientDmBx', 'point_grant_email')
        """)
        table_count = cursor.fetchone()[0]
        assert table_count == 3
        
        conn.close()
    
    def test_02_azurite_storage_connection(self):
        """Azurite Storage connection test"""
        session = self._get_no_proxy_session()
        response = session.get("http://localhost:10000/devstoreaccount1", timeout=10)
        assert response.status_code in [200, 400]  # 400 is also normal (authentication error but connection success)
    
    def test_03_ir_simulator_connection(self):
        """IR Simulator connection test"""
        session = self._get_no_proxy_session()
        try:
            response = session.get("http://localhost:8080/", timeout=10)
            assert response.status_code == 200
        except requests.exceptions.ConnectionError:
            # Skip if IR Simulator is not running (expected in no-proxy configuration)
            pytest.skip("IR Simulator not running (expected in no-proxy configuration)")
    
    def test_04_reproducible_data_validation(self):
        """Reproducible test data validation"""
        conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            "SERVER=localhost,1433;"
            "DATABASE=TGMATestDB;"
            "UID=sa;"
            "PWD=YourStrong!Passw0rd123;"
            "TrustServerCertificate=yes;"
        )
        
        conn = pyodbc.connect(conn_str, timeout=10)
        cursor = conn.cursor()
        
        # 再現可能なE2Eテストデータの存在確認
        expected_counts = {
            'client_dm': 15,        # 12 regular + 3 bulk clients
            'ClientDmBx': 7,        # 5 regular + 2 bulk entries  
            'point_grant_email': 5, # 5 email records
            'marketing_client_dm': 5 # 5 marketing records
        }
        
        print("🔍 Validating reproducible test data...")
        
        for table, expected_count in expected_counts.items():
            cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE client_id LIKE 'E2E_%'")
            actual_count = cursor.fetchone()[0]
            print(f"  📊 {table}: {actual_count}/{expected_count} E2E records")
            assert actual_count == expected_count, f"Expected {expected_count} E2E records in {table}, got {actual_count}"
        
        # 特定のテストデータの内容確認
        cursor.execute("SELECT client_id, client_name, status FROM client_dm WHERE client_id = 'E2E_CLIENT_001'")
        result = cursor.fetchone()
        assert result is not None, "E2E_CLIENT_001 should exist"
        assert result[1] == 'E2E Active Client 1', "Client name should match expected value"
        assert result[2] == 'ACTIVE', "Client status should be ACTIVE"
        
        print("✅ All reproducible test data validated successfully!")
        conn.close()
    
    def test_05_data_consistency_validation(self):
        """データ整合性の検証"""
        conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            "SERVER=localhost,1433;"
            "DATABASE=TGMATestDB;"
            "UID=sa;"
            "PWD=YourStrong!Passw0rd123;"
            "TrustServerCertificate=yes;"
        )
        
        conn = pyodbc.connect(conn_str, timeout=10)
        cursor = conn.cursor()
        
        print("🔍 Validating data consistency across tables...")
        
        # client_dm と ClientDmBx の整合性確認
        cursor.execute("""
            SELECT c.client_id 
            FROM client_dm c 
            LEFT JOIN ClientDmBx b ON c.client_id = b.client_id 
            WHERE c.client_id LIKE 'E2E_%' AND b.client_id IS NULL
        """)
        orphaned_clients = cursor.fetchall()
        
        # E2E_CLIENT_006 以降は ClientDmBx にデータがないことが期待される
        expected_orphans = 8  # E2E_CLIENT_006〜012 + BULK_003
        assert len(orphaned_clients) == expected_orphans, f"Expected {expected_orphans} orphaned clients, got {len(orphaned_clients)}"
        
        # point_grant_email のデータ整合性確認
        cursor.execute("""
            SELECT COUNT(*) 
            FROM point_grant_email p 
            INNER JOIN client_dm c ON p.client_id = c.client_id 
            WHERE p.client_id LIKE 'E2E_%'
        """)
        consistent_emails = cursor.fetchone()[0]
        assert consistent_emails == 5, f"Expected 5 consistent email records, got {consistent_emails}"
        
        print("✅ Data consistency validation passed!")
        conn.close()
    
    def test_06_complete_integration(self):
        """完全統合テスト - 再現可能バージョン"""
        print("🔄 Running complete integration test with reproducible data...")
        
        # 1. SQL接続を維持しながら
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 18 for SQL Server};"
            "SERVER=localhost,1433;"
            "DATABASE=TGMATestDB;"
            "UID=sa;"
            "PWD=YourStrong!Passw0rd123;"
            "TrustServerCertificate=yes;",
            timeout=10
        )
        
        # 2. Azuriteにリクエスト
        session = self._get_no_proxy_session()
        azurite_response = session.get("http://localhost:10000/devstoreaccount1", timeout=5)
        
        # 3. IR Simulatorにリクエスト（エラーハンドリング付き）
        try:
            ir_response = session.get("http://localhost:8080/", timeout=5)
            ir_available = True
        except requests.exceptions.ConnectionError:
            ir_response = None
            ir_available = False
        
        # 4. 再現可能なテストデータでSQL確認
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM client_dm WHERE client_id LIKE 'E2E_%'")
        e2e_client_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM point_grant_email WHERE client_id LIKE 'E2E_%'")
        e2e_email_count = cursor.fetchone()[0]
        
        # 特定のE2Eクライアントのデータ確認
        cursor.execute("""
            SELECT c.client_id, c.client_name, c.status,
                   b.segment, b.score,
                   p.points_granted, p.status as email_status
            FROM client_dm c
            LEFT JOIN ClientDmBx b ON c.client_id = b.client_id
            LEFT JOIN point_grant_email p ON c.client_id = p.client_id
            WHERE c.client_id = 'E2E_CLIENT_001'
        """)
        client_data = cursor.fetchone()
        
        conn.close()
        
        # すべてのサービスが応答することを確認
        assert azurite_response.status_code in [200, 400]
        if ir_available:
            assert ir_response.status_code == 200
        
        # 再現可能なデータ構造の確認
        assert e2e_client_count == 15, f"Expected 15 E2E clients, got {e2e_client_count}"
        assert e2e_email_count == 5, f"Expected 5 E2E emails, got {e2e_email_count}"
        
        # 特定クライアントの詳細データ確認
        assert client_data is not None, "E2E_CLIENT_001 should have complete data"
        assert client_data[0] == 'E2E_CLIENT_001'
        assert client_data[1] == 'E2E Active Client 1'
        assert client_data[2] == 'ACTIVE'
        assert client_data[3] == 'PREMIUM'  # segment
        assert client_data[4] == 95.5  # score
        assert client_data[5] == 1000  # points_granted
        assert client_data[6] == 'SENT'  # email_status
        
        print("✅ Complete integration test passed with reproducible data!")

    @pytest.fixture(autouse=True)
    def log_test_info(self, request):
        """各テストの実行ログ"""
        test_name = request.node.name
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # テスト開始
        print(f"\n[{timestamp}] 開始: {test_name}")
        
        yield
        
        # テスト完了
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] 完了: {test_name}")

if __name__ == "__main__":
    # pytest実行用
    pytest.main([__file__, "-v", "-s"])
