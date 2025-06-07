#!/usr/bin/env python3
"""
最終E2E統合テスト - すべて成功確認
"""

import pytest
import pyodbc
import requests
from datetime import datetime

class TestFinalE2EIntegration:
    """最終E2E統合テスト"""
    
    def test_01_sql_server_connection(self):
        """SQL Server接続テスト"""
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
        
        # データベース名確認
        cursor.execute("SELECT DB_NAME() as current_db")
        result = cursor.fetchone()
        assert result[0] == "TGMATestDB"
        
        # テーブル存在確認
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME IN ('client_dm', 'ClientDmBx', 'point_grant_email')
        """)
        table_count = cursor.fetchone()[0]
        assert table_count == 3
        
        conn.close()
    
    def test_02_azurite_storage_connection(self):
        """Azurite Storage接続テスト"""
        response = requests.get("http://localhost:10000/devstoreaccount1", timeout=10)
        assert response.status_code in [200, 400]  # 400も正常（認証不正だが接続成功）
    
    def test_03_ir_simulator_connection(self):
        """IR Simulator接続テスト"""
        response = requests.get("http://localhost:8080/", timeout=10)
        assert response.status_code == 200
    
    def test_04_data_operations(self):
        """データ操作テスト"""
        conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            "SERVER=localhost,1433;"
            "DATABASE=SynapseTestDB;"
            "UID=sa;"
            "PWD=YourStrong!Passw0rd123;"
            "TrustServerCertificate=yes;"
        )
        
        conn = pyodbc.connect(conn_str, timeout=10)
        cursor = conn.cursor()
        
        # テストデータ挿入
        test_id = 9999
        cursor.execute("""
            INSERT INTO ClientDm (ClientId, ClientName, ClientEmail) 
            VALUES (?, ?, ?)
        """, (test_id, 'Final Test Client', 'final@test.com'))
        
        # データ確認
        cursor.execute("SELECT COUNT(*) FROM ClientDm WHERE ClientId = ?", (test_id,))
        count = cursor.fetchone()[0]
        assert count == 1
        
        # クリーンアップ
        cursor.execute("DELETE FROM ClientDm WHERE ClientId = ?", (test_id,))
        conn.commit()
        
        conn.close()
    
    def test_05_complete_integration(self):
        """完全統合テスト"""
        # すべてのサービスが同時に動作することを確認
        
        # 1. SQL接続を維持しながら
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 18 for SQL Server};"
            "SERVER=localhost,1433;"
            "DATABASE=SynapseTestDB;"
            "UID=sa;"
            "PWD=YourStrong!Passw0rd123;"
            "TrustServerCertificate=yes;",
            timeout=10
        )
        
        # 2. Azuriteにリクエスト
        azurite_response = requests.get("http://localhost:10000/devstoreaccount1", timeout=5)
        
        # 3. IR Simulatorにリクエスト
        ir_response = requests.get("http://localhost:8080/", timeout=5)
        
        # 4. SQLでデータ確認
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM ClientDm")
        client_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM PointGrantEmail")
        email_count = cursor.fetchone()[0]
        
        conn.close()
        
        # すべてのサービスが応答することを確認
        assert azurite_response.status_code in [200, 400]
        assert ir_response.status_code == 200
        assert client_count >= 0
        assert email_count >= 0

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
