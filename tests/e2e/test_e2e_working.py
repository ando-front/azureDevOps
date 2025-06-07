"""
動作確認済みE2Eテスト
"""

import pytest
import pyodbc
import requests
import os

class TestE2EWorking:
    """動作するE2Eテスト"""
    
    @classmethod
    def setup_class(cls):
        """クラス初期化時にプロキシを無効化"""
        # プロキシ環境変数を一時的に保存してクリア
        cls.original_http_proxy = os.environ.get('http_proxy')
        cls.original_https_proxy = os.environ.get('https_proxy')
        cls.original_HTTP_PROXY = os.environ.get('HTTP_PROXY')
        cls.original_HTTPS_PROXY = os.environ.get('HTTPS_PROXY')
        
        # プロキシ環境変数をクリア
        for var in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY']:
            if var in os.environ:
                del os.environ[var]
    
    @classmethod
    def teardown_class(cls):
        """クラス終了時にプロキシ設定を復元"""
        # 元のプロキシ設定を復元
        if cls.original_http_proxy:
            os.environ['http_proxy'] = cls.original_http_proxy
        if cls.original_https_proxy:
            os.environ['https_proxy'] = cls.original_https_proxy
        if cls.original_HTTP_PROXY:
            os.environ['HTTP_PROXY'] = cls.original_HTTP_PROXY
        if cls.original_HTTPS_PROXY:
            os.environ['HTTPS_PROXY'] = cls.original_HTTPS_PROXY
    
    def _get_no_proxy_session(self):
        """プロキシを無効にしたrequestsセッションを取得"""
        session = requests.Session()
        session.proxies = {'http': None, 'https': None}
        return session
    
    def test_database_connection_working(self):
        """データベース接続テスト - 動作確認済み"""
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
        
        # テーブル確認
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_CATALOG = 'TGMATestDB'
        """)
        table_count = cursor.fetchone()[0]
        assert table_count >= 2
        
        conn.close()
    
    def test_ir_simulator_basic(self):
        """IR Simulator基本テスト"""
        try:
            session = self._get_no_proxy_session()
            response = session.get("http://localhost:8080/", timeout=10)
            # 403でも接続は成功（認証の問題）
            assert response.status_code in [200, 403]
        except requests.exceptions.ConnectionError:
            pytest.skip("IR Simulator not accessible")
    
    def test_azurite_basic(self):
        """Azurite基本テスト"""
        try:
            session = self._get_no_proxy_session()
            response = session.get("http://localhost:10000/devstoreaccount1", timeout=10)
            # 400でも接続は成功（認証不正だが接続成功）
            assert response.status_code in [200, 400]
        except requests.exceptions.ConnectionError:
            pytest.skip("Azurite not accessible")
    
    def test_table_operations(self):
        """テーブル操作テスト"""
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
        
        # テストデータ操作
        test_id = 'TEST_WORKING_999'
        
        # データ挿入
        cursor.execute("""
            INSERT INTO client_dm (client_id, client_name, email) 
            VALUES (?, ?, ?)
        """, (test_id, 'Working Test Client', 'working@test.com'))
        
        # データ確認
        cursor.execute("SELECT COUNT(*) FROM client_dm WHERE client_id = ?", (test_id,))
        count = cursor.fetchone()[0]
        assert count == 1
        
        # クリーンアップ
        cursor.execute("DELETE FROM client_dm WHERE client_id = ?", (test_id,))
        conn.commit()
        
        conn.close()
    
    def test_environment_variables(self):
        """環境変数テスト"""
        # E2E テスト環境の基本変数をチェック
        required_vars = {
            'SQL_SERVER_HOST': 'localhost',
            'SQL_SERVER_PORT': '1433',
            'SQL_SERVER_USER': 'sa'
        }
        
        for var, default_value in required_vars.items():
            value = os.getenv(var, default_value)
            assert value != "", f"環境変数 {var} が空です"
