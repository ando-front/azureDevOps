"""
E2E Database Schema Validation Tests
データベーススキーマの検証とテーブル構造の確認を行うテスト
"""

import pytest
import os
from typing import List, Dict
from .conftest import ODBCDriverManager
from tests.helpers.reproducible_e2e_helper import setup_reproducible_test_class, cleanup_reproducible_test_class

# pyodbc接続テスト用の条件付きインポート
# TODO: 技術的負債 - pyodbc依存のスキップ機能
# 理想的にはSQLAlchemy、psycopg2、またはHTTP APIベースのDB接続テストに移行
# pyodbcの代替案:
# 1. SQLAlchemy + PostgreSQL/MySQL ドライバー
# 2. RESTful Database API（Azure SQL Database REST API）
# 3. docker exec sqlcmd による直接クエリ実行
try:
    import pyodbc
    PYODBC_AVAILABLE = True
except ImportError:
    PYODBC_AVAILABLE = False


class TestDatabaseSchemaValidation:
    """データベーススキーマの検証テストクラス"""
    
    @classmethod
    def setup_class(cls):
        """再現可能テスト環境のセットアップ"""
        setup_reproducible_test_class()
        # Disable proxy settings for tests
        for var in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY']:
            if var in os.environ:
                del os.environ[var]

    @classmethod 
    def teardown_class(cls):
        """再現可能テスト環境のクリーンアップ"""
        cleanup_reproducible_test_class()

    @pytest.fixture(scope="class")
    def db_connection(self):
        """データベース接続のフィクスチャ"""
        if not PYODBC_AVAILABLE:
            pytest.skip("pyodbc not available - database schema test skipped")
        
        # CI環境ではsqlserver-testコンテナを使用
        connection_string = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            "SERVER=sqlserver-test,1433;"  # CIではsqlserver-testコンテナを使用
            "DATABASE=TGMATestDB;"
            "UID=sa;"
            "PWD=YourStrong!Passw0rd123;"
            "TrustServerCertificate=yes;"
            "Connection Timeout=60;"  # 接続タイムアウトを60秒に増加
            "Command Timeout=60;"     # コマンドタイムアウトも追加
        )
        
        conn = None
        try:
            # リトライ機能付きの接続（接続タイムアウト対策）
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    conn = pyodbc.connect(connection_string, timeout=60)  # pyodbc内部タイムアウトも60秒
                    break
                except Exception as e:
                    print(f"Connection attempt {attempt + 1} failed: {e}")
                    if attempt < max_retries - 1:
                        import time
                        time.sleep(5)
                    else:
                        # 接続に失敗した場合はスキップ
                        pytest.skip(f"Database connection failed: {e}")
            yield conn
        except Exception as e:
            print(f"データベース接続エラー: {e}")
            print(f"接続文字列: {connection_string}")
            pytest.skip(f"Database connection failed: {e}")
        finally:
            if conn:
                conn.close()

    def test_core_tables_exist(self, db_connection):
        """コアテーブルの存在確認"""
        cursor = db_connection.cursor()
        
        expected_tables = [
            'client_dm',
            'ClientDmBx',
            'point_grant_email',
            'marketing_client_dm'
        ]
        
        for table in expected_tables:
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = '{table}' AND TABLE_SCHEMA = 'dbo'
            """)
            
            count = cursor.fetchone()[0]
            assert count == 1, f"テーブル '{table}' が存在しません"

    def test_e2e_tracking_tables_exist(self, db_connection):
        """E2E追跡テーブルの存在確認"""
        cursor = db_connection.cursor()
        
        e2e_tables = [
            ('e2e_test_execution_log', 'etl'),
            ('test_data_management', 'staging')
        ]
        
        for table, schema in e2e_tables:
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = '{table}' AND TABLE_SCHEMA = '{schema}'
            """)
            
            count = cursor.fetchone()[0]
            assert count == 1, f"E2Eテーブル '{schema}.{table}' が存在しません"

    def test_enhanced_table_columns(self, db_connection):
        """強化されたテーブルカラムの確認"""
        cursor = db_connection.cursor()
        
        # client_dmテーブルのstatusカラム確認
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'client_dm' AND COLUMN_NAME = 'status'
        """)
        assert cursor.fetchone()[0] == 1, "client_dm.statusカラムが存在しません"

        # ClientDmBxテーブルのsegmentカラム確認（実際のスキーマに存在）
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'ClientDmBx' AND COLUMN_NAME = 'segment'
        """)
        assert cursor.fetchone()[0] == 1, "ClientDmBx.segmentカラムが存在しません"

        # point_grant_emailテーブルのemail_sent_dateカラム確認（実際のスキーマに存在）
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'point_grant_email' AND COLUMN_NAME = 'email_sent_date'
        """)
        assert cursor.fetchone()[0] == 1, "point_grant_email.email_sent_dateカラムが存在しません"

        # marketing_client_dmテーブルのengagement_scoreカラム確認（実際のスキーマに存在）
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'marketing_client_dm' AND COLUMN_NAME = 'engagement_score'
        """)
        assert cursor.fetchone()[0] == 1, "marketing_client_dm.engagement_scoreカラムが存在しません"

    def test_e2e_test_data_exists(self, db_connection):
        """E2Eテストデータの存在確認"""
        cursor = db_connection.cursor()
        
        # 各テーブルでE2E_プレフィックスのデータが存在することを確認
        tables_to_check = [
            'client_dm',
            'ClientDmBx', 
            'point_grant_email',
            'marketing_client_dm'
        ]
        
        for table in tables_to_check:
            if table == 'client_dm':
                id_column = 'client_id'
            elif table == 'ClientDmBx':
                id_column = 'client_id'
            elif table == 'point_grant_email':
                id_column = 'client_id'
            elif table == 'marketing_client_dm':
                id_column = 'client_id'
            
            cursor.execute(f"""
                SELECT COUNT(*) FROM {table} 
                WHERE {id_column} LIKE 'E2E_%'
            """)
            
            count = cursor.fetchone()[0]
            assert count >= 3, f"テーブル '{table}' にE2Eテストデータが不足しています（期待値: 3以上, 実際: {count}）"

    def test_validation_procedures_exist(self, db_connection):
        """検証プロシージャの存在確認"""
        cursor = db_connection.cursor()
        
        expected_procedures = [
            'ValidateE2ETestData',
            'GetE2ETestSummary'
        ]
        
        for procedure in expected_procedures:
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.ROUTINES 
                WHERE ROUTINE_NAME = '{procedure}' AND ROUTINE_TYPE = 'PROCEDURE'
            """)
            
            count = cursor.fetchone()[0]
            assert count == 1, f"プロシージャ '{procedure}' が存在しません"

    @pytest.mark.skipif(
        os.getenv('E2E_ENABLE_VIEW_TESTS', 'false').lower() != 'true',
        reason="ビューテストは環境変数 E2E_ENABLE_VIEW_TESTS=true で有効化可能"
    )
    def test_summary_views_exist(self, db_connection):
        """サマリービューの存在確認"""
        cursor = db_connection.cursor()
        
        expected_views = [
            'vw_e2e_test_summary',
            'vw_e2e_data_status'
        ]
        
        for view in expected_views:
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.VIEWS 
                WHERE TABLE_NAME = '{view}'
            """)
            
            count = cursor.fetchone()[0]
            assert count == 1, f"ビュー '{view}' が存在しません"

    def test_data_integrity_constraints(self, db_connection):
        """データ整合性制約の確認"""
        cursor = db_connection.cursor()
        
        # E2Eテストデータの整合性確認
        cursor.execute("""
            SELECT COUNT(*) FROM client_dm c
            INNER JOIN ClientDmBx b ON c.client_id = b.client_id
            WHERE c.client_id LIKE 'E2E_%'
        """)
        
        join_count = cursor.fetchone()[0]
        assert join_count >= 5, f"client_dmとClientDmBxの結合データが不足しています（実際: {join_count}）"

    def test_bulk_test_data_performance(self, db_connection):
        """大量テストデータのパフォーマンス確認"""
        cursor = db_connection.cursor()
        
        # BULK_プレフィックスのデータの存在確認
        cursor.execute("""
            SELECT COUNT(*) FROM client_dm 
            WHERE client_id LIKE 'E2E_BULK_%'
        """)
        bulk_count = cursor.fetchone()[0]
        assert bulk_count >= 1, f"大量テスト用データが不足しています（実際: {bulk_count}）"

    @pytest.mark.skipif(
        os.getenv('E2E_ENABLE_ERROR_TESTS', 'false').lower() != 'true',
        reason="エラーハンドリングテストは環境変数 E2E_ENABLE_ERROR_TESTS=true で有効化可能"
    )
    def test_error_handling_test_data(self, db_connection):
        """エラーハンドリング用テストデータの確認"""
        cursor = db_connection.cursor()
        
        # ERROR_プレフィックスのデータの存在確認
        cursor.execute("""
            SELECT COUNT(*) FROM client_dm 
            WHERE client_id LIKE 'E2E_ERROR_%'
        """)
        
        error_count = cursor.fetchone()[0]
        assert error_count >= 1, f"エラーハンドリング用テストデータが不足しています（実際: {error_count}）"

    @pytest.mark.skipif(
        os.getenv('E2E_ENABLE_SPECIAL_CHAR_TESTS', 'false').lower() != 'true',
        reason="特殊文字テストは環境変数 E2E_ENABLE_SPECIAL_CHAR_TESTS=true で有効化可能"
    )
    def test_special_character_test_data(self, db_connection):
        """特殊文字テストデータの確認"""
        cursor = db_connection.cursor()
        
        # SPECIAL_プレフィックスのデータの存在確認
        cursor.execute("""
            SELECT COUNT(*) FROM client_dm 
            WHERE client_id LIKE 'E2E_SPECIAL_%'
        """)
        
        special_count = cursor.fetchone()[0]
        assert special_count >= 1, f"特殊文字テストデータが不足しています（実際: {special_count}）"
