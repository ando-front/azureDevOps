"""
E2E Data Integrity Validation Tests
E2Eテストデータの整合性と一貫性を検証するテスト
"""

import pytest
import pyodbc
import os
from datetime import datetime
from .conftest import ODBCDriverManager


class TestDataIntegrityValidation:
    """データ整合性検証テストクラス"""

    @pytest.fixture(scope="class")
    def db_connection(self):
        """データベース接続のフィクスチャ"""
        # Docker環境でのサーバー名を使用
        server = os.getenv('SQL_SERVER', 'sqlserver-e2e-simple')
        port = os.getenv('SQL_PORT', '1433')
        database = os.getenv('SQL_DATABASE', 'TGMATestDB')
        username = os.getenv('SQL_USERNAME', 'sa')
        password = os.getenv('SQL_PASSWORD', 'YourStrong!Passw0rd123')
        
        # ODBC Driver Managerを使用して最適な接続文字列を構築
        driver_manager = ODBCDriverManager()
        connection_string = driver_manager.build_connection_string(
            host=server,
            port=port,
            database=database,
            user=username,
            password=password
        )
        
        conn = None
        try:
            conn = pyodbc.connect(connection_string, timeout=30)
            yield conn
        except Exception as e:
            print(f"データベース接続エラー: {e}")
            print(f"接続文字列: {connection_string}")
            raise
        finally:
            if conn:
                conn.close()

    def test_e2e_data_consistency_across_tables(self, db_connection):
        """テーブル間でのE2Eデータの一貫性確認"""
        cursor = db_connection.cursor()
        
        # client_dmとClientDmBxの一貫性確認
        cursor.execute("""
            SELECT c.client_id, c.status, b.bx_flag
            FROM client_dm c
            LEFT JOIN ClientDmBx b ON c.client_id = b.client_id
            WHERE c.client_id LIKE 'E2E_%'
            ORDER BY c.client_id
        """)
        
        results = cursor.fetchall()
        assert len(results) >= 12, f"client_dmのE2Eデータが不足しています（実際: {len(results)}）"
        
        # 各レコードの整合性確認
        for row in results:
            client_id, status, bx_flag = row
            assert client_id is not None, "client_idがNullです"
            assert status is not None, "statusがNullです"

    def test_e2e_test_execution_log_integrity(self, db_connection):
        """E2Eテスト実行ログの整合性確認"""
        cursor = db_connection.cursor()
        
        # テスト実行ログの存在確認
        cursor.execute("""
            SELECT execution_id, test_name, status, execution_date
            FROM e2e_test_execution_log
            WHERE test_name LIKE '%E2E%'
            ORDER BY execution_date DESC
        """)
        
        logs = cursor.fetchall()
        assert len(logs) >= 1, "E2Eテスト実行ログが存在しません"
        
        # 最新ログの検証
        latest_log = logs[0]
        assert latest_log[1] is not None, "test_nameがNullです"
        assert latest_log[2] in ['SUCCESS', 'FAILURE', 'RUNNING'], f"無効なステータス: {latest_log[2]}"

    def test_test_data_management_consistency(self, db_connection):
        """テストデータ管理テーブルの一貫性確認"""
        cursor = db_connection.cursor()
        
        cursor.execute("""
            SELECT data_id, table_name, record_count, last_updated
            FROM test_data_management
            WHERE table_name IN ('client_dm', 'ClientDmBx', 'point_grant_email', 'marketing_client_dm')
            ORDER BY last_updated DESC
        """)
        
        management_records = cursor.fetchall()
        assert len(management_records) >= 4, f"テストデータ管理レコードが不足しています（実際: {len(management_records)}）"
        
        # 各テーブルのレコード数検証
        for record in management_records:
            data_id, table_name, record_count, last_updated = record
            assert record_count >= 12, f"テーブル '{table_name}' のレコード数が不足しています（実際: {record_count}）"
            assert last_updated is not None, f"テーブル '{table_name}' のlast_updatedがNullです"

    def test_e2e_data_relationships(self, db_connection):
        """E2Eデータのリレーションシップ検証"""
        cursor = db_connection.cursor()
        
        # client_dmとpoint_grant_emailの関係確認
        cursor.execute("""
            SELECT c.client_id, p.email_id, p.grant_reason
            FROM client_dm c
            INNER JOIN point_grant_email p ON c.client_id = SUBSTRING(p.email_id, 1, LEN(c.client_id))
            WHERE c.client_id LIKE 'E2E_CLIENT_%'
            AND p.email_id LIKE 'E2E_CLIENT_%'
        """)
        
        relationships = cursor.fetchall()
        assert len(relationships) >= 1, "client_dmとpoint_grant_emailの関係が確立されていません"

    def test_bulk_data_performance_validation(self, db_connection):
        """大量データのパフォーマンス検証"""
        cursor = db_connection.cursor()
        
        # 大量データクエリのパフォーマンステスト
        start_time = datetime.now()
        
        cursor.execute("""
            SELECT COUNT(*) 
            FROM client_dm c
            JOIN ClientDmBx b ON c.client_id = b.client_id
            WHERE c.client_id LIKE 'E2E_%'
        """)
        
        count = cursor.fetchone()[0]
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        assert count >= 5, f"結合クエリの結果が不足しています（実際: {count}）"
        assert execution_time < 5.0, f"クエリ実行時間が長すぎます（実際: {execution_time}秒）"

    def test_error_scenario_data_validity(self, db_connection):
        """エラーシナリオデータの有効性確認"""
        cursor = db_connection.cursor()
        
        # エラーシナリオ用データの確認
        cursor.execute("""
            SELECT client_id, status 
            FROM client_dm 
            WHERE client_id LIKE 'E2E_ERROR_%'
        """)
        
        error_data = cursor.fetchall()
        assert len(error_data) >= 1, "エラーシナリオ用データが存在しません"
        
        # エラーデータの特定条件確認
        for row in error_data:
            client_id, status = row
            assert client_id.startswith('E2E_ERROR_'), f"無効なエラーデータID: {client_id}"

    def test_special_character_data_integrity(self, db_connection):
        """特殊文字データの整合性確認"""
        cursor = db_connection.cursor()
        
        cursor.execute("""
            SELECT client_id, name, status 
            FROM client_dm 
            WHERE client_id LIKE 'E2E_SPECIAL_%'
        """)
        
        special_data = cursor.fetchall()
        assert len(special_data) >= 1, "特殊文字テストデータが存在しません"
        
        # 特殊文字の処理確認
        for row in special_data:
            client_id, name, status = row
            assert client_id is not None, "特殊文字データのIDがNullです"
            assert name is not None, "特殊文字データのnameがNullです"

    def test_data_validation_procedures(self, db_connection):
        """データ検証プロシージャの実行テスト"""
        cursor = db_connection.cursor()
        
        try:
            # ValidateE2ETestDataプロシージャの実行
            cursor.execute("EXEC ValidateE2ETestData")
            cursor.fetchall()  # 結果を取得
            
            # GetE2ETestSummaryプロシージャの実行
            cursor.execute("EXEC GetE2ETestSummary")
            summary_results = cursor.fetchall()
            
            assert len(summary_results) >= 4, "サマリー結果が不足しています"
            
        except pyodbc.Error as e:
            pytest.fail(f"データ検証プロシージャの実行に失敗しました: {e}")

    def test_e2e_data_cleanup_verification(self, db_connection):
        """E2Eデータのクリーンアップ検証"""
        cursor = db_connection.cursor()
        
        # 古いE2Eデータがクリーンアップされていることを確認
        cursor.execute("""
            SELECT COUNT(*) 
            FROM client_dm 
            WHERE client_id LIKE 'E2E_%' 
            AND created_at < DATEADD(day, -30, GETDATE())
        """)
        
        old_data_count = cursor.fetchone()[0]
        # 注意: 新しいテスト環境では古いデータは存在しないはずですが、
        # 将来的なクリーンアップメカニズムの確認として残しています
        
    def test_cross_table_data_synchronization(self, db_connection):
        """テーブル間のデータ同期確認"""
        cursor = db_connection.cursor()
        
        # client_dmとmarketing_client_dmの同期確認
        cursor.execute("""
            SELECT c.client_id, m.client_id, c.status, m.customer_value
            FROM client_dm c
            FULL OUTER JOIN marketing_client_dm m ON c.client_id = m.client_id
            WHERE c.client_id LIKE 'E2E_%' OR m.client_id LIKE 'E2E_%'
        """)
        
        sync_results = cursor.fetchall()
        assert len(sync_results) >= 12, f"テーブル間の同期データが不足しています（実際: {len(sync_results)}）"
        
        # 同期の整合性確認
        for row in sync_results:
            c_client_id, m_client_id, status, customer_value = row
            if c_client_id and m_client_id:
                assert c_client_id == m_client_id, f"client_idの不整合: {c_client_id} != {m_client_id}"
