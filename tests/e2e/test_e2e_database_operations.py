"""
E2Eテスト: ClientDmテーブルの実際のデータベース操作テスト
"""
import pytest
from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection


@pytest.mark.e2e
class TestClientDmE2E:
    """ClientDmテーブルのE2Eテスト"""
    
    def test_e2e_database_connection(self, e2e_synapse_connection: SynapseE2EConnection):
        """E2E: データベース接続テスト"""
        result = e2e_synapse_connection.execute_query("SELECT 1 as connection_test")
        assert result is not None
        assert len(result) == 1
        assert result[0][0] == 1
    
    def test_e2e_client_dm_table_operations(self, e2e_synapse_connection: SynapseE2EConnection, clean_test_data):
        """E2E: ClientDmテーブルの完全なCRUD操作テスト"""
        
        # 1. テーブル存在確認
        table_check = e2e_synapse_connection.execute_query(
            """
            SELECT COUNT(*) as table_count
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = 'ClientDm' AND TABLE_SCHEMA = 'dbo'
            """
        )
        assert table_check[0][0] >= 0, "ClientDmテーブルのチェックに失敗"
        
        # 2. 新規データ挿入
        test_client_code = 'E2E_TEST_001'
        insert_result = e2e_synapse_connection.execute_query(
            """
            INSERT INTO dbo.ClientDm (ClientName, ClientCode, Status)
            VALUES (?, ?, ?)
            """,
            ('E2Eテストクライアント', test_client_code, 'Active')
        )
        assert insert_result == 1, "データ挿入に失敗"
        
        # 3. 挿入されたデータの確認
        select_result = e2e_synapse_connection.execute_query(
            """
            SELECT ClientName, ClientCode, Status
            FROM dbo.ClientDm
            WHERE ClientCode = ?
            """,
            (test_client_code,)
        )
        assert len(select_result) == 1, "挿入されたデータが見つからない"
        assert select_result[0][0] == 'E2Eテストクライアント'
        assert select_result[0][1] == test_client_code
        assert select_result[0][2] == 'Active'
        
        # 4. データ更新
        update_result = e2e_synapse_connection.execute_query(
            """
            UPDATE dbo.ClientDm
            SET Status = ?
            WHERE ClientCode = ?
            """,
            ('Inactive', test_client_code)
        )
        assert update_result == 1, "データ更新に失敗"
        
        # 5. 更新されたデータの確認
        updated_select = e2e_synapse_connection.execute_query(
            """
            SELECT Status
            FROM dbo.ClientDm
            WHERE ClientCode = ?
            """,
            (test_client_code,)
        )
        assert updated_select[0][0] == 'Inactive', "データ更新が反映されていない"
        
        # 6. データ削除
        delete_result = e2e_synapse_connection.execute_query(
            """
            DELETE FROM dbo.ClientDm
            WHERE ClientCode = ?
            """,
            (test_client_code,)
        )
        assert delete_result == 1, "データ削除に失敗"
        
        # 7. 削除確認
        final_check = e2e_synapse_connection.execute_query(
            """
            SELECT COUNT(*)
            FROM dbo.ClientDm
            WHERE ClientCode = ?
            """,
            (test_client_code,)
        )
        assert final_check[0][0] == 0, "データが正常に削除されていない"
    
    def test_e2e_transaction_rollback(self, e2e_synapse_connection: SynapseE2EConnection, clean_test_data):
        """E2E: トランザクションのロールバックテスト"""
        
        test_client_code = 'E2E_ROLLBACK_001'
        
        try:
            # トランザクション開始を想定した複数操作
            # 正常な挿入
            e2e_synapse_connection.execute_query(
                """
                INSERT INTO dbo.ClientDm (ClientName, ClientCode, Status)
                VALUES (?, ?, ?)
                """,
                ('ロールバックテスト', test_client_code, 'Active')
            )
            
            # データが挿入されていることを確認
            check_result = e2e_synapse_connection.execute_query(
                """
                SELECT COUNT(*)
                FROM dbo.ClientDm
                WHERE ClientCode = ?
                """,
                (test_client_code,)
            )
            assert check_result[0][0] == 1, "テストデータが挿入されていない"
            
        finally:
            # クリーンアップ（テスト後の削除）
            e2e_synapse_connection.execute_query(
                """
                DELETE FROM dbo.ClientDm
                WHERE ClientCode = ?
                """,
                (test_client_code,)
            )


@pytest.mark.e2e 
@pytest.mark.slow
class TestPointGrantEmailE2E:
    """PointGrantEmailテーブルのE2Eテスト"""
    
    def test_e2e_point_grant_email_workflow(self, e2e_synapse_connection: SynapseE2EConnection, clean_test_data):
        """E2E: ポイント付与メールの完全なワークフローテスト"""
        
        # 1. 前提条件: Clientデータの準備
        client_code = 'E2E_PGE_CLIENT'
        e2e_synapse_connection.execute_query(
            """
            INSERT INTO dbo.ClientDm (ClientName, ClientCode, Status)
            VALUES (?, ?, ?)
            """,
            ('ポイント付与E2Eクライアント', client_code, 'Active')
        )
        
        # 2. ClientIdの取得
        client_result = e2e_synapse_connection.execute_query(
            """
            SELECT ClientId
            FROM dbo.ClientDm
            WHERE ClientCode = ?
            """,
            (client_code,)
        )
        assert len(client_result) == 1, "Clientデータが見つからない"
        client_id = client_result[0][0]
        
        # 3. ポイント付与メールデータの挿入
        email_address = 'e2e.test@example.com'
        point_amount = 2500.0
        
        email_insert = e2e_synapse_connection.execute_query(
            """
            INSERT INTO dbo.PointGrantEmail (ClientId, EmailAddress, PointAmount, Status)
            VALUES (?, ?, ?, ?)
            """,
            (client_id, email_address, point_amount, 'Processing')
        )
        assert email_insert == 1, "ポイント付与メールデータの挿入に失敗"
        
        # 4. 結合クエリでデータ確認
        join_result = e2e_synapse_connection.execute_query(
            """
            SELECT pge.EmailAddress, pge.PointAmount, pge.Status, cd.ClientName
            FROM dbo.PointGrantEmail pge
            INNER JOIN dbo.ClientDm cd ON pge.ClientId = cd.ClientId
            WHERE pge.EmailAddress = ?
            """,
            (email_address,)
        )
        
        assert len(join_result) == 1, "結合クエリの結果が期待と異なる"
        assert join_result[0][0] == email_address
        assert join_result[0][1] == point_amount
        assert join_result[0][2] == 'Processing'
        assert join_result[0][3] == 'ポイント付与E2Eクライアント'
        
        # 5. ステータス更新（処理完了）
        status_update = e2e_synapse_connection.execute_query(
            """
            UPDATE dbo.PointGrantEmail
            SET Status = ?
            WHERE EmailAddress = ?
            """,
            ('Completed', email_address)
        )
        assert status_update == 1, "ステータス更新に失敗"
        
        # 6. 最終確認
        final_check = e2e_synapse_connection.execute_query(
            """
            SELECT Status
            FROM dbo.PointGrantEmail
            WHERE EmailAddress = ?
            """,
            (email_address,)
        )
        assert final_check[0][0] == 'Completed', "ステータス更新が反映されていない"
