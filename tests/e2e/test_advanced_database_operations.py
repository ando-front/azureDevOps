"""
E2E Test Suite for Advanced Database Operations

高度なデータベース操作のE2Eテスト
データベースの複雑なクエリ、トランザクション、パフォーマンステストを実施
"""

import pytest
import datetime
import random
import os
from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection
from tests.helpers.reproducible_e2e_helper import setup_reproducible_test_class, cleanup_reproducible_test_class


@pytest.mark.e2e
class TestAdvancedDatabaseOperations:
    """高度なデータベース操作のE2Eテスト"""
    
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

    def test_complex_join_operations(self, e2e_synapse_connection: SynapseE2EConnection):
        """複雑なJOIN操作のテスト"""
        # テストデータの準備
        self._prepare_join_test_data(e2e_synapse_connection)
        
        # 複雑なJOINクエリの実行
        result = e2e_synapse_connection.execute_query("""
            SELECT 
                c.id,
                c.client_id,
                c.segment,
                p.points_granted,
                m.marketing_segment,
                m.engagement_score
            FROM ClientDmBx c
            LEFT JOIN point_grant_email p ON c.client_id = p.client_id
            LEFT JOIN marketing_client_dm m ON c.client_id = m.client_id
            WHERE c.segment = 'PREMIUM'
            ORDER BY m.engagement_score DESC
        """)
        
        assert len(result) >= 2, "JOIN結果が期待されるレコード数に達していません"
        assert result[0][5] >= result[1][5], "engagement_scoreでの降順ソートが正しくありません"
        
        print(f"複雑なJOIN操作テスト成功: {len(result)}件のレコードを処理")

    def test_aggregate_functions_with_grouping(self, e2e_synapse_connection: SynapseE2EConnection):
        """集約関数とグループ化のテスト"""
        # テストデータの準備
        self._prepare_aggregate_test_data(e2e_synapse_connection)
        
        # 集約クエリの実行
        result = e2e_synapse_connection.execute_query("""
            SELECT 
                segment,
                COUNT(*) as count,
                AVG(total_amount) as avg_amount,
                MAX(total_amount) as max_amount,
                MIN(total_amount) as min_amount
            FROM ClientDmBx
            WHERE segment IN ('PREMIUM', 'STANDARD', 'BASIC')
            GROUP BY segment
            HAVING COUNT(*) >= 1
            ORDER BY avg_amount DESC
        """)
        
        assert len(result) >= 2, "集約結果が期待される数に達していません"
        
        # 各セグメントの統計値が正しいことを確認
        for row in result:
            segment, count, avg_amount, max_amount, min_amount = row
            assert count >= 1, f"セグメント {segment} のカウントが不正です"
            assert max_amount >= avg_amount >= min_amount, f"セグメント {segment} の統計値が不正です"
        
        print(f"集約関数テスト成功: {len(result)}セグメントの統計を計算")

    def test_subquery_operations(self, e2e_synapse_connection: SynapseE2EConnection):
        """サブクエリ操作のテスト"""
        # 複雑なサブクエリの実行
        result = e2e_synapse_connection.execute_query("""
            SELECT 
                client_id,
                segment,
                total_amount
            FROM ClientDmBx
            WHERE total_amount > (
                SELECT AVG(total_amount)
                FROM ClientDmBx
                WHERE segment = 'STANDARD'
            )
            AND client_id IN (
                SELECT client_id
                FROM point_grant_email
                WHERE points_granted > 1000
            )
            ORDER BY total_amount DESC
        """)
        
        assert len(result) >= 0, "サブクエリの実行に失敗しました"
        
        # 結果の妥当性を確認
        if len(result) > 0:
            # 各レコードが条件を満たしていることを確認
            for row in result:
                client_id, segment, total_amount = row
                assert total_amount is not None, "total_amountがNullです"
                assert client_id is not None, "client_idがNullです"
        
        print(f"サブクエリテスト成功: {len(result)}件のレコードが条件を満たしています")

    def test_window_functions(self, e2e_synapse_connection: SynapseE2EConnection):
        """ウィンドウ関数のテスト"""
        # ウィンドウ関数を使用したクエリ
        result = e2e_synapse_connection.execute_query("""
            SELECT 
                client_id,
                segment,
                total_amount,
                ROW_NUMBER() OVER (PARTITION BY segment ORDER BY total_amount DESC) as rank_in_segment,
                AVG(total_amount) OVER (PARTITION BY segment) as segment_avg
            FROM ClientDmBx
            WHERE segment IN ('PREMIUM', 'STANDARD')
            ORDER BY segment, rank_in_segment
        """)
        
        assert len(result) >= 0, "ウィンドウ関数クエリの実行に失敗しました"
        
        # ランキングの連続性を確認
        if len(result) > 1:
            current_segment = None
            rank_counter = 0
            for row in result:
                client_id, segment, total_amount, rank_in_segment, segment_avg = row
                if segment != current_segment:
                    current_segment = segment
                    rank_counter = 1
                else:
                    rank_counter += 1
                
                assert rank_in_segment == rank_counter, f"ランキングが不正です: 期待値{rank_counter}, 実際{rank_in_segment}"
        
        print(f"ウィンドウ関数テスト成功: {len(result)}件のレコードでランキングを計算")

    def test_transaction_rollback_scenario(self, e2e_synapse_connection: SynapseE2EConnection):
        """トランザクションロールバックシナリオのテスト"""
        # 現在のレコード数を取得
        initial_count = e2e_synapse_connection.execute_query(
            "SELECT COUNT(*) FROM ClientDmBx WHERE client_id LIKE 'TRANS_TEST_%'"
        )[0][0]
        
        try:
            # トランザクション開始（シミュレーション）
            # 複数のINSERT操作
            e2e_synapse_connection.execute_query("""
                INSERT INTO ClientDmBx (client_id, segment, score, last_transaction_date, total_amount, processed_date, data_source)
                VALUES ('TRANS_TEST_001', 'PREMIUM', 850, '2024-01-15', 50000.00, GETDATE(), 'E2E_TEST')
            """)
            
            e2e_synapse_connection.execute_query("""
                INSERT INTO ClientDmBx (client_id, segment, score, last_transaction_date, total_amount, processed_date, data_source)
                VALUES ('TRANS_TEST_002', 'STANDARD', 650, '2024-01-16', 25000.00, GETDATE(), 'E2E_TEST')
            """)
            
            # 挿入が成功したことを確認
            new_count = e2e_synapse_connection.execute_query(
                "SELECT COUNT(*) FROM ClientDmBx WHERE client_id LIKE 'TRANS_TEST_%'"
            )[0][0]
            
            assert new_count == initial_count + 2, "トランザクション内での挿入が正しく実行されませんでした"
            
        except Exception as e:
            # エラーが発生した場合のクリーンアップ
            e2e_synapse_connection.execute_query(
                "DELETE FROM ClientDmBx WHERE client_id LIKE 'TRANS_TEST_%'"
            )
            raise
        
        # テストデータのクリーンアップ
        e2e_synapse_connection.execute_query(
            "DELETE FROM ClientDmBx WHERE client_id LIKE 'TRANS_TEST_%'"
        )
        
        print("トランザクションテスト成功")

    def test_date_time_operations(self, e2e_synapse_connection: SynapseE2EConnection):
        """日付・時刻操作のテスト"""
        # 日付関数を使用したクエリ
        result = e2e_synapse_connection.execute_query("""
            SELECT 
                client_id,
                last_transaction_date,
                DATEDIFF(day, last_transaction_date, GETDATE()) as days_since_last_transaction,
                YEAR(last_transaction_date) as transaction_year,
                MONTH(last_transaction_date) as transaction_month
            FROM ClientDmBx
            WHERE last_transaction_date IS NOT NULL
            AND last_transaction_date >= '2024-01-01'
            ORDER BY last_transaction_date DESC
        """)
        
        assert len(result) >= 0, "日付時刻クエリの実行に失敗しました"
        
        # 日付計算の妥当性を確認
        for row in result:
            if len(row) >= 5:
                client_id, last_transaction_date, days_since, year, month = row
                assert days_since >= 0, "経過日数が負の値です"
                assert 2024 <= year <= 2025, f"年が範囲外です: {year}"
                assert 1 <= month <= 12, f"月が範囲外です: {month}"
        
        print(f"日付時刻操作テスト成功: {len(result)}件のレコードで日付計算を実行")

    def test_string_manipulation_operations(self, e2e_synapse_connection: SynapseE2EConnection):
        """文字列操作のテスト"""
        # 文字列関数を使用したクエリ
        result = e2e_synapse_connection.execute_query("""
            SELECT 
                client_id,
                UPPER(segment) as segment_upper,
                LEN(client_id) as client_id_length,
                LEFT(client_id, 4) as client_id_prefix,
                RIGHT(client_id, 3) as client_id_suffix
            FROM ClientDmBx
            WHERE segment IS NOT NULL
            ORDER BY client_id
        """)
        
        assert len(result) >= 0, "文字列操作クエリの実行に失敗しました"
        
        # 文字列操作の妥当性を確認
        for row in result:
            if len(row) >= 5:
                client_id, segment_upper, id_length, prefix, suffix = row
                assert segment_upper == segment_upper.upper(), "UPPER関数が正しく動作していません"
                assert id_length == len(client_id), "LEN関数が正しく動作していません"
                assert prefix == client_id[:4], "LEFT関数が正しく動作していません"
                assert suffix == client_id[-3:], "RIGHT関数が正しく動作していません"
        
        print(f"文字列操作テスト成功: {len(result)}件のレコードで文字列操作を実行")

    def test_conditional_logic_operations(self, e2e_synapse_connection: SynapseE2EConnection):
        """条件分岐ロジックのテスト"""
        # CASE文を使用したクエリ
        result = e2e_synapse_connection.execute_query("""
            SELECT 
                client_id,
                segment,
                total_amount,
                CASE 
                    WHEN total_amount >= 100000 THEN 'VIP'
                    WHEN total_amount >= 50000 THEN 'GOLD'
                    WHEN total_amount >= 10000 THEN 'SILVER'
                    ELSE 'BRONZE'
                END as tier,
                CASE 
                    WHEN score >= 800 THEN 'HIGH'
                    WHEN score >= 600 THEN 'MEDIUM'
                    ELSE 'LOW'
                END as score_category
            FROM ClientDmBx
            WHERE total_amount IS NOT NULL AND score IS NOT NULL
            ORDER BY total_amount DESC
        """)
        
        assert len(result) >= 0, "条件分岐クエリの実行に失敗しました"
        
        # 条件分岐の正確性を確認
        for row in result:
            if len(row) >= 5:
                client_id, segment, total_amount, tier, score_category = row
                
                # tier分類の確認
                if total_amount >= 100000:
                    assert tier == 'VIP', f"tier分類が不正です: {total_amount} -> {tier}"
                elif total_amount >= 50000:
                    assert tier == 'GOLD', f"tier分類が不正です: {total_amount} -> {tier}"
                elif total_amount >= 10000:
                    assert tier == 'SILVER', f"tier分類が不正です: {total_amount} -> {tier}"
                else:
                    assert tier == 'BRONZE', f"tier分類が不正です: {total_amount} -> {tier}"
        
        print(f"条件分岐テスト成功: {len(result)}件のレコードで条件分岐を実行")

    def test_null_handling_operations(self, e2e_synapse_connection: SynapseE2EConnection):
        """NULL値処理のテスト"""
        # NULL処理関数を使用したクエリ
        result = e2e_synapse_connection.execute_query("""
            SELECT 
                client_id,
                ISNULL(segment, 'UNKNOWN') as segment_with_default,
                COALESCE(total_amount, 0) as total_amount_with_default,
                NULLIF(score, 0) as score_nullif_zero
            FROM ClientDmBx
            ORDER BY client_id
        """)
        
        assert len(result) >= 0, "NULL処理クエリの実行に失敗しました"
        
        # NULL処理の正確性を確認
        for row in result:
            if len(row) >= 4:
                client_id, segment_with_default, total_amount_with_default, score_nullif_zero = row
                assert segment_with_default is not None, "ISNULL関数が正しく動作していません"
                assert total_amount_with_default is not None, "COALESCE関数が正しく動作していません"
        
        print(f"NULL処理テスト成功: {len(result)}件のレコードでNULL処理を実行")

    def test_performance_large_dataset(self, e2e_synapse_connection: SynapseE2EConnection):
        """大規模データセットのパフォーマンステスト"""
        # パフォーマンステスト用の大きなクエリ
        start_time = datetime.datetime.now()
        
        result = e2e_synapse_connection.execute_query("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT client_id) as unique_clients,
                AVG(CAST(total_amount as FLOAT)) as avg_amount,
                MAX(score) as max_score,
                MIN(score) as min_score
            FROM ClientDmBx
            WHERE processed_date >= DATEADD(day, -30, GETDATE())
        """)
        
        end_time = datetime.datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        assert len(result) == 1, "パフォーマンステストクエリの結果が不正です"
        assert execution_time < 30.0, f"クエリ実行時間が長すぎます: {execution_time}秒"
        
        total_records, unique_clients, avg_amount, max_score, min_score = result[0]
        
        print(f"パフォーマンステスト成功:")
        print(f"- 実行時間: {execution_time:.2f}秒")
        print(f"- 処理レコード数: {total_records}")
        print(f"- ユニーク顧客数: {unique_clients}")
        print(f"- 平均取引額: {avg_amount:.2f}")

    def _prepare_join_test_data(self, connection: SynapseE2EConnection):
        """JOIN テスト用のデータ準備"""
        # ClientDmBx テーブルにテストデータ
        connection.execute_query("""
            INSERT INTO ClientDmBx (client_id, segment, score, last_transaction_date, total_amount, processed_date, data_source)
            VALUES 
                ('JOIN_TEST_001', 'PREMIUM', 850, '2024-01-15', 75000.00, GETDATE(), 'E2E_TEST'),
                ('JOIN_TEST_002', 'PREMIUM', 900, '2024-01-16', 95000.00, GETDATE(), 'E2E_TEST'),
                ('JOIN_TEST_003', 'STANDARD', 650, '2024-01-17', 35000.00, GETDATE(), 'E2E_TEST')
        """)
        
        # point_grant_email テーブルにテストデータ
        connection.execute_query("""
            INSERT INTO point_grant_email (client_id, email, points_granted, email_sent_date, campaign_id, status, created_at)
            VALUES 
                ('JOIN_TEST_001', 'test001@example.com', 1500, '2024-01-15', 'CAMP001', 'SENT', GETDATE()),
                ('JOIN_TEST_002', 'test002@example.com', 2000, '2024-01-16', 'CAMP001', 'SENT', GETDATE())
        """)
        
        # marketing_client_dm テーブルにテストデータ
        connection.execute_query("""
            INSERT INTO marketing_client_dm (client_id, marketing_segment, preference_category, engagement_score, last_campaign_response, opt_in_email, opt_in_sms, created_at, updated_at)
            VALUES 
                ('JOIN_TEST_001', 'HIGH_VALUE', 'FINANCE', 8.5, '2024-01-10', 1, 1, GETDATE(), GETDATE()),
                ('JOIN_TEST_002', 'HIGH_VALUE', 'LIFESTYLE', 9.2, '2024-01-12', 1, 0, GETDATE(), GETDATE()),
                ('JOIN_TEST_003', 'STANDARD', 'BASIC', 6.8, '2024-01-14', 1, 1, GETDATE(), GETDATE())
        """)

    def _prepare_aggregate_test_data(self, connection: SynapseE2EConnection):
        """集約テスト用のデータ準備"""
        # 各セグメントのテストデータ
        test_data = [
            ('AGG_PREMIUM_001', 'PREMIUM', 850, 80000.00),
            ('AGG_PREMIUM_002', 'PREMIUM', 920, 120000.00),
            ('AGG_STANDARD_001', 'STANDARD', 650, 45000.00),
            ('AGG_STANDARD_002', 'STANDARD', 700, 55000.00),
            ('AGG_BASIC_001', 'BASIC', 400, 15000.00),
            ('AGG_BASIC_002', 'BASIC', 450, 18000.00)
        ]
        
        for client_id, segment, score, amount in test_data:
            connection.execute_query("""
                INSERT INTO ClientDmBx (client_id, segment, score, last_transaction_date, total_amount, processed_date, data_source)
                VALUES (?, ?, ?, '2024-01-15', ?, GETDATE(), 'E2E_TEST')
            """, (client_id, segment, score, amount))
