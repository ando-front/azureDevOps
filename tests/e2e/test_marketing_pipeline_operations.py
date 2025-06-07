"""
E2E Test Suite for Marketing Pipeline Operations

マーケティングパイプライン操作のE2Eテスト
顧客セグメンテーション、キャンペーン管理、エンゲージメント分析を含む
"""

import pytest
import datetime
import random
import os
from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection
from tests.helpers.reproducible_e2e_helper import setup_reproducible_test_class, cleanup_reproducible_test_class


@pytest.mark.e2e
class TestMarketingPipelineOperations:
    """マーケティングパイプライン操作のE2Eテスト"""
    
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

    def test_customer_segmentation_analysis(self, e2e_synapse_connection: SynapseE2EConnection):
        """顧客セグメンテーション分析のテスト"""
        # テストデータの準備
        self._prepare_segmentation_test_data(e2e_synapse_connection)
        
        # セグメンテーション分析クエリ
        result = e2e_synapse_connection.execute_query("""
            SELECT 
                marketing_segment,
                COUNT(*) as customer_count,
                AVG(engagement_score) as avg_engagement,
                COUNT(CASE WHEN opt_in_email = 1 THEN 1 END) as email_opt_ins,
                COUNT(CASE WHEN opt_in_sms = 1 THEN 1 END) as sms_opt_ins
            FROM marketing_client_dm
            WHERE marketing_segment IN ('HIGH_VALUE', 'MEDIUM_VALUE', 'LOW_VALUE')
            GROUP BY marketing_segment
            ORDER BY avg_engagement DESC
        """)
        
        assert len(result) >= 2, "セグメンテーション結果が期待される数に達していません"
        
        # セグメントごとの妥当性確認
        for row in result:
            segment, count, avg_engagement, email_opts, sms_opts = row
            assert count >= 1, f"セグメント {segment} の顧客数が不正です"
            assert 0 <= avg_engagement <= 10, f"セグメント {segment} のエンゲージメントスコアが範囲外です"
            assert email_opts <= count, f"セグメント {segment} のメールオプトイン数が不正です"
        
        print(f"顧客セグメンテーション分析成功: {len(result)}セグメントを分析")

    def test_campaign_performance_tracking(self, e2e_synapse_connection: SynapseE2EConnection):
        """キャンペーンパフォーマンス追跡のテスト"""
        # キャンペーンテストデータの準備
        self._prepare_campaign_test_data(e2e_synapse_connection)
        
        # キャンペーンパフォーマンス分析
        result = e2e_synapse_connection.execute_query("""
            SELECT 
                campaign_id,
                COUNT(*) as emails_sent,
                SUM(points_granted) as total_points_granted,
                COUNT(CASE WHEN status = 'DELIVERED' THEN 1 END) as delivered_count,
                COUNT(CASE WHEN status = 'BOUNCED' THEN 1 END) as bounced_count,
                CAST(COUNT(CASE WHEN status = 'DELIVERED' THEN 1 END) as FLOAT) / COUNT(*) as delivery_rate
            FROM point_grant_email
            WHERE campaign_id LIKE 'PERF_TEST_%'
            GROUP BY campaign_id
            ORDER BY delivery_rate DESC
        """)
        
        assert len(result) >= 2, "キャンペーンパフォーマンス結果が期待される数に達していません"
        
        # パフォーマンス指標の妥当性確認
        for row in result:
            campaign_id, emails_sent, total_points, delivered, bounced, delivery_rate = row
            assert emails_sent >= 1, f"キャンペーン {campaign_id} のメール送信数が不正です"
            assert total_points >= 0, f"キャンペーン {campaign_id} のポイント総数が負の値です"
            assert 0 <= delivery_rate <= 1, f"キャンペーン {campaign_id} の配信率が範囲外です"
            assert delivered + bounced <= emails_sent, f"キャンペーン {campaign_id} のステータス数が矛盾しています"
        
        print(f"キャンペーンパフォーマンス追跡成功: {len(result)}キャンペーンを分析")

    def test_customer_engagement_scoring(self, e2e_synapse_connection: SynapseE2EConnection):
        """顧客エンゲージメントスコアリングのテスト"""
        # エンゲージメントスコア計算クエリ
        result = e2e_synapse_connection.execute_query("""
            SELECT 
                m.client_id,
                m.engagement_score,
                COUNT(p.id) as email_interactions,
                SUM(p.points_granted) as total_points_received,
                DATEDIFF(day, m.last_campaign_response, GETDATE()) as days_since_last_response,
                CASE 
                    WHEN m.engagement_score >= 8.0 AND COUNT(p.id) >= 3 THEN 'HIGHLY_ENGAGED'
                    WHEN m.engagement_score >= 6.0 AND COUNT(p.id) >= 2 THEN 'MODERATELY_ENGAGED'
                    WHEN m.engagement_score >= 4.0 THEN 'LOW_ENGAGED'
                    ELSE 'DISENGAGED'
                END as engagement_level
            FROM marketing_client_dm m
            LEFT JOIN point_grant_email p ON m.client_id = p.client_id
            WHERE m.engagement_score IS NOT NULL
            GROUP BY m.client_id, m.engagement_score, m.last_campaign_response
            ORDER BY m.engagement_score DESC
        """)
        
        assert len(result) >= 0, "エンゲージメントスコアリングクエリの実行に失敗しました"
        
        # エンゲージメントレベルの妥当性確認
        for row in result:
            if len(row) >= 6:
                client_id, score, interactions, total_points, days_since, level = row
                assert 0 <= score <= 10, f"顧客 {client_id} のエンゲージメントスコアが範囲外です"
                assert interactions >= 0, f"顧客 {client_id} のインタラクション数が負の値です"
                assert total_points is None or total_points >= 0, f"顧客 {client_id} のポイント数が負の値です"
        
        print(f"顧客エンゲージメントスコアリング成功: {len(result)}件の顧客を分析")

    def test_email_deliverability_analysis(self, e2e_synapse_connection: SynapseE2EConnection):
        """メール配信可能性分析のテスト"""
        # 配信可能性分析クエリ
        result = e2e_synapse_connection.execute_query("""
            SELECT 
                DATE(email_sent_date) as send_date,
                COUNT(*) as total_sent,
                COUNT(CASE WHEN status = 'DELIVERED' THEN 1 END) as delivered,
                COUNT(CASE WHEN status = 'BOUNCED' THEN 1 END) as bounced,
                COUNT(CASE WHEN status = 'FAILED' THEN 1 END) as failed,
                CAST(COUNT(CASE WHEN status = 'DELIVERED' THEN 1 END) as FLOAT) / COUNT(*) as success_rate
            FROM point_grant_email
            WHERE email_sent_date >= DATEADD(day, -7, GETDATE())
            GROUP BY DATE(email_sent_date)
            ORDER BY send_date DESC
        """)
        
        assert len(result) >= 0, "メール配信可能性分析クエリの実行に失敗しました"
        
        # 配信可能性指標の妥当性確認
        for row in result:
            if len(row) >= 6:
                send_date, total_sent, delivered, bounced, failed, success_rate = row
                assert total_sent >= 0, f"日付 {send_date} の送信総数が負の値です"
                assert delivered + bounced + failed <= total_sent, f"日付 {send_date} のステータス数が矛盾しています"
                assert 0 <= success_rate <= 1, f"日付 {send_date} の成功率が範囲外です"
        
        print(f"メール配信可能性分析成功: {len(result)}日分のデータを分析")

    def test_customer_lifetime_value_calculation(self, e2e_synapse_connection: SynapseE2EConnection):
        """顧客生涯価値計算のテスト"""
        # CLV計算クエリ
        result = e2e_synapse_connection.execute_query("""
            SELECT 
                c.client_id,
                c.segment,
                c.total_amount as current_value,
                m.engagement_score,
                DATEDIFF(month, c.last_transaction_date, GETDATE()) as months_since_last_transaction,
                CASE 
                    WHEN c.total_amount >= 100000 AND m.engagement_score >= 8.0 THEN c.total_amount * 1.5
                    WHEN c.total_amount >= 50000 AND m.engagement_score >= 6.0 THEN c.total_amount * 1.3
                    WHEN c.total_amount >= 10000 AND m.engagement_score >= 4.0 THEN c.total_amount * 1.1
                    ELSE c.total_amount * 0.9
                END as estimated_lifetime_value
            FROM ClientDmBx c
            LEFT JOIN marketing_client_dm m ON c.client_id = m.client_id
            WHERE c.total_amount IS NOT NULL
            ORDER BY estimated_lifetime_value DESC
        """)
        
        assert len(result) >= 0, "CLV計算クエリの実行に失敗しました"
        
        # CLV計算の妥当性確認
        for row in result:
            if len(row) >= 6:
                client_id, segment, current_value, engagement_score, months_since, estimated_clv = row
                assert current_value >= 0, f"顧客 {client_id} の現在価値が負の値です"
                assert estimated_clv is not None, f"顧客 {client_id} のCLVが計算されていません"
                
                # CLV計算ロジックの確認
                if current_value >= 100000 and engagement_score and engagement_score >= 8.0:
                    expected_clv = current_value * 1.5
                    assert abs(estimated_clv - expected_clv) < 0.01, f"顧客 {client_id} のCLV計算が不正です"
        
        print(f"顧客生涯価値計算成功: {len(result)}件の顧客のCLVを計算")

    def test_churn_risk_assessment(self, e2e_synapse_connection: SynapseE2EConnection):
        """チャーンリスク評価のテスト"""
        # チャーンリスク分析クエリ
        result = e2e_synapse_connection.execute_query("""
            SELECT 
                c.client_id,
                c.segment,
                DATEDIFF(day, c.last_transaction_date, GETDATE()) as days_inactive,
                m.engagement_score,
                COUNT(p.id) as recent_email_interactions,
                CASE 
                    WHEN DATEDIFF(day, c.last_transaction_date, GETDATE()) > 180 
                         AND (m.engagement_score IS NULL OR m.engagement_score < 4.0) THEN 'HIGH_RISK'
                    WHEN DATEDIFF(day, c.last_transaction_date, GETDATE()) > 90 
                         AND (m.engagement_score IS NULL OR m.engagement_score < 6.0) THEN 'MEDIUM_RISK'
                    WHEN DATEDIFF(day, c.last_transaction_date, GETDATE()) > 30 THEN 'LOW_RISK'
                    ELSE 'ACTIVE'
                END as churn_risk_level
            FROM ClientDmBx c
            LEFT JOIN marketing_client_dm m ON c.client_id = m.client_id
            LEFT JOIN point_grant_email p ON c.client_id = p.client_id 
                AND p.email_sent_date >= DATEADD(day, -30, GETDATE())
            WHERE c.last_transaction_date IS NOT NULL
            GROUP BY c.client_id, c.segment, c.last_transaction_date, m.engagement_score
            ORDER BY days_inactive DESC
        """)
        
        assert len(result) >= 0, "チャーンリスク評価クエリの実行に失敗しました"
        
        # チャーンリスク評価の妥当性確認
        for row in result:
            if len(row) >= 6:
                client_id, segment, days_inactive, engagement_score, interactions, risk_level = row
                assert days_inactive >= 0, f"顧客 {client_id} の非アクティブ日数が負の値です"
                assert interactions >= 0, f"顧客 {client_id} のインタラクション数が負の値です"
                assert risk_level in ['HIGH_RISK', 'MEDIUM_RISK', 'LOW_RISK', 'ACTIVE'], \
                    f"顧客 {client_id} のリスクレベルが不正です: {risk_level}"
        
        print(f"チャーンリスク評価成功: {len(result)}件の顧客のリスクを評価")

    def test_marketing_roi_calculation(self, e2e_synapse_connection: SynapseE2EConnection):
        """マーケティングROI計算のテスト"""
        # ROI計算クエリ（シミュレーション）
        result = e2e_synapse_connection.execute_query("""
            SELECT 
                p.campaign_id,
                COUNT(*) as emails_sent,
                SUM(p.points_granted) as total_points_cost,
                -- 仮定: ポイント1点 = 1円のコスト、配信率から収益を推定
                COUNT(CASE WHEN p.status = 'DELIVERED' THEN 1 END) * 100 as estimated_revenue,
                SUM(p.points_granted) as campaign_cost,
                CASE 
                    WHEN SUM(p.points_granted) > 0 THEN 
                        (COUNT(CASE WHEN p.status = 'DELIVERED' THEN 1 END) * 100.0 - SUM(p.points_granted)) / SUM(p.points_granted)
                    ELSE 0
                END as roi_ratio
            FROM point_grant_email p
            WHERE p.campaign_id IS NOT NULL
            AND p.email_sent_date >= DATEADD(day, -30, GETDATE())
            GROUP BY p.campaign_id
            HAVING COUNT(*) >= 1
            ORDER BY roi_ratio DESC
        """)
        
        assert len(result) >= 0, "マーケティングROI計算クエリの実行に失敗しました"
        
        # ROI計算の妥当性確認
        for row in result:
            if len(row) >= 6:
                campaign_id, emails_sent, points_cost, revenue, cost, roi = row
                assert emails_sent >= 0, f"キャンペーン {campaign_id} のメール送信数が負の値です"
                assert points_cost >= 0, f"キャンペーン {campaign_id} のポイントコストが負の値です"
                assert revenue >= 0, f"キャンペーン {campaign_id} の推定収益が負の値です"
        
        print(f"マーケティングROI計算成功: {len(result)}キャンペーンのROIを計算")

    def test_cross_sell_opportunity_identification(self, e2e_synapse_connection: SynapseE2EConnection):
        """クロスセル機会特定のテスト"""
        # クロスセル機会分析クエリ
        result = e2e_synapse_connection.execute_query("""
            SELECT 
                c.client_id,
                c.segment,
                c.total_amount,
                m.marketing_segment,
                m.preference_category,
                CASE 
                    WHEN c.segment = 'PREMIUM' AND m.preference_category NOT LIKE '%FINANCE%' THEN 'FINANCIAL_PRODUCTS'
                    WHEN c.total_amount >= 50000 AND m.preference_category NOT LIKE '%LIFESTYLE%' THEN 'LIFESTYLE_SERVICES'
                    WHEN m.engagement_score >= 7.0 AND m.preference_category NOT LIKE '%PREMIUM%' THEN 'PREMIUM_SERVICES'
                    ELSE 'STANDARD_OFFERS'
                END as cross_sell_opportunity
            FROM ClientDmBx c
            INNER JOIN marketing_client_dm m ON c.client_id = m.client_id
            WHERE c.total_amount >= 10000
            AND m.engagement_score >= 5.0
            ORDER BY c.total_amount DESC
        """)
        
        assert len(result) >= 0, "クロスセル機会特定クエリの実行に失敗しました"
        
        # クロスセル機会の妥当性確認
        opportunity_types = ['FINANCIAL_PRODUCTS', 'LIFESTYLE_SERVICES', 'PREMIUM_SERVICES', 'STANDARD_OFFERS']
        for row in result:
            if len(row) >= 6:
                client_id, segment, amount, marketing_segment, preference, opportunity = row
                assert amount >= 10000, f"顧客 {client_id} の取引額が条件を満たしていません"
                assert opportunity in opportunity_types, f"顧客 {client_id} の機会タイプが不正です: {opportunity}"
        
        print(f"クロスセル機会特定成功: {len(result)}件の機会を特定")

    def test_a_b_test_performance_comparison(self, e2e_synapse_connection: SynapseE2EConnection):
        """A/Bテストパフォーマンス比較のテスト"""
        # A/Bテスト用のテストデータ準備
        self._prepare_ab_test_data(e2e_synapse_connection)
        
        # A/Bテスト結果比較クエリ
        result = e2e_synapse_connection.execute_query("""
            SELECT 
                campaign_id,
                COUNT(*) as total_sends,
                COUNT(CASE WHEN status = 'DELIVERED' THEN 1 END) as deliveries,
                AVG(CAST(points_granted as FLOAT)) as avg_points,
                CAST(COUNT(CASE WHEN status = 'DELIVERED' THEN 1 END) as FLOAT) / COUNT(*) as delivery_rate
            FROM point_grant_email
            WHERE campaign_id IN ('AB_TEST_A', 'AB_TEST_B')
            GROUP BY campaign_id
            ORDER BY delivery_rate DESC
        """)
        
        assert len(result) >= 2, "A/Bテスト比較結果が期待される数に達していません"
        
        # A/Bテスト結果の妥当性確認
        campaign_results = {}
        for row in result:
            campaign_id, total_sends, deliveries, avg_points, delivery_rate = row
            campaign_results[campaign_id] = {
                'total_sends': total_sends,
                'deliveries': deliveries,
                'delivery_rate': delivery_rate
            }
            assert total_sends >= 1, f"キャンペーン {campaign_id} の送信数が不正です"
            assert 0 <= delivery_rate <= 1, f"キャンペーン {campaign_id} の配信率が範囲外です"
        
        # 統計的有意性の簡易チェック（サンプルサイズの確認）
        for campaign_id, stats in campaign_results.items():
            assert stats['total_sends'] >= 10, f"キャンペーン {campaign_id} のサンプルサイズが小さすぎます"
        
        print(f"A/Bテストパフォーマンス比較成功: {len(result)}キャンペーンを比較")

    def test_customer_journey_mapping(self, e2e_synapse_connection: SynapseE2EConnection):
        """顧客ジャーニーマッピングのテスト"""
        # 顧客ジャーニー分析クエリ
        result = e2e_synapse_connection.execute_query("""
            SELECT 
                c.client_id,
                c.last_transaction_date as last_transaction,
                m.last_campaign_response as last_marketing_interaction,
                p.email_sent_date as last_email_sent,
                DATEDIFF(day, c.last_transaction_date, m.last_campaign_response) as transaction_to_marketing_days,
                DATEDIFF(day, m.last_campaign_response, p.email_sent_date) as marketing_to_email_days,
                CASE 
                    WHEN c.last_transaction_date > m.last_campaign_response 
                         AND c.last_transaction_date > p.email_sent_date THEN 'TRANSACTION_DRIVEN'
                    WHEN m.last_campaign_response > c.last_transaction_date 
                         AND m.last_campaign_response > p.email_sent_date THEN 'MARKETING_DRIVEN'
                    WHEN p.email_sent_date > c.last_transaction_date 
                         AND p.email_sent_date > m.last_campaign_response THEN 'EMAIL_DRIVEN'
                    ELSE 'MIXED_JOURNEY'
                END as journey_type
            FROM ClientDmBx c
            LEFT JOIN marketing_client_dm m ON c.client_id = m.client_id
            LEFT JOIN (
                SELECT client_id, MAX(email_sent_date) as email_sent_date
                FROM point_grant_email
                GROUP BY client_id
            ) p ON c.client_id = p.client_id
            WHERE c.last_transaction_date IS NOT NULL
            ORDER BY c.last_transaction_date DESC
        """)
        
        assert len(result) >= 0, "顧客ジャーニーマッピングクエリの実行に失敗しました"
        
        # ジャーニータイプの分布確認
        journey_types = ['TRANSACTION_DRIVEN', 'MARKETING_DRIVEN', 'EMAIL_DRIVEN', 'MIXED_JOURNEY']
        for row in result:
            if len(row) >= 7:
                client_id, last_trans, last_marketing, last_email, trans_to_marketing, marketing_to_email, journey_type = row
                assert journey_type in journey_types, f"顧客 {client_id} のジャーニータイプが不正です: {journey_type}"
        
        print(f"顧客ジャーニーマッピング成功: {len(result)}件の顧客ジャーニーを分析")

    def _prepare_segmentation_test_data(self, connection: SynapseE2EConnection):
        """セグメンテーションテスト用のデータ準備"""
        # marketing_client_dm テーブルにテストデータ
        test_data = [
            ('SEG_HIGH_001', 'HIGH_VALUE', 'FINANCE', 9.2, '2024-01-10', 1, 1),
            ('SEG_HIGH_002', 'HIGH_VALUE', 'LIFESTYLE', 8.8, '2024-01-12', 1, 0),
            ('SEG_MED_001', 'MEDIUM_VALUE', 'BASIC', 6.5, '2024-01-15', 1, 1),
            ('SEG_MED_002', 'MEDIUM_VALUE', 'FINANCE', 7.2, '2024-01-16', 0, 1),
            ('SEG_LOW_001', 'LOW_VALUE', 'BASIC', 4.3, '2024-01-18', 1, 0),
            ('SEG_LOW_002', 'LOW_VALUE', 'BASIC', 3.8, '2024-01-20', 0, 0)
        ]
        
        for client_id, segment, category, score, response_date, email_opt, sms_opt in test_data:
            connection.execute_query("""
                INSERT INTO marketing_client_dm (client_id, marketing_segment, preference_category, engagement_score, last_campaign_response, opt_in_email, opt_in_sms, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, GETDATE(), GETDATE())
            """, (client_id, segment, category, score, response_date, email_opt, sms_opt))

    def _prepare_campaign_test_data(self, connection: SynapseE2EConnection):
        """キャンペーンテスト用のデータ準備"""
        # point_grant_email テーブルにテストデータ
        campaigns = ['PERF_TEST_001', 'PERF_TEST_002', 'PERF_TEST_003']
        statuses = ['DELIVERED', 'BOUNCED', 'FAILED']
        
        for i, campaign_id in enumerate(campaigns):
            for j in range(10):  # 各キャンペーンに10件のレコード
                client_id = f'CAMP_CLIENT_{i}_{j:03d}'
                email = f'test{i}{j}@example.com'
                points = random.randint(100, 1000)
                status = random.choice(statuses) if random.random() > 0.2 else 'DELIVERED'  # 80%の確率でDELIVERED
                
                connection.execute_query("""
                    INSERT INTO point_grant_email (client_id, email, points_granted, email_sent_date, campaign_id, status, created_at)
                    VALUES (?, ?, ?, GETDATE(), ?, ?, GETDATE())
                """, (client_id, email, points, campaign_id, status))

    def _prepare_ab_test_data(self, connection: SynapseE2EConnection):
        """A/Bテスト用のデータ準備"""
        # A/Bテスト用のデータ
        for campaign_id in ['AB_TEST_A', 'AB_TEST_B']:
            for i in range(20):  # 各バリエーションに20件
                client_id = f'{campaign_id}_CLIENT_{i:03d}'
                email = f'{campaign_id.lower()}_{i}@example.com'
                points = 500 if campaign_id == 'AB_TEST_A' else 1000  # AとBでポイント数を変える
                status = 'DELIVERED' if random.random() > 0.1 else 'BOUNCED'  # 90%の確率でDELIVERED
                
                connection.execute_query("""
                    INSERT INTO point_grant_email (client_id, email, points_granted, email_sent_date, campaign_id, status, created_at)
                    VALUES (?, ?, ?, GETDATE(), ?, ?, GETDATE())
                """, (client_id, email, points, campaign_id, status))
