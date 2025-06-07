"""
E2E Test Suite for Comprehensive Business Analytics Operations

包括的なビジネス分析操作のE2Eテスト
売上分析、顧客行動分析、製品分析、地域別分析などを含む
"""
import os
import pytest
import time
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Any, Tuple
import logging

# テスト環境のセットアップ
from tests.helpers.reproducible_e2e_helper import (
    setup_reproducible_test_class, 
    cleanup_reproducible_test_class
)
from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection

# ロガーの設定
logger = logging.getLogger(__name__)

class TestComprehensiveBusinessAnalytics:
    """包括的なビジネス分析のE2Eテスト"""
    
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

    def test_sales_revenue_analysis(self):
        """売上収益分析のテスト"""
        connection = SynapseE2EConnection()
        
        # テストデータの挿入
        test_sales_data = [
            (1, 1, '2024-01-15', 150.50, 'completed'),
            (2, 2, '2024-01-16', 275.25, 'completed'),  
            (3, 1, '2024-01-17', 89.99, 'completed'),
            (4, 3, '2024-02-01', 450.00, 'completed'),
            (5, 2, '2024-02-02', 125.75, 'completed')
        ]
        
        for sale_id, client_id, sale_date, amount, status in test_sales_data:
            connection.execute_query(f"""
                INSERT INTO sales_transactions (id, client_id, sale_date, amount, status) 
                VALUES ({sale_id}, {client_id}, '{sale_date}', {amount}, '{status}')
            """)
        
        # 月別売上分析
        monthly_results = connection.execute_query("""
            SELECT 
                YEAR(sale_date) as year,
                MONTH(sale_date) as month,
                COUNT(*) as transaction_count,
                SUM(amount) as total_revenue,
                AVG(amount) as avg_transaction_value,
                MAX(amount) as max_transaction,
                MIN(amount) as min_transaction
            FROM sales_transactions 
            WHERE status = 'completed'
            GROUP BY YEAR(sale_date), MONTH(sale_date)
            ORDER BY year, month
        """)
        
        assert len(monthly_results) >= 2, "月別売上データが正しく集計されていません"
        
        # 1月の売上検証
        jan_data = [r for r in monthly_results if r[1] == 1][0]
        assert jan_data[2] == 3, "1月の取引数が正しくありません"
        assert abs(jan_data[3] - 515.74) < 0.01, "1月の総売上が正しくありません"
        
        logger.info(f"売上収益分析テスト完了: {len(monthly_results)}ヶ月のデータを分析")

    def test_customer_segmentation_analysis(self):
        """顧客セグメンテーション分析のテスト"""
        connection = SynapseE2EConnection()
        
        # 顧客セグメンテーション分析
        segmentation_results = connection.execute_query("""
            WITH customer_metrics AS (
                SELECT 
                    c.id,
                    c.segment,
                    COUNT(st.id) as transaction_count,
                    SUM(st.amount) as total_spent,
                    AVG(st.amount) as avg_transaction_value,
                    DATEDIFF(day, MAX(st.sale_date), GETDATE()) as days_since_last_purchase
                FROM ClientDmBx c
                LEFT JOIN sales_transactions st ON c.client_id = st.client_id
                GROUP BY c.id, c.segment
            )
            SELECT 
                segment,
                COUNT(*) as customer_count,
                AVG(transaction_count) as avg_transactions_per_customer,
                AVG(total_spent) as avg_lifetime_value,
                AVG(avg_transaction_value) as avg_order_value,
                AVG(days_since_last_purchase) as avg_days_since_purchase
            FROM customer_metrics
            WHERE segment IS NOT NULL
            GROUP BY segment
            ORDER BY avg_lifetime_value DESC
        """)
        
        assert len(segmentation_results) > 0, "顧客セグメンテーション結果が取得できません"
        
        # 各セグメントの基本検証
        for segment_data in segmentation_results:
            segment_name = segment_data[0]
            customer_count = segment_data[1]
            avg_ltv = segment_data[3]
            
            assert customer_count > 0, f"セグメント {segment_name} の顧客数が0です"
            assert avg_ltv is not None, f"セグメント {segment_name} の平均LTVが計算されていません"
        
        logger.info(f"顧客セグメンテーション分析テスト完了: {len(segmentation_results)}セグメントを分析")

    def test_product_performance_analysis(self):
        """製品パフォーマンス分析のテスト"""
        connection = SynapseE2EConnection()
        
        # 製品データの挿入
        product_data = [
            (1, 'Electronics', 'Smartphone', 599.99),
            (2, 'Electronics', 'Laptop', 1299.99),
            (3, 'Clothing', 'T-Shirt', 29.99),
            (4, 'Books', 'Programming Guide', 49.99),
            (5, 'Home', 'Coffee Maker', 89.99)
        ]
        
        for prod_id, category, name, price in product_data:
            connection.execute_query(f"""
                INSERT INTO products (id, category, name, price) 
                VALUES ({prod_id}, '{category}', '{name}', {price})
            """)
        
        # 製品売上データ
        product_sales_data = [
            (1, 1, 2, 599.99 * 2),  # Smartphone × 2
            (2, 2, 1, 1299.99),     # Laptop × 1  
            (3, 3, 5, 29.99 * 5),   # T-Shirt × 5
            (4, 4, 3, 49.99 * 3),   # Programming Guide × 3
            (5, 5, 1, 89.99)        # Coffee Maker × 1
        ]
        
        for sale_id, product_id, quantity, total_amount in product_sales_data:
            connection.execute_query(f"""
                INSERT INTO product_sales (id, product_id, quantity, total_amount) 
                VALUES ({sale_id}, {product_id}, {quantity}, {total_amount})
            """)
        
        # 製品パフォーマンス分析
        performance_results = connection.execute_query("""
            SELECT 
                p.category,
                p.name,
                SUM(ps.quantity) as total_quantity_sold,
                SUM(ps.total_amount) as total_revenue,
                AVG(ps.total_amount / ps.quantity) as avg_selling_price,
                COUNT(ps.id) as number_of_sales
            FROM products p
            INNER JOIN product_sales ps ON p.id = ps.product_id
            GROUP BY p.category, p.name, p.id
            ORDER BY total_revenue DESC
        """)
        
        assert len(performance_results) == 5, "全製品のパフォーマンス データが取得できません"
        
        # トップ売上製品の検証（Laptop）
        top_product = performance_results[0]
        assert top_product[1] == 'Laptop', "トップ売上製品が正しくありません"
        assert abs(top_product[3] - 1299.99) < 0.01, "トップ製品の売上金額が正しくありません"
        
        logger.info(f"製品パフォーマンス分析テスト完了: {len(performance_results)}製品を分析")

    def test_regional_sales_analysis(self):
        """地域別売上分析のテスト"""
        connection = SynapseE2EConnection()
        
        # 地域データの挿入
        regional_data = [
            (1, 1, 'Tokyo', 'Japan', 'Asia'),
            (2, 2, 'Osaka', 'Japan', 'Asia'),
            (3, 3, 'New York', 'USA', 'North America'),
            (4, 4, 'London', 'UK', 'Europe'),
            (5, 5, 'Sydney', 'Australia', 'Oceania')
        ]
        
        for region_id, client_id, city, country, continent in regional_data:
            connection.execute_query(f"""
                INSERT INTO client_regions (id, client_id, city, country, continent) 
                VALUES ({region_id}, {client_id}, '{city}', '{country}', '{continent}')
            """)
        
        # 地域別売上分析
        regional_results = connection.execute_query("""
            SELECT 
                cr.continent,
                cr.country,
                COUNT(DISTINCT cr.client_id) as unique_customers,
                COUNT(st.id) as total_transactions,
                SUM(st.amount) as total_revenue,
                AVG(st.amount) as avg_transaction_value
            FROM client_regions cr
            LEFT JOIN sales_transactions st ON cr.client_id = st.client_id
            WHERE st.status = 'completed'
            GROUP BY cr.continent, cr.country
            ORDER BY total_revenue DESC
        """)
        
        assert len(regional_results) > 0, "地域別売上データが取得できません"
        
        # 各地域の基本検証
        total_global_revenue = sum([r[4] or 0 for r in regional_results])
        assert total_global_revenue > 0, "グローバル売上合計が0です"
        
        # 大陸別集計の検証
        continent_totals = {}
        for result in regional_results:
            continent = result[0]
            revenue = result[4] or 0
            continent_totals[continent] = continent_totals.get(continent, 0) + revenue
        
        assert len(continent_totals) > 0, "大陸別データが集計されていません"
        
        logger.info(f"地域別売上分析テスト完了: {len(regional_results)}地域、{len(continent_totals)}大陸を分析")

    def test_time_series_trend_analysis(self):
        """時系列トレンド分析のテスト"""
        connection = SynapseE2EConnection()
        
        # 過去12ヶ月のトレンドデータ生成
        base_date = datetime(2024, 1, 1)
        trend_data = []
        
        for month in range(12):
            current_date = base_date + timedelta(days=30 * month)
            # 季節性を考慮した売上パターン（12月と1月は高売上）
            seasonal_multiplier = 1.5 if month in [0, 11] else 1.0
            base_sales = 1000 + (month * 50)  # 月次成長トレンド
            monthly_sales = base_sales * seasonal_multiplier
            
            trend_data.append((
                month + 1,
                current_date.strftime('%Y-%m-%d'),
                int(monthly_sales),
                f'2024-{month+1:02d}'
            ))
        
        for trend_id, trend_date, sales_amount, period in trend_data:
            connection.execute_query(f"""
                INSERT INTO monthly_sales_trends (id, sales_date, sales_amount, period) 
                VALUES ({trend_id}, '{trend_date}', {sales_amount}, '{period}')
            """)
        
        # トレンド分析クエリ
        trend_results = connection.execute_query("""
            WITH monthly_trends AS (
                SELECT 
                    period,
                    sales_amount,
                    LAG(sales_amount) OVER (ORDER BY period) as prev_month_sales,
                    sales_amount - LAG(sales_amount) OVER (ORDER BY period) as month_over_month_change,
                    CASE 
                        WHEN LAG(sales_amount) OVER (ORDER BY period) > 0 
                        THEN ((sales_amount - LAG(sales_amount) OVER (ORDER BY period)) * 100.0 / LAG(sales_amount) OVER (ORDER BY period))
                        ELSE 0
                    END as month_over_month_pct
                FROM monthly_sales_trends
            )
            SELECT 
                period,
                sales_amount,
                prev_month_sales,
                month_over_month_change,
                month_over_month_pct,
                AVG(sales_amount) OVER (ORDER BY period ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as three_month_avg
            FROM monthly_trends
            ORDER BY period
        """)
        
        assert len(trend_results) == 12, "12ヶ月のトレンドデータが正しく取得できません"
        
        # 成長率の検証
        positive_growth_months = [r for r in trend_results if r[4] and r[4] > 0]
        assert len(positive_growth_months) >= 6, "成長月数が期待値を下回っています"
        
        # 季節性の検証（12月は高売上）
        december_data = [r for r in trend_results if r[0] == '2024-12'][0]
        november_data = [r for r in trend_results if r[0] == '2024-11'][0]
        assert december_data[1] > november_data[1], "12月の季節性効果が確認できません"
        
        logger.info(f"時系列トレンド分析テスト完了: {len(trend_results)}ヶ月のトレンドを分析")

    def test_customer_lifetime_value_calculation(self):
        """顧客生涯価値（CLV）計算のテスト"""
        connection = SynapseE2EConnection()
        
        # CLV計算クエリ
        clv_results = connection.execute_query("""
            WITH customer_summary AS (
                SELECT 
                    st.client_id,
                    COUNT(st.id) as total_orders,
                    SUM(st.amount) as total_revenue,
                    AVG(st.amount) as avg_order_value,
                    MIN(st.sale_date) as first_purchase_date,
                    MAX(st.sale_date) as last_purchase_date,
                    DATEDIFF(day, MIN(st.sale_date), MAX(st.sale_date)) as customer_lifespan_days
                FROM sales_transactions st
                WHERE st.status = 'completed'
                GROUP BY st.client_id
            ),
            clv_calculation AS (
                SELECT 
                    client_id,
                    total_orders,
                    total_revenue,
                    avg_order_value,
                    customer_lifespan_days,
                    CASE 
                        WHEN customer_lifespan_days > 0 
                        THEN (total_orders * 365.0) / customer_lifespan_days  -- 年間注文頻度
                        ELSE total_orders 
                    END as annual_order_frequency,
                    CASE 
                        WHEN customer_lifespan_days > 0 
                        THEN avg_order_value * ((total_orders * 365.0) / customer_lifespan_days) * 3  -- 3年CLV予測
                        ELSE avg_order_value * total_orders
                    END as predicted_clv_3_years
                FROM customer_summary
            )
            SELECT 
                client_id,
                total_orders,
                total_revenue,
                avg_order_value,
                annual_order_frequency,
                predicted_clv_3_years,
                CASE 
                    WHEN predicted_clv_3_years >= 1500 THEN 'High Value'
                    WHEN predicted_clv_3_years >= 500 THEN 'Medium Value'
                    ELSE 'Low Value'
                END as clv_segment
            FROM clv_calculation
            ORDER BY predicted_clv_3_years DESC
        """)
        
        assert len(clv_results) > 0, "CLV計算結果が取得できません"
        
        # セグメント分布の検証
        segment_distribution = {}
        for result in clv_results:
            segment = result[6]
            segment_distribution[segment] = segment_distribution.get(segment, 0) + 1
        
        assert 'High Value' in segment_distribution, "高価値顧客セグメントが存在しません"
        
        # 上位顧客のCLV検証
        top_customer = clv_results[0]
        assert top_customer[5] > 0, "トップ顧客のCLVが計算されていません"
        
        logger.info(f"CLV計算テスト完了: {len(clv_results)}顧客のCLVを計算、{len(segment_distribution)}セグメントに分類")

    def test_cohort_retention_analysis(self):
        """コホート保持率分析のテスト"""
        connection = SynapseE2EConnection()
        
        # コホート分析データの準備
        cohort_data = [
            (1, 1, '2024-01-01', 'acquisition'),  # 1月コホート
            (2, 1, '2024-02-15', 'retention'),    # 1ヶ月後リテンション
            (3, 1, '2024-03-20', 'retention'),    # 2ヶ月後リテンション
            (4, 2, '2024-01-15', 'acquisition'),  # 1月コホート
            (5, 2, '2024-02-20', 'retention'),    # 1ヶ月後リテンション
            (6, 3, '2024-02-01', 'acquisition'),  # 2月コホート
            (7, 3, '2024-03-10', 'retention'),    # 1ヶ月後リテンション
        ]
        
        for activity_id, client_id, activity_date, activity_type in cohort_data:
            connection.execute_query(f"""
                INSERT INTO customer_activities (id, client_id, activity_date, activity_type) 
                VALUES ({activity_id}, {client_id}, '{activity_date}', '{activity_type}')
            """)
        
        # コホート保持率分析
        cohort_results = connection.execute_query("""
            WITH customer_cohorts AS (
                SELECT 
                    client_id,
                    MIN(activity_date) as cohort_month,
                    YEAR(MIN(activity_date)) as cohort_year,
                    MONTH(MIN(activity_date)) as cohort_month_num
                FROM customer_activities 
                WHERE activity_type = 'acquisition'
                GROUP BY client_id
            ),
            cohort_activity AS (
                SELECT 
                    cc.cohort_year,
                    cc.cohort_month_num,
                    ca.client_id,
                    ca.activity_date,
                    DATEDIFF(month, cc.cohort_month, ca.activity_date) as period_number
                FROM customer_cohorts cc
                INNER JOIN customer_activities ca ON cc.client_id = ca.client_id
            ),
            cohort_table AS (
                SELECT 
                    cohort_year,
                    cohort_month_num,
                    period_number,
                    COUNT(DISTINCT client_id) as customers
                FROM cohort_activity
                GROUP BY cohort_year, cohort_month_num, period_number
            ),
            cohort_sizes AS (
                SELECT 
                    cohort_year,
                    cohort_month_num,
                    customers as cohort_size
                FROM cohort_table 
                WHERE period_number = 0
            )
            SELECT 
                ct.cohort_year,
                ct.cohort_month_num,
                ct.period_number,
                ct.customers,
                cs.cohort_size,
                (ct.customers * 100.0 / cs.cohort_size) as retention_rate
            FROM cohort_table ct
            INNER JOIN cohort_sizes cs ON ct.cohort_year = cs.cohort_year 
                AND ct.cohort_month_num = cs.cohort_month_num
            ORDER BY ct.cohort_year, ct.cohort_month_num, ct.period_number
        """)
        
        assert len(cohort_results) > 0, "コホート分析結果が取得できません"
        
        # 保持率の基本検証
        retention_rates = [r[5] for r in cohort_results if r[2] > 0]  # period_number > 0
        assert len(retention_rates) > 0, "リテンション率が計算されていません"
        
        # 1月コホートの検証
        jan_cohort_data = [r for r in cohort_results if r[0] == 2024 and r[1] == 1]
        assert len(jan_cohort_data) >= 2, "1月コホートのデータが不足しています"
        
        logger.info(f"コホート保持率分析テスト完了: {len(cohort_results)}のコホート期間を分析")

    def test_advanced_statistical_analysis(self):
        """高度な統計分析のテスト"""
        connection = SynapseE2EConnection()
        
        # 統計分析クエリ
        stats_results = connection.execute_query("""
            WITH sales_statistics AS (
                SELECT 
                    client_id,
                    COUNT(*) as transaction_count,
                    SUM(amount) as total_amount,
                    AVG(amount) as mean_amount,
                    MIN(amount) as min_amount,
                    MAX(amount) as max_amount,
                    -- 標準偏差の計算
                    SQRT(SUM(POWER(amount - AVG(amount) OVER (PARTITION BY client_id), 2)) / COUNT(*)) as std_deviation,
                    -- パーセンタイルの近似
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY amount) OVER (PARTITION BY client_id) as q1,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) OVER (PARTITION BY client_id) as median,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY amount) OVER (PARTITION BY client_id) as q3
                FROM sales_transactions 
                WHERE status = 'completed'
                GROUP BY client_id
            ),
            outlier_detection AS (
                SELECT 
                    *,
                    q3 - q1 as iqr,
                    q1 - (1.5 * (q3 - q1)) as lower_bound,
                    q3 + (1.5 * (q3 - q1)) as upper_bound
                FROM sales_statistics
            )
            SELECT 
                client_id,
                transaction_count,
                total_amount,
                mean_amount,
                std_deviation,
                median,
                iqr,
                CASE 
                    WHEN max_amount > upper_bound OR min_amount < lower_bound THEN 'Has Outliers'
                    ELSE 'No Outliers'
                END as outlier_status,
                CASE 
                    WHEN std_deviation > mean_amount * 0.5 THEN 'High Variability'
                    WHEN std_deviation > mean_amount * 0.2 THEN 'Medium Variability'  
                    ELSE 'Low Variability'
                END as variability_category
            FROM outlier_detection
            ORDER BY std_deviation DESC
        """)
        
        assert len(stats_results) > 0, "統計分析結果が取得できません"
        
        # 統計指標の検証
        for result in stats_results:
            client_id = result[0]
            mean_amount = result[3]
            std_deviation = result[4]
            median = result[5]
            
            assert mean_amount > 0, f"顧客 {client_id} の平均金額が正しくありません"
            assert std_deviation >= 0, f"顧客 {client_id} の標準偏差が正しくありません"
            assert median > 0, f"顧客 {client_id} の中央値が正しくありません"
        
        # 変動性カテゴリの分布検証
        variability_dist = {}
        for result in stats_results:
            category = result[8]
            variability_dist[category] = variability_dist.get(category, 0) + 1
        
        assert len(variability_dist) > 0, "変動性カテゴリが分類されていません"
        
        logger.info(f"高度統計分析テスト完了: {len(stats_results)}顧客の統計指標を分析、{len(variability_dist)}カテゴリに分類")

    def test_predictive_modeling_simulation(self):
        """予測モデリングシミュレーションのテスト"""
        connection = SynapseE2EConnection()
        
        # 予測モデル用のフィーチャー生成
        feature_results = connection.execute_query("""
            WITH customer_features AS (
                SELECT 
                    c.client_id,
                    c.segment,
                    COUNT(st.id) as historical_transactions,
                    SUM(st.amount) as historical_spend,
                    AVG(st.amount) as avg_transaction_value,
                    MAX(st.sale_date) as last_purchase_date,
                    DATEDIFF(day, MAX(st.sale_date), GETDATE()) as days_since_last_purchase,
                    -- 購買行動パターン
                    COUNT(DISTINCT YEAR(st.sale_date)) as active_years,
                    COUNT(DISTINCT MONTH(st.sale_date)) as active_months,
                    -- トレンド指標
                    CASE 
                        WHEN COUNT(st.id) >= 5 THEN 'Frequent'
                        WHEN COUNT(st.id) >= 3 THEN 'Regular'  
                        ELSE 'Occasional'
                    END as purchase_frequency_category,
                    -- チャーン予測指標
                    CASE 
                        WHEN DATEDIFF(day, MAX(st.sale_date), GETDATE()) > 90 THEN 'High Risk'
                        WHEN DATEDIFF(day, MAX(st.sale_date), GETDATE()) > 60 THEN 'Medium Risk'
                        ELSE 'Low Risk' 
                    END as churn_risk_category
                FROM ClientDmBx c
                LEFT JOIN sales_transactions st ON c.client_id = st.client_id AND st.status = 'completed'
                GROUP BY c.client_id, c.segment
            ),
            prediction_features AS (
                SELECT 
                    *,
                    -- 予測スコア（簡単な線形モデル）
                    (historical_transactions * 0.3 + 
                     (historical_spend / 100) * 0.4 + 
                     CASE WHEN days_since_last_purchase <= 30 THEN 10 ELSE 0 END +
                     active_months * 0.2) as engagement_score,
                    -- 将来購買確率
                    CASE 
                        WHEN days_since_last_purchase <= 30 AND historical_transactions >= 3 THEN 0.8
                        WHEN days_since_last_purchase <= 60 AND historical_transactions >= 2 THEN 0.6
                        WHEN days_since_last_purchase <= 90 THEN 0.4
                        ELSE 0.2
                    END as predicted_purchase_probability
                FROM customer_features
            )
            SELECT 
                client_id,
                segment,
                historical_transactions,
                historical_spend,
                purchase_frequency_category,
                churn_risk_category,
                engagement_score,
                predicted_purchase_probability,
                CASE 
                    WHEN predicted_purchase_probability >= 0.7 THEN 'High Probability'
                    WHEN predicted_purchase_probability >= 0.5 THEN 'Medium Probability'
                    ELSE 'Low Probability'
                END as purchase_likelihood_segment
            FROM prediction_features
            ORDER BY engagement_score DESC
        """)
        
        assert len(feature_results) > 0, "予測モデリング結果が取得できません"
        
        # 予測結果の検証
        high_prob_customers = [r for r in feature_results if r[8] == 'High Probability']
        medium_prob_customers = [r for r in feature_results if r[8] == 'Medium Probability']
        low_prob_customers = [r for r in feature_results if r[8] == 'Low Probability']
        
        total_customers = len(feature_results)
        assert total_customers > 0, "総顧客数が0です"
        
        # セグメント分布の検証
        high_prob_pct = len(high_prob_customers) / total_customers * 100
        medium_prob_pct = len(medium_prob_customers) / total_customers * 100
        low_prob_pct = len(low_prob_customers) / total_customers * 100
        
        assert abs(high_prob_pct + medium_prob_pct + low_prob_pct - 100) < 0.1, "確率セグメントの合計が100%になりません"
        
        # エンゲージメントスコアの検証
        engagement_scores = [r[6] for r in feature_results if r[6] is not None]
        assert len(engagement_scores) > 0, "エンゲージメント スコアが計算されていません"
        assert max(engagement_scores) > min(engagement_scores), "エンゲージメント スコアに差がありません"
        
        logger.info(f"予測モデリングテスト完了: {total_customers}顧客を分析、高確率{len(high_prob_customers)}、中確率{len(medium_prob_customers)}、低確率{len(low_prob_customers)}顧客")

    def test_advanced_business_kpi_dashboard(self):
        """高度なビジネスKPIダッシュボードのテスト"""
        connection = SynapseE2EConnection()
        
        # 包括的KPI計算
        kpi_results = connection.execute_query("""
            WITH kpi_calculations AS (
                -- 売上関連KPI
                SELECT 
                    'Revenue KPIs' as kpi_category,
                    'Total Revenue' as kpi_name,
                    SUM(amount) as kpi_value,
                    'USD' as unit,
                    GETDATE() as calculation_date
                FROM sales_transactions 
                WHERE status = 'completed'
                
                UNION ALL
                
                SELECT 
                    'Revenue KPIs',
                    'Average Order Value',
                    AVG(amount),
                    'USD',
                    GETDATE()
                FROM sales_transactions 
                WHERE status = 'completed'
                
                UNION ALL
                
                -- 顧客関連KPI
                SELECT 
                    'Customer KPIs',
                    'Total Active Customers',
                    COUNT(DISTINCT client_id),
                    'Count',
                    GETDATE()
                FROM sales_transactions 
                WHERE status = 'completed'
                
                UNION ALL
                
                SELECT 
                    'Customer KPIs',
                    'Customer Acquisition Rate',
                    COUNT(DISTINCT client_id) * 100.0 / 
                        (SELECT COUNT(*) FROM ClientDmBx WHERE client_id IS NOT NULL), 
                    'Percentage',
                    GETDATE()
                FROM sales_transactions 
                WHERE status = 'completed'
                
                UNION ALL
                
                -- オペレーション関連KPI
                SELECT 
                    'Operations KPIs',
                    'Total Transactions',
                    COUNT(*),
                    'Count',
                    GETDATE()
                FROM sales_transactions 
                WHERE status = 'completed'
                
                UNION ALL
                
                SELECT 
                    'Operations KPIs',
                    'Transaction Success Rate',
                    COUNT(CASE WHEN status = 'completed' THEN 1 END) * 100.0 / COUNT(*),
                    'Percentage', 
                    GETDATE()
                FROM sales_transactions
                
                UNION ALL
                
                -- マーケティング関連KPI  
                SELECT 
                    'Marketing KPIs',
                    'Email Campaign Engagement',
                    COUNT(CASE WHEN status = 'sent' THEN 1 END) * 100.0 / 
                        NULLIF(COUNT(CASE WHEN status IN ('sent', 'failed') THEN 1 END), 0),
                    'Percentage',
                    GETDATE()
                FROM point_grant_email
            )
            SELECT 
                kpi_category,
                kpi_name,
                ROUND(kpi_value, 2) as kpi_value,
                unit,
                calculation_date,
                -- KPIのヘルス状態判定
                CASE 
                    WHEN kpi_name LIKE '%Rate' AND kpi_value >= 80 THEN 'Excellent'
                    WHEN kpi_name LIKE '%Rate' AND kpi_value >= 60 THEN 'Good'
                    WHEN kpi_name LIKE '%Rate' AND kpi_value >= 40 THEN 'Fair'
                    WHEN kpi_name LIKE '%Rate' THEN 'Poor'
                    WHEN kpi_name LIKE 'Total%' AND kpi_value > 0 THEN 'Active'
                    ELSE 'Unknown'
                END as kpi_health_status,
                -- 前期比較（シミュレーション）
                CASE 
                    WHEN kpi_name LIKE '%Revenue%' THEN kpi_value * 0.85  -- 15%成長想定
                    WHEN kpi_name LIKE '%Customer%' THEN kpi_value * 0.90  -- 10%成長想定
                    ELSE kpi_value * 0.95  -- 5%成長想定
                END as previous_period_value,
                ROUND(
                    ((kpi_value - 
                        CASE 
                            WHEN kpi_name LIKE '%Revenue%' THEN kpi_value * 0.85
                            WHEN kpi_name LIKE '%Customer%' THEN kpi_value * 0.90
                            ELSE kpi_value * 0.95
                        END
                    ) * 100.0 / NULLIF(
                        CASE 
                            WHEN kpi_name LIKE '%Revenue%' THEN kpi_value * 0.85
                            WHEN kpi_name LIKE '%Customer%' THEN kpi_value * 0.90
                            ELSE kpi_value * 0.95
                        END, 0)), 2
                ) as period_over_period_change_pct
            FROM kpi_calculations
            WHERE kpi_value IS NOT NULL
            ORDER BY kpi_category, kpi_name
        """)
        
        assert len(kpi_results) >= 6, "包括的KPI結果が不足しています"
        
        # KPIカテゴリの検証
        kpi_categories = set([r[0] for r in kpi_results])
        expected_categories = {'Revenue KPIs', 'Customer KPIs', 'Operations KPIs', 'Marketing KPIs'}
        
        for expected_cat in expected_categories:
            assert expected_cat in kpi_categories, f"KPIカテゴリ '{expected_cat}' が見つかりません"
        
        # KPIヘルス状態の分布検証
        health_distribution = {}
        for result in kpi_results:
            health_status = result[5]
            health_distribution[health_status] = health_distribution.get(health_status, 0) + 1
        
        assert len(health_distribution) > 0, "KPIヘルス状態が計算されていません"
        
        # 成長率の検証
        growth_rates = [r[8] for r in kpi_results if r[8] is not None and r[8] != 0]
        positive_growth_kpis = [r for r in growth_rates if r > 0]
        
        assert len(positive_growth_kpis) > 0, "正の成長を示すKPIがありません"
        
        logger.info(f"高度ビジネスKPIダッシュボードテスト完了: {len(kpi_results)}のKPIを計算、{len(kpi_categories)}カテゴリ、{len(health_distribution)}のヘルス状態")

    def test_data_warehouse_performance_optimization(self):
        """データウェアハウスパフォーマンス最適化のテスト"""
        connection = SynapseE2EConnection()
        
        # パフォーマンステストデータの生成
        start_time = time.time()
        
        # 大量データ処理のシミュレーション
        performance_results = connection.execute_query("""
            WITH performance_metrics AS (
                SELECT 
                    'Query Performance' as metric_category,
                    'Complex Join Performance' as metric_name,
                    COUNT(*) as execution_count,
                    AVG(DATEDIFF(second, '2024-01-01', sale_date)) as avg_processing_time
                FROM sales_transactions st
                INNER JOIN ClientDmBx c ON st.client_id = c.client_id
                LEFT JOIN point_grant_email pge ON c.client_id = pge.client_id
                WHERE st.status = 'completed'
                GROUP BY 'Query Performance', 'Complex Join Performance'
                
                UNION ALL
                
                SELECT 
                    'Data Volume',
                    'Total Records Processed',
                    COUNT(*),
                    SUM(LEN(CAST(amount as VARCHAR))) / COUNT(*)  -- 平均データサイズ
                FROM sales_transactions
                
                UNION ALL
                
                SELECT 
                    'Index Efficiency',
                    'Indexed Column Access',
                    COUNT(DISTINCT client_id),
                    AVG(amount)
                FROM sales_transactions
                WHERE client_id IS NOT NULL  -- インデックス効率のテスト
                
                UNION ALL
                
                SELECT 
                    'Memory Usage',
                    'In-Memory Processing',
                    COUNT(*),
                    SUM(CAST(amount as BIGINT)) / 1000000  -- MB単位でのメモリ使用量推定
                FROM (
                    SELECT TOP 1000 amount 
                    FROM sales_transactions 
                    ORDER BY sale_date DESC
                ) recent_data
            ),
            optimization_recommendations AS (
                SELECT 
                    *,
                    CASE 
                        WHEN metric_name LIKE '%Performance%' AND avg_processing_time > 100 THEN 'Needs Index Optimization'
                        WHEN metric_name LIKE '%Volume%' AND execution_count > 10000 THEN 'Consider Partitioning'
                        WHEN metric_name LIKE '%Memory%' AND avg_processing_time > 50 THEN 'Optimize Memory Usage'
                        ELSE 'Performance Acceptable'
                    END as optimization_recommendation,
                    CASE 
                        WHEN avg_processing_time <= 10 THEN 'Excellent'
                        WHEN avg_processing_time <= 50 THEN 'Good'
                        WHEN avg_processing_time <= 100 THEN 'Fair'
                        ELSE 'Poor'
                    END as performance_grade
                FROM performance_metrics
            )
            SELECT 
                metric_category,
                metric_name,
                execution_count,
                ROUND(avg_processing_time, 2) as avg_processing_time,
                optimization_recommendation,
                performance_grade
            FROM optimization_recommendations
            ORDER BY metric_category, avg_processing_time DESC
        """)
        
        query_execution_time = time.time() - start_time
        
        assert len(performance_results) >= 4, "パフォーマンス メトリクス結果が不足しています"
        assert query_execution_time < 30, f"クエリ実行時間が長すぎます: {query_execution_time:.2f}秒"
        
        # パフォーマンスグレードの検証
        performance_grades = {}
        for result in performance_results:
            grade = result[5]
            performance_grades[grade] = performance_grades.get(grade, 0) + 1
        
        # 少なくとも一つは Good または Excellent であることを確認
        good_performance_count = performance_grades.get('Good', 0) + performance_grades.get('Excellent', 0)
        assert good_performance_count > 0, "パフォーマンスが基準を満たしていません"
        
        # 最適化推奨事項の検証
        optimization_needs = [r for r in performance_results if r[4] != 'Performance Acceptable']
        logger.info(f"パフォーマンス最適化テスト完了: {len(performance_results)}メトリクスを評価、{len(optimization_needs)}項目で最適化推奨")
        
        logger.info(f"データウェアハウス パフォーマンス最適化テスト完了: クエリ実行時間 {query_execution_time:.2f}秒、{len(performance_grades)}のパフォーマンスグレード")
