"""
E2E Test Suite for Real-Time Streaming and IoT Data Processing

リアルタイムストリーミングとIoTデータ処理のE2Eテスト
ストリーミングデータ、IoTセンサー、リアルタイム分析を含む
"""
import os
import pytest
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple
import logging
import random

# テスト環境のセットアップ
from tests.helpers.reproducible_e2e_helper import (
    setup_reproducible_test_class, 
    cleanup_reproducible_test_class,
    get_reproducible_synapse_connection
)

# ロガーの設定
logger = logging.getLogger(__name__)

class TestRealTimeStreamingAndIoTProcessing:
    """リアルタイムストリーミングとIoTデータ処理のE2Eテスト"""
    
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

    def test_streaming_data_ingestion(self):
        """ストリーミングデータ取り込みのテスト"""
        connection = get_reproducible_synapse_connection()
        
        # ストリーミングデータのシミュレーション
        current_time = datetime.now()
        streaming_data = []
        
        # 様々なタイプのストリーミングイベント生成
        event_types = ['user_click', 'page_view', 'purchase', 'session_start', 'session_end']
        
        for i in range(50):
            event_time = current_time + timedelta(seconds=i)
            event_type = random.choice(event_types)
            user_id = random.randint(1, 10)
            
            event_data = {
                'event_id': i + 1,
                'event_type': event_type,
                'user_id': user_id,
                'timestamp': event_time.isoformat(),
                'metadata': {
                    'ip_address': f'192.168.1.{random.randint(1, 255)}',
                    'user_agent': 'TestAgent/1.0',
                    'session_id': f'sess_{user_id}_{random.randint(1000, 9999)}'
                }
            }
            
            if event_type == 'purchase':
                event_data['amount'] = round(random.uniform(10, 500), 2)
                event_data['product_id'] = random.randint(100, 199)
            elif event_type in ['user_click', 'page_view']:
                event_data['page_url'] = f'/page/{random.randint(1, 20)}'
                event_data['referrer'] = 'https://example.com'
            
            streaming_data.append(event_data)
        
        # ストリーミングデータの挿入
        for event in streaming_data:
            connection.execute_query(f"""
                INSERT INTO streaming_events (event_id, event_type, user_id, event_timestamp, event_data) 
                VALUES ({event['event_id']}, '{event['event_type']}', {event['user_id']}, '{event['timestamp']}', '{json.dumps(event)}')
            """)
        
        # ストリーミング取り込み分析
        ingestion_results = connection.execute_query("""
            WITH streaming_analysis AS (
                SELECT 
                    event_type,
                    COUNT(*) as event_count,
                    COUNT(DISTINCT user_id) as unique_users,
                    MIN(event_timestamp) as first_event,
                    MAX(event_timestamp) as last_event,
                    DATEDIFF(second, MIN(event_timestamp), MAX(event_timestamp)) as time_span_seconds,
                    AVG(LEN(event_data)) as avg_event_size_bytes
                FROM streaming_events
                GROUP BY event_type
            ),
            throughput_metrics AS (
                SELECT 
                    *,
                    CASE 
                        WHEN time_span_seconds > 0 THEN event_count * 1.0 / time_span_seconds
                        ELSE event_count
                    END as events_per_second,
                    CASE 
                        WHEN time_span_seconds > 0 THEN (event_count * avg_event_size_bytes) / time_span_seconds / 1024
                        ELSE event_count * avg_event_size_bytes / 1024
                    END as kb_per_second
                FROM streaming_analysis
            )
            SELECT 
                event_type,
                event_count,
                unique_users,
                first_event,
                last_event,
                time_span_seconds,
                ROUND(avg_event_size_bytes, 2) as avg_event_size_bytes,
                ROUND(events_per_second, 2) as events_per_second,
                ROUND(kb_per_second, 2) as kb_per_second,
                CASE 
                    WHEN events_per_second >= 10 THEN 'High Throughput'
                    WHEN events_per_second >= 5 THEN 'Medium Throughput'
                    WHEN events_per_second >= 1 THEN 'Low Throughput'
                    ELSE 'Very Low Throughput'
                END as throughput_category
            FROM throughput_metrics
            ORDER BY events_per_second DESC
        """)
        
        assert len(ingestion_results) > 0, "ストリーミング取り込み結果が取得できません"
        
        # 取り込み品質の検証
        total_events = sum([r[1] for r in ingestion_results])
        unique_users_total = len(set([r[2] for r in ingestion_results]))
        high_throughput_types = [r for r in ingestion_results if r[9] == 'High Throughput']
        
        assert total_events == 50, f"期待されるイベント数と一致しません: {total_events}"
        assert unique_users_total >= 5, f"ユニークユーザー数が少なすぎます: {unique_users_total}"
        
        # スループットの検証
        total_throughput = sum([r[7] for r in ingestion_results])
        assert total_throughput > 0, "総スループットが0です"
        
        logger.info(f"ストリーミングデータ取り込みテスト完了: {total_events}イベント、{unique_users_total}ユニークユーザー、総スループット{total_throughput:.2f}events/sec")

    def test_iot_sensor_data_processing(self):
        """IoTセンサーデータ処理のテスト"""
        connection = get_reproducible_synapse_connection()
        
        # IoTセンサーデータの生成
        sensor_types = ['temperature', 'humidity', 'pressure', 'motion', 'light']
        sensor_locations = ['warehouse_a', 'warehouse_b', 'office_floor_1', 'office_floor_2', 'datacenter']
        
        iot_data = []
        base_time = datetime.now()
        
        for i in range(100):
            sensor_id = f'sensor_{random.randint(1, 20):03d}'
            sensor_type = random.choice(sensor_types)
            location = random.choice(sensor_locations)
            timestamp = base_time + timedelta(minutes=i)
            
            # センサータイプに基づく値生成
            if sensor_type == 'temperature':
                value = round(random.uniform(18.0, 35.0), 2)
                unit = 'celsius'
            elif sensor_type == 'humidity':
                value = round(random.uniform(30.0, 80.0), 2)
                unit = 'percentage'
            elif sensor_type == 'pressure':
                value = round(random.uniform(990.0, 1020.0), 2)
                unit = 'hpa'
            elif sensor_type == 'motion':
                value = random.choice([0, 1])  # Binary: motion detected or not
                unit = 'binary'
            else:  # light
                value = round(random.uniform(0, 1000), 2)
                unit = 'lux'
            
            # 異常値の意図的挿入（5%の確率）
            if random.random() < 0.05:
                if sensor_type == 'temperature':
                    value = random.choice([50.0, -10.0])  # 異常な温度
                elif sensor_type == 'humidity':
                    value = random.choice([100.0, 0.0])   # 異常な湿度
            
            iot_data.append({
                'sensor_id': sensor_id,
                'sensor_type': sensor_type,
                'location': location,
                'timestamp': timestamp,
                'value': value,
                'unit': unit,
                'quality_score': random.uniform(0.8, 1.0)
            })
        
        # IoTデータの挿入
        for data in iot_data:
            connection.execute_query(f"""
                INSERT INTO iot_sensor_data (sensor_id, sensor_type, location, measurement_timestamp, sensor_value, unit, quality_score) 
                VALUES ('{data['sensor_id']}', '{data['sensor_type']}', '{data['location']}', '{data['timestamp']}', {data['value']}, '{data['unit']}', {data['quality_score']})
            """)
        
        # IoTデータ処理分析
        iot_processing_results = connection.execute_query("""
            WITH sensor_analysis AS (
                SELECT 
                    sensor_type,
                    location,
                    COUNT(*) as measurement_count,
                    AVG(sensor_value) as avg_value,
                    MIN(sensor_value) as min_value,
                    MAX(sensor_value) as max_value,
                    STDEV(sensor_value) as std_deviation,
                    AVG(quality_score) as avg_quality_score,
                    -- 異常値検出（統計的手法）
                    AVG(sensor_value) + (2 * STDEV(sensor_value)) as upper_threshold,
                    AVG(sensor_value) - (2 * STDEV(sensor_value)) as lower_threshold
                FROM iot_sensor_data
                GROUP BY sensor_type, location
            ),
            anomaly_detection AS (
                SELECT 
                    sa.*,
                    -- 異常値カウント
                    (SELECT COUNT(*) 
                     FROM iot_sensor_data isd 
                     WHERE isd.sensor_type = sa.sensor_type 
                       AND isd.location = sa.location
                       AND (isd.sensor_value > sa.upper_threshold OR isd.sensor_value < sa.lower_threshold)
                    ) as anomaly_count
                FROM sensor_analysis sa
            ),
            processing_summary AS (
                SELECT 
                    sensor_type,
                    location,
                    measurement_count,
                    ROUND(avg_value, 2) as avg_value,
                    ROUND(min_value, 2) as min_value,
                    ROUND(max_value, 2) as max_value,
                    ROUND(std_deviation, 2) as std_deviation,
                    ROUND(avg_quality_score, 3) as avg_quality_score,
                    anomaly_count,
                    ROUND(anomaly_count * 100.0 / measurement_count, 1) as anomaly_percentage,
                    CASE 
                        WHEN avg_quality_score >= 0.95 THEN 'Excellent'
                        WHEN avg_quality_score >= 0.90 THEN 'Good'
                        WHEN avg_quality_score >= 0.80 THEN 'Fair'
                        ELSE 'Poor'
                    END as data_quality_rating,
                    CASE 
                        WHEN anomaly_percentage <= 2 THEN 'Normal Operation'
                        WHEN anomaly_percentage <= 5 THEN 'Minor Issues'
                        WHEN anomaly_percentage <= 10 THEN 'Significant Issues'
                        ELSE 'Critical Issues'
                    END as anomaly_status
                FROM anomaly_detection
            )
            SELECT * FROM processing_summary
            ORDER BY sensor_type, location
        """)
        
        assert len(iot_processing_results) > 0, "IoTデータ処理結果が取得できません"
        
        # データ品質の検証
        total_measurements = sum([r[2] for r in iot_processing_results])
        high_quality_sensors = [r for r in iot_processing_results if r[10] in ['Excellent', 'Good']]
        critical_anomaly_sensors = [r for r in iot_processing_results if r[11] == 'Critical Issues']
        
        assert total_measurements == 100, f"測定値数が期待値と一致しません: {total_measurements}"
        assert len(high_quality_sensors) >= len(iot_processing_results) * 0.7, "高品質センサーの割合が低すぎます"
        
        # 異常検出の有効性検証
        total_anomalies = sum([r[8] for r in iot_processing_results])
        avg_anomaly_rate = sum([r[9] for r in iot_processing_results]) / len(iot_processing_results)
        
        # 異常値が適切に検出されていることを確認（5%程度の異常値を挿入したため）
        assert 3 <= avg_anomaly_rate <= 15, f"異常検出率が範囲外です: {avg_anomaly_rate:.1f}%"
        
        logger.info(f"IoTセンサーデータ処理テスト完了: {total_measurements}測定値、{len(high_quality_sensors)}高品質センサー、平均異常率{avg_anomaly_rate:.1f}%、{len(critical_anomaly_sensors)}重要問題センサー")

    def test_real_time_analytics_processing(self):
        """リアルタイム分析処理のテスト"""
        connection = get_reproducible_synapse_connection()
        
        # リアルタイム分析のシミュレーション
        real_time_start = time.time()
        
        # リアルタイムダッシュボード用のメトリクス計算
        analytics_results = connection.execute_query("""
            WITH real_time_metrics AS (
                -- ストリーミングイベントの5分間ウィンドウ分析
                SELECT 
                    'Streaming Events' as metric_category,
                    'Events per 5min Window' as metric_name,
                    DATEPART(hour, event_timestamp) as time_hour,
                    DATEPART(minute, event_timestamp) / 5 * 5 as time_window,
                    COUNT(*) as metric_value,
                    COUNT(DISTINCT user_id) as unique_entities
                FROM streaming_events
                WHERE event_timestamp >= DATEADD(hour, -1, GETDATE())
                GROUP BY DATEPART(hour, event_timestamp), DATEPART(minute, event_timestamp) / 5 * 5
                
                UNION ALL
                
                -- IoTセンサーデータのリアルタイム集約
                SELECT 
                    'IoT Sensors' as metric_category,
                    'Average Sensor Values per 10min' as metric_name,
                    DATEPART(hour, measurement_timestamp) as time_hour,
                    DATEPART(minute, measurement_timestamp) / 10 * 10 as time_window,
                    COUNT(*) as metric_value,
                    COUNT(DISTINCT sensor_id) as unique_entities
                FROM iot_sensor_data
                WHERE measurement_timestamp >= DATEADD(hour, -1, GETDATE())
                GROUP BY DATEPART(hour, measurement_timestamp), DATEPART(minute, measurement_timestamp) / 10 * 10
            ),
            trend_analysis AS (
                SELECT 
                    metric_category,
                    metric_name,
                    time_hour,
                    time_window,
                    metric_value,
                    unique_entities,
                    -- トレンド計算
                    LAG(metric_value) OVER (PARTITION BY metric_category ORDER BY time_hour, time_window) as prev_value,
                    metric_value - LAG(metric_value) OVER (PARTITION BY metric_category ORDER BY time_hour, time_window) as value_change,
                    -- 変化率計算
                    CASE 
                        WHEN LAG(metric_value) OVER (PARTITION BY metric_category ORDER BY time_hour, time_window) > 0
                        THEN (metric_value - LAG(metric_value) OVER (PARTITION BY metric_category ORDER BY time_hour, time_window)) * 100.0 / 
                             LAG(metric_value) OVER (PARTITION BY metric_category ORDER BY time_hour, time_window)
                        ELSE 0
                    END as change_percentage
                FROM real_time_metrics
            ),
            alerting_conditions AS (
                SELECT 
                    *,
                    -- アラート条件
                    CASE 
                        WHEN ABS(change_percentage) >= 50 THEN 'Critical Alert'
                        WHEN ABS(change_percentage) >= 25 THEN 'Warning Alert'
                        WHEN ABS(change_percentage) >= 10 THEN 'Information Alert'
                        ELSE 'Normal'
                    END as alert_level,
                    -- パフォーマンス指標
                    CASE 
                        WHEN metric_value >= 20 THEN 'High Activity'
                        WHEN metric_value >= 10 THEN 'Medium Activity'
                        WHEN metric_value >= 5 THEN 'Low Activity'
                        ELSE 'Very Low Activity'
                    END as activity_level
                FROM trend_analysis
                WHERE prev_value IS NOT NULL  -- 初回値は除外
            )
            SELECT 
                metric_category,
                metric_name,
                time_hour,
                time_window,
                metric_value,
                unique_entities,
                prev_value,
                value_change,
                ROUND(change_percentage, 1) as change_percentage,
                alert_level,
                activity_level
            FROM alerting_conditions
            ORDER BY metric_category, time_hour, time_window
        """)
        
        real_time_processing_time = time.time() - real_time_start
        
        assert len(analytics_results) > 0, "リアルタイム分析結果が取得できません"
        assert real_time_processing_time < 5, f"リアルタイム処理時間が長すぎます: {real_time_processing_time:.2f}秒"
        
        # アラート生成の検証
        alert_distribution = {}
        activity_distribution = {}
        
        for result in analytics_results:
            alert_level = result[9]
            activity_level = result[10]
            
            alert_distribution[alert_level] = alert_distribution.get(alert_level, 0) + 1
            activity_distribution[activity_level] = activity_distribution.get(activity_level, 0) + 1
        
        # 少なくとも何らかのアクティビティが検出されることを確認
        total_alerts = len(analytics_results)
        normal_alerts = alert_distribution.get('Normal', 0)
        
        assert total_alerts > 0, "リアルタイム分析でアクティビティが検出されません"
        
        # 異常検出の確認
        critical_alerts = alert_distribution.get('Critical Alert', 0)
        warning_alerts = alert_distribution.get('Warning Alert', 0)
        
        # リアルタイム分析の効率性検証
        high_activity_windows = activity_distribution.get('High Activity', 0)
        medium_activity_windows = activity_distribution.get('Medium Activity', 0)
        
        logger.info(f"リアルタイム分析処理テスト完了: {total_alerts}時間ウィンドウを分析、処理時間{real_time_processing_time:.2f}秒、{critical_alerts}重要アラート、{warning_alerts}警告アラート、{high_activity_windows}高アクティビティ期間")

    def test_event_driven_processing(self):
        """イベント駆動処理のテスト"""
        connection = get_reproducible_synapse_connection()
        
        # イベントトリガー定義
        event_triggers = [
            (1, 'high_temperature_alert', 'temperature > 30', 'iot_sensor_data', 'Send maintenance notification'),
            (2, 'user_conversion_event', 'event_type = purchase AND amount > 100', 'streaming_events', 'Trigger loyalty program'),
            (3, 'system_anomaly_detection', 'quality_score < 0.5', 'iot_sensor_data', 'System health check'),
            (4, 'high_traffic_alert', 'event_count > 10', 'streaming_events', 'Scale up resources'),
            (5, 'data_quality_issue', 'anomaly_percentage > 10', 'iot_sensor_data', 'Data validation review')
        ]
        
        for trigger_id, trigger_name, condition, source_table, action_description in event_triggers:
            connection.execute_query(f"""
                INSERT INTO event_triggers (id, trigger_name, trigger_condition, source_table, action_description, created_at) 
                VALUES ({trigger_id}, '{trigger_name}', '{condition}', '{source_table}', '{action_description}', GETDATE())
            """)
        
        # イベント駆動処理のシミュレーション
        event_processing_results = connection.execute_query("""
            WITH trigger_evaluation AS (
                -- 温度アラートトリガー
                SELECT 
                    'high_temperature_alert' as trigger_name,
                    COUNT(*) as triggered_count,
                    MAX(sensor_value) as max_trigger_value,
                    MAX(measurement_timestamp) as last_trigger_time
                FROM iot_sensor_data
                WHERE sensor_type = 'temperature' AND sensor_value > 30
                
                UNION ALL
                
                -- ユーザー コンバージョンイベント
                SELECT 
                    'user_conversion_event' as trigger_name,
                    COUNT(*) as triggered_count,
                    MAX(CAST(JSON_VALUE(event_data, '$.amount') as DECIMAL(10,2))) as max_trigger_value,
                    MAX(event_timestamp) as last_trigger_time
                FROM streaming_events
                WHERE event_type = 'purchase' 
                  AND TRY_CAST(JSON_VALUE(event_data, '$.amount') as DECIMAL(10,2)) > 100
                
                UNION ALL
                
                -- システム異常検出
                SELECT 
                    'system_anomaly_detection' as trigger_name,
                    COUNT(*) as triggered_count,
                    MIN(quality_score) as max_trigger_value,
                    MAX(measurement_timestamp) as last_trigger_time
                FROM iot_sensor_data
                WHERE quality_score < 0.5
                
                UNION ALL
                
                -- 高トラフィックアラート（5分間ウィンドウ）
                SELECT 
                    'high_traffic_alert' as trigger_name,
                    COUNT(*) as triggered_count,
                    MAX(event_count) as max_trigger_value,
                    MAX(window_time) as last_trigger_time
                FROM (
                    SELECT 
                        DATEADD(minute, DATEDIFF(minute, '1900-01-01', event_timestamp) / 5 * 5, '1900-01-01') as window_time,
                        COUNT(*) as event_count
                    FROM streaming_events
                    GROUP BY DATEADD(minute, DATEDIFF(minute, '1900-01-01', event_timestamp) / 5 * 5, '1900-01-01')
                    HAVING COUNT(*) > 10
                ) high_traffic_windows
            ),
            trigger_actions AS (
                SELECT 
                    te.trigger_name,
                    te.triggered_count,
                    te.max_trigger_value,
                    te.last_trigger_time,
                    et.action_description,
                    -- アクション優先度
                    CASE 
                        WHEN te.trigger_name LIKE '%alert%' AND te.triggered_count >= 5 THEN 'High Priority'
                        WHEN te.trigger_name LIKE '%alert%' AND te.triggered_count >= 1 THEN 'Medium Priority'
                        WHEN te.triggered_count >= 10 THEN 'High Priority'
                        WHEN te.triggered_count >= 5 THEN 'Medium Priority'
                        WHEN te.triggered_count >= 1 THEN 'Low Priority'
                        ELSE 'No Action Required'
                    END as action_priority,
                    -- 実行ステータス
                    CASE 
                        WHEN te.triggered_count > 0 THEN 'Action Triggered'
                        ELSE 'No Trigger'
                    END as execution_status
                FROM trigger_evaluation te
                INNER JOIN event_triggers et ON te.trigger_name = et.trigger_name
            )
            SELECT 
                trigger_name,
                triggered_count,
                max_trigger_value,
                last_trigger_time,
                action_description,
                action_priority,
                execution_status,
                -- 効果測定
                CASE 
                    WHEN execution_status = 'Action Triggered' AND action_priority = 'High Priority' THEN 'Critical Response Required'
                    WHEN execution_status = 'Action Triggered' AND action_priority = 'Medium Priority' THEN 'Standard Response'
                    WHEN execution_status = 'Action Triggered' THEN 'Monitoring Response'
                    ELSE 'No Response Needed'
                END as response_category
            FROM trigger_actions
            ORDER BY 
                CASE action_priority 
                    WHEN 'High Priority' THEN 1
                    WHEN 'Medium Priority' THEN 2
                    WHEN 'Low Priority' THEN 3
                    ELSE 4
                END,
                triggered_count DESC
        """)
        
        assert len(event_processing_results) > 0, "イベント駆動処理結果が取得できません"
        
        # イベント処理の有効性検証
        triggered_events = [r for r in event_processing_results if r[6] == 'Action Triggered']
        high_priority_actions = [r for r in event_processing_results if r[5] == 'High Priority']
        critical_responses = [r for r in event_processing_results if r[7] == 'Critical Response Required']
        
        assert len(triggered_events) >= 2, f"トリガーされたイベントが少なすぎます: {len(triggered_events)}"
        
        # 応答時間の検証（リアルタイム処理の要件）
        for result in triggered_events:
            trigger_name = result[0]
            last_trigger_time = result[3]
            
            # 最後のトリガー時刻が合理的な範囲内であることを確認
            if last_trigger_time:
                time_diff = datetime.now() - last_trigger_time
                assert time_diff.total_seconds() < 3600, f"トリガー {trigger_name} の応答時間が遅すぎます"
        
        # イベント駆動処理のカバレッジ検証
        trigger_coverage = {}
        for result in event_processing_results:
            trigger_name = result[0]
            triggered_count = result[1]
            trigger_coverage[trigger_name] = triggered_count
        
        active_triggers = [t for t, count in trigger_coverage.items() if count > 0]
        coverage_rate = len(active_triggers) / len(trigger_coverage) * 100
        
        logger.info(f"イベント駆動処理テスト完了: {len(triggered_events)}イベントトリガー、{len(high_priority_actions)}高優先度アクション、{len(critical_responses)}緊急対応、カバレッジ{coverage_rate:.1f}%")

    def test_stream_processing_windowing(self):
        """ストリーム処理ウィンドウ機能のテスト"""
        connection = get_reproducible_synapse_connection()
        
        # ウィンドウ処理のシミュレーション
        windowing_results = connection.execute_query("""
            WITH time_windows AS (
                -- タンブリングウィンドウ（5分間隔）
                SELECT 
                    'Tumbling_5min' as window_type,
                    DATEADD(minute, DATEDIFF(minute, '1900-01-01', event_timestamp) / 5 * 5, '1900-01-01') as window_start,
                    DATEADD(minute, (DATEDIFF(minute, '1900-01-01', event_timestamp) / 5 * 5) + 5, '1900-01-01') as window_end,
                    COUNT(*) as event_count,
                    COUNT(DISTINCT user_id) as unique_users,
                    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchase_events,
                    AVG(CASE WHEN event_type = 'purchase' THEN TRY_CAST(JSON_VALUE(event_data, '$.amount') as DECIMAL(10,2)) END) as avg_purchase_amount
                FROM streaming_events
                GROUP BY DATEADD(minute, DATEDIFF(minute, '1900-01-01', event_timestamp) / 5 * 5, '1900-01-01')
                
                UNION ALL
                
                -- スライディングウィンドウ（10分間ウィンドウ、5分間隔でスライド）
                SELECT 
                    'Sliding_10min_5min' as window_type,
                    DATEADD(minute, DATEDIFF(minute, '1900-01-01', event_timestamp) / 5 * 5, '1900-01-01') as window_start,
                    DATEADD(minute, (DATEDIFF(minute, '1900-01-01', event_timestamp) / 5 * 5) + 10, '1900-01-01') as window_end,
                    COUNT(*) as event_count,
                    COUNT(DISTINCT user_id) as unique_users,
                    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchase_events,
                    AVG(CASE WHEN event_type = 'purchase' THEN TRY_CAST(JSON_VALUE(event_data, '$.amount') as DECIMAL(10,2)) END) as avg_purchase_amount
                FROM streaming_events se1
                WHERE EXISTS (
                    SELECT 1 FROM streaming_events se2 
                    WHERE se2.event_timestamp BETWEEN 
                        DATEADD(minute, DATEDIFF(minute, '1900-01-01', se1.event_timestamp) / 5 * 5, '1900-01-01')
                        AND DATEADD(minute, (DATEDIFF(minute, '1900-01-01', se1.event_timestamp) / 5 * 5) + 10, '1900-01-01')
                )
                GROUP BY DATEADD(minute, DATEDIFF(minute, '1900-01-01', event_timestamp) / 5 * 5, '1900-01-01')
            ),
            window_analytics AS (
                SELECT 
                    window_type,
                    window_start,
                    window_end,
                    event_count,
                    unique_users,
                    purchase_events,
                    ROUND(COALESCE(avg_purchase_amount, 0), 2) as avg_purchase_amount,
                    -- ウィンドウ効率性メトリクス
                    ROUND(event_count * 1.0 / DATEDIFF(minute, window_start, window_end), 2) as events_per_minute,
                    ROUND(COALESCE(purchase_events * 100.0 / NULLIF(event_count, 0), 0), 1) as purchase_conversion_rate,
                    -- ウィンドウ サイズ分類
                    CASE 
                        WHEN event_count >= 20 THEN 'High Volume Window'
                        WHEN event_count >= 10 THEN 'Medium Volume Window'
                        WHEN event_count >= 5 THEN 'Low Volume Window'
                        ELSE 'Very Low Volume Window'
                    END as volume_category
                FROM time_windows
                WHERE event_count > 0
            ),
            window_comparison AS (
                SELECT 
                    *,
                    -- 前ウィンドウとの比較
                    LAG(event_count) OVER (PARTITION BY window_type ORDER BY window_start) as prev_window_events,
                    event_count - LAG(event_count) OVER (PARTITION BY window_type ORDER BY window_start) as event_count_change,
                    -- ウィンドウトレンド
                    CASE 
                        WHEN LAG(event_count) OVER (PARTITION BY window_type ORDER BY window_start) IS NULL THEN 'Initial Window'
                        WHEN event_count > LAG(event_count) OVER (PARTITION BY window_type ORDER BY window_start) THEN 'Increasing Trend'
                        WHEN event_count < LAG(event_count) OVER (PARTITION BY window_type ORDER BY window_start) THEN 'Decreasing Trend'
                        ELSE 'Stable Trend'
                    END as trend_direction
                FROM window_analytics
            )
            SELECT 
                window_type,
                window_start,
                window_end,
                event_count,
                unique_users,
                purchase_events,
                avg_purchase_amount,
                events_per_minute,
                purchase_conversion_rate,
                volume_category,
                prev_window_events,
                event_count_change,
                trend_direction
            FROM window_comparison
            ORDER BY window_type, window_start
        """)
        
        assert len(windowing_results) > 0, "ウィンドウ処理結果が取得できません"
        
        # ウィンドウ処理の有効性検証
        tumbling_windows = [r for r in windowing_results if r[0] == 'Tumbling_5min']
        sliding_windows = [r for r in windowing_results if r[0] == 'Sliding_10min_5min']
        
        assert len(tumbling_windows) > 0, "タンブリングウィンドウ結果がありません"
        assert len(sliding_windows) > 0, "スライディングウィンドウ結果がありません"
        
        # ウィンドウ効率性の検証
        high_volume_windows = [r for r in windowing_results if r[9] == 'High Volume Window']
        total_events_in_windows = sum([r[3] for r in windowing_results])
        
        # コンバージョン率の分析
        conversion_rates = [r[8] for r in windowing_results if r[8] > 0]
        avg_conversion_rate = sum(conversion_rates) / len(conversion_rates) if conversion_rates else 0
        
        # トレンド分析
        trend_distribution = {}
        for result in windowing_results:
            trend = result[12]
            trend_distribution[trend] = trend_distribution.get(trend, 0) + 1
        
        increasing_trends = trend_distribution.get('Increasing Trend', 0)
        stable_trends = trend_distribution.get('Stable Trend', 0)
        
        # ウィンドウサイズ最適化の検証
        window_efficiency = {}
        for result in windowing_results:
            window_type = result[0]
            events_per_minute = result[7]
            if window_type not in window_efficiency:
                window_efficiency[window_type] = []
            window_efficiency[window_type].append(events_per_minute)
        
        # 各ウィンドウタイプの平均効率
        for window_type, efficiency_list in window_efficiency.items():
            avg_efficiency = sum(efficiency_list) / len(efficiency_list)
            assert avg_efficiency > 0, f"ウィンドウタイプ {window_type} の効率が0です"
        
        logger.info(f"ストリーム処理ウィンドウテスト完了: {len(windowing_results)}ウィンドウ、{len(high_volume_windows)}高ボリューム、平均コンバージョン率{avg_conversion_rate:.1f}%、{increasing_trends}増加トレンド、{stable_trends}安定トレンド")

    def test_complex_event_processing(self):
        """複合イベント処理（CEP）のテスト"""
        connection = get_reproducible_synapse_connection()
        
        # 複合イベントパターンの検出
        cep_results = connection.execute_query("""
            WITH user_sessions AS (
                -- ユーザーセッションの再構築
                SELECT 
                    user_id,
                    event_timestamp,
                    event_type,
                    event_data,
                    -- セッション識別（30分間隔でセッション境界を設定）
                    SUM(CASE WHEN DATEDIFF(minute, LAG(event_timestamp) OVER (PARTITION BY user_id ORDER BY event_timestamp), event_timestamp) > 30 THEN 1 ELSE 0 END) 
                        OVER (PARTITION BY user_id ORDER BY event_timestamp) as session_id
                FROM streaming_events
            ),
            session_patterns AS (
                SELECT 
                    user_id,
                    session_id,
                    MIN(event_timestamp) as session_start,
                    MAX(event_timestamp) as session_end,
                    COUNT(*) as total_events,
                    -- イベントシーケンスの分析
                    COUNT(CASE WHEN event_type = 'session_start' THEN 1 END) as session_start_events,
                    COUNT(CASE WHEN event_type = 'page_view' THEN 1 END) as page_view_events,
                    COUNT(CASE WHEN event_type = 'user_click' THEN 1 END) as click_events,
                    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) as purchase_events,
                    COUNT(CASE WHEN event_type = 'session_end' THEN 1 END) as session_end_events,
                    -- 購入金額の集計
                    SUM(TRY_CAST(JSON_VALUE(event_data, '$.amount') as DECIMAL(10,2))) as total_purchase_amount,
                    -- セッション期間
                    DATEDIFF(minute, MIN(event_timestamp), MAX(event_timestamp)) as session_duration_minutes
                FROM user_sessions
                GROUP BY user_id, session_id
            ),
            complex_patterns AS (
                SELECT 
                    user_id,
                    session_id,
                    session_start,
                    session_end,
                    total_events,
                    page_view_events,
                    click_events,
                    purchase_events,
                    COALESCE(total_purchase_amount, 0) as total_purchase_amount,
                    session_duration_minutes,
                    -- 複合パターンの検出
                    CASE 
                        WHEN page_view_events >= 3 AND click_events >= 2 AND purchase_events >= 1 THEN 'High Engagement Purchase'
                        WHEN page_view_events >= 5 AND click_events >= 3 AND purchase_events = 0 THEN 'High Engagement No Purchase'
                        WHEN page_view_events >= 2 AND purchase_events >= 1 THEN 'Quick Purchase'
                        WHEN page_view_events >= 10 AND click_events >= 5 THEN 'Extensive Browsing'
                        WHEN total_events = 1 AND page_view_events = 1 THEN 'Single Page View'
                        ELSE 'Standard Session'
                    END as session_pattern,
                    -- 異常パターンの検出
                    CASE 
                        WHEN session_duration_minutes > 120 THEN 'Unusually Long Session'
                        WHEN session_duration_minutes < 1 AND total_events > 5 THEN 'Rapid Fire Events'
                        WHEN click_events > page_view_events * 3 THEN 'Excessive Clicking'
                        WHEN purchase_events > 0 AND session_duration_minutes < 2 THEN 'Suspiciously Quick Purchase'
                        ELSE 'Normal Behavior'
                    END as anomaly_pattern
                FROM session_patterns
                WHERE total_events > 0
            ),
            pattern_analysis AS (
                SELECT 
                    session_pattern,
                    anomaly_pattern,
                    COUNT(*) as pattern_count,
                    AVG(total_events) as avg_events_per_session,
                    AVG(session_duration_minutes) as avg_session_duration,
                    SUM(purchase_events) as total_purchases,
                    SUM(total_purchase_amount) as total_revenue,
                    AVG(total_purchase_amount) as avg_purchase_amount,
                    -- コンバージョン指標
                    COUNT(CASE WHEN purchase_events > 0 THEN 1 END) * 100.0 / COUNT(*) as conversion_rate,
                    -- エンゲージメント指標
                    AVG(page_view_events + click_events) as avg_engagement_actions
                FROM complex_patterns
                GROUP BY session_pattern, anomaly_pattern
            )
            SELECT 
                session_pattern,
                anomaly_pattern,
                pattern_count,
                ROUND(avg_events_per_session, 1) as avg_events_per_session,
                ROUND(avg_session_duration, 1) as avg_session_duration_minutes,
                total_purchases,
                ROUND(total_revenue, 2) as total_revenue,
                ROUND(avg_purchase_amount, 2) as avg_purchase_amount,
                ROUND(conversion_rate, 1) as conversion_rate_percent,
                ROUND(avg_engagement_actions, 1) as avg_engagement_actions,
                -- パターンの重要度評価
                CASE 
                    WHEN session_pattern LIKE '%Purchase%' AND conversion_rate >= 50 THEN 'High Value Pattern'
                    WHEN session_pattern LIKE '%Engagement%' THEN 'Engagement Pattern'
                    WHEN anomaly_pattern != 'Normal Behavior' THEN 'Anomaly Pattern'
                    ELSE 'Standard Pattern'
                END as pattern_significance
            FROM pattern_analysis
            ORDER BY pattern_count DESC, total_revenue DESC
        """)
        
        assert len(cep_results) > 0, "複合イベント処理結果が取得できません"
        
        # CEP パターンの有効性検証
        high_value_patterns = [r for r in cep_results if r[10] == 'High Value Pattern']
        anomaly_patterns = [r for r in cep_results if r[10] == 'Anomaly Pattern']
        engagement_patterns = [r for r in cep_results if r[10] == 'Engagement Pattern']
        
        total_patterns_detected = len(cep_results)
        total_sessions_analyzed = sum([r[2] for r in cep_results])
        total_revenue_from_patterns = sum([r[6] for r in cep_results])
        
        assert total_patterns_detected >= 3, f"検出されたパターンが少なすぎます: {total_patterns_detected}"
        assert total_sessions_analyzed > 0, "分析されたセッションが0です"
        
        # パターン分布の検証
        pattern_distribution = {}
        for result in cep_results:
            pattern = result[0]
            count = result[2]
            pattern_distribution[pattern] = count
        
        # 高価値パターンの検証
        if high_value_patterns:
            avg_high_value_conversion = sum([r[8] for r in high_value_patterns]) / len(high_value_patterns)
            assert avg_high_value_conversion >= 30, f"高価値パターンのコンバージョン率が低すぎます: {avg_high_value_conversion:.1f}%"
        
        # 異常パターンの検証
        anomaly_sessions = sum([r[2] for r in anomaly_patterns])
        anomaly_rate = anomaly_sessions / total_sessions_analyzed * 100 if total_sessions_analyzed > 0 else 0
        
        # CEPの検出精度検証
        meaningful_patterns = [r for r in cep_results if r[10] in ['High Value Pattern', 'Engagement Pattern', 'Anomaly Pattern']]
        detection_accuracy = len(meaningful_patterns) / total_patterns_detected * 100
        
        assert detection_accuracy >= 50, f"CEP検出精度が低すぎます: {detection_accuracy:.1f}%"
        
        logger.info(f"複合イベント処理テスト完了: {total_patterns_detected}パターン検出、{total_sessions_analyzed}セッション分析、{len(high_value_patterns)}高価値パターン、{len(anomaly_patterns)}異常パターン、売上{total_revenue_from_patterns:.2f}、異常率{anomaly_rate:.1f}%")
