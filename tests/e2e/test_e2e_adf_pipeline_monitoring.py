"""
E2Eテスト: Azure Data Factory パイプライン監視とスケジューリング

このモジュールは、ADFパイプラインの監視、スケジューリング、トリガー機能のE2Eテストを提供します。
パイプラインの実行状態監視、エラー通知、スケジュール管理を含みます。
"""
import pytest
import json
import time
import os
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection
from tests.helpers.reproducible_e2e_helper import setup_reproducible_test_class, cleanup_reproducible_test_class


@pytest.mark.e2e
@pytest.mark.adf
@pytest.mark.monitoring
class TestADFPipelineMonitoring:
 
       
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



    def _get_no_proxy_session(self):
        """Get a requests session with proxy disabled"""
        session = requests.Session()
        session.proxies = {'http': None, 'https': None}
        return session
    """ADFパイプライン監視のE2Eテスト"""
    
    def test_e2e_pipeline_execution_status_tracking(self, e2e_synapse_connection: SynapseE2EConnection):
        """E2E: パイプライン実行状態追跡テスト"""
        
        # 1. パイプライン実行前の状態記録
        execution_id = f"TEST_EXEC_{int(time.time())}"
        start_time = datetime.now()
        
        # 実行状態記録テーブルの準備
        self._initialize_execution_log_table(e2e_synapse_connection)
        
        # 2. パイプライン実行開始の記録
        e2e_synapse_connection.execute_query(
            """
            INSERT INTO pipeline_execution_log 
            (execution_id, pipeline_name, status, start_time, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (execution_id, 'pi_Insert_ClientDmBx', 'InProgress', start_time, datetime.now())
        )
        
        # 3. パイプライン実行のシミュレーション
        try:
            # 実際のパイプライン処理をシミュレート
            self._simulate_pipeline_execution(e2e_synapse_connection, execution_id)
            
            # 成功状態の更新
            end_time = datetime.now()
            execution_duration = (end_time - start_time).total_seconds()
            
            e2e_synapse_connection.execute_query(
                """
                UPDATE pipeline_execution_log 
                SET status = ?, end_time = ?, duration_seconds = ?, updated_at = ?
                WHERE execution_id = ?
                """,
                ('Succeeded', end_time, execution_duration, datetime.now(), execution_id)
            )
            
        except Exception as e:
            # エラー状態の更新
            error_time = datetime.now()
            error_duration = (error_time - start_time).total_seconds()
            
            e2e_synapse_connection.execute_query(
                """
                UPDATE pipeline_execution_log 
                SET status = ?, end_time = ?, duration_seconds = ?, error_message = ?, updated_at = ?
                WHERE execution_id = ?
                """,
                ('Failed', error_time, error_duration, str(e), datetime.now(), execution_id)
            )
            
            pytest.fail(f"パイプライン実行中にエラーが発生: {e}")
        
        # 4. 実行状態の検証
        execution_result = e2e_synapse_connection.execute_query(
            """
            SELECT status, duration_seconds, error_message
            FROM pipeline_execution_log
            WHERE execution_id = ?
            """,
            (execution_id,)
        )
        
        assert len(execution_result) == 1, "実行ログが正しく記録されていない"
        status, duration, error_msg = execution_result[0]
        
        assert status == 'Succeeded', f"パイプライン実行が失敗: {error_msg}"
        assert duration > 0, "実行時間が正しく記録されていない"
        assert duration < 300, f"実行時間が長すぎます: {duration}秒"
    
    def test_e2e_pipeline_retry_mechanism(self, e2e_synapse_connection: SynapseE2EConnection):
        """E2E: パイプラインリトライメカニズムテスト"""
        
        execution_id = f"RETRY_TEST_{int(time.time())}"
        max_retries = 3
        retry_count = 0
        
        # 実行ログテーブルの初期化
        self._initialize_execution_log_table(e2e_synapse_connection)
        
        while retry_count < max_retries:
            try:
                retry_count += 1
                start_time = datetime.now()
                
                # リトライ試行の記録
                e2e_synapse_connection.execute_query(
                    """
                    INSERT INTO pipeline_execution_log 
                    (execution_id, pipeline_name, status, start_time, retry_count, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (f"{execution_id}_retry_{retry_count}", 'pi_Insert_ClientDmBx', 
                     'InProgress', start_time, retry_count, datetime.now())
                )
                
                # 意図的にエラーを発生させる（最初の2回）
                if retry_count <= 2:
                    # 無効なクエリでエラーをシミュレート
                    try:
                        e2e_synapse_connection.execute_query("SELECT * FROM non_existent_table")
                    except Exception as e:
                        # エラーログの記録
                        e2e_synapse_connection.execute_query(
                            """
                            UPDATE pipeline_execution_log 
                            SET status = ?, error_message = ?, updated_at = ?
                            WHERE execution_id = ?
                            """,
                            ('Failed', str(e), datetime.now(), f"{execution_id}_retry_{retry_count}")
                        )
                        
                        if retry_count < max_retries:
                            print(f"リトライ {retry_count}/{max_retries}: {e}")
                            time.sleep(1)  # リトライ間隔
                            continue
                        else:
                            raise e
                else:
                    # 3回目は成功させる
                    self._simulate_pipeline_execution(e2e_synapse_connection, f"{execution_id}_retry_{retry_count}")
                    
                    # 成功の記録
                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()
                    
                    e2e_synapse_connection.execute_query(
                        """
                        UPDATE pipeline_execution_log 
                        SET status = ?, end_time = ?, duration_seconds = ?, updated_at = ?
                        WHERE execution_id = ?
                        """,
                        ('Succeeded', end_time, duration, datetime.now(), f"{execution_id}_retry_{retry_count}")
                    )
                    break
                    
            except Exception as e:
                if retry_count >= max_retries:
                    pytest.fail(f"最大リトライ回数に達しても失敗: {e}")
        
        # リトライ結果の検証
        retry_results = e2e_synapse_connection.execute_query(
            """
            SELECT execution_id, status, retry_count, error_message
            FROM pipeline_execution_log
            WHERE execution_id LIKE ?
            ORDER BY retry_count
            """,
            (f"{execution_id}_retry_%",)
        )
        
        assert len(retry_results) == max_retries, "期待されるリトライ回数と一致しない"
        
        # 最初の2回は失敗、3回目は成功を確認
        for i, (exec_id, status, retry_num, error_msg) in enumerate(retry_results):
            if i < 2:  # 最初の2回
                assert status == 'Failed', f"リトライ{retry_num}が失敗していない"
                assert error_msg is not None, f"リトライ{retry_num}のエラーメッセージが記録されていない"
            else:  # 3回目
                assert status == 'Succeeded', f"最終リトライ{retry_num}が成功していない"
    
    def test_e2e_pipeline_timeout_handling(self, e2e_synapse_connection: SynapseE2EConnection):
        """E2E: パイプラインタイムアウト処理テスト"""
        
        execution_id = f"TIMEOUT_TEST_{int(time.time())}"
        timeout_seconds = 10  # 10秒のタイムアウト
        
        # 実行ログテーブルの初期化
        self._initialize_execution_log_table(e2e_synapse_connection)
        
        start_time = datetime.now()
        
        # パイプライン実行開始の記録
        e2e_synapse_connection.execute_query(
            """
            INSERT INTO pipeline_execution_log 
            (execution_id, pipeline_name, status, start_time, timeout_seconds, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (execution_id, 'pi_long_running_test', 'InProgress', start_time, timeout_seconds, datetime.now())
        )
        
        try:
            # 長時間実行をシミュレート
            self._simulate_long_running_pipeline(e2e_synapse_connection, timeout_seconds + 5)
            
        except TimeoutError:
            # タイムアウトの記録
            timeout_time = datetime.now()
            actual_duration = (timeout_time - start_time).total_seconds()
            
            e2e_synapse_connection.execute_query(
                """
                UPDATE pipeline_execution_log 
                SET status = ?, end_time = ?, duration_seconds = ?, 
                    error_message = ?, updated_at = ?
                WHERE execution_id = ?
                """,
                ('Timeout', timeout_time, actual_duration, 
                 f'Pipeline timed out after {timeout_seconds} seconds', 
                 datetime.now(), execution_id)
            )
        
        # タイムアウト結果の検証
        timeout_result = e2e_synapse_connection.execute_query(
            """
            SELECT status, duration_seconds, error_message
            FROM pipeline_execution_log
            WHERE execution_id = ?
            """,
            (execution_id,)
        )
        
        assert len(timeout_result) == 1, "タイムアウトログが記録されていない"
        status, duration, error_msg = timeout_result[0]
        
        assert status == 'Timeout', "タイムアウト状態が正しく記録されていない"
        assert duration >= timeout_seconds, "タイムアウト時間が正しく記録されていない"
        assert 'timeout' in error_msg.lower(), "タイムアウトエラーメッセージが適切ではない"
    
    def test_e2e_pipeline_resource_monitoring(self, e2e_synapse_connection: SynapseE2EConnection):
        """E2E: パイプラインリソース監視テスト"""
        
        execution_id = f"RESOURCE_TEST_{int(time.time())}"
        
        # リソース監視テーブルの初期化
        self._initialize_resource_monitoring_table(e2e_synapse_connection)
        
        # パイプライン実行前のリソース状態記録
        start_time = datetime.now()
        
        e2e_synapse_connection.execute_query(
            """
            INSERT INTO pipeline_resource_usage 
            (execution_id, measurement_time, cpu_usage_percent, memory_usage_mb, 
             disk_io_mb, network_io_mb, active_connections)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (execution_id, start_time, 15.5, 512.0, 100.0, 50.0, 5)
        )
        
        # パイプライン実行中のリソース使用量をシミュレート
        for i in range(3):
            time.sleep(1)  # 1秒間隔でリソース監視
            
            measurement_time = datetime.now()
            # 実行中はリソース使用量が増加
            cpu_usage = 15.5 + (i * 20.0)
            memory_usage = 512.0 + (i * 256.0)
            disk_io = 100.0 + (i * 50.0)
            network_io = 50.0 + (i * 25.0)
            connections = 5 + i
            
            e2e_synapse_connection.execute_query(
                """
                INSERT INTO pipeline_resource_usage 
                (execution_id, measurement_time, cpu_usage_percent, memory_usage_mb, 
                 disk_io_mb, network_io_mb, active_connections)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (execution_id, measurement_time, cpu_usage, memory_usage, 
                 disk_io, network_io, connections)
            )
        
        # パイプライン実行完了後のリソース状態
        end_time = datetime.now()
        e2e_synapse_connection.execute_query(
            """
            INSERT INTO pipeline_resource_usage 
            (execution_id, measurement_time, cpu_usage_percent, memory_usage_mb, 
             disk_io_mb, network_io_mb, active_connections)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (execution_id, end_time, 12.0, 256.0, 20.0, 10.0, 2)
        )
        
        # リソース使用状況の分析
        resource_summary = e2e_synapse_connection.execute_query(
            """
            SELECT 
                COUNT(*) as measurement_count,
                AVG(cpu_usage_percent) as avg_cpu,
                MAX(cpu_usage_percent) as max_cpu,
                AVG(memory_usage_mb) as avg_memory,
                MAX(memory_usage_mb) as max_memory,
                SUM(disk_io_mb) as total_disk_io,
                SUM(network_io_mb) as total_network_io
            FROM pipeline_resource_usage
            WHERE execution_id = ?
            """,
            (execution_id,)
        )
        
        summary = resource_summary[0]
        measurement_count, avg_cpu, max_cpu, avg_memory, max_memory, total_disk_io, total_network_io = summary
        
        # リソース使用状況の検証
        assert measurement_count >= 4, "十分なリソース測定が行われていない"
        assert max_cpu > avg_cpu, "ピーク時のCPU使用率が記録されていない"
        assert max_memory > avg_memory, "ピーク時のメモリ使用量が記録されていない"
        assert total_disk_io > 0, "ディスクI/Oが記録されていない"
        assert total_network_io > 0, "ネットワークI/Oが記録されていない"
        
        # リソース閾値チェック
        assert max_cpu <= 100.0, "CPU使用率が100%を超えている"
        assert max_memory <= 2048.0, "メモリ使用量が制限を超えている"
        
        print(f"\nリソース使用状況サマリー:")
        print(f"- 測定回数: {measurement_count}")
        print(f"- 平均CPU使用率: {avg_cpu:.1f}%")
        print(f"- 最大CPU使用率: {max_cpu:.1f}%")
        print(f"- 平均メモリ使用量: {avg_memory:.1f}MB")
        print(f"- 最大メモリ使用量: {max_memory:.1f}MB")
        print(f"- 総ディスクI/O: {total_disk_io:.1f}MB")
        print(f"- 総ネットワークI/O: {total_network_io:.1f}MB")
    
    def _initialize_execution_log_table(self, connection: SynapseE2EConnection):
        """パイプライン実行ログテーブルの初期化"""
        try:
            connection.execute_query(
                """
                CREATE TABLE pipeline_execution_log (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    execution_id NVARCHAR(100) NOT NULL,
                    pipeline_name NVARCHAR(200) NOT NULL,
                    status NVARCHAR(50) NOT NULL,
                    start_time DATETIME2,
                    end_time DATETIME2,
                    duration_seconds DECIMAL(10,2),
                    retry_count INT DEFAULT 0,
                    timeout_seconds INT,
                    error_message NVARCHAR(MAX),
                    created_at DATETIME2 DEFAULT GETDATE(),
                    updated_at DATETIME2 DEFAULT GETDATE()
                )
                """
            )
        except Exception:
            # テーブルが既に存在する場合は無視
            pass
    
    def _initialize_resource_monitoring_table(self, connection: SynapseE2EConnection):
        """リソース監視テーブルの初期化"""
        try:
            connection.execute_query(
                """
                CREATE TABLE pipeline_resource_usage (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    execution_id NVARCHAR(100) NOT NULL,
                    measurement_time DATETIME2 NOT NULL,
                    cpu_usage_percent DECIMAL(5,2),
                    memory_usage_mb DECIMAL(10,2),
                    disk_io_mb DECIMAL(10,2),
                    network_io_mb DECIMAL(10,2),
                    active_connections INT,
                    created_at DATETIME2 DEFAULT GETDATE()
                )
                """
            )
        except Exception:
            # テーブルが既に存在する場合は無視
            pass
    
    def _simulate_pipeline_execution(self, connection: SynapseE2EConnection, execution_id: str):
        """パイプライン実行のシミュレーション"""
        # 簡単なSQLクエリを実行してパイプライン処理をシミュレート
        connection.execute_query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES")
        time.sleep(2)  # 処理時間のシミュレート
        connection.execute_query("SELECT GETDATE()")
    
    def _simulate_long_running_pipeline(self, connection: SynapseE2EConnection, duration_seconds: int):
        """長時間実行パイプラインのシミュレーション"""
        start_time = time.time()
        timeout_time = start_time + duration_seconds
    
    
    
    

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



    def _get_no_proxy_session(self):
        """Get a requests session with proxy disabled"""
        session = requests.Session()
        session.proxies = {'http': None, 'https': None}
        return session

    def _simulate_long_running_process(self, connection, timeout_seconds=30):
        """長時間実行プロセスのシミュレーション"""
        start_time = time.time()
        timeout_time = start_time + timeout_seconds
        
        while time.time() < timeout_time:
            # 長時間処理をシミュレート
            connection.execute_query("WAITFOR DELAY '00:00:02'")  # 2秒待機
            
            # タイムアウトチェック（実際のパイプラインエンジンが行う処理）
            current_time = time.time()
            if current_time - start_time > 10:  # 10秒でタイムアウト
                raise TimeoutError(f"Pipeline execution timed out after 10 seconds")


@pytest.mark.e2e
@pytest.mark.adf
@pytest.mark.scheduling
class TestADFPipelineScheduling:
    """ADFパイプラインスケジューリングのE2Eテスト"""
    
    def test_e2e_schedule_trigger_validation(self, e2e_synapse_connection: SynapseE2EConnection):
        """E2E: スケジュールトリガーの検証テスト"""
        
        # 1. スケジュール設定テーブルの初期化
        self._initialize_schedule_table(e2e_synapse_connection)
        
        # 2. 複数のスケジュールパターンをテスト
        schedule_configs = [
            {
                'pipeline_name': 'pi_Insert_ClientDmBx',
                'schedule_type': 'daily',
                'schedule_time': '02:00:00',
                'timezone': 'Tokyo Standard Time',
                'is_active': True
            },
            {
                'pipeline_name': 'pi_PointGrantEmail',
                'schedule_type': 'weekly',
                'schedule_time': '03:00:00',
                'schedule_day': 'Monday',
                'timezone': 'Tokyo Standard Time',
                'is_active': True
            },
            {
                'pipeline_name': 'pi_Copy_marketing_client_dm',
                'schedule_type': 'monthly',
                'schedule_time': '01:00:00',
                'schedule_day': '1',
                'timezone': 'Tokyo Standard Time',
                'is_active': True
            }
        ]
        
        # 3. スケジュール設定の登録
        for config in schedule_configs:
            e2e_synapse_connection.execute_query(
                """
                INSERT INTO pipeline_schedule_config 
                (pipeline_name, schedule_type, schedule_time, schedule_day, 
                 timezone, is_active, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (config['pipeline_name'], config['schedule_type'], config['schedule_time'],
                 config.get('schedule_day'), config['timezone'], config['is_active'], datetime.now())
            )
        
        # 4. 次回実行予定時刻の計算テスト
        for config in schedule_configs:
            next_execution = self._calculate_next_execution_time(config)
            
            # 次回実行時刻が未来であることを確認
            assert next_execution > datetime.now(), \
                f"次回実行時刻が過去になっています: {config['pipeline_name']}"
            
            # スケジュールタイプに応じた妥当性チェック
            if config['schedule_type'] == 'daily':
                # 今日または明日の指定時刻であることを確認
                expected_time = datetime.now().replace(
                    hour=int(config['schedule_time'].split(':')[0]),
                    minute=int(config['schedule_time'].split(':')[1]),
                    second=0, microsecond=0
                )
                if expected_time <= datetime.now():
                    expected_time += timedelta(days=1)
                
                time_diff = abs((next_execution - expected_time).total_seconds())
                assert time_diff < 86400, f"日次スケジュールの時刻が不正: {config['pipeline_name']}"
        
        # 5. スケジュール競合チェック
        concurrent_schedules = e2e_synapse_connection.execute_query(
            """
            SELECT schedule_time, COUNT(*) as pipeline_count
            FROM pipeline_schedule_config
            WHERE is_active = 1
            GROUP BY schedule_time
            HAVING COUNT(*) > 1
            """
        )
        
        # 同時実行数の警告
        for schedule_time, pipeline_count in concurrent_schedules:
            print(f"警告: {schedule_time}に{pipeline_count}個のパイプラインが同時実行予定")
    
    def test_e2e_trigger_dependency_chain(self, e2e_synapse_connection: SynapseE2EConnection):
        """E2E: トリガー依存チェーンのテスト"""
        
        # 1. パイプライン依存関係テーブルの初期化
        self._initialize_dependency_table(e2e_synapse_connection)
        
        # 2. 依存関係の設定
        dependencies = [
            ('pi_Insert_ClientDmBx', 'pi_Copy_marketing_client_dm', 'success'),
            ('pi_Copy_marketing_client_dm', 'pi_PointGrantEmail', 'success'),
            ('pi_PointGrantEmail', 'pi_Send_ActionPointCurrentMonthEntryList', 'success')
        ]
        
        for parent_pipeline, child_pipeline, dependency_type in dependencies:
            e2e_synapse_connection.execute_query(
                """
                INSERT INTO pipeline_dependencies 
                (parent_pipeline, child_pipeline, dependency_type, is_active, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (parent_pipeline, child_pipeline, dependency_type, True, datetime.now())
            )
        
        # 3. 依存チェーンの実行シミュレーション
        execution_chain = []
        start_pipeline = 'pi_Insert_ClientDmBx'
        
        # 最初のパイプライン実行
        execution_id_1 = self._execute_pipeline_with_dependencies(
            e2e_synapse_connection, start_pipeline, execution_chain
        )
        
        # 4. 依存関係に基づく後続パイプライン実行
        self._execute_dependent_pipelines(e2e_synapse_connection, execution_chain)
        
        # 5. 実行チェーンの検証
        assert len(execution_chain) == len(dependencies) + 1, "依存チェーンの実行数が不正"
        
        # 実行順序の検証
        expected_order = [
            'pi_Insert_ClientDmBx',
            'pi_Copy_marketing_client_dm',
            'pi_PointGrantEmail',
            'pi_Send_ActionPointCurrentMonthEntryList'
        ]
        
        actual_order = [exec_info['pipeline_name'] for exec_info in execution_chain]
        assert actual_order == expected_order, f"実行順序が不正: {actual_order}"
        
        # 実行時間の検証（後続パイプラインは前のパイプライン完了後に実行）
        for i in range(1, len(execution_chain)):
            prev_end_time = execution_chain[i-1]['end_time']
            current_start_time = execution_chain[i]['start_time']
            
            assert current_start_time >= prev_end_time, \
                f"依存関係が守られていません: {execution_chain[i]['pipeline_name']}"
    
    def test_e2e_conditional_trigger_logic(self, e2e_synapse_connection: SynapseE2EConnection):
        """E2E: 条件付きトリガーロジックのテスト"""
        
        # 1. 条件付きトリガー設定テーブルの初期化
        self._initialize_conditional_trigger_table(e2e_synapse_connection)
        
        # 2. 条件付きトリガーの設定
        conditional_triggers = [
            {
                'trigger_name': 'data_freshness_check',
                'pipeline_name': 'pi_Insert_ClientDmBx',
                'condition_type': 'data_age',
                'condition_value': '24',  # 24時間以内のデータが存在する場合
                'condition_table': '[omni].[omni_ods_cloak_trn_usageservice]',
                'is_active': True
            },
            {
                'trigger_name': 'file_availability_check',
                'pipeline_name': 'pi_PointGrantEmail',
                'condition_type': 'file_exists',
                'condition_value': 'DAM_PointAdd.tsv',
                'condition_path': 'forRcvry/{date}/DCPDA016/',
                'is_active': True
            },
            {
                'trigger_name': 'business_hours_check',
                'pipeline_name': 'pi_Copy_marketing_client_dm',
                'condition_type': 'time_window',
                'condition_value': '02:00-06:00',
                'is_active': True
            }
        ]
        
        # 3. 条件付きトリガーの登録
        for trigger in conditional_triggers:
            e2e_synapse_connection.execute_query(
                """
                INSERT INTO conditional_triggers 
                (trigger_name, pipeline_name, condition_type, condition_value, 
                 condition_table, condition_path, is_active, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (trigger['trigger_name'], trigger['pipeline_name'], trigger['condition_type'],
                 trigger['condition_value'], trigger.get('condition_table'), 
                 trigger.get('condition_path'), trigger['is_active'], datetime.now())
            )
        
        # 4. 各条件の評価テスト
        for trigger in conditional_triggers:
            condition_met = self._evaluate_trigger_condition(e2e_synapse_connection, trigger)
            
            # 条件評価結果の記録
            e2e_synapse_connection.execute_query(
                """
                INSERT INTO trigger_evaluation_log 
                (trigger_name, evaluation_time, condition_met, evaluation_details)
                VALUES (?, ?, ?, ?)
                """,
                (trigger['trigger_name'], datetime.now(), condition_met, 
                 f"Condition type: {trigger['condition_type']}, Value: {trigger['condition_value']}")
            )
            
            print(f"トリガー '{trigger['trigger_name']}': 条件充足 = {condition_met}")
        
        # 5. 条件充足時のパイプライン実行テスト
        eligible_pipelines = e2e_synapse_connection.execute_query(
            """
            SELECT ct.pipeline_name, ct.trigger_name
            FROM conditional_triggers ct
            INNER JOIN trigger_evaluation_log tel ON ct.trigger_name = tel.trigger_name
            WHERE ct.is_active = 1 AND tel.condition_met = 1
            AND tel.evaluation_time = (
                SELECT MAX(evaluation_time) 
                FROM trigger_evaluation_log 
                WHERE trigger_name = tel.trigger_name
            )
            """
        )
        
        executed_pipelines = []
        for pipeline_name, trigger_name in eligible_pipelines:
            execution_id = f"CONDITIONAL_{trigger_name}_{int(time.time())}"
            
            # 条件付き実行の記録
            e2e_synapse_connection.execute_query(
                """
                INSERT INTO pipeline_execution_log 
                (execution_id, pipeline_name, trigger_type, trigger_name, 
                 status, start_time, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (execution_id, pipeline_name, 'conditional', trigger_name,
                 'InProgress', datetime.now(), datetime.now())
            )
            
            executed_pipelines.append((pipeline_name, execution_id))
        
        # 条件付き実行の検証
        assert len(executed_pipelines) > 0, "条件を満たすパイプラインが実行されていない"
        
        print(f"条件付きトリガーにより {len(executed_pipelines)} 個のパイプラインが実行されました")
    
    def _initialize_schedule_table(self, connection: SynapseE2EConnection):
        """スケジュール設定テーブルの初期化"""
        try:
            connection.execute_query(
                """
                CREATE TABLE pipeline_schedule_config (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    pipeline_name NVARCHAR(200) NOT NULL,
                    schedule_type NVARCHAR(50) NOT NULL,
                    schedule_time TIME NOT NULL,
                    schedule_day NVARCHAR(20),
                    timezone NVARCHAR(100) DEFAULT 'Tokyo Standard Time',
                    is_active BIT DEFAULT 1,
                    created_at DATETIME2 DEFAULT GETDATE(),
                    updated_at DATETIME2 DEFAULT GETDATE()
                )
                """
            )
        except Exception:
            pass
    
    def _initialize_dependency_table(self, connection: SynapseE2EConnection):
        """パイプライン依存関係テーブルの初期化"""
        try:
            connection.execute_query(
                """
                CREATE TABLE pipeline_dependencies (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    parent_pipeline NVARCHAR(200) NOT NULL,
                    child_pipeline NVARCHAR(200) NOT NULL,
                    dependency_type NVARCHAR(50) NOT NULL,
                    is_active BIT DEFAULT 1,
                    created_at DATETIME2 DEFAULT GETDATE()
                )
                """
            )
        except Exception:
            pass
    
    def _initialize_conditional_trigger_table(self, connection: SynapseE2EConnection):
        """条件付きトリガーテーブルの初期化"""
        try:
            connection.execute_query(
                """
                CREATE TABLE conditional_triggers (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    trigger_name NVARCHAR(200) NOT NULL,
                    pipeline_name NVARCHAR(200) NOT NULL,
                    condition_type NVARCHAR(50) NOT NULL,
                    condition_value NVARCHAR(500),
                    condition_table NVARCHAR(200),
                    condition_path NVARCHAR(500),
                    is_active BIT DEFAULT 1,
                    created_at DATETIME2 DEFAULT GETDATE()
                )
                """
            )
            
            connection.execute_query(
                """
                CREATE TABLE trigger_evaluation_log (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    trigger_name NVARCHAR(200) NOT NULL,
                    evaluation_time DATETIME2 NOT NULL,
                    condition_met BIT NOT NULL,
                    evaluation_details NVARCHAR(MAX),
                    created_at DATETIME2 DEFAULT GETDATE()
                )
                """
            )
        except Exception:
            pass
    
    def _calculate_next_execution_time(self, schedule_config: Dict[str, Any]) -> datetime:
        """次回実行時刻の計算"""
        current_time = datetime.now()
        schedule_time = datetime.strptime(schedule_config['schedule_time'], '%H:%M:%S').time()
        
        if schedule_config['schedule_type'] == 'daily':
            next_execution = current_time.replace(
                hour=schedule_time.hour, 
                minute=schedule_time.minute, 
                second=schedule_time.second, 
                microsecond=0
            )
            if next_execution <= current_time:
                next_execution += timedelta(days=1)
                
        elif schedule_config['schedule_type'] == 'weekly':
            # 簡略化された週次スケジュール計算
            next_execution = current_time.replace(
                hour=schedule_time.hour, 
                minute=schedule_time.minute, 
                second=schedule_time.second, 
                microsecond=0
            )
            # 次週の同じ曜日に設定（簡略化）
            next_execution += timedelta(days=7)
            
        elif schedule_config['schedule_type'] == 'monthly':
            # 簡略化された月次スケジュール計算
            next_execution = current_time.replace(
                day=int(schedule_config.get('schedule_day', '1')),
                hour=schedule_time.hour, 
                minute=schedule_time.minute, 
                second=schedule_time.second, 
                microsecond=0
            )
            if next_execution <= current_time:
                # 次月に設定（簡略化）
                if next_execution.month == 12:
                    next_execution = next_execution.replace(year=next_execution.year + 1, month=1)
                else:
                    next_execution = next_execution.replace(month=next_execution.month + 1)
        
        return next_execution
    
    def _execute_pipeline_with_dependencies(self, connection: SynapseE2EConnection, 
                                          pipeline_name: str, execution_chain: List[Dict]) -> str:
        """依存関係を考慮したパイプライン実行"""
        execution_id = f"DEP_{pipeline_name}_{int(time.time())}"
        start_time = datetime.now()
        
        # パイプライン実行のシミュレーション
        time.sleep(1)  # 実行時間のシミュレート
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        execution_info = {
            'execution_id': execution_id,
            'pipeline_name': pipeline_name,
            'start_time': start_time,
            'end_time': end_time,
            'duration': duration,
            'status': 'Succeeded'
        }
        
        execution_chain.append(execution_info)
        return execution_id
    
    def _execute_dependent_pipelines(self, connection: SynapseE2EConnection, execution_chain: List[Dict]):
        """依存するパイプラインの実行"""
        # 依存関係を取得して順次実行
        dependencies = connection.execute_query(
            """
            SELECT child_pipeline, dependency_type
            FROM pipeline_dependencies
            WHERE parent_pipeline = ? AND is_active = 1
            ORDER BY id
            """,
            (execution_chain[-1]['pipeline_name'],)
        )
        
        for child_pipeline, dependency_type in dependencies:
            if dependency_type == 'success' and execution_chain[-1]['status'] == 'Succeeded':
                self._execute_pipeline_with_dependencies(connection, child_pipeline, execution_chain)
                # 再帰的に次の依存パイプラインを実行
                if len(dependencies) > 0:
                    self._execute_dependent_pipelines(connection, execution_chain)
    
    def _evaluate_trigger_condition(self, connection: SynapseE2EConnection, 
                                   trigger_config: Dict[str, Any]) -> bool:
        """トリガー条件の評価"""
        try:
            if trigger_config['condition_type'] == 'data_age':
                # データの新しさチェック
                hours_threshold = int(trigger_config['condition_value'])
                threshold_time = datetime.now() - timedelta(hours=hours_threshold)
                
                result = connection.execute_query(
                    f"""
                    SELECT COUNT(*)
                    FROM {trigger_config['condition_table']}
                    WHERE OUTPUT_DATE >= ?
                    """,
                    (threshold_time,)
                )
                return result[0][0] > 0
                
            elif trigger_config['condition_type'] == 'file_exists':
                # ファイル存在チェック（モック）
                # 実際の実装ではBlobストレージをチェック
                return True  # テスト用に常にTrue
                
            elif trigger_config['condition_type'] == 'time_window':
                # 時間ウィンドウチェック
                time_range = trigger_config['condition_value'].split('-')
                start_time = datetime.strptime(time_range[0], '%H:%M').time()
                end_time = datetime.strptime(time_range[1], '%H:%M').time()
                current_time = datetime.now().time()
                
                return start_time <= current_time <= end_time
                
        except Exception as e:
            print(f"条件評価エラー: {e}")
            return False
        
        return False
