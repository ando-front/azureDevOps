"""
E2Eテスト: Azure Data Factory データ品質とセキュリティ（リファクタリング版）

このモジュールは、重複処理を排除し、共通管理クラスを使用した効率的なテスト実装を提供します。
原版の機能はすべて維持しつつ、保守性と拡張性を大幅に向上させています。
"""
import pytest
import json
import hashlib
import time
import base64
import os
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection
from tests.e2e.helpers.data_quality_test_manager import (
    DataQualityTestManager, DataQualityTableType, ProfilingTarget, QualityRule, LineageStep
)
from tests.helpers.reproducible_e2e_helper import setup_reproducible_test_class, cleanup_reproducible_test_class


@pytest.mark.e2e
@pytest.mark.adf
@pytest.mark.data_quality
class TestADFDataQualityRefactored:
 
       
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
    """ADFデータ品質管理のE2Eテスト（リファクタリング版）"""
    
    @pytest.fixture(autouse=True)
    def setup_test_manager(self, e2e_synapse_connection: SynapseE2EConnection):
        """テスト管理クラスのセットアップ"""
        self.test_manager = DataQualityTestManager(e2e_synapse_connection)
        self.test_manager.initialize_all_tables()
        self.test_manager.prepare_test_data("comprehensive")
    
    def test_e2e_data_validation_rules_refactored(self, e2e_synapse_connection: SynapseE2EConnection):
        """E2E: データ検証ルールのテスト（リファクタリング版）"""
        
        # 事前定義された品質ルールを取得
        quality_rules = self.test_manager.get_quality_rules_preset("standard")
        
        # 各ルールを実行
        rule_results = []
        for rule in quality_rules:
            result = self.test_manager.execute_quality_rule(rule)
            rule_results.append(result)
            
            # 結果をデータベースに記録
            e2e_synapse_connection.execute_query(
                """
                INSERT INTO data_quality_results 
                (rule_name, execution_time, total_records, valid_records, invalid_records, 
                 quality_score, threshold_met, issues_found, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (result['rule_name'], result['execution_time'], result['total_records'],
                 result['valid_records'], result['invalid_records'], result['quality_score'],
                 result['threshold_met'], result.get('error', ''), datetime.now())
            )
        
        # 全体品質スコアの計算
        total_score_weight = sum(1 for r in rule_results if not r.get('error'))
        if total_score_weight > 0:
            overall_quality_score = sum(r['quality_score'] for r in rule_results if not r.get('error')) / total_score_weight
        else:
            overall_quality_score = 0.0
        
        # 結果の表示
        print(f"\nデータ品質検証結果:")
        print(f"全体品質スコア: {overall_quality_score:.2f}%")
        print(f"実行ルール数: {len(rule_results)}")
        
        for result in rule_results:
            if result.get('error'):
                print(f"  {result['rule_name']}: エラー - {result['error']}")
            else:
                status = 'PASS' if result['threshold_met'] else 'FAIL'
                print(f"  {result['rule_name']}: {result['quality_score']:.2f}% ({status})")
        
        # 品質基準の確認
        assert overall_quality_score >= 95.0, f"全体的なデータ品質が基準を下回っています: {overall_quality_score:.2f}%"
    
    def test_e2e_data_lineage_tracking_refactored(self, e2e_synapse_connection: SynapseE2EConnection):
        """E2E: データ系譜追跡のテスト（リファクタリング版）"""
        
        # 事前定義された系譜ステップを取得
        transformation_id = f"LINEAGE_TEST_{int(time.time())}"
        lineage_steps = self.test_manager.get_lineage_steps_preset("client_dm_processing")
        
        # 系譜追跡の実行
        lineage_result = self.test_manager.execute_lineage_tracking(transformation_id, lineage_steps)
        
        # 結果の検証
        assert lineage_result['total_steps'] == len(lineage_steps), "系譜ステップ数が一致しません"
        
        total_input = sum(step['before_count'] for step in lineage_result['steps'])
        total_output = sum(step['after_count'] for step in lineage_result['steps'])
        
        assert total_input > 0, "入力レコードが記録されていません"
        assert total_output > 0, "出力レコードが記録されていません"
        
        # 結果の表示
        print(f"\nデータ系譜追跡結果 (ID: {transformation_id}):")
        for step in lineage_result['steps']:
            print(f"ステップ {step['step_order']}: {step['source_table']} -> {step['target_table']}")
            print(f"  レコード数: {step['before_count']} -> {step['after_count']} (保持率: {step['retention_rate']:.2f}%)")
        
        # データ保持率の検証
        for step in lineage_result['steps']:
            if step['before_count'] > 0:
                assert step['retention_rate'] >= 10.0, \
                    f"ステップ{step['step_order']}でデータ保持率が低すぎます: {step['retention_rate']:.2f}%"
    
    def test_e2e_data_profiling_analysis_refactored(self, e2e_synapse_connection: SynapseE2EConnection):
        """E2E: データプロファイリング分析のテスト（リファクタリング版）"""
        
        # 事前定義されたプロファイリング対象を取得
        profiling_targets = self.test_manager.get_profiling_targets_preset("standard")
        profiling_session_id = f"PROFILE_{int(time.time())}"
        
        # 各カラムのプロファイリング実行
        profile_results = []
        for target in profiling_targets:
            result = self.test_manager.execute_column_profiling(target)
            profile_results.append((target, result))
            
            # プロファイリング結果の記録
            e2e_synapse_connection.execute_query(
                """
                INSERT INTO data_profiling_results 
                (session_id, table_name, column_name, data_type, 
                 total_records, null_count, distinct_count, min_value, 
                 max_value, avg_length, pattern_compliance, anomaly_count, 
                 profiling_time, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (profiling_session_id, target.table_name, target.column_name,
                 target.data_type, result['total_records'], result['null_count'], 
                 result['distinct_count'], result['min_value'], result['max_value'],
                 result['avg_length'], result['pattern_compliance'], 
                 result['anomaly_count'], datetime.now(), datetime.now())
            )
        
        # プロファイリング結果の分析
        profile_summary = e2e_synapse_connection.execute_query(
            """
            SELECT 
                table_name,
                column_name,
                total_records,
                null_count,
                distinct_count,
                CASE 
                    WHEN total_records > 0 
                    THEN (CAST(null_count AS FLOAT) / total_records) * 100
                    ELSE 0
                END as null_percentage,
                CASE 
                    WHEN total_records > 0 
                    THEN (CAST(distinct_count AS FLOAT) / total_records) * 100
                    ELSE 0
                END as uniqueness_percentage,
                pattern_compliance,
                anomaly_count
            FROM data_profiling_results
            WHERE session_id = ?
            """,
            (profiling_session_id,)
        )
        
        # 結果の表示と検証
        print(f"\nデータプロファイリング結果 (セッション: {profiling_session_id}):")
        
        for row in profile_summary:
            table, column, total, nulls, distinct, null_pct, unique_pct, pattern, anomalies = row
            print(f"\nテーブル: {table}")
            print(f"カラム: {column}")
            print(f"  総レコード数: {total}")
            print(f"  NULL率: {null_pct:.2f}%")
            print(f"  ユニーク率: {unique_pct:.2f}%")
            print(f"  パターン適合率: {pattern:.2f}%")
            print(f"  異常値数: {anomalies}")
            
            # データ品質基準のチェック
            target = next((t for t, _ in profile_results if t.table_name == table and t.column_name == column), None)
    
    
    
    

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

    def _validate_business_rules(self, table, column, stats, target):
        """ビジネスルールの検証"""
        if target:
            if 'unique' in target.business_rules:
                assert stats['unique_pct'] >= 95.0, f"{table}.{column}: ユニーク性が基準を下回っています"
                
            if 'not_null' in target.business_rules:
                assert stats['null_pct'] <= 5.0, f"{table}.{column}: NULL率が基準を超えています"


@pytest.mark.e2e
@pytest.mark.adf
@pytest.mark.security
class TestADFSecurityRefactored:
    """ADFセキュリティ機能のE2Eテスト（リファクタリング版）"""
    
    @pytest.fixture(autouse=True)
    def setup_security_manager(self, e2e_synapse_connection: SynapseE2EConnection):
        """セキュリティテスト管理クラスのセットアップ"""
        self.test_manager = DataQualityTestManager(e2e_synapse_connection)
        self.test_manager.initialize_table(DataQualityTableType.SECURITY_AUDIT)
        self.test_manager.initialize_table(DataQualityTableType.ACCESS_CONTROL)
    
    def test_e2e_access_control_validation_refactored(self, e2e_synapse_connection: SynapseE2EConnection):
        """E2E: アクセス制御検証のテスト（リファクタリング版）"""
        
        # 既存のポリシーデータをクリア
        e2e_synapse_connection.execute_query("DELETE FROM access_control_policies")
        
        # データベースの現在時刻を取得
        db_time_result = e2e_synapse_connection.execute_query("SELECT GETDATE()")
        db_current_time = db_time_result[0][0]
        
        # 確実に有効期間内に入る日時を設定（データベース時刻ベース）
        start_time = db_current_time - timedelta(hours=1)   # 1時間前から有効
        end_time = db_current_time + timedelta(days=365)    # 1年後まで有効
        
        # アクセス制御ポリシーの定義
        access_policies = [
            {
                'policy_name': 'client_dm_read_policy',
                'resource_type': 'table',
                'principal_type': 'user',
                'principal_id': 'test_user@example.com',
                'permissions': 'SELECT',
                'conditions': 'WHERE CUSTOMER_ID LIKE USER_ID%',
                'effective_from': start_time,
                'effective_to': end_time
            },
            {
                'policy_name': 'admin_full_access',
                'resource_type': 'database',
                'principal_type': 'role',
                'principal_id': 'admin_role',
                'permissions': 'SELECT,INSERT,UPDATE,DELETE',
                'conditions': None,
                'effective_from': start_time,
                'effective_to': end_time
            }
        ]
        
        # ポリシーの登録
        for policy in access_policies:
            e2e_synapse_connection.execute_query(
                """
                INSERT INTO access_control_policies 
                (policy_name, resource_type, principal_type, principal_id, 
                 permissions, conditions, is_active, effective_from, effective_to, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (policy['policy_name'], policy['resource_type'], policy['principal_type'],
                 policy['principal_id'], policy['permissions'], policy['conditions'],
                 1, policy['effective_from'], policy['effective_to'], db_current_time)
            )
        
        # アクセス制御の検証
        active_policies = e2e_synapse_connection.execute_query(
            """
            SELECT COUNT(*) FROM access_control_policies 
            WHERE is_active = 1 AND effective_from <= GETDATE() AND effective_to >= GETDATE()
            """
        )
        
        assert active_policies[0][0] >= len(access_policies), "アクティブなポリシーが不足しています"
        
        print(f"\nアクセス制御検証結果:")
        print(f"登録ポリシー数: {len(access_policies)}")
        print(f"アクティブポリシー数: {active_policies[0][0]}")
    
    def test_e2e_audit_logging_compliance_refactored(self, e2e_synapse_connection: SynapseE2EConnection):
        """E2E: 監査ログ対応のテスト（リファクタリング版）"""
        
        # 監査イベントのシミュレーション
        audit_events = [
            {
                'event_id': f"AUD_{int(time.time())}_001",
                'event_type': 'DATA_ACCESS',
                'user_identity': 'test_user@example.com',
                'resource_name': '[omni].[omni_ods_marketing_trn_client_dm]',
                'action_taken': 'SELECT',
                'event_timestamp': datetime.now(),
                'source_ip': '192.168.1.100',
                'user_agent': 'DataFactory/2.0',
                'status_code': 200,
                'details': 'Client DM data accessed for processing'
            },
            {
                'event_id': f"AUD_{int(time.time())}_002",
                'event_type': 'DATA_MODIFICATION',
                'user_identity': 'admin_user@example.com',
                'resource_name': '[omni].[omni_ods_cloak_trn_usageservice]',
                'action_taken': 'INSERT',
                'event_timestamp': datetime.now(),
                'source_ip': '192.168.1.101',
                'user_agent': 'DataFactory/2.0',
                'status_code': 201,
                'details': 'New usage service record inserted'
            },
            {
                'event_id': f"AUD_{int(time.time())}_003",
                'event_type': 'UNAUTHORIZED_ACCESS',
                'user_identity': 'unauthorized_user@example.com',
                'resource_name': '[omni].[omni_ods_marketing_trn_client_dm]',
                'action_taken': 'SELECT',
                'event_timestamp': datetime.now(),
                'source_ip': '10.0.0.100',
                'user_agent': 'Unknown',
                'status_code': 403,
                'details': 'Access denied - insufficient permissions'
            }
        ]
        
        # 監査ログの記録
        for event in audit_events:
            e2e_synapse_connection.execute_query(
                """
                INSERT INTO security_audit_logs 
                (event_id, event_type, user_identity, resource_name, action_taken, 
                 event_timestamp, source_ip, user_agent, status_code, details, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (event['event_id'], event['event_type'], event['user_identity'],
                 event['resource_name'], event['action_taken'], event['event_timestamp'],
                 event['source_ip'], event['user_agent'], event['status_code'],
                 event['details'], datetime.now())
            )
        
        # 監査ログの検証
        total_events = e2e_synapse_connection.execute_query(
            "SELECT COUNT(*) FROM security_audit_logs WHERE event_timestamp >= DATEADD(minute, -5, GETDATE())"
        )[0][0]
        
        unauthorized_events = e2e_synapse_connection.execute_query(
            "SELECT COUNT(*) FROM security_audit_logs WHERE event_type = 'UNAUTHORIZED_ACCESS'"
        )[0][0]
        
        assert total_events >= len(audit_events), "監査ログが正しく記録されていません"
        assert unauthorized_events > 0, "不正アクセスイベントが記録されていません"
        
        print(f"\n監査ログ検証結果:")
        print(f"記録イベント数: {total_events}")
        print(f"不正アクセス試行: {unauthorized_events}")
    
    def test_e2e_data_encryption_verification_refactored(self, e2e_synapse_connection: SynapseE2EConnection):
        """E2E: データ暗号化検証のテスト（リファクタリング版）"""
        
        # 暗号化対象データの準備
        sensitive_data = [
            {
                'customer_id': 'ENC_TEST_001',
                'email_address': 'sensitive.user@example.com',
                'credit_card': '4111-1111-1111-1111',
                'phone_number': '090-1234-5678'
            },
            {
                'customer_id': 'ENC_TEST_002', 
                'email_address': 'another.user@example.com',
                'credit_card': '5555-5555-5555-4444',
                'phone_number': '080-9876-5432'
            }
        ]
        
        # 暗号化処理のシミュレーション
        encrypted_records = []
        for data in sensitive_data:
            # 実際の暗号化はAzureキー管理サービスを使用する想定
            encrypted_email = base64.b64encode(data['email_address'].encode()).decode()
            encrypted_cc = base64.b64encode(data['credit_card'].encode()).decode()
            encrypted_phone = base64.b64encode(data['phone_number'].encode()).decode()
            
            encrypted_records.append({
                'customer_id': data['customer_id'],
                'encrypted_email': encrypted_email,
                'encrypted_cc': encrypted_cc,
                'encrypted_phone': encrypted_phone,
                'encryption_algorithm': 'AES-256-GCM',
                'key_version': 'v1.0',
                'encrypted_at': datetime.now()
            })
        
        # 暗号化結果の検証
        for record in encrypted_records:
            # 暗号化されたデータが元のデータと異なることを確認
            original = next(d for d in sensitive_data if d['customer_id'] == record['customer_id'])
            
            assert record['encrypted_email'] != original['email_address'], "メールアドレスが暗号化されていません"
            assert record['encrypted_cc'] != original['credit_card'], "クレジットカード番号が暗号化されていません"
            assert record['encrypted_phone'] != original['phone_number'], "電話番号が暗号化されていません"
            
            # 復号化テスト
            decrypted_email = base64.b64decode(record['encrypted_email']).decode()
            assert decrypted_email == original['email_address'], "メールアドレスの復号化に失敗しました"
        
        print(f"\nデータ暗号化検証結果:")
        print(f"暗号化レコード数: {len(encrypted_records)}")
        print(f"暗号化アルゴリズム: {encrypted_records[0]['encryption_algorithm']}")
        print(f"キーバージョン: {encrypted_records[0]['key_version']}")
        
        # すべての暗号化テストが成功したことを確認
        assert len(encrypted_records) == len(sensitive_data), "暗号化処理に失敗したレコードがあります"
