"""
Azure Data Factory Pipeline E2E Tests - LINE ID Link Info Send Pipeline
テスト対象パイプライン: pi_Send_LINEIDLinkInfo

このパイプラインの主要機能:
1. データベースからLINE ID連携情報データを抽出してCSV作成
2. 作成したCSVファイルをgzip圧縮してBlobStorageに保存
3. gzipファイルをSFMCにSFTP転送

テストシナリオ:
- 正常データ処理（標準的なLINE ID連携データセット）
- 大容量データ処理（性能テスト）
- データ品質検証（必須フィールド、フォーマット）
- SFTP転送検証（ファイル完全性、転送時間）
- エラーハンドリング（接続障害、無効データ）
- LINE ID固有の検証（ID形式、連携ステータス）
"""

import pytest
import asyncio
import os
import requests
from datetime import datetime, timedelta
import pytz
from typing import Dict, List, Any
import logging
import gzip
import csv
import json
import re
from unittest.mock import patch, MagicMock

# テスト用ヘルパーモジュール
from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection


class TestPipelineLINEIDLinkInfo:

    @classmethod
    def setup_class(cls):
        """Disable proxy settings for tests"""
        # Store and clear proxy environment variables
        for var in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY']:
            if var in os.environ:
                del os.environ[var]

    def _get_no_proxy_session(self):
        """Get a requests session with proxy disabled"""
        session = requests.Session()
        session.proxies = {'http': None, 'https': None}
        return session
    """LINE ID Link Info Send パイプライン専用E2Eテストクラス"""
    
    PIPELINE_NAME = "pi_Send_LINEIDLinkInfo"
    
    @pytest.fixture(scope="class")
    def pipeline_helper(self, e2e_synapse_connection):
        """パイプライン専用のヘルパーインスタンス"""
        return e2e_synapse_connection
    
    @pytest.fixture(scope="class")
    def test_data_setup(self, pipeline_helper):
        """テストデータのセットアップ"""
        # LINE ID連携情報テスト用データ
        test_line_id_data = [
            {
                "connection_key": "LINE001",
                "customer_key": "CUST001",
                "line_user_id": "U1234567890abcdef1234567890abcdef",
                "display_name": "テストユーザー1",
                "link_status": "LINKED",
                "link_date": "2024-01-15",
                "last_interaction_date": "2024-01-20",
                "friend_status": "FRIEND",
                "block_status": "UNBLOCKED",
                "message_receive_setting": "ENABLED",
                "promotional_message_consent": "GRANTED",
                "service_notification_consent": "GRANTED",
                "output_datetime": datetime.now().strftime('%Y/%m/%d %H:%M:%S')
            },
            {
                "connection_key": "LINE002",
                "customer_key": "CUST002",
                "line_user_id": "U9876543210fedcba9876543210fedcba",
                "display_name": "テストユーザー2",
                "link_status": "UNLINKED",
                "link_date": "2024-01-10",
                "last_interaction_date": "2024-01-18",
                "friend_status": "UNFRIEND",
                "block_status": "BLOCKED",
                "message_receive_setting": "DISABLED",
                "promotional_message_consent": "REVOKED",
                "service_notification_consent": "PENDING",
                "output_datetime": datetime.now().strftime('%Y/%m/%d %H:%M:%S')
            }
        ]
        
        return {
            "test_data": test_line_id_data,
            "expected_columns": ["connection_key", "customer_key", "line_user_id", 
                               "display_name", "link_status", "link_date", 
                               "last_interaction_date", "friend_status", "block_status",
                               "message_receive_setting", "promotional_message_consent",
                               "service_notification_consent", "output_datetime"]
        }
    
    @pytest.mark.asyncio
    async def test_basic_pipeline_execution(self, pipeline_helper, test_data_setup):
        """基本パイプライン実行テスト"""
        logger = logging.getLogger(__name__)
        logger.info(f"Starting basic execution test for {self.PIPELINE_NAME}")
        
        try:
            # パイプライン実行
            run_id = await pipeline_helper.run_pipeline(self.PIPELINE_NAME)
            
            # 実行完了まで監視
            status = await pipeline_helper.wait_for_pipeline_completion(
                run_id, timeout_minutes=30
            )
            
            assert status == "Succeeded", f"Pipeline failed with status: {status}"
            
            # 出力ファイル検証
            output_files = await pipeline_helper.list_output_files(
                container_name="line-id-link-info", 
                date_path=datetime.now().strftime('%Y/%m/%d')
            )
            
            assert len(output_files) > 0, "No output files found"
            
            # CSVファイル内容検証
            csv_content = await pipeline_helper.download_and_extract_csv(output_files[0])
            assert len(csv_content) > 0, "CSV content is empty"
            
            # カラム検証
            expected_cols = test_data_setup["expected_columns"]
            actual_cols = list(csv_content[0].keys())
            
            for col in expected_cols:
                assert col in actual_cols, f"Required column {col} not found in CSV"
            
            logger.info(f"Basic execution test passed for {self.PIPELINE_NAME}")
            
        except Exception as e:
            logger.error(f"Basic execution test failed: {str(e)}")
            raise
    
    @pytest.mark.asyncio
    async def test_large_dataset_performance(self, pipeline_helper):
        """大容量データセット性能テスト"""
        logger = logging.getLogger(__name__)
        logger.info(f"Starting large dataset performance test for {self.PIPELINE_NAME}")
        
        # 大容量データを想定したパラメータ設定
        large_dataset_params = {
            "test_mode": "performance",
            "record_limit": 200000,
            "memory_limit_mb": 2048
        }
        
        start_time = datetime.now()
        
        try:
            run_id = await pipeline_helper.run_pipeline(
                self.PIPELINE_NAME, 
                parameters=large_dataset_params
            )
            
            status = await pipeline_helper.wait_for_pipeline_completion(
                run_id, timeout_minutes=60
            )
            
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            assert status == "Succeeded", f"Large dataset test failed: {status}"
            assert execution_time < 3600, f"Execution took too long: {execution_time}s"
            
            logger.info(f"Large dataset test completed in {execution_time}s")
            
        except Exception as e:
            logger.error(f"Large dataset performance test failed: {str(e)}")
            raise
    
    @pytest.mark.asyncio
    async def test_data_quality_validation(self, pipeline_helper, test_data_setup):
        """データ品質検証テスト"""
        logger = logging.getLogger(__name__)
        logger.info(f"Starting data quality validation for {self.PIPELINE_NAME}")
        
        try:
            run_id = await pipeline_helper.run_pipeline(self.PIPELINE_NAME)
            
            status = await pipeline_helper.wait_for_pipeline_completion(
                run_id, timeout_minutes=30
            )
            
            assert status == "Succeeded", f"Pipeline failed: {status}"
            
            # 出力ファイル取得
            output_files = await pipeline_helper.list_output_files(
                container_name="line-id-link-info",
                date_path=datetime.now().strftime('%Y/%m/%d')
            )
            
            csv_content = await pipeline_helper.download_and_extract_csv(output_files[0])
            
            # データ品質チェック
            line_user_id_pattern = re.compile(r'^U[0-9a-f]{32}$')
            
            for row in csv_content:
                # 必須フィールドの非NULL検証
                assert row.get("connection_key"), "connection_key cannot be empty"
                assert row.get("customer_key"), "customer_key cannot be empty"
                assert row.get("line_user_id"), "line_user_id cannot be empty"
                assert row.get("link_status"), "link_status cannot be empty"
                
                # LINE User IDの形式検証
                line_user_id = row.get("line_user_id", "")
                assert line_user_id_pattern.match(line_user_id), \
                    f"Invalid LINE User ID format: {line_user_id}"
                
                # 連携ステータスの値検証
                link_status = row.get("link_status", "")
                assert link_status in ["LINKED", "UNLINKED", "PENDING"], \
                    f"Invalid link_status: {link_status}"
                
                # フレンドステータスの値検証
                friend_status = row.get("friend_status", "")
                assert friend_status in ["FRIEND", "UNFRIEND", "UNKNOWN"], \
                    f"Invalid friend_status: {friend_status}"
                
                # ブロックステータスの値検証
                block_status = row.get("block_status", "")
                assert block_status in ["BLOCKED", "UNBLOCKED", "UNKNOWN"], \
                    f"Invalid block_status: {block_status}"
                
                # メッセージ受信設定の値検証
                msg_setting = row.get("message_receive_setting", "")
                assert msg_setting in ["ENABLED", "DISABLED", "UNKNOWN"], \
                    f"Invalid message_receive_setting: {msg_setting}"
                
                # 同意設定の値検証
                promo_consent = row.get("promotional_message_consent", "")
                assert promo_consent in ["GRANTED", "REVOKED", "PENDING", "UNKNOWN"], \
                    f"Invalid promotional_message_consent: {promo_consent}"
                
                service_consent = row.get("service_notification_consent", "")
                assert service_consent in ["GRANTED", "REVOKED", "PENDING", "UNKNOWN"], \
                    f"Invalid service_notification_consent: {service_consent}"
                
                # 日時フィールドの検証
                if row.get("link_date"):
                    link_date = row.get("link_date")
                    assert len(link_date) >= 10, f"Invalid link_date format: {link_date}"
                
                if row.get("last_interaction_date"):
                    last_interaction = row.get("last_interaction_date")
                    assert len(last_interaction) >= 10, f"Invalid last_interaction_date format: {last_interaction}"
            
            logger.info(f"Data quality validation passed for {self.PIPELINE_NAME}")
            
        except Exception as e:
            logger.error(f"Data quality validation failed: {str(e)}")
            raise
    
    @pytest.mark.asyncio
    async def test_line_specific_validations(self, pipeline_helper):
        """LINE固有の検証テスト"""
        logger = logging.getLogger(__name__)
        logger.info(f"Starting LINE-specific validations for {self.PIPELINE_NAME}")
        
        try:
            run_id = await pipeline_helper.run_pipeline(self.PIPELINE_NAME)
            
            status = await pipeline_helper.wait_for_pipeline_completion(
                run_id, timeout_minutes=30
            )
            
            assert status == "Succeeded", f"Pipeline failed: {status}"
            
            # 出力ファイル取得
            output_files = await pipeline_helper.list_output_files(
                container_name="line-id-link-info",
                date_path=datetime.now().strftime('%Y/%m/%d')
            )
            
            csv_content = await pipeline_helper.download_and_extract_csv(output_files[0])
            
            # LINE固有の検証
            linked_users = 0
            friend_users = 0
            blocked_users = 0
            consent_granted_users = 0
            
            for row in csv_content:
                # 連携ユーザー数カウント
                if row.get("link_status") == "LINKED":
                    linked_users += 1
                
                # フレンドユーザー数カウント
                if row.get("friend_status") == "FRIEND":
                    friend_users += 1
                
                # ブロックユーザー数カウント
                if row.get("block_status") == "BLOCKED":
                    blocked_users += 1
                
                # 同意済みユーザー数カウント
                if row.get("promotional_message_consent") == "GRANTED":
                    consent_granted_users += 1
                
                # 論理的整合性チェック
                link_status = row.get("link_status", "")
                friend_status = row.get("friend_status", "")
                block_status = row.get("block_status", "")
                
                # 連携していないユーザーはフレンドにならない
                if link_status == "UNLINKED":
                    assert friend_status != "FRIEND", \
                        f"Unlinked user cannot be friend: {row.get('connection_key')}"
                
                # ブロックされたユーザーはフレンドではない
                if block_status == "BLOCKED":
                    assert friend_status != "FRIEND", \
                        f"Blocked user cannot be friend: {row.get('connection_key')}"
            
            total_users = len(csv_content)
            if total_users > 0:
                linked_ratio = linked_users / total_users
                friend_ratio = friend_users / total_users
                blocked_ratio = blocked_users / total_users
                consent_ratio = consent_granted_users / total_users
                
                logger.info(f"LINE metrics - Linked: {linked_ratio:.2%}, "
                          f"Friends: {friend_ratio:.2%}, "
                          f"Blocked: {blocked_ratio:.2%}, "
                          f"Consent: {consent_ratio:.2%}")
                
                # 合理的な比率であることを確認
                assert linked_ratio >= 0.0, "Linked ratio should be non-negative"
                assert friend_ratio <= linked_ratio, "Friend ratio should not exceed linked ratio"
                assert blocked_ratio >= 0.0, "Blocked ratio should be non-negative"
            
            logger.info(f"LINE-specific validations passed for {self.PIPELINE_NAME}")
            
        except Exception as e:
            logger.error(f"LINE-specific validations failed: {str(e)}")
            raise
    
    @pytest.mark.asyncio
    async def test_sftp_transfer_validation(self, pipeline_helper):
        """SFTP転送検証テスト"""
        logger = logging.getLogger(__name__)
        logger.info(f"Starting SFTP transfer validation for {self.PIPELINE_NAME}")
        
        try:
            run_id = await pipeline_helper.run_pipeline(self.PIPELINE_NAME)
            
            status = await pipeline_helper.wait_for_pipeline_completion(
                run_id, timeout_minutes=30
            )
            
            assert status == "Succeeded", f"Pipeline failed: {status}"
            
            # SFTP転送後のファイル存在確認
            sftp_files = await pipeline_helper.list_sftp_files(
                remote_path="/line_id_link_info",
                file_pattern="*.gz"
            )
            
            assert len(sftp_files) > 0, "No files found on SFTP server"
            
            # ファイルサイズとMD5チェック
            for file_info in sftp_files:
                assert file_info["size"] > 0, f"File {file_info['name']} is empty"
                
                # ローカルファイルとの整合性確認
                local_hash = await pipeline_helper.calculate_file_hash(file_info["local_path"])
                remote_hash = await pipeline_helper.calculate_sftp_file_hash(file_info["remote_path"])
                
                assert local_hash == remote_hash, f"File integrity check failed for {file_info['name']}"
            
            logger.info(f"SFTP transfer validation passed for {self.PIPELINE_NAME}")
            
        except Exception as e:
            logger.error(f"SFTP transfer validation failed: {str(e)}")
            raise
    
    @pytest.mark.asyncio
    async def test_privacy_compliance_validation(self, pipeline_helper):
        """プライバシー関連コンプライアンス検証テスト"""
        logger = logging.getLogger(__name__)
        logger.info(f"Starting privacy compliance validation for {self.PIPELINE_NAME}")
        
        try:
            run_id = await pipeline_helper.run_pipeline(self.PIPELINE_NAME)
            
            status = await pipeline_helper.wait_for_pipeline_completion(
                run_id, timeout_minutes=30
            )
            
            assert status == "Succeeded", f"Pipeline failed: {status}"
            
            # 出力ファイル取得
            output_files = await pipeline_helper.list_output_files(
                container_name="line-id-link-info",
                date_path=datetime.now().strftime('%Y/%m/%d')
            )
            
            csv_content = await pipeline_helper.download_and_extract_csv(output_files[0])
            
            # プライバシーコンプライアンスチェック
            for row in csv_content:
                # 表示名にPII（個人識別情報）が含まれていないことを確認
                display_name = row.get("display_name", "")
                if display_name:
                    # メールアドレス形式のチェック
                    assert "@" not in display_name, \
                        f"Display name should not contain email: {display_name}"
                    
                    # 電話番号形式のチェック
                    phone_pattern = re.compile(r'\d{10,11}')
                    assert not phone_pattern.search(display_name), \
                        f"Display name should not contain phone number: {display_name}"
                
                # 同意がない場合の取り扱い確認
                promo_consent = row.get("promotional_message_consent", "")
                service_consent = row.get("service_notification_consent", "")
                
                # 同意が明示的に取り消されている場合の確認
                if promo_consent == "REVOKED":
                    # このような場合のデータ取り扱いポリシーを確認
                    logger.info(f"User {row.get('connection_key')} has revoked promotional consent")
                
                if service_consent == "REVOKED":
                    logger.info(f"User {row.get('connection_key')} has revoked service consent")
            
            logger.info(f"Privacy compliance validation passed for {self.PIPELINE_NAME}")
            
        except Exception as e:
            logger.error(f"Privacy compliance validation failed: {str(e)}")
            raise
    
    @pytest.mark.asyncio
    async def test_error_handling_scenarios(self, pipeline_helper):
        """エラーハンドリングシナリオテスト"""
        logger = logging.getLogger(__name__)
        logger.info(f"Starting error handling tests for {self.PIPELINE_NAME}")
        
        error_scenarios = [
            {
                "name": "database_connection_failure",
                "params": {"force_db_error": True},
                "expected_status": "Failed"
            },
            {
                "name": "sftp_connection_failure", 
                "params": {"force_sftp_error": True},
                "expected_status": "Failed"
            },
            {
                "name": "invalid_line_user_id",
                "params": {"inject_invalid_line_ids": True},
                "expected_status": "Failed"
            },
            {
                "name": "missing_link_status",
                "params": {"inject_missing_link_status": True},
                "expected_status": "Failed"
            }
        ]
        
        for scenario in error_scenarios:
            try:
                logger.info(f"Testing error scenario: {scenario['name']}")
                
                run_id = await pipeline_helper.run_pipeline(
                    self.PIPELINE_NAME,
                    parameters=scenario["params"]
                )
                
                status = await pipeline_helper.wait_for_pipeline_completion(
                    run_id, timeout_minutes=15
                )
                
                assert status == scenario["expected_status"], \
                    f"Expected {scenario['expected_status']}, got {status}"
                
                logger.info(f"Error scenario {scenario['name']} handled correctly")
                
            except Exception as e:
                logger.error(f"Error handling test failed for {scenario['name']}: {str(e)}")
                raise
    
    @pytest.mark.asyncio
    async def test_timezone_handling(self, pipeline_helper):
        """タイムゾーン処理テスト"""
        logger = logging.getLogger(__name__)
        logger.info(f"Starting timezone handling test for {self.PIPELINE_NAME}")
        
        # 異なるタイムゾーンでのテスト
        timezones = ["UTC", "Asia/Tokyo", "America/New_York"]
        
        for tz_name in timezones:
            try:
                tz_params = {
                    "execution_timezone": tz_name,
                    "output_timezone": "Asia/Tokyo"  # 日本時間での出力
                }
                
                run_id = await pipeline_helper.run_pipeline(
                    self.PIPELINE_NAME,
                    parameters=tz_params
                )
                
                status = await pipeline_helper.wait_for_pipeline_completion(
                    run_id, timeout_minutes=30
                )
                
                assert status == "Succeeded", f"Timezone test failed for {tz_name}: {status}"
                
                # 出力時間の検証
                output_files = await pipeline_helper.list_output_files(
                    container_name="line-id-link-info",
                    date_path=datetime.now().strftime('%Y/%m/%d')
                )
                
                csv_content = await pipeline_helper.download_and_extract_csv(output_files[0])
                
                # 出力時間が日本時間で記録されていることを確認
                for row in csv_content:
                    output_dt_str = row.get("output_datetime", "")
                    if output_dt_str:
                        # 日本時間フォーマットの確認
                        assert len(output_dt_str) >= 16, f"Invalid datetime format: {output_dt_str}"
                
                logger.info(f"Timezone handling test passed for {tz_name}")
                
            except Exception as e:
                logger.error(f"Timezone handling test failed for {tz_name}: {str(e)}")
                raise


if __name__ == "__main__":
    # テスト実行のためのエントリーポイント
    pytest.main([__file__, "-v", "--tb=short"])
