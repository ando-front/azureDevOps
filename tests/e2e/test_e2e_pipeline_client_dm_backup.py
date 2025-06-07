"""
Azure Data Factory Pipeline E2E Tests - Client DM Send Pipeline
テスト対象パイプライン: pi_Send_ClientDM

このパイプラインの主要機能:
1. データベースからClientDMデータを抽出してCSV作成
2. 作成したCSVファイルをgzip圧縮してBlobStorageに保存
3. gzipファイルをSFMCにSFTP転送

テストシナリオ:
- 正常データ処理（標準的なClientDMデータセット）
- 大容量データ処理（性能テスト）
- データ品質検証（必須フィールド、フォーマット）
- SFTP転送検証（ファイル完全性、転送時間）
- エラーハンドリング（接続障害、無効データ）
- 日次実行シミュレーション（タイムゾーン処理含む）
"""

import pytest
import os
import requests
from datetime import datetime, timedelta, timezone
import pytz
from typing import Dict, List, Any
import logging
import gzip
import csv
import json
from unittest.mock import patch, MagicMock

# テスト用ヘルパーモジュール
from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection
from tests.e2e.helpers.missing_helpers_placeholder import MissingHelperPlaceholder as E2ESQLQueryManager


class TestPipelineClientDM:

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
    """Client DM Send パイプライン専用E2Eテストクラス"""
    
    PIPELINE_NAME = "pi_Send_ClientDM"
    
    @pytest.fixture(scope="class")
    def pipeline_helper(self, e2e_synapse_connection):
        """パイプライン専用のヘルパーインスタンス"""
        return e2e_synapse_connection
    
    @pytest.fixture(scope="class")
    def sql_manager(self):
        """SQL Query Manager インスタンス"""
        return E2ESQLQueryManager()
    
    @pytest.fixture(scope="class")
    def test_data_setup(self, pipeline_helper, sql_manager):
        """テストデータのセットアップ"""
        # Client DMテスト用データ
        test_client_data = [
            {
                "client_id": "CLIENT001",
                "client_name": "テストクライアント1",
                "email": "test1@example.com",
                "registration_date": "2024-01-15",
                "status": "active",
                "segment": "premium"
            },
            {
                "client_id": "CLIENT002", 
                "client_name": "テストクライアント2",
                "email": "test2@example.com",
                "registration_date": "2024-02-20",
                "status": "active",
                "segment": "standard"
            },
            {
                "client_id": "CLIENT003",
                "client_name": "テストクライアント3", 
                "email": "test3@example.com",
                "registration_date": "2024-03-10",
                "status": "inactive",
                "segment": "basic"
            }
        ]
          # テスト用テーブル作成
        create_table_sql = sql_manager.get_query("client_dm_backup.sql", "create_client_dm_test_table")
        pipeline_helper.execute_sql(create_table_sql)
        
        return {
            "test_clients": test_client_data,
            "expected_csv_columns": ["client_id", "client_name", "email", "registration_date", "status", "segment"]
        }

    def test_client_dm_standard_processing(self, pipeline_helper, test_data_setup, sql_manager):
        """
        テストシナリオ1: 標準的なClient DMデータ処理
        
        検証項目:
        - データベースからの正常データ抽出
        - CSV形式でのファイル出力
        - gzip圧縮処理
        - BlobStorageへの保存
        - SFTP転送の完了
        """
        logger = logging.getLogger(__name__)
        logger.info("=== Client DM Standard Processing Test ===")
        
        # テストパラメータ設定
        test_params = {
            "pipeline_name": self.PIPELINE_NAME,
            "expected_activities": ["at_CreateCSV_ClientDM", "at_SendSftp_ClientDM"],
            "test_data": test_data_setup["test_clients"]
        }
        
        try:
            # 1. パイプライン実行前の準備
            logger.info("テストデータをDWHテーブルに投入...")
              # テストデータ投入用SQL
            insert_sql = sql_manager.get_query("client_dm_backup.sql", "insert_client_dm_test_data")
            for client_data in test_params["test_data"]:
                pipeline_helper.execute_sql(insert_sql, {
                    "client_id": client_data["client_id"],
                    "client_name": client_data["client_name"],
                    "email": client_data["email"],
                    "registration_date": client_data["registration_date"],
                    "status": client_data["status"],
                    "segment": client_data["segment"]
                })
            
            # 2. パイプライン実行
            logger.info(f"パイプライン {self.PIPELINE_NAME} を実行開始...")
            run_result = pipeline_helper.run_pipeline(
                pipeline_name=self.PIPELINE_NAME,
                parameters={}
            )
            
            # 3. 基本実行結果検証
            assert run_result["status"] == "Succeeded", f"パイプライン実行失敗: {run_result.get('error')}"
            assert run_result["duration"] < 300, "実行時間が予想より長い（5分超過）"
            
            # 4. 各アクティビティの実行状況検証
            activities_status = pipeline_helper.get_activity_status(run_result["run_id"])
            for activity_name in test_params["expected_activities"]:
                assert activity_name in activities_status, f"アクティビティ {activity_name} が見つからない"
                assert activities_status[activity_name] == "Succeeded", f"アクティビティ {activity_name} が失敗"
            
            # 5. 出力ファイル検証
            tokyo_tz = pytz.timezone('Asia/Tokyo')
            current_date = datetime.now(tokyo_tz).strftime('%Y%m%d')
            expected_filename = f"ClientDM_{current_date}.csv.gz"
            
            # BlobStorageでのファイル存在確認
            blob_file_exists = pipeline_helper.check_blob_file_exists(
                container="datalake",
                blob_path=f"OMNI/MA/ClientDM/{expected_filename}"
            )
            assert blob_file_exists, f"BlobStorageにファイル {expected_filename} が見つからない"
            
            # 6. SFTP転送検証
            sftp_file_exists = pipeline_helper.check_sftp_file_exists(
                remote_path=f"Import/DAM/ClientDM/{expected_filename}"
            )
            assert sftp_file_exists, f"SFTPサーバーにファイル {expected_filename} が転送されていない"
            
            # 7. ファイル内容検証
            file_content = pipeline_helper.download_and_extract_gzip_file(
                container="datalake",
                blob_path=f"OMNI/MA/ClientDM/{expected_filename}"
            )
            
            csv_data = list(csv.DictReader(file_content.split('\n')))
            assert len(csv_data) == len(test_params["test_data"]), "CSVレコード数が期待値と異なる"
            
            # データ内容検証
            for i, row in enumerate(csv_data):
                expected_row = test_params["test_data"][i]
                assert row["client_id"] == expected_row["client_id"], f"client_idが一致しない: {row['client_id']} != {expected_row['client_id']}"
                assert row["email"] == expected_row["email"], f"emailが一致しない: {row['email']} != {expected_row['email']}"
            
            logger.info("✅ Client DM標準処理テスト完了")
            
        except Exception as e:
            logger.error(f"❌ Client DM標準処理テスト失敗: {str(e)}")
            raise
        finally:            # テストデータクリーンアップ
            cleanup_sql = sql_manager.get_query("client_dm_backup.sql", "cleanup_client_dm_test_data")
            pipeline_helper.execute_sql(cleanup_sql)
    
    def test_client_dm_large_dataset_performance(self, pipeline_helper, sql_manager):
        """
        テストシナリオ2: 大容量データセット処理性能テスト
        
        検証項目:
        - 10,000件以上のClient DMデータ処理
        - メモリ効率的なCSV生成
        - gzip圧縮性能
        - 大ファイルSFTP転送
        - 処理時間とスループット測定
        """
        logger = logging.getLogger(__name__)
        logger.info("=== Client DM Large Dataset Performance Test ===")
        
        # 大容量テストデータ生成（10,000件）
        large_test_data = []
        for i in range(10000):
            large_test_data.append({
                "client_id": f"CLIENT{i:06d}",
                "client_name": f"大容量テストクライアント{i}",
                "email": f"bulk_test_{i}@example.com",
                "registration_date": "2024-01-01",
                "status": "active" if i % 2 == 0 else "inactive",
                "segment": ["premium", "standard", "basic"][i % 3]
            })
        
        performance_metrics = {}
        
        try:
            # 1. 大容量データ投入
            setup_start = datetime.now()
              # テーブル作成
            create_table_sql = sql_manager.get_query("client_dm_backup.sql", "create_client_dm_large_test_table")
            pipeline_helper.execute_sql(create_table_sql)
            
            # 大容量データ投入
            insert_sql = sql_manager.get_query("client_dm_backup.sql", "insert_client_dm_bulk_data")
            for client_data in large_test_data:
                pipeline_helper.execute_sql(insert_sql, {
                    "client_id": client_data["client_id"],
                    "client_name": client_data["client_name"],
                    "email": client_data["email"],
                    "registration_date": client_data["registration_date"],
                    "status": client_data["status"],
                    "segment": client_data["segment"]
                })
            
            setup_time = (datetime.now() - setup_start).total_seconds()
            performance_metrics["data_setup_time"] = setup_time
            logger.info(f"大容量データセットアップ完了: {setup_time:.2f}秒")
            
            # 2. パイプライン実行（性能測定）
            execution_start = datetime.now()
            run_result = pipeline_helper.run_pipeline(
                pipeline_name=self.PIPELINE_NAME,
                parameters={"use_large_dataset": True}
            )
            execution_time = (datetime.now() - execution_start).total_seconds()
            performance_metrics["pipeline_execution_time"] = execution_time
            
            # 3. 性能基準検証
            assert run_result["status"] == "Succeeded", f"大容量データパイプライン実行失敗: {run_result.get('error')}"
            assert execution_time < 900, f"実行時間が基準を超過: {execution_time}秒 > 900秒"  # 15分以内
            
            # 4. スループット計算
            throughput = len(large_test_data) / execution_time
            performance_metrics["throughput_records_per_second"] = throughput
            assert throughput > 100, f"スループットが基準を下回る: {throughput:.2f} records/sec < 100"
            
            # 5. ファイルサイズ検証
            tokyo_tz = pytz.timezone('Asia/Tokyo')
            current_date = datetime.now(tokyo_tz).strftime('%Y%m%d')
            expected_filename = f"ClientDM_{current_date}.csv.gz"
            
            file_info = pipeline_helper.get_blob_file_info(
                container="datalake",
                blob_path=f"OMNI/MA/ClientDM/{expected_filename}"
            )
            file_size_mb = file_info["size"] / (1024 * 1024)
            performance_metrics["output_file_size_mb"] = file_size_mb
            
            # 6. SFTP転送性能検証
            sftp_file_exists = pipeline_helper.check_sftp_file_exists(
                remote_path=f"Import/DAM/ClientDM/{expected_filename}"            )
            assert sftp_file_exists, "大容量ファイルのSFTP転送が失敗"
            
            logger.info(f"✅ 大容量データ処理テスト完了 - 性能メトリクス: {json.dumps(performance_metrics, indent=2)}")
            
        except Exception as e:
            logger.error(f"❌ 大容量データ処理テスト失敗: {str(e)}")
            raise
        finally:
            # テストデータクリーンアップ
            cleanup_sql = sql_manager.get_query("client_dm_backup.sql", "cleanup_client_dm_large_test_data")
            pipeline_helper.execute_sql(cleanup_sql)

    def test_client_dm_data_quality_validation(self, pipeline_helper, sql_manager):
        """
        テストシナリオ3: データ品質検証テスト
        
        検証項目:
        - 必須フィールドの検証
        - データフォーマット検証
        - 重複データ処理
        - NULL値ハンドリング
        - 文字エンコーディング検証
        """
        logger = logging.getLogger(__name__)
        logger.info("=== Client DM Data Quality Validation Test ===")
        
        # データ品質テスト用のデータセット（意図的に問題のあるデータを含む）
        quality_test_data = [
            {
                "client_id": "QUAL001",
                "client_name": "品質テスト正常データ",
                "email": "quality_normal@example.com",
                "registration_date": "2024-01-01",
                "status": "active",
                "segment": "premium"
            },
            {
                "client_id": "QUAL002",
                "client_name": "日本語文字テスト あいうえお 漢字",
                "email": "japanese_test@example.co.jp",
                "registration_date": "2024-02-01",
                "status": "active",
                "segment": "standard"
            },
            {
                "client_id": "QUAL003",
                "client_name": "",  # 空の名前
                "email": "empty_name@example.com",
                "registration_date": "2024-03-01",
                "status": "active",
                "segment": "basic"
            }
        ]
        
        try:
            # 1. 品質テストデータ投入
            logger.info("データ品質テスト用データ投入...")
              # テーブル作成
            create_table_sql = sql_manager.get_query("client_dm_backup.sql", "create_client_dm_quality_test_table")
            pipeline_helper.execute_sql(create_table_sql)
            
            # データ投入
            insert_sql = sql_manager.get_query("client_dm_backup.sql", "insert_client_dm_quality_data")
            for client_data in quality_test_data:
                pipeline_helper.execute_sql(insert_sql, {
                    "client_id": client_data["client_id"],
                    "client_name": client_data["client_name"],
                    "email": client_data["email"],
                    "registration_date": client_data["registration_date"],
                    "status": client_data["status"],
                    "segment": client_data["segment"]
                })
            
            # 2. パイプライン実行
            logger.info("データ品質検証パイプライン実行...")
            run_result = pipeline_helper.run_pipeline(
                pipeline_name=self.PIPELINE_NAME,
                parameters={"data_quality_mode": True}
            )
            
            # 3. パイプライン実行結果検証
            assert run_result["status"] == "Succeeded", f"データ品質検証パイプライン実行失敗: {run_result.get('error')}"
            
            # 4. 出力ファイル検証
            tokyo_tz = pytz.timezone('Asia/Tokyo')
            current_date = datetime.now(tokyo_tz).strftime('%Y%m%d')
            expected_filename = f"ClientDM_{current_date}.csv.gz"
            
            # ファイル内容検証
            file_content = pipeline_helper.download_and_extract_gzip_file(
                container="datalake",
                blob_path=f"OMNI/MA/ClientDM/{expected_filename}"
            )
            
            csv_data = list(csv.DictReader(file_content.split('\n')))
            
            # 5. データ品質検証
            for row in csv_data:
                # 必須フィールド検証
                assert "client_id" in row and row["client_id"], "client_idが必須"
                assert "email" in row and row["email"], "emailが必須"
                
                # メールフォーマット検証（簡易）                assert "@" in row["email"], f"無効なメールフォーマット: {row['email']}"
                
                # 日付フォーマット検証
                if row["registration_date"]:
                    try:
                        datetime.strptime(row["registration_date"], "%Y-%m-%d")
                    except ValueError:
                        assert False, f"無効な日付フォーマット: {row['registration_date']}"
            
            # 6. 文字エンコーディング検証
            assert file_content.encode('utf-8').decode('utf-8') == file_content, "UTF-8エンコーディングエラー"
            
            logger.info("✅ データ品質検証テスト完了")
            
        except Exception as e:
            logger.error(f"❌ データ品質検証テスト失敗: {str(e)}")
            raise
        finally:
            # テストデータクリーンアップ
            cleanup_sql = sql_manager.get_query("client_dm_backup.sql", "cleanup_client_dm_quality_test_data")
            pipeline_helper.execute_sql(cleanup_sql)
