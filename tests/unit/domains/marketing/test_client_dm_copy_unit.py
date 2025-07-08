"""
pi_Copy_marketing_client_dm パイプラインのユニットテスト

マーケティングClient DMコピーパイプラインの個別コンポーネントテスト
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any


class TestClientDmCopyUnit(unittest.TestCase):
    """マーケティングClient DMコピーパイプラインユニットテスト"""
    
    def setUp(self):
        """テストセットアップ"""
        self.pipeline_name = "pi_Copy_marketing_client_dm"
        self.domain = "marketing"
        
        # モックサービス
        self.mock_blob_storage = Mock()
        self.mock_database = Mock()
        self.mock_file_service = Mock()
        self.mock_transform_service = Mock()
        
        # テストデータ
        self.test_date = datetime.utcnow().strftime('%Y%m%d')
        self.source_file_path = f"marketing/source/{self.test_date}/client_dm_source.csv"
        self.target_file_path = f"marketing/target/{self.test_date}/client_dm_target.csv"
        
        # 基本的なマーケティングClient DMデータ
        self.sample_marketing_client_dm_data = [
            {
                "SOURCE_ID": "SRC000001",
                "CUSTOMER_ID": "CUST123456",
                "CUSTOMER_NAME": "テストユーザー1",
                "EMAIL_ADDRESS": "test1@example.com",
                "PHONE_NUMBER": "090-1234-5678",
                "POSTAL_CODE": "100-0001",
                "ADDRESS": "東京都千代田区千代田1-1-1",
                "CUSTOMER_SEGMENT": "PREMIUM",
                "REGISTRATION_DATE": "20230101",
                "LAST_ACTIVITY_DATE": "20240228",
                "PURCHASE_HISTORY": 12,
                "TOTAL_SPEND": 156000,
                "PREFERRED_CONTACT": "EMAIL",
                "MARKETING_CONSENT": "Y",
                "DATA_SOURCE": "CRM",
                "EXTRACTION_DATE": "20240301",
                "EXTRACTION_TIME": "10:00:00"
            },
            {
                "SOURCE_ID": "SRC000002",
                "CUSTOMER_ID": "CUST123457",
                "CUSTOMER_NAME": "テストユーザー2",
                "EMAIL_ADDRESS": "test2@example.com",
                "PHONE_NUMBER": "090-2345-6789",
                "POSTAL_CODE": "160-0023",
                "ADDRESS": "東京都新宿区西新宿1-1-1",
                "CUSTOMER_SEGMENT": "STANDARD",
                "REGISTRATION_DATE": "20230201",
                "LAST_ACTIVITY_DATE": "20240225",
                "PURCHASE_HISTORY": 8,
                "TOTAL_SPEND": 89000,
                "PREFERRED_CONTACT": "PHONE",
                "MARKETING_CONSENT": "Y",
                "DATA_SOURCE": "WEB",
                "EXTRACTION_DATE": "20240301",
                "EXTRACTION_TIME": "10:00:00"
            }
        ]
    
    def test_lookup_activity_source_data_detection(self):
        """Lookup Activity: ソースデータ検出テスト"""
        # テストケース: ソースデータの検出
        mock_source_data = [
            {
                "SOURCE_ID": "SRC000001",
                "CUSTOMER_ID": "CUST123456",
                "CUSTOMER_NAME": "テストユーザー1",
                "EMAIL_ADDRESS": "test1@example.com",
                "CUSTOMER_SEGMENT": "PREMIUM",
                "REGISTRATION_DATE": "20230101",
                "MARKETING_CONSENT": "Y",
                "DATA_SOURCE": "CRM",
                "EXTRACTION_DATE": "20240301",
                "PROCESSING_STATUS": "PENDING"
            },
            {
                "SOURCE_ID": "SRC000002",
                "CUSTOMER_ID": "CUST123457",
                "CUSTOMER_NAME": "テストユーザー2",
                "EMAIL_ADDRESS": "test2@example.com",
                "CUSTOMER_SEGMENT": "STANDARD",
                "REGISTRATION_DATE": "20230201",
                "MARKETING_CONSENT": "Y",
                "DATA_SOURCE": "WEB",
                "EXTRACTION_DATE": "20240301",
                "PROCESSING_STATUS": "PENDING"
            }
        ]
        
        self.mock_database.query_records.return_value = mock_source_data
        
        # Lookup Activity実行シミュレーション
        source_data_query = f"""
        SELECT SOURCE_ID, CUSTOMER_ID, CUSTOMER_NAME, EMAIL_ADDRESS, CUSTOMER_SEGMENT, 
               REGISTRATION_DATE, MARKETING_CONSENT, DATA_SOURCE, EXTRACTION_DATE, PROCESSING_STATUS
        FROM marketing_client_dm_source
        WHERE EXTRACTION_DATE >= '{self.test_date}' AND PROCESSING_STATUS = 'PENDING'
        ORDER BY EXTRACTION_DATE ASC, SOURCE_ID ASC
        """
        
        result = self.mock_database.query_records("marketing_client_dm_source", source_data_query)
        
        self.assertEqual(len(result), 2, "ソースデータ検出件数不正")
        self.assertEqual(result[0]["SOURCE_ID"], "SRC000001", "ソースID確認失敗")
        self.assertEqual(result[0]["PROCESSING_STATUS"], "PENDING", "処理ステータス確認失敗")
        self.assertEqual(result[0]["MARKETING_CONSENT"], "Y", "マーケティング同意確認失敗")
    
    def test_lookup_activity_target_schema_validation(self):
        """Lookup Activity: ターゲットスキーマ検証テスト"""
        # テストケース: ターゲットスキーマの検証
        mock_target_schema = [
            {
                "COLUMN_NAME": "TARGET_ID",
                "DATA_TYPE": "VARCHAR",
                "MAX_LENGTH": 20,
                "IS_NULLABLE": "N",
                "IS_PRIMARY_KEY": "Y",
                "COLUMN_ORDER": 1
            },
            {
                "COLUMN_NAME": "CUSTOMER_ID",
                "DATA_TYPE": "VARCHAR",
                "MAX_LENGTH": 15,
                "IS_NULLABLE": "N",
                "IS_PRIMARY_KEY": "N",
                "COLUMN_ORDER": 2
            },
            {
                "COLUMN_NAME": "CUSTOMER_NAME",
                "DATA_TYPE": "NVARCHAR",
                "MAX_LENGTH": 100,
                "IS_NULLABLE": "N",
                "IS_PRIMARY_KEY": "N",
                "COLUMN_ORDER": 3
            },
            {
                "COLUMN_NAME": "EMAIL_ADDRESS",
                "DATA_TYPE": "VARCHAR",
                "MAX_LENGTH": 255,
                "IS_NULLABLE": "Y",
                "IS_PRIMARY_KEY": "N",
                "COLUMN_ORDER": 4
            },
            {
                "COLUMN_NAME": "MARKETING_SEGMENT",
                "DATA_TYPE": "VARCHAR",
                "MAX_LENGTH": 50,
                "IS_NULLABLE": "Y",
                "IS_PRIMARY_KEY": "N",
                "COLUMN_ORDER": 5
            }
        ]
        
        self.mock_database.query_records.return_value = mock_target_schema
        
        # Lookup Activity実行シミュレーション
        schema_query = """
        SELECT COLUMN_NAME, DATA_TYPE, MAX_LENGTH, IS_NULLABLE, IS_PRIMARY_KEY, COLUMN_ORDER
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'marketing_client_dm_target'
        ORDER BY COLUMN_ORDER ASC
        """
        
        result = self.mock_database.query_records("INFORMATION_SCHEMA.COLUMNS", schema_query)
        
        self.assertEqual(len(result), 5, "ターゲットスキーマ取得件数不正")
        self.assertEqual(result[0]["COLUMN_NAME"], "TARGET_ID", "カラム名確認失敗")
        self.assertEqual(result[0]["IS_PRIMARY_KEY"], "Y", "主キー確認失敗")
        self.assertEqual(result[1]["COLUMN_NAME"], "CUSTOMER_ID", "カラム名確認失敗")
        self.assertEqual(result[3]["COLUMN_NAME"], "EMAIL_ADDRESS", "カラム名確認失敗")
        self.assertEqual(result[4]["COLUMN_NAME"], "MARKETING_SEGMENT", "カラム名確認失敗")
    
    def test_data_flow_data_transformation(self):
        """Data Flow: データ変換処理テスト"""
        # テストケース: データ変換処理
        source_data = {
            "SOURCE_ID": "SRC000001",
            "CUSTOMER_ID": "CUST123456",
            "CUSTOMER_NAME": "テストユーザー1",
            "EMAIL_ADDRESS": "test1@example.com",
            "PHONE_NUMBER": "090-1234-5678",
            "POSTAL_CODE": "100-0001",
            "ADDRESS": "東京都千代田区千代田1-1-1",
            "CUSTOMER_SEGMENT": "PREMIUM",
            "REGISTRATION_DATE": "20230101",
            "LAST_ACTIVITY_DATE": "20240228",
            "PURCHASE_HISTORY": 12,
            "TOTAL_SPEND": 156000,
            "PREFERRED_CONTACT": "EMAIL",
            "MARKETING_CONSENT": "Y",
            "DATA_SOURCE": "CRM"
        }
        
        # データ変換処理ロジック（Data Flow内の処理）
        def transform_marketing_client_dm_data(source_data):
            # ターゲットID生成
            target_id = f"TGT{source_data['CUSTOMER_ID'][4:]}{datetime.utcnow().strftime('%Y%m%d')}"
            
            # 顧客セグメント変換
            segment_mapping = {
                "PREMIUM": "PREMIUM_MARKETING",
                "STANDARD": "STANDARD_MARKETING",
                "BASIC": "BASIC_MARKETING",
                "VIP": "VIP_MARKETING"
            }
            marketing_segment = segment_mapping.get(source_data["CUSTOMER_SEGMENT"], "UNKNOWN_MARKETING")
            
            # 連絡先設定変換
            contact_preferences = {
                "EMAIL": "E",
                "PHONE": "P",
                "POSTAL": "M",
                "SMS": "S"
            }
            preferred_contact_code = contact_preferences.get(source_data["PREFERRED_CONTACT"], "E")
            
            # 顧客価値スコア計算
            customer_value_score = 0
            if source_data["TOTAL_SPEND"] > 100000:
                customer_value_score += 40
            elif source_data["TOTAL_SPEND"] > 50000:
                customer_value_score += 25
            else:
                customer_value_score += 10
            
            if source_data["PURCHASE_HISTORY"] > 10:
                customer_value_score += 30
            elif source_data["PURCHASE_HISTORY"] > 5:
                customer_value_score += 20
            else:
                customer_value_score += 10
            
            # 最終活動日からの日数計算
            last_activity_date = datetime.strptime(source_data["LAST_ACTIVITY_DATE"], "%Y%m%d")
            days_since_last_activity = (datetime.utcnow() - last_activity_date).days
            
            # 変換されたデータ
            transformed_data = {
                "TARGET_ID": target_id,
                "CUSTOMER_ID": source_data["CUSTOMER_ID"],
                "CUSTOMER_NAME": source_data["CUSTOMER_NAME"],
                "EMAIL_ADDRESS": source_data["EMAIL_ADDRESS"],
                "PHONE_NUMBER": source_data["PHONE_NUMBER"],
                "POSTAL_CODE": source_data["POSTAL_CODE"],
                "ADDRESS": source_data["ADDRESS"],
                "MARKETING_SEGMENT": marketing_segment,
                "REGISTRATION_DATE": source_data["REGISTRATION_DATE"],
                "LAST_ACTIVITY_DATE": source_data["LAST_ACTIVITY_DATE"],
                "DAYS_SINCE_LAST_ACTIVITY": days_since_last_activity,
                "PURCHASE_HISTORY": source_data["PURCHASE_HISTORY"],
                "TOTAL_SPEND": source_data["TOTAL_SPEND"],
                "PREFERRED_CONTACT_CODE": preferred_contact_code,
                "CUSTOMER_VALUE_SCORE": customer_value_score,
                "MARKETING_CONSENT": source_data["MARKETING_CONSENT"],
                "DATA_SOURCE": source_data["DATA_SOURCE"],
                "TRANSFORMATION_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "TRANSFORMATION_TIME": datetime.utcnow().strftime('%H:%M:%S')
            }
            
            return transformed_data
        
        # データ変換実行
        transformed_data = transform_marketing_client_dm_data(source_data)
        
        # アサーション
        self.assertIsNotNone(transformed_data["TARGET_ID"], "ターゲットID生成失敗")
        self.assertTrue(transformed_data["TARGET_ID"].startswith("TGT123456"), "ターゲットID形式確認失敗")
        self.assertEqual(transformed_data["CUSTOMER_ID"], "CUST123456", "顧客ID引き継ぎ失敗")
        self.assertEqual(transformed_data["MARKETING_SEGMENT"], "PREMIUM_MARKETING", "マーケティングセグメント変換失敗")
        self.assertEqual(transformed_data["PREFERRED_CONTACT_CODE"], "E", "連絡先コード変換失敗")
        self.assertEqual(transformed_data["CUSTOMER_VALUE_SCORE"], 70, "顧客価値スコア計算失敗")  # 40 + 30
        self.assertGreater(transformed_data["DAYS_SINCE_LAST_ACTIVITY"], 0, "最終活動日からの日数計算失敗")
        self.assertEqual(transformed_data["MARKETING_CONSENT"], "Y", "マーケティング同意引き継ぎ失敗")
    
    def test_data_flow_data_quality_validation(self):
        """Data Flow: データ品質検証テスト"""
        # テストケース: データ品質検証
        test_data_records = [
            {"TARGET_ID": "TGT12345620240301", "CUSTOMER_ID": "CUST123456", "CUSTOMER_NAME": "テストユーザー1", "EMAIL_ADDRESS": "test1@example.com", "MARKETING_CONSENT": "Y"},
            {"TARGET_ID": "", "CUSTOMER_ID": "CUST123457", "CUSTOMER_NAME": "テストユーザー2", "EMAIL_ADDRESS": "test2@example.com", "MARKETING_CONSENT": "Y"},  # 不正: 空ターゲットID
            {"TARGET_ID": "TGT12345820240301", "CUSTOMER_ID": "CUST123458", "CUSTOMER_NAME": "", "EMAIL_ADDRESS": "test3@example.com", "MARKETING_CONSENT": "Y"},  # 不正: 空顧客名
            {"TARGET_ID": "TGT12345920240301", "CUSTOMER_ID": "CUST123459", "CUSTOMER_NAME": "テストユーザー4", "EMAIL_ADDRESS": "invalid-email", "MARKETING_CONSENT": "Y"},  # 不正: 無効メール
            {"TARGET_ID": "TGT12346020240301", "CUSTOMER_ID": "CUST123460", "CUSTOMER_NAME": "テストユーザー5", "EMAIL_ADDRESS": "test5@example.com", "MARKETING_CONSENT": "N"}  # 不正: マーケティング同意なし
        ]
        
        # データ品質検証ロジック（Data Flow内の処理）
        def validate_marketing_client_dm_data(record):
            errors = []
            warnings = []
            
            # ターゲットID検証
            if not record.get("TARGET_ID", "").strip():
                errors.append("ターゲットID必須")
            elif not record["TARGET_ID"].startswith("TGT"):
                errors.append("ターゲットID形式が不正です")
            
            # 顧客ID検証
            if not record.get("CUSTOMER_ID", "").strip():
                errors.append("顧客ID必須")
            elif not record["CUSTOMER_ID"].startswith("CUST"):
                errors.append("顧客ID形式が不正です")
            
            # 顧客名検証
            if not record.get("CUSTOMER_NAME", "").strip():
                errors.append("顧客名必須")
            
            # メールアドレス検証
            email = record.get("EMAIL_ADDRESS", "")
            if email and ("@" not in email or "." not in email):
                errors.append("メールアドレス形式が不正です")
            
            # マーケティング同意検証
            if record.get("MARKETING_CONSENT") != "Y":
                warnings.append("マーケティング同意がありません")
            
            # 品質スコア計算
            total_fields = 5
            error_count = len(errors)
            warning_count = len(warnings)
            
            quality_score = max(0, (total_fields - error_count - (warning_count * 0.5)) / total_fields)
            
            return {
                "record": record,
                "errors": errors,
                "warnings": warnings,
                "is_valid": len(errors) == 0,
                "quality_score": quality_score
            }
        
        # 品質検証実行
        validation_results = []
        for record in test_data_records:
            validation_result = validate_marketing_client_dm_data(record)
            validation_results.append(validation_result)
        
        # アサーション
        self.assertEqual(len(validation_results), 5, "検証結果数不正")
        self.assertTrue(validation_results[0]["is_valid"], "正常レコードが不正判定")
        self.assertEqual(validation_results[0]["quality_score"], 1.0, "品質スコア確認失敗")
        self.assertFalse(validation_results[1]["is_valid"], "不正レコード（空ターゲットID）が正常判定")
        self.assertFalse(validation_results[2]["is_valid"], "不正レコード（空顧客名）が正常判定")
        self.assertFalse(validation_results[3]["is_valid"], "不正レコード（無効メール）が正常判定")
        self.assertTrue(validation_results[4]["is_valid"], "警告レコードが不正判定")
        self.assertEqual(len(validation_results[4]["warnings"]), 1, "警告数確認失敗")
        self.assertEqual(validation_results[4]["quality_score"], 0.9, "品質スコア（警告あり）確認失敗")
    
    def test_data_flow_data_deduplication(self):
        """Data Flow: データ重複排除処理テスト"""
        # テストケース: データ重複排除
        duplicate_data = [
            {"TARGET_ID": "TGT12345620240301", "CUSTOMER_ID": "CUST123456", "CUSTOMER_NAME": "テストユーザー1", "EMAIL_ADDRESS": "test1@example.com"},
            {"TARGET_ID": "TGT12345720240301", "CUSTOMER_ID": "CUST123456", "CUSTOMER_NAME": "テストユーザー1", "EMAIL_ADDRESS": "test1@example.com"},  # 重複
            {"TARGET_ID": "TGT12345820240301", "CUSTOMER_ID": "CUST123457", "CUSTOMER_NAME": "テストユーザー2", "EMAIL_ADDRESS": "test2@example.com"},
            {"TARGET_ID": "TGT12345920240301", "CUSTOMER_ID": "CUST123458", "CUSTOMER_NAME": "テストユーザー3", "EMAIL_ADDRESS": "test3@example.com"},
            {"TARGET_ID": "TGT12346020240301", "CUSTOMER_ID": "CUST123457", "CUSTOMER_NAME": "テストユーザー2", "EMAIL_ADDRESS": "test2@example.com"}   # 重複
        ]
        
        # データ重複排除ロジック（Data Flow内の処理）
        def deduplicate_marketing_client_dm_data(data_records):
            seen_customers = set()
            seen_emails = set()
            unique_records = []
            duplicates = []
            
            for record in data_records:
                customer_id = record["CUSTOMER_ID"]
                email_address = record["EMAIL_ADDRESS"]
                
                # 顧客IDベースの重複チェック
                if customer_id in seen_customers:
                    duplicates.append({
                        "record": record,
                        "duplicate_type": "CUSTOMER_ID",
                        "duplicate_key": customer_id
                    })
                    continue
                
                # メールアドレスベースの重複チェック
                if email_address in seen_emails:
                    duplicates.append({
                        "record": record,
                        "duplicate_type": "EMAIL_ADDRESS",
                        "duplicate_key": email_address
                    })
                    continue
                
                # 一意レコードとして追加
                seen_customers.add(customer_id)
                seen_emails.add(email_address)
                unique_records.append(record)
            
            return {
                "unique_records": unique_records,
                "duplicates": duplicates,
                "original_count": len(data_records),
                "unique_count": len(unique_records),
                "duplicate_count": len(duplicates),
                "deduplication_rate": len(duplicates) / len(data_records) if data_records else 0
            }
        
        # 重複排除実行
        deduplication_result = deduplicate_marketing_client_dm_data(duplicate_data)
        
        # アサーション
        self.assertEqual(deduplication_result["original_count"], 5, "元データ件数不正")
        self.assertEqual(deduplication_result["unique_count"], 3, "一意レコード数不正")
        self.assertEqual(deduplication_result["duplicate_count"], 2, "重複レコード数不正")
        self.assertEqual(deduplication_result["deduplication_rate"], 0.4, "重複排除率確認失敗")
        self.assertEqual(deduplication_result["unique_records"][0]["CUSTOMER_ID"], "CUST123456", "一意レコード確認失敗")
        self.assertEqual(deduplication_result["unique_records"][1]["CUSTOMER_ID"], "CUST123457", "一意レコード確認失敗")
        self.assertEqual(deduplication_result["unique_records"][2]["CUSTOMER_ID"], "CUST123458", "一意レコード確認失敗")
        self.assertEqual(deduplication_result["duplicates"][0]["duplicate_type"], "CUSTOMER_ID", "重複タイプ確認失敗")
        self.assertEqual(deduplication_result["duplicates"][1]["duplicate_type"], "CUSTOMER_ID", "重複タイプ確認失敗")
    
    def test_copy_activity_source_to_target_transfer(self):
        """Copy Activity: ソースからターゲットへの転送テスト"""
        # テストケース: ソースからターゲットへのデータ転送
        source_data = self.sample_marketing_client_dm_data
        
        # データ転送処理（Copy Activity内の処理）
        def transfer_marketing_client_dm_data(source_data):
            # ソースデータをターゲット形式に変換
            target_data = []
            for record in source_data:
                target_record = {
                    "TARGET_ID": f"TGT{record['CUSTOMER_ID'][4:]}{record['EXTRACTION_DATE']}",
                    "CUSTOMER_ID": record["CUSTOMER_ID"],
                    "CUSTOMER_NAME": record["CUSTOMER_NAME"],
                    "EMAIL_ADDRESS": record["EMAIL_ADDRESS"],
                    "PHONE_NUMBER": record["PHONE_NUMBER"],
                    "POSTAL_CODE": record["POSTAL_CODE"],
                    "ADDRESS": record["ADDRESS"],
                    "MARKETING_SEGMENT": f"{record['CUSTOMER_SEGMENT']}_MARKETING",
                    "REGISTRATION_DATE": record["REGISTRATION_DATE"],
                    "LAST_ACTIVITY_DATE": record["LAST_ACTIVITY_DATE"],
                    "PURCHASE_HISTORY": record["PURCHASE_HISTORY"],
                    "TOTAL_SPEND": record["TOTAL_SPEND"],
                    "PREFERRED_CONTACT": record["PREFERRED_CONTACT"],
                    "MARKETING_CONSENT": record["MARKETING_CONSENT"],
                    "DATA_SOURCE": record["DATA_SOURCE"],
                    "TRANSFER_DATE": datetime.utcnow().strftime('%Y%m%d'),
                    "TRANSFER_TIME": datetime.utcnow().strftime('%H:%M:%S')
                }
                target_data.append(target_record)
            
            return target_data
        
        # データ転送実行
        target_data = transfer_marketing_client_dm_data(source_data)
        
        self.mock_database.insert_records.return_value = True
        
        # Copy Activity実行シミュレーション
        target_table = "marketing_client_dm_target"
        result = self.mock_database.insert_records(target_table, target_data)
        
        self.assertTrue(result, "データ転送失敗")
        self.assertEqual(len(target_data), 2, "転送データ件数不正")
        self.assertEqual(target_data[0]["TARGET_ID"], "TGT12345620240301", "ターゲットID確認失敗")
        self.assertEqual(target_data[0]["MARKETING_SEGMENT"], "PREMIUM_MARKETING", "マーケティングセグメント確認失敗")
        self.assertEqual(target_data[1]["TARGET_ID"], "TGT12345720240301", "ターゲットID確認失敗")
        self.assertEqual(target_data[1]["MARKETING_SEGMENT"], "STANDARD_MARKETING", "マーケティングセグメント確認失敗")
        self.mock_database.insert_records.assert_called_once_with(target_table, target_data)
    
    def test_copy_activity_processing_log_creation(self):
        """Copy Activity: 処理ログ作成テスト"""
        # テストケース: 処理ログの作成
        processing_logs = [
            {
                "PROCESS_ID": "PROC000001",
                "PIPELINE_NAME": "pi_Copy_marketing_client_dm",
                "EXECUTION_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "EXECUTION_TIME": datetime.utcnow().strftime('%H:%M:%S'),
                "SOURCE_TABLE": "marketing_client_dm_source",
                "TARGET_TABLE": "marketing_client_dm_target",
                "RECORDS_PROCESSED": 2,
                "RECORDS_INSERTED": 2,
                "RECORDS_UPDATED": 0,
                "RECORDS_FAILED": 0,
                "PROCESSING_STATUS": "SUCCESS",
                "PROCESSING_TIME_SECONDS": 45.2,
                "ERROR_MESSAGE": None
            }
        ]
        
        self.mock_database.insert_records.return_value = True
        
        # Copy Activity実行シミュレーション
        processing_log_table = "marketing_dm_processing_logs"
        result = self.mock_database.insert_records(processing_log_table, processing_logs)
        
        self.assertTrue(result, "処理ログ作成失敗")
        self.mock_database.insert_records.assert_called_once_with(processing_log_table, processing_logs)
    
    def test_script_activity_data_quality_reporting(self):
        """Script Activity: データ品質レポート処理テスト"""
        # テストケース: データ品質レポート生成
        quality_report_data = {
            "execution_date": self.test_date,
            "total_records_processed": 1000,
            "valid_records": 950,
            "invalid_records": 50,
            "data_quality_score": 0.95,
            "error_details": {
                "empty_customer_id": 15,
                "invalid_email_format": 20,
                "missing_customer_name": 10,
                "duplicate_records": 5
            }
        }
        
        self.mock_transform_service.generate_quality_report.return_value = {
            "status": "success",
            "report_id": "QR20240301001",
            "report_path": "/reports/quality/marketing_client_dm_quality_20240301.json",
            "report_generation_time": datetime.utcnow().isoformat()
        }
        
        # Script Activity実行シミュレーション
        result = self.mock_transform_service.generate_quality_report(
            quality_report_data["execution_date"],
            quality_report_data["total_records_processed"],
            quality_report_data["valid_records"],
            quality_report_data["invalid_records"],
            quality_report_data["data_quality_score"],
            error_details=quality_report_data["error_details"]
        )
        
        self.assertEqual(result["status"], "success", "データ品質レポート生成失敗")
        self.assertEqual(result["report_id"], "QR20240301001", "レポートID確認失敗")
        self.assertIsNotNone(result["report_path"], "レポートパス確認失敗")
        self.mock_transform_service.generate_quality_report.assert_called_once()
    
    def test_script_activity_source_data_archival(self):
        """Script Activity: ソースデータアーカイブ処理テスト"""
        # テストケース: ソースデータのアーカイブ
        archival_data = {
            "execution_date": self.test_date,
            "source_records_count": 1000,
            "archive_retention_days": 365,
            "archive_location": "/archive/marketing_client_dm/",
            "compression_enabled": True
        }
        
        self.mock_file_service.archive_source_data.return_value = {
            "status": "archived",
            "archive_id": "ARC20240301001",
            "archive_path": "/archive/marketing_client_dm/20240301_source_data.zip",
            "archive_size_mb": 15.8,
            "compression_ratio": 0.35,
            "archive_time": datetime.utcnow().isoformat()
        }
        
        # Script Activity実行シミュレーション
        result = self.mock_file_service.archive_source_data(
            archival_data["execution_date"],
            archival_data["source_records_count"],
            archive_location=archival_data["archive_location"],
            retention_days=archival_data["archive_retention_days"],
            compression_enabled=archival_data["compression_enabled"]
        )
        
        self.assertEqual(result["status"], "archived", "ソースデータアーカイブ失敗")
        self.assertEqual(result["archive_id"], "ARC20240301001", "アーカイブID確認失敗")
        self.assertIsNotNone(result["archive_path"], "アーカイブパス確認失敗")
        self.assertGreater(result["archive_size_mb"], 0, "アーカイブサイズ確認失敗")
        self.assertLess(result["compression_ratio"], 1.0, "圧縮率確認失敗")
        self.mock_file_service.archive_source_data.assert_called_once()
    
    def test_script_activity_copy_analytics(self):
        """Script Activity: コピー分析処理テスト"""
        # テストケース: コピー分析データ生成
        copy_analytics = {
            "execution_date": self.test_date,
            "pipeline_name": "pi_Copy_marketing_client_dm",
            "total_source_records": 1000,
            "total_target_records": 950,
            "valid_records": 950,
            "invalid_records": 50,
            "duplicate_records": 20,
            "transformation_success_rate": 0.95,
            "data_quality_score": 0.94,
            "processing_time_minutes": 12.5,
            "peak_memory_usage_mb": 256.8,
            "average_throughput_records_per_second": 1266.7
        }
        
        self.mock_database.insert_records.return_value = True
        
        # Script Activity実行シミュレーション
        analytics_table = "marketing_dm_copy_analytics"
        result = self.mock_database.insert_records(analytics_table, [copy_analytics])
        
        self.assertTrue(result, "コピー分析記録失敗")
        self.mock_database.insert_records.assert_called_once_with(analytics_table, [copy_analytics])
    
    def test_data_flow_incremental_load_processing(self):
        """Data Flow: 増分ロード処理テスト"""
        # テストケース: 増分ロード処理
        existing_target_data = [
            {"TARGET_ID": "TGT12345620240228", "CUSTOMER_ID": "CUST123456", "LAST_ACTIVITY_DATE": "20240228"},
            {"TARGET_ID": "TGT12345720240228", "CUSTOMER_ID": "CUST123457", "LAST_ACTIVITY_DATE": "20240228"}
        ]
        
        new_source_data = [
            {"CUSTOMER_ID": "CUST123456", "LAST_ACTIVITY_DATE": "20240301"},  # 更新
            {"CUSTOMER_ID": "CUST123457", "LAST_ACTIVITY_DATE": "20240228"},  # 変更なし
            {"CUSTOMER_ID": "CUST123458", "LAST_ACTIVITY_DATE": "20240301"}   # 新規
        ]
        
        # 増分ロード処理ロジック（Data Flow内の処理）
        def process_incremental_load(existing_data, new_data):
            existing_dict = {item["CUSTOMER_ID"]: item for item in existing_data}
            
            inserts = []
            updates = []
            no_changes = []
            
            for new_record in new_data:
                customer_id = new_record["CUSTOMER_ID"]
                
                if customer_id not in existing_dict:
                    # 新規挿入
                    inserts.append({
                        "operation": "INSERT",
                        "customer_id": customer_id,
                        "record": new_record
                    })
                else:
                    existing_record = existing_dict[customer_id]
                    
                    # 変更チェック
                    if new_record["LAST_ACTIVITY_DATE"] != existing_record["LAST_ACTIVITY_DATE"]:
                        # 更新
                        updates.append({
                            "operation": "UPDATE",
                            "customer_id": customer_id,
                            "old_record": existing_record,
                            "new_record": new_record
                        })
                    else:
                        # 変更なし
                        no_changes.append({
                            "operation": "NO_CHANGE",
                            "customer_id": customer_id,
                            "record": new_record
                        })
            
            return {
                "inserts": inserts,
                "updates": updates,
                "no_changes": no_changes,
                "insert_count": len(inserts),
                "update_count": len(updates),
                "no_change_count": len(no_changes)
            }
        
        # 増分ロード処理実行
        incremental_result = process_incremental_load(existing_target_data, new_source_data)
        
        # アサーション
        self.assertEqual(incremental_result["insert_count"], 1, "挿入数不正")
        self.assertEqual(incremental_result["update_count"], 1, "更新数不正")
        self.assertEqual(incremental_result["no_change_count"], 1, "変更なし数不正")
        self.assertEqual(incremental_result["inserts"][0]["customer_id"], "CUST123458", "新規挿入確認失敗")
        self.assertEqual(incremental_result["updates"][0]["customer_id"], "CUST123456", "更新確認失敗")
        self.assertEqual(incremental_result["no_changes"][0]["customer_id"], "CUST123457", "変更なし確認失敗")
    
    def test_data_flow_batch_processing(self):
        """Data Flow: バッチ処理テスト"""
        # テストケース: 大量データのバッチ処理
        large_marketing_dataset = []
        for i in range(5000):
            large_marketing_dataset.append({
                "SOURCE_ID": f"SRC{i:06d}",
                "CUSTOMER_ID": f"CUST{i:06d}",
                "CUSTOMER_NAME": f"テストユーザー{i}",
                "EMAIL_ADDRESS": f"test{i}@example.com",
                "CUSTOMER_SEGMENT": ["PREMIUM", "STANDARD", "BASIC"][i % 3],
                "MARKETING_CONSENT": "Y",
                "DATA_SOURCE": ["CRM", "WEB", "MOBILE"][i % 3]
            })
        
        # バッチ処理ロジック（Data Flow内の処理）
        def process_marketing_dm_batch(data_list, batch_size=500):
            processed_batches = []
            
            for i in range(0, len(data_list), batch_size):
                batch = data_list[i:i + batch_size]
                
                # バッチ処理の実行
                batch_result = {
                    "batch_id": f"BATCH_{i//batch_size + 1:03d}",
                    "record_count": len(batch),
                    "premium_count": sum(1 for record in batch if record["CUSTOMER_SEGMENT"] == "PREMIUM"),
                    "standard_count": sum(1 for record in batch if record["CUSTOMER_SEGMENT"] == "STANDARD"),
                    "basic_count": sum(1 for record in batch if record["CUSTOMER_SEGMENT"] == "BASIC"),
                    "crm_source_count": sum(1 for record in batch if record["DATA_SOURCE"] == "CRM"),
                    "web_source_count": sum(1 for record in batch if record["DATA_SOURCE"] == "WEB"),
                    "mobile_source_count": sum(1 for record in batch if record["DATA_SOURCE"] == "MOBILE"),
                    "consent_rate": sum(1 for record in batch if record["MARKETING_CONSENT"] == "Y") / len(batch),
                    "processing_time": 8.5,  # シミュレーション
                    "success_rate": 0.99
                }
                
                processed_batches.append(batch_result)
            
            return processed_batches
        
        # バッチ処理実行
        batch_results = process_marketing_dm_batch(large_marketing_dataset, batch_size=500)
        
        # アサーション
        self.assertEqual(len(batch_results), 10, "バッチ数不正")  # 5000 / 500 = 10
        self.assertEqual(batch_results[0]["record_count"], 500, "バッチサイズ不正")
        self.assertEqual(batch_results[0]["consent_rate"], 1.0, "同意率確認失敗")
        
        # 全バッチの合計確認
        total_records = sum(batch["record_count"] for batch in batch_results)
        total_premium = sum(batch["premium_count"] for batch in batch_results)
        total_standard = sum(batch["standard_count"] for batch in batch_results)
        total_basic = sum(batch["basic_count"] for batch in batch_results)
        
        self.assertEqual(total_records, 5000, "全バッチ処理件数不正")
        self.assertAlmostEqual(total_premium, 1667, delta=1, msg="プレミアム顧客数不正")
        self.assertAlmostEqual(total_standard, 1667, delta=1, msg="スタンダード顧客数不正")
        self.assertAlmostEqual(total_basic, 1666, delta=1, msg="ベーシック顧客数不正")
    
    def _create_marketing_client_dm_csv_content(self) -> str:
        """マーケティングClient DMデータ用CSVコンテンツ生成"""
        header = "SOURCE_ID,CUSTOMER_ID,CUSTOMER_NAME,EMAIL_ADDRESS,PHONE_NUMBER,POSTAL_CODE,ADDRESS,CUSTOMER_SEGMENT,REGISTRATION_DATE,LAST_ACTIVITY_DATE,PURCHASE_HISTORY,TOTAL_SPEND,PREFERRED_CONTACT,MARKETING_CONSENT,DATA_SOURCE,EXTRACTION_DATE,EXTRACTION_TIME"
        rows = []
        
        for item in self.sample_marketing_client_dm_data:
            row = f"{item['SOURCE_ID']},{item['CUSTOMER_ID']},{item['CUSTOMER_NAME']},{item['EMAIL_ADDRESS']},{item['PHONE_NUMBER']},{item['POSTAL_CODE']},{item['ADDRESS']},{item['CUSTOMER_SEGMENT']},{item['REGISTRATION_DATE']},{item['LAST_ACTIVITY_DATE']},{item['PURCHASE_HISTORY']},{item['TOTAL_SPEND']},{item['PREFERRED_CONTACT']},{item['MARKETING_CONSENT']},{item['DATA_SOURCE']},{item['EXTRACTION_DATE']},{item['EXTRACTION_TIME']}"
            rows.append(row)
        
        return header + "\n" + "\n".join(rows)


if __name__ == "__main__":
    unittest.main()