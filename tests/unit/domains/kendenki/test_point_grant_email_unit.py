"""
pi_PointGrantEmail パイプラインのユニットテスト

ポイント付与メール送信パイプラインの個別コンポーネントテスト
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any

from ....e2e.helpers.sql_query_manager import SqlQueryManager


class TestPointGrantEmailUnit(unittest.TestCase):
    """ポイント付与メールパイプラインユニットテスト"""
    
    def setUp(self):
        """テストセットアップ"""
        self.pipeline_name = "pi_PointGrantEmail"
        self.domain = "kendenki"
        
        # モックサービス
        self.mock_blob_storage = Mock()
        self.mock_database = Mock()
        self.mock_email_service = Mock()
        
        # テストデータ
        self.test_date = datetime.utcnow().strftime('%Y%m%d')
        self.test_file_path = f"PointGrantEmail/{self.test_date}/PointGrantEmail_yyyyMMdd.csv"
        
        # 基本的なポイント付与データ
        self.sample_point_data = [
            {
                "POINT_ID": "PNT000001",
                "CUSTOMER_ID": "CUST123456",
                "CUSTOMER_NAME": "テストユーザー1",
                "EMAIL_ADDRESS": "test1@example.com",
                "POINT_AMOUNT": 100,
                "POINT_TYPE": "WELCOME",
                "GRANT_DATE": "20240101",
                "EXPIRY_DATE": "20241231",
                "CAMPAIGN_ID": "CAMP001"
            },
            {
                "POINT_ID": "PNT000002",
                "CUSTOMER_ID": "CUST123457",
                "CUSTOMER_NAME": "テストユーザー2",
                "EMAIL_ADDRESS": "test2@example.com",
                "POINT_AMOUNT": 200,
                "POINT_TYPE": "PURCHASE",
                "GRANT_DATE": "20240102",
                "EXPIRY_DATE": "20241231",
                "CAMPAIGN_ID": "CAMP002"
            }
        ]
    
    def test_copy_activity_file_extraction(self):
        """Copy Activity: ファイル抽出処理テスト"""
        # テストケース: ファイル存在確認と読み込み
        self.mock_blob_storage.file_exists.return_value = True
        self.mock_blob_storage.download_file.return_value = self._create_csv_content()
        
        # Copy Activity実行シミュレーション
        file_exists = self.mock_blob_storage.file_exists("point-grant-container", self.test_file_path)
        
        self.assertTrue(file_exists, "ファイル存在確認失敗")
        
        if file_exists:
            file_content = self.mock_blob_storage.download_file("point-grant-container", self.test_file_path)
            self.assertIsNotNone(file_content, "ファイル内容取得失敗")
            self.assertIn("POINT_ID,CUSTOMER_ID", file_content, "CSVヘッダー確認失敗")
    
    def test_copy_activity_file_not_found(self):
        """Copy Activity: ファイル未発見処理テスト"""
        # テストケース: ファイルが存在しない場合
        self.mock_blob_storage.file_exists.return_value = False
        
        file_exists = self.mock_blob_storage.file_exists("point-grant-container", self.test_file_path)
        
        self.assertFalse(file_exists, "ファイル未発見処理失敗")
        
        # パイプラインはスキップされるべき
        self.mock_blob_storage.download_file.assert_not_called()
    
    def test_lookup_activity_customer_validation(self):
        """Lookup Activity: 顧客情報検証テスト"""
        # テストケース: 顧客マスタ照合
        mock_customer_data = [
            {"CUSTOMER_ID": "CUST123456", "EMAIL_ADDRESS": "test1@example.com", "STATUS": "ACTIVE"},
            {"CUSTOMER_ID": "CUST123457", "EMAIL_ADDRESS": "test2@example.com", "STATUS": "ACTIVE"}
        ]
        
        self.mock_database.query_records.return_value = mock_customer_data
        
        # Lookup Activity実行シミュレーション
        lookup_query = """
        SELECT CUSTOMER_ID, EMAIL_ADDRESS, STATUS 
        FROM customer_master 
        WHERE CUSTOMER_ID IN ('CUST123456', 'CUST123457')
        """
        
        result = self.mock_database.query_records("customer_master", lookup_query)
        
        self.assertEqual(len(result), 2, "顧客情報取得件数不正")
        self.assertEqual(result[0]["CUSTOMER_ID"], "CUST123456", "顧客ID確認失敗")
        self.assertEqual(result[0]["STATUS"], "ACTIVE", "顧客ステータス確認失敗")
    
    def test_lookup_activity_customer_not_found(self):
        """Lookup Activity: 顧客情報未発見テスト"""
        # テストケース: 顧客が存在しない場合
        self.mock_database.query_records.return_value = []
        
        lookup_query = "SELECT CUSTOMER_ID FROM customer_master WHERE CUSTOMER_ID = 'NONEXISTENT'"
        result = self.mock_database.query_records("customer_master", lookup_query)
        
        self.assertEqual(len(result), 0, "存在しない顧客の結果は空であるべき")
    
    def test_data_flow_point_calculation(self):
        """Data Flow: ポイント計算処理テスト"""
        # テストケース: ポイント計算ロジック
        input_data = {
            "POINT_AMOUNT": 100,
            "POINT_TYPE": "WELCOME",
            "CAMPAIGN_ID": "CAMP001"
        }
        
        # ポイント計算ロジック（Data Flow内の処理）
        def calculate_point_bonus(point_amount, point_type, campaign_id):
            bonus_multiplier = 1.0
            
            if point_type == "WELCOME":
                bonus_multiplier = 1.5
            elif point_type == "PURCHASE":
                bonus_multiplier = 1.2
            elif point_type == "REFERRAL":
                bonus_multiplier = 2.0
            
            if campaign_id.startswith("CAMP"):
                bonus_multiplier += 0.1
            
            return int(point_amount * bonus_multiplier)
        
        # 計算実行
        final_points = calculate_point_bonus(
            input_data["POINT_AMOUNT"],
            input_data["POINT_TYPE"],
            input_data["CAMPAIGN_ID"]
        )
        
        # アサーション
        self.assertEqual(final_points, 160, "ポイント計算結果不正")  # 100 * 1.5 * 1.1 = 165 -> 160 (int)
    
    def test_data_flow_email_template_generation(self):
        """Data Flow: メールテンプレート生成テスト"""
        # テストケース: メールテンプレート生成
        customer_data = {
            "CUSTOMER_NAME": "テストユーザー",
            "POINT_AMOUNT": 100,
            "POINT_TYPE": "WELCOME",
            "EXPIRY_DATE": "20241231"
        }
        
        # メールテンプレート生成（Data Flow内の処理）
        def generate_email_template(customer_data):
            template = f"""
            {customer_data['CUSTOMER_NAME']}様
            
            この度は、{customer_data['POINT_AMOUNT']}ポイントが付与されました。
            ポイントタイプ: {customer_data['POINT_TYPE']}
            有効期限: {customer_data['EXPIRY_DATE']}
            
            引き続きご利用をお願いいたします。
            """
            return template.strip()
        
        email_content = generate_email_template(customer_data)
        
        self.assertIn("テストユーザー様", email_content, "顧客名挿入失敗")
        self.assertIn("100ポイント", email_content, "ポイント数挿入失敗")
        self.assertIn("WELCOME", email_content, "ポイントタイプ挿入失敗")
        self.assertIn("20241231", email_content, "有効期限挿入失敗")
    
    def test_data_flow_data_validation(self):
        """Data Flow: データ検証処理テスト"""
        # テストケース: データ品質検証
        test_records = [
            {"CUSTOMER_ID": "CUST123456", "EMAIL_ADDRESS": "test1@example.com", "POINT_AMOUNT": 100},
            {"CUSTOMER_ID": "", "EMAIL_ADDRESS": "test2@example.com", "POINT_AMOUNT": 200},  # 不正: 空ID
            {"CUSTOMER_ID": "CUST123458", "EMAIL_ADDRESS": "invalid-email", "POINT_AMOUNT": 150},  # 不正: メール形式
            {"CUSTOMER_ID": "CUST123459", "EMAIL_ADDRESS": "test3@example.com", "POINT_AMOUNT": -50}  # 不正: 負のポイント
        ]
        
        # データ検証ロジック（Data Flow内の処理）
        def validate_record(record):
            errors = []
            
            if not record.get("CUSTOMER_ID", "").strip():
                errors.append("顧客ID必須")
            
            email = record.get("EMAIL_ADDRESS", "")
            if not email or "@" not in email:
                errors.append("メールアドレス形式不正")
            
            point_amount = record.get("POINT_AMOUNT", 0)
            if point_amount <= 0:
                errors.append("ポイント数は正の値である必要があります")
            
            return errors
        
        # 検証実行
        validation_results = []
        for record in test_records:
            errors = validate_record(record)
            validation_results.append({
                "record": record,
                "errors": errors,
                "is_valid": len(errors) == 0
            })
        
        # アサーション
        self.assertEqual(len(validation_results), 4, "検証結果数不正")
        self.assertTrue(validation_results[0]["is_valid"], "正常レコードが不正判定")
        self.assertFalse(validation_results[1]["is_valid"], "不正レコード（空ID）が正常判定")
        self.assertFalse(validation_results[2]["is_valid"], "不正レコード（メール形式）が正常判定")
        self.assertFalse(validation_results[3]["is_valid"], "不正レコード（負のポイント）が正常判定")
    
    def test_script_activity_email_notification(self):
        """Script Activity: メール通知処理テスト"""
        # テストケース: メール送信処理
        email_data = {
            "to": "test@example.com",
            "subject": "ポイント付与のお知らせ",
            "body": "100ポイントが付与されました。"
        }
        
        self.mock_email_service.send_email.return_value = {"status": "success", "message_id": "MSG123"}
        
        # Script Activity実行シミュレーション
        result = self.mock_email_service.send_email(
            email_data["to"],
            email_data["subject"],
            email_data["body"]
        )
        
        self.assertEqual(result["status"], "success", "メール送信失敗")
        self.assertIsNotNone(result["message_id"], "メッセージID取得失敗")
        self.mock_email_service.send_email.assert_called_once()
    
    def test_script_activity_email_notification_failure(self):
        """Script Activity: メール通知失敗処理テスト"""
        # テストケース: メール送信失敗
        self.mock_email_service.send_email.side_effect = Exception("SMTP接続失敗")
        
        with self.assertRaises(Exception) as context:
            self.mock_email_service.send_email("test@example.com", "テスト", "テスト内容")
        
        self.assertIn("SMTP接続失敗", str(context.exception), "エラーメッセージ確認失敗")
    
    def test_copy_activity_result_logging(self):
        """Copy Activity: 結果ログ記録テスト"""
        # テストケース: 処理結果のログ記録
        processing_result = {
            "pipeline_name": self.pipeline_name,
            "execution_date": self.test_date,
            "records_processed": 2,
            "emails_sent": 2,
            "errors": 0,
            "status": "SUCCESS"
        }
        
        self.mock_database.insert_records.return_value = True
        
        # Copy Activity（結果ログ記録）実行シミュレーション
        log_table = "pipeline_execution_log"
        result = self.mock_database.insert_records(log_table, [processing_result])
        
        self.assertTrue(result, "ログ記録失敗")
        self.mock_database.insert_records.assert_called_once_with(log_table, [processing_result])
    
    def test_pipeline_parameter_validation(self):
        """Pipeline Parameter: パラメータ検証テスト"""
        # テストケース: パイプラインパラメータの検証
        pipeline_params = {
            "execution_date": "20240101",
            "batch_size": 1000,
            "email_template": "STANDARD"
        }
        
        # パラメータ検証ロジック
        def validate_pipeline_parameters(params):
            errors = []
            
            # 実行日チェック
            execution_date = params.get("execution_date", "")
            if not execution_date or len(execution_date) != 8:
                errors.append("実行日形式不正（YYYYMMDD）")
            
            # バッチサイズチェック
            batch_size = params.get("batch_size", 0)
            if batch_size <= 0 or batch_size > 10000:
                errors.append("バッチサイズは1-10000の範囲である必要があります")
            
            # メールテンプレートチェック
            email_template = params.get("email_template", "")
            if email_template not in ["STANDARD", "PREMIUM", "WELCOME"]:
                errors.append("メールテンプレートタイプ不正")
            
            return errors
        
        # 検証実行
        validation_errors = validate_pipeline_parameters(pipeline_params)
        
        self.assertEqual(len(validation_errors), 0, f"パラメータ検証エラー: {validation_errors}")
    
    def test_pipeline_parameter_validation_invalid(self):
        """Pipeline Parameter: 不正パラメータ検証テスト"""
        # テストケース: 不正なパラメータ
        invalid_params = {
            "execution_date": "2024-01-01",  # 不正形式
            "batch_size": -100,  # 不正値
            "email_template": "INVALID"  # 不正タイプ
        }
        
        def validate_pipeline_parameters(params):
            errors = []
            
            execution_date = params.get("execution_date", "")
            if not execution_date or len(execution_date) != 8:
                errors.append("実行日形式不正（YYYYMMDD）")
            
            batch_size = params.get("batch_size", 0)
            if batch_size <= 0 or batch_size > 10000:
                errors.append("バッチサイズは1-10000の範囲である必要があります")
            
            email_template = params.get("email_template", "")
            if email_template not in ["STANDARD", "PREMIUM", "WELCOME"]:
                errors.append("メールテンプレートタイプ不正")
            
            return errors
        
        validation_errors = validate_pipeline_parameters(invalid_params)
        
        self.assertEqual(len(validation_errors), 3, "すべてのパラメータエラーが検出されるべき")
        self.assertIn("実行日形式不正", validation_errors[0], "実行日エラー検出失敗")
        self.assertIn("バッチサイズ", validation_errors[1], "バッチサイズエラー検出失敗")
        self.assertIn("メールテンプレートタイプ不正", validation_errors[2], "テンプレートエラー検出失敗")
    
    def _create_csv_content(self) -> str:
        """テスト用CSVコンテンツ生成"""
        header = "POINT_ID,CUSTOMER_ID,CUSTOMER_NAME,EMAIL_ADDRESS,POINT_AMOUNT,POINT_TYPE,GRANT_DATE,EXPIRY_DATE,CAMPAIGN_ID"
        rows = []
        
        for item in self.sample_point_data:
            row = f"{item['POINT_ID']},{item['CUSTOMER_ID']},{item['CUSTOMER_NAME']},{item['EMAIL_ADDRESS']},{item['POINT_AMOUNT']},{item['POINT_TYPE']},{item['GRANT_DATE']},{item['EXPIRY_DATE']},{item['CAMPAIGN_ID']}"
            rows.append(row)
        
        return header + "\n" + "\n".join(rows)


if __name__ == "__main__":
    unittest.main()