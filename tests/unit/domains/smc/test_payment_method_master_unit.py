"""
pi_Send_PaymentMethodMaster パイプラインのユニットテスト

支払い方法マスタ送信パイプラインの個別コンポーネントテスト
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any


class TestPaymentMethodMasterUnit(unittest.TestCase):
    """支払い方法マスタパイプラインユニットテスト"""
    
    def setUp(self):
        """テストセットアップ"""
        self.pipeline_name = "pi_Send_PaymentMethodMaster"
        self.domain = "smc"
        
        # モックサービス
        self.mock_blob_storage = Mock()
        self.mock_database = Mock()
        self.mock_sftp_service = Mock()
        
        # テストデータ
        self.test_date = datetime.utcnow().strftime('%Y%m%d')
        self.test_file_path = f"PaymentMethodMaster/{self.test_date}/payment_method_master.csv"
        
        # 基本的な支払い方法マスタデータ
        self.sample_payment_method_data = [
            {
                "PAYMENT_METHOD_ID": "PM000001",
                "CUSTOMER_ID": "CUST123456",
                "CUSTOMER_NAME": "テストユーザー1",
                "METHOD_TYPE": "CREDIT_CARD",
                "CARD_NUMBER": "4111111111111111",
                "CARD_HOLDER_NAME": "TEST USER 1",
                "EXPIRY_DATE": "2025/12",
                "BANK_CODE": "",
                "ACCOUNT_NUMBER": "",
                "ACCOUNT_HOLDER": "",
                "STATUS": "ACTIVE",
                "REGISTRATION_DATE": "20240115"
            },
            {
                "PAYMENT_METHOD_ID": "PM000002",
                "CUSTOMER_ID": "CUST123457",
                "CUSTOMER_NAME": "テストユーザー2",
                "METHOD_TYPE": "BANK_TRANSFER",
                "CARD_NUMBER": "",
                "CARD_HOLDER_NAME": "",
                "EXPIRY_DATE": "",
                "BANK_CODE": "0001",
                "ACCOUNT_NUMBER": "1234567",
                "ACCOUNT_HOLDER": "テストユーザー2",
                "STATUS": "ACTIVE",
                "REGISTRATION_DATE": "20240120"
            }
        ]
    
    def test_lookup_activity_payment_method_extraction(self):
        """Lookup Activity: 支払い方法データ抽出テスト"""
        # テストケース: 支払い方法マスタデータの抽出
        mock_payment_methods = [
            {
                "PAYMENT_METHOD_ID": "PM000001",
                "CUSTOMER_ID": "CUST123456",
                "METHOD_TYPE": "CREDIT_CARD",
                "CARD_NUMBER": "4111111111111111",
                "EXPIRY_DATE": "2025/12",
                "STATUS": "ACTIVE"
            },
            {
                "PAYMENT_METHOD_ID": "PM000002",
                "CUSTOMER_ID": "CUST123457",
                "METHOD_TYPE": "BANK_TRANSFER",
                "BANK_CODE": "0001",
                "ACCOUNT_NUMBER": "1234567",
                "STATUS": "ACTIVE"
            }
        ]
        
        self.mock_database.query_records.return_value = mock_payment_methods
        
        # Lookup Activity実行シミュレーション
        payment_method_query = """
        SELECT PAYMENT_METHOD_ID, CUSTOMER_ID, METHOD_TYPE, CARD_NUMBER, 
               EXPIRY_DATE, BANK_CODE, ACCOUNT_NUMBER, STATUS
        FROM payment_method_master
        WHERE STATUS = 'ACTIVE'
        """
        
        result = self.mock_database.query_records("payment_method_master", payment_method_query)
        
        self.assertEqual(len(result), 2, "支払い方法データ抽出件数不正")
        self.assertEqual(result[0]["PAYMENT_METHOD_ID"], "PM000001", "支払い方法ID確認失敗")
        self.assertEqual(result[0]["METHOD_TYPE"], "CREDIT_CARD", "支払い方法タイプ確認失敗")
        self.assertEqual(result[0]["STATUS"], "ACTIVE", "ステータス確認失敗")
    
    def test_lookup_activity_customer_payment_preferences(self):
        """Lookup Activity: 顧客支払い設定取得テスト"""
        # テストケース: 顧客の支払い設定情報取得
        mock_payment_preferences = [
            {
                "CUSTOMER_ID": "CUST123456",
                "PREFERRED_METHOD": "CREDIT_CARD",
                "AUTO_PAYMENT": "Y",
                "PAYMENT_NOTIFICATION": "Y",
                "BILLING_CYCLE": "MONTHLY"
            },
            {
                "CUSTOMER_ID": "CUST123457",
                "PREFERRED_METHOD": "BANK_TRANSFER",
                "AUTO_PAYMENT": "Y",
                "PAYMENT_NOTIFICATION": "N",
                "BILLING_CYCLE": "MONTHLY"
            }
        ]
        
        self.mock_database.query_records.return_value = mock_payment_preferences
        
        # Lookup Activity実行シミュレーション
        preferences_query = """
        SELECT CUSTOMER_ID, PREFERRED_METHOD, AUTO_PAYMENT, PAYMENT_NOTIFICATION, BILLING_CYCLE
        FROM customer_payment_preferences
        WHERE CUSTOMER_ID IN ('CUST123456', 'CUST123457')
        """
        
        result = self.mock_database.query_records("customer_payment_preferences", preferences_query)
        
        self.assertEqual(len(result), 2, "顧客支払い設定取得件数不正")
        self.assertEqual(result[0]["PREFERRED_METHOD"], "CREDIT_CARD", "優先支払い方法確認失敗")
        self.assertEqual(result[0]["AUTO_PAYMENT"], "Y", "自動支払い設定確認失敗")
    
    def test_data_flow_payment_method_validation(self):
        """Data Flow: 支払い方法データ検証テスト"""
        # テストケース: 支払い方法データの検証
        test_payment_methods = [
            {"METHOD_TYPE": "CREDIT_CARD", "CARD_NUMBER": "4111111111111111", "EXPIRY_DATE": "2025/12"},
            {"METHOD_TYPE": "CREDIT_CARD", "CARD_NUMBER": "1234", "EXPIRY_DATE": "2025/12"},  # 不正: カード番号短い
            {"METHOD_TYPE": "BANK_TRANSFER", "BANK_CODE": "0001", "ACCOUNT_NUMBER": "1234567"},
            {"METHOD_TYPE": "BANK_TRANSFER", "BANK_CODE": "", "ACCOUNT_NUMBER": "1234567"}  # 不正: 銀行コード空
        ]
        
        # 支払い方法検証ロジック（Data Flow内の処理）
        def validate_payment_method(payment_method):
            errors = []
            method_type = payment_method.get("METHOD_TYPE", "")
            
            if method_type == "CREDIT_CARD":
                # クレジットカード検証
                card_number = payment_method.get("CARD_NUMBER", "")
                expiry_date = payment_method.get("EXPIRY_DATE", "")
                
                if not card_number or len(card_number) < 13:
                    errors.append("クレジットカード番号が不正です")
                
                if not expiry_date or "/" not in expiry_date:
                    errors.append("有効期限形式が不正です（YYYY/MM）")
                
            elif method_type == "BANK_TRANSFER":
                # 銀行振込検証
                bank_code = payment_method.get("BANK_CODE", "")
                account_number = payment_method.get("ACCOUNT_NUMBER", "")
                
                if not bank_code or len(bank_code) != 4:
                    errors.append("銀行コードが不正です（4桁）")
                
                if not account_number or len(account_number) < 6:
                    errors.append("口座番号が不正です")
            
            return errors
        
        # 検証実行
        validation_results = []
        for method in test_payment_methods:
            errors = validate_payment_method(method)
            validation_results.append({
                "method": method,
                "errors": errors,
                "is_valid": len(errors) == 0
            })
        
        # アサーション
        self.assertEqual(len(validation_results), 4, "検証結果数不正")
        self.assertTrue(validation_results[0]["is_valid"], "正常クレジットカードが不正判定")
        self.assertFalse(validation_results[1]["is_valid"], "不正クレジットカードが正常判定")
        self.assertTrue(validation_results[2]["is_valid"], "正常銀行振込が不正判定")
        self.assertFalse(validation_results[3]["is_valid"], "不正銀行振込が正常判定")
    
    def test_data_flow_card_number_masking(self):
        """Data Flow: カード番号マスキング処理テスト"""
        # テストケース: クレジットカード番号のマスキング
        card_scenarios = [
            {"CARD_NUMBER": "4111111111111111", "EXPECTED_MASKED": "4111********1111"},
            {"CARD_NUMBER": "5555555555554444", "EXPECTED_MASKED": "5555********4444"},
            {"CARD_NUMBER": "378282246310005", "EXPECTED_MASKED": "3782*******0005"}  # AMEX
        ]
        
        # カード番号マスキングロジック（Data Flow内の処理）
        def mask_card_number(card_number):
            if not card_number or len(card_number) < 12:
                return card_number
            
            # 最初の4桁と最後の4桁を保持、中間をマスク
            if len(card_number) >= 16:
                return card_number[:4] + "*" * 8 + card_number[-4:]
            else:
                # AMEXなど15桁の場合
                return card_number[:4] + "*" * 7 + card_number[-4:]
        
        # マスキング実行
        for scenario in card_scenarios:
            masked = mask_card_number(scenario["CARD_NUMBER"])
            self.assertEqual(masked, scenario["EXPECTED_MASKED"], 
                           f"カード番号マスキング失敗: {scenario['CARD_NUMBER']}")
    
    def test_data_flow_expiry_date_validation(self):
        """Data Flow: 有効期限検証テスト"""
        # テストケース: クレジットカード有効期限の検証
        expiry_scenarios = [
            {"EXPIRY_DATE": "2025/12", "CURRENT_DATE": "2024/03", "EXPECTED_VALID": True},
            {"EXPIRY_DATE": "2024/02", "CURRENT_DATE": "2024/03", "EXPECTED_VALID": False},  # 期限切れ
            {"EXPIRY_DATE": "2024/03", "CURRENT_DATE": "2024/03", "EXPECTED_VALID": True},  # 当月
            {"EXPIRY_DATE": "invalid", "CURRENT_DATE": "2024/03", "EXPECTED_VALID": False}  # 形式不正
        ]
        
        # 有効期限検証ロジック（Data Flow内の処理）
        def validate_expiry_date(expiry_date, current_date):
            try:
                if "/" not in expiry_date:
                    return False
                
                expiry_year, expiry_month = expiry_date.split("/")
                current_year, current_month = current_date.split("/")
                
                expiry_year = int(expiry_year)
                expiry_month = int(expiry_month)
                current_year = int(current_year)
                current_month = int(current_month)
                
                # 年の比較
                if expiry_year > current_year:
                    return True
                elif expiry_year < current_year:
                    return False
                else:
                    # 同年の場合、月の比較
                    return expiry_month >= current_month
                    
            except (ValueError, IndexError):
                return False
        
        # 検証実行
        for scenario in expiry_scenarios:
            is_valid = validate_expiry_date(scenario["EXPIRY_DATE"], scenario["CURRENT_DATE"])
            self.assertEqual(is_valid, scenario["EXPECTED_VALID"], 
                           f"有効期限検証失敗: {scenario['EXPIRY_DATE']}")
    
    def test_data_flow_payment_method_categorization(self):
        """Data Flow: 支払い方法分類処理テスト"""
        # テストケース: 支払い方法の分類
        method_data = [
            {"METHOD_TYPE": "CREDIT_CARD", "CARD_NUMBER": "4111111111111111"},  # VISA
            {"METHOD_TYPE": "CREDIT_CARD", "CARD_NUMBER": "5555555555554444"},  # MasterCard
            {"METHOD_TYPE": "CREDIT_CARD", "CARD_NUMBER": "378282246310005"},   # AMEX
            {"METHOD_TYPE": "BANK_TRANSFER", "BANK_CODE": "0001"},              # 銀行振込
            {"METHOD_TYPE": "CONVENIENCE_STORE", "PAYMENT_CODE": "12345"}       # コンビニ支払い
        ]
        
        # 支払い方法分類ロジック（Data Flow内の処理）
        def categorize_payment_method(method):
            method_type = method.get("METHOD_TYPE", "")
            
            if method_type == "CREDIT_CARD":
                card_number = method.get("CARD_NUMBER", "")
                if card_number.startswith("4"):
                    return "VISA"
                elif card_number.startswith("5"):
                    return "MASTERCARD"
                elif card_number.startswith("3"):
                    return "AMEX"
                else:
                    return "OTHER_CARD"
            elif method_type == "BANK_TRANSFER":
                return "BANK_TRANSFER"
            elif method_type == "CONVENIENCE_STORE":
                return "CONVENIENCE_STORE"
            else:
                return "UNKNOWN"
        
        # 分類実行
        expected_categories = ["VISA", "MASTERCARD", "AMEX", "BANK_TRANSFER", "CONVENIENCE_STORE"]
        
        for i, method in enumerate(method_data):
            category = categorize_payment_method(method)
            self.assertEqual(category, expected_categories[i], 
                           f"支払い方法分類失敗: {method}")
    
    def test_copy_activity_payment_method_file_generation(self):
        """Copy Activity: 支払い方法ファイル生成テスト"""
        # テストケース: 支払い方法マスタファイルの生成
        payment_methods = self.sample_payment_method_data
        
        # CSV生成（Copy Activity内の処理）
        def generate_payment_method_csv(payment_methods):
            if not payment_methods:
                return ""
            
            header = ",".join(payment_methods[0].keys())
            rows = []
            
            for method in payment_methods:
                # カード番号マスキング
                if method["METHOD_TYPE"] == "CREDIT_CARD" and method["CARD_NUMBER"]:
                    method_copy = method.copy()
                    card_number = method_copy["CARD_NUMBER"]
                    if len(card_number) >= 12:
                        method_copy["CARD_NUMBER"] = card_number[:4] + "*" * 8 + card_number[-4:]
                    method = method_copy
                
                row = ",".join(str(method[key]) for key in method.keys())
                rows.append(row)
            
            return header + "\n" + "\n".join(rows)
        
        # CSV生成実行
        csv_content = generate_payment_method_csv(payment_methods)
        
        # アサーション
        self.assertIn("PAYMENT_METHOD_ID,CUSTOMER_ID", csv_content, "CSVヘッダー確認失敗")
        self.assertIn("PM000001,CUST123456", csv_content, "データ行確認失敗")
        self.assertIn("4111********1111", csv_content, "カード番号マスキング確認失敗")
        self.assertIn("CREDIT_CARD", csv_content, "支払い方法タイプ確認失敗")
    
    def test_copy_activity_sftp_upload(self):
        """Copy Activity: SFTP アップロード処理テスト"""
        # テストケース: SFTPサーバーへのマスタファイルアップロード
        master_csv_content = self._create_payment_method_csv_content()
        sftp_path = f"/Import/PaymentMethodMaster/payment_method_master_{self.test_date}.csv"
        
        self.mock_sftp_service.upload_file.return_value = {
            "status": "success",
            "file_path": sftp_path,
            "file_size": len(master_csv_content),
            "upload_time": datetime.utcnow().isoformat()
        }
        
        # Copy Activity実行シミュレーション
        result = self.mock_sftp_service.upload_file(
            sftp_path,
            master_csv_content,
            encoding="utf-8"
        )
        
        self.assertEqual(result["status"], "success", "SFTPアップロード失敗")
        self.assertEqual(result["file_path"], sftp_path, "SFTPファイルパス確認失敗")
        self.assertGreater(result["file_size"], 0, "SFTPファイルサイズ確認失敗")
        self.mock_sftp_service.upload_file.assert_called_once()
    
    def test_script_activity_payment_method_sync(self):
        """Script Activity: 支払い方法同期処理テスト"""
        # テストケース: 支払い方法データの同期処理
        sync_data = {
            "sync_date": self.test_date,
            "total_methods": 150,
            "credit_cards": 90,
            "bank_transfers": 50,
            "convenience_stores": 10,
            "active_methods": 140,
            "inactive_methods": 10,
            "sync_status": "SUCCESS"
        }
        
        self.mock_database.insert_records.return_value = True
        
        # Script Activity実行シミュレーション
        sync_table = "payment_method_sync_log"
        result = self.mock_database.insert_records(sync_table, [sync_data])
        
        self.assertTrue(result, "支払い方法同期記録失敗")
        self.mock_database.insert_records.assert_called_once_with(sync_table, [sync_data])
    
    def test_script_activity_expiry_notification(self):
        """Script Activity: 有効期限通知処理テスト"""
        # テストケース: クレジットカード有効期限通知
        expiry_notifications = [
            {
                "CUSTOMER_ID": "CUST123456",
                "PAYMENT_METHOD_ID": "PM000001",
                "EXPIRY_DATE": "2024/05",
                "NOTIFICATION_TYPE": "EXPIRY_WARNING",
                "NOTIFICATION_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "DAYS_UNTIL_EXPIRY": 30
            }
        ]
        
        self.mock_database.insert_records.return_value = True
        
        # Script Activity実行シミュレーション
        notification_table = "payment_method_expiry_notifications"
        result = self.mock_database.insert_records(notification_table, expiry_notifications)
        
        self.assertTrue(result, "有効期限通知処理失敗")
        self.mock_database.insert_records.assert_called_once_with(notification_table, expiry_notifications)
    
    def test_lookup_activity_payment_method_statistics(self):
        """Lookup Activity: 支払い方法統計取得テスト"""
        # テストケース: 支払い方法の統計情報取得
        payment_statistics = [
            {
                "METHOD_TYPE": "CREDIT_CARD",
                "TOTAL_COUNT": 90,
                "ACTIVE_COUNT": 85,
                "INACTIVE_COUNT": 5,
                "USAGE_RATE": 0.85
            },
            {
                "METHOD_TYPE": "BANK_TRANSFER",
                "TOTAL_COUNT": 50,
                "ACTIVE_COUNT": 48,
                "INACTIVE_COUNT": 2,
                "USAGE_RATE": 0.48
            }
        ]
        
        self.mock_database.query_records.return_value = payment_statistics
        
        # Lookup Activity実行シミュレーション
        statistics_query = """
        SELECT METHOD_TYPE, 
               COUNT(*) as TOTAL_COUNT,
               SUM(CASE WHEN STATUS = 'ACTIVE' THEN 1 ELSE 0 END) as ACTIVE_COUNT,
               SUM(CASE WHEN STATUS = 'INACTIVE' THEN 1 ELSE 0 END) as INACTIVE_COUNT,
               CAST(SUM(CASE WHEN STATUS = 'ACTIVE' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) as USAGE_RATE
        FROM payment_method_master
        GROUP BY METHOD_TYPE
        """
        
        result = self.mock_database.query_records("payment_method_master", statistics_query)
        
        self.assertEqual(len(result), 2, "支払い方法統計取得件数不正")
        self.assertEqual(result[0]["METHOD_TYPE"], "CREDIT_CARD", "支払い方法タイプ確認失敗")
        self.assertEqual(result[0]["TOTAL_COUNT"], 90, "総件数確認失敗")
        self.assertEqual(result[0]["ACTIVE_COUNT"], 85, "アクティブ件数確認失敗")
    
    def test_data_flow_payment_method_deduplication(self):
        """Data Flow: 支払い方法重複除去テスト"""
        # テストケース: 重複する支払い方法の除去
        duplicate_methods = [
            {"CUSTOMER_ID": "CUST123456", "METHOD_TYPE": "CREDIT_CARD", "CARD_NUMBER": "4111111111111111", "REGISTRATION_DATE": "20240115"},
            {"CUSTOMER_ID": "CUST123456", "METHOD_TYPE": "CREDIT_CARD", "CARD_NUMBER": "4111111111111111", "REGISTRATION_DATE": "20240120"},  # 重複
            {"CUSTOMER_ID": "CUST123456", "METHOD_TYPE": "BANK_TRANSFER", "BANK_CODE": "0001", "ACCOUNT_NUMBER": "1234567", "REGISTRATION_DATE": "20240118"},
            {"CUSTOMER_ID": "CUST123457", "METHOD_TYPE": "CREDIT_CARD", "CARD_NUMBER": "5555555555554444", "REGISTRATION_DATE": "20240116"}
        ]
        
        # 重複除去ロジック（Data Flow内の処理）
        def deduplicate_payment_methods(methods):
            seen = set()
            unique_methods = []
            
            for method in methods:
                # 重複キーの生成
                if method["METHOD_TYPE"] == "CREDIT_CARD":
                    dedup_key = (method["CUSTOMER_ID"], method["METHOD_TYPE"], method["CARD_NUMBER"])
                elif method["METHOD_TYPE"] == "BANK_TRANSFER":
                    dedup_key = (method["CUSTOMER_ID"], method["METHOD_TYPE"], method["BANK_CODE"], method["ACCOUNT_NUMBER"])
                else:
                    dedup_key = (method["CUSTOMER_ID"], method["METHOD_TYPE"])
                
                if dedup_key not in seen:
                    seen.add(dedup_key)
                    unique_methods.append(method)
                else:
                    # 重複の場合、より新しい登録日を保持
                    for i, existing in enumerate(unique_methods):
                        if (existing["CUSTOMER_ID"] == method["CUSTOMER_ID"] and 
                            existing["METHOD_TYPE"] == method["METHOD_TYPE"]):
                            if method["REGISTRATION_DATE"] > existing["REGISTRATION_DATE"]:
                                unique_methods[i] = method
                            break
            
            return unique_methods
        
        # 重複除去実行
        unique_methods = deduplicate_payment_methods(duplicate_methods)
        
        # アサーション
        self.assertEqual(len(unique_methods), 3, "重複除去後の件数不正")
        
        # 重複除去されたクレジットカードの確認
        credit_cards = [m for m in unique_methods if m["METHOD_TYPE"] == "CREDIT_CARD" and m["CUSTOMER_ID"] == "CUST123456"]
        self.assertEqual(len(credit_cards), 1, "クレジットカード重複除去失敗")
        self.assertEqual(credit_cards[0]["REGISTRATION_DATE"], "20240120", "より新しい登録日が保持されていない")
    
    def _create_payment_method_csv_content(self) -> str:
        """支払い方法マスタデータ用CSVコンテンツ生成"""
        header = "PAYMENT_METHOD_ID,CUSTOMER_ID,CUSTOMER_NAME,METHOD_TYPE,CARD_NUMBER,CARD_HOLDER_NAME,EXPIRY_DATE,BANK_CODE,ACCOUNT_NUMBER,ACCOUNT_HOLDER,STATUS,REGISTRATION_DATE"
        rows = []
        
        for item in self.sample_payment_method_data:
            # カード番号マスキング
            card_number = item["CARD_NUMBER"]
            if card_number and len(card_number) >= 12:
                card_number = card_number[:4] + "********" + card_number[-4:]
            
            row = f"{item['PAYMENT_METHOD_ID']},{item['CUSTOMER_ID']},{item['CUSTOMER_NAME']},{item['METHOD_TYPE']},{card_number},{item['CARD_HOLDER_NAME']},{item['EXPIRY_DATE']},{item['BANK_CODE']},{item['ACCOUNT_NUMBER']},{item['ACCOUNT_HOLDER']},{item['STATUS']},{item['REGISTRATION_DATE']}"
            rows.append(row)
        
        return header + "\n" + "\n".join(rows)


if __name__ == "__main__":
    unittest.main()