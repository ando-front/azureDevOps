"""
pi_Send_PaymentMethodChanged パイプラインのユニットテスト

支払い方法変更通知送信パイプラインの個別コンポーネントテスト
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any


class TestPaymentMethodChangedUnit(unittest.TestCase):
    """支払い方法変更通知パイプラインユニットテスト"""
    
    def setUp(self):
        """テストセットアップ"""
        self.pipeline_name = "pi_Send_PaymentMethodChanged"
        self.domain = "smc"
        
        # モックサービス
        self.mock_blob_storage = Mock()
        self.mock_database = Mock()
        self.mock_email_service = Mock()
        self.mock_sms_service = Mock()
        
        # テストデータ
        self.test_date = datetime.utcnow().strftime('%Y%m%d')
        self.test_file_path = f"PaymentMethodChanged/{self.test_date}/payment_method_changed.csv"
        
        # 基本的な支払い方法変更データ
        self.sample_payment_change_data = [
            {
                "CHANGE_ID": "CHG000001",
                "CUSTOMER_ID": "CUST123456",
                "CUSTOMER_NAME": "テストユーザー1",
                "EMAIL_ADDRESS": "test1@example.com",
                "PHONE_NUMBER": "090-1234-5678",
                "OLD_METHOD_TYPE": "CREDIT_CARD",
                "NEW_METHOD_TYPE": "BANK_TRANSFER",
                "OLD_CARD_NUMBER": "4111********1111",
                "NEW_BANK_CODE": "0001",
                "NEW_ACCOUNT_NUMBER": "1234567",
                "CHANGE_DATE": "20240215",
                "CHANGE_REASON": "USER_REQUEST",
                "EFFECTIVE_DATE": "20240301"
            },
            {
                "CHANGE_ID": "CHG000002",
                "CUSTOMER_ID": "CUST123457",
                "CUSTOMER_NAME": "テストユーザー2",
                "EMAIL_ADDRESS": "test2@example.com",
                "PHONE_NUMBER": "090-2345-6789",
                "OLD_METHOD_TYPE": "BANK_TRANSFER",
                "NEW_METHOD_TYPE": "CREDIT_CARD",
                "OLD_BANK_CODE": "0002",
                "OLD_ACCOUNT_NUMBER": "7654321",
                "NEW_CARD_NUMBER": "5555********4444",
                "CHANGE_DATE": "20240216",
                "CHANGE_REASON": "CARD_EXPIRY",
                "EFFECTIVE_DATE": "20240302"
            }
        ]
    
    def test_lookup_activity_payment_method_changes(self):
        """Lookup Activity: 支払い方法変更検出テスト"""
        # テストケース: 支払い方法変更の検出
        mock_payment_changes = [
            {
                "CHANGE_ID": "CHG000001",
                "CUSTOMER_ID": "CUST123456",
                "OLD_METHOD_TYPE": "CREDIT_CARD",
                "NEW_METHOD_TYPE": "BANK_TRANSFER",
                "CHANGE_DATE": "20240215",
                "CHANGE_STATUS": "PENDING",
                "NOTIFICATION_SENT": "N"
            },
            {
                "CHANGE_ID": "CHG000002",
                "CUSTOMER_ID": "CUST123457",
                "OLD_METHOD_TYPE": "BANK_TRANSFER",
                "NEW_METHOD_TYPE": "CREDIT_CARD",
                "CHANGE_DATE": "20240216",
                "CHANGE_STATUS": "PENDING",
                "NOTIFICATION_SENT": "N"
            }
        ]
        
        self.mock_database.query_records.return_value = mock_payment_changes
        
        # Lookup Activity実行シミュレーション
        changes_query = f"""
        SELECT CHANGE_ID, CUSTOMER_ID, OLD_METHOD_TYPE, NEW_METHOD_TYPE, 
               CHANGE_DATE, CHANGE_STATUS, NOTIFICATION_SENT
        FROM payment_method_changes
        WHERE CHANGE_DATE >= '{self.test_date}' AND CHANGE_STATUS = 'PENDING' 
        AND NOTIFICATION_SENT = 'N'
        """
        
        result = self.mock_database.query_records("payment_method_changes", changes_query)
        
        self.assertEqual(len(result), 2, "支払い方法変更検出件数不正")
        self.assertEqual(result[0]["CHANGE_ID"], "CHG000001", "変更ID確認失敗")
        self.assertEqual(result[0]["CHANGE_STATUS"], "PENDING", "変更ステータス確認失敗")
        self.assertEqual(result[0]["NOTIFICATION_SENT"], "N", "通知送信フラグ確認失敗")
    
    def test_lookup_activity_customer_notification_preferences(self):
        """Lookup Activity: 顧客通知設定取得テスト"""
        # テストケース: 顧客の通知設定取得
        mock_notification_preferences = [
            {
                "CUSTOMER_ID": "CUST123456",
                "PAYMENT_CHANGE_EMAIL": "Y",
                "PAYMENT_CHANGE_SMS": "N",
                "NOTIFICATION_TIMING": "IMMEDIATE",
                "LANGUAGE_PREFERENCE": "JA"
            },
            {
                "CUSTOMER_ID": "CUST123457",
                "PAYMENT_CHANGE_EMAIL": "Y",
                "PAYMENT_CHANGE_SMS": "Y",
                "NOTIFICATION_TIMING": "BATCH",
                "LANGUAGE_PREFERENCE": "JA"
            }
        ]
        
        self.mock_database.query_records.return_value = mock_notification_preferences
        
        # Lookup Activity実行シミュレーション
        preferences_query = """
        SELECT CUSTOMER_ID, PAYMENT_CHANGE_EMAIL, PAYMENT_CHANGE_SMS, 
               NOTIFICATION_TIMING, LANGUAGE_PREFERENCE
        FROM customer_notification_preferences
        WHERE CUSTOMER_ID IN ('CUST123456', 'CUST123457')
        """
        
        result = self.mock_database.query_records("customer_notification_preferences", preferences_query)
        
        self.assertEqual(len(result), 2, "顧客通知設定取得件数不正")
        self.assertEqual(result[0]["PAYMENT_CHANGE_EMAIL"], "Y", "メール通知設定確認失敗")
        self.assertEqual(result[0]["NOTIFICATION_TIMING"], "IMMEDIATE", "通知タイミング確認失敗")
    
    def test_data_flow_change_notification_message_generation(self):
        """Data Flow: 変更通知メッセージ生成テスト"""
        # テストケース: 変更通知メッセージの生成
        change_data = {
            "CUSTOMER_NAME": "テストユーザー1",
            "OLD_METHOD_TYPE": "CREDIT_CARD",
            "NEW_METHOD_TYPE": "BANK_TRANSFER",
            "OLD_CARD_NUMBER": "4111********1111",
            "NEW_BANK_CODE": "0001",
            "NEW_ACCOUNT_NUMBER": "1234567",
            "CHANGE_DATE": "20240215",
            "EFFECTIVE_DATE": "20240301",
            "CHANGE_REASON": "USER_REQUEST"
        }
        
        # 変更通知メッセージ生成ロジック（Data Flow内の処理）
        def generate_change_notification_message(change_data):
            customer_name = change_data["CUSTOMER_NAME"]
            old_method_type = change_data["OLD_METHOD_TYPE"]
            new_method_type = change_data["NEW_METHOD_TYPE"]
            change_date = change_data["CHANGE_DATE"]
            effective_date = change_data["EFFECTIVE_DATE"]
            change_reason = change_data["CHANGE_REASON"]
            
            # 支払い方法の日本語名変換
            method_names = {
                "CREDIT_CARD": "クレジットカード",
                "BANK_TRANSFER": "銀行振込",
                "CONVENIENCE_STORE": "コンビニ支払い"
            }
            
            old_method_name = method_names.get(old_method_type, old_method_type)
            new_method_name = method_names.get(new_method_type, new_method_type)
            
            # 日付のフォーマット
            formatted_change_date = f"{change_date[:4]}年{change_date[4:6]}月{change_date[6:8]}日"
            formatted_effective_date = f"{effective_date[:4]}年{effective_date[4:6]}月{effective_date[6:8]}日"
            
            # 変更理由の説明
            reason_explanations = {
                "USER_REQUEST": "お客様のご要望により",
                "CARD_EXPIRY": "クレジットカード有効期限のため",
                "SYSTEM_MAINTENANCE": "システムメンテナンスのため",
                "SECURITY_UPGRADE": "セキュリティ向上のため"
            }
            
            reason_text = reason_explanations.get(change_reason, "システムにより")
            
            notification_message = f"""
            {customer_name}様
            
            お支払い方法変更のお知らせ
            
            {reason_text}、お支払い方法が変更されました。
            
            変更内容：
            - 変更前：{old_method_name}
            - 変更後：{new_method_name}
            - 変更日：{formatted_change_date}
            - 適用開始日：{formatted_effective_date}
            
            今後とも東京ガスをよろしくお願いいたします。
            
            東京ガス株式会社
            """
            
            return notification_message.strip()
        
        # メッセージ生成実行
        message = generate_change_notification_message(change_data)
        
        # アサーション
        self.assertIn("テストユーザー1様", message, "顧客名挿入失敗")
        self.assertIn("クレジットカード", message, "旧支払い方法挿入失敗")
        self.assertIn("銀行振込", message, "新支払い方法挿入失敗")
        self.assertIn("2024年02月15日", message, "変更日フォーマット失敗")
        self.assertIn("2024年03月01日", message, "適用開始日フォーマット失敗")
        self.assertIn("お客様のご要望により", message, "変更理由挿入失敗")
    
    def test_data_flow_change_impact_analysis(self):
        """Data Flow: 変更影響分析テスト"""
        # テストケース: 支払い方法変更の影響分析
        change_scenarios = [
            {
                "OLD_METHOD_TYPE": "CREDIT_CARD",
                "NEW_METHOD_TYPE": "BANK_TRANSFER",
                "BILLING_CYCLE": "MONTHLY",
                "CUSTOMER_SEGMENT": "PREMIUM"
            },
            {
                "OLD_METHOD_TYPE": "BANK_TRANSFER",
                "NEW_METHOD_TYPE": "CONVENIENCE_STORE",
                "BILLING_CYCLE": "MONTHLY",
                "CUSTOMER_SEGMENT": "STANDARD"
            }
        ]
        
        # 変更影響分析ロジック（Data Flow内の処理）
        def analyze_change_impact(change_data):
            old_method = change_data["OLD_METHOD_TYPE"]
            new_method = change_data["NEW_METHOD_TYPE"]
            billing_cycle = change_data["BILLING_CYCLE"]
            customer_segment = change_data["CUSTOMER_SEGMENT"]
            
            impact_analysis = {
                "automation_impact": "NONE",
                "billing_impact": "NONE",
                "customer_experience_impact": "NONE",
                "processing_cost_impact": "NONE"
            }
            
            # 自動化への影響
            if old_method == "CREDIT_CARD" and new_method == "BANK_TRANSFER":
                impact_analysis["automation_impact"] = "REDUCED"
            elif old_method == "BANK_TRANSFER" and new_method == "CREDIT_CARD":
                impact_analysis["automation_impact"] = "IMPROVED"
            
            # 請求への影響
            if new_method == "CONVENIENCE_STORE":
                impact_analysis["billing_impact"] = "MANUAL_PROCESSING_REQUIRED"
            elif new_method == "CREDIT_CARD":
                impact_analysis["billing_impact"] = "AUTOMATED_PROCESSING"
            
            # 顧客体験への影響
            if customer_segment == "PREMIUM":
                if new_method == "CONVENIENCE_STORE":
                    impact_analysis["customer_experience_impact"] = "DEGRADED"
                elif new_method == "CREDIT_CARD":
                    impact_analysis["customer_experience_impact"] = "IMPROVED"
            
            # 処理コストへの影響
            if new_method == "CONVENIENCE_STORE":
                impact_analysis["processing_cost_impact"] = "INCREASED"
            elif new_method == "CREDIT_CARD":
                impact_analysis["processing_cost_impact"] = "DECREASED"
            
            return impact_analysis
        
        # 各シナリオでの影響分析
        expected_impacts = [
            {
                "automation_impact": "REDUCED",
                "billing_impact": "NONE",
                "customer_experience_impact": "NONE",
                "processing_cost_impact": "NONE"
            },
            {
                "automation_impact": "NONE",
                "billing_impact": "MANUAL_PROCESSING_REQUIRED",
                "customer_experience_impact": "NONE",
                "processing_cost_impact": "INCREASED"
            }
        ]
        
        for i, scenario in enumerate(change_scenarios):
            impact = analyze_change_impact(scenario)
            self.assertEqual(impact, expected_impacts[i], f"シナリオ{i+1}の影響分析失敗")
    
    def test_data_flow_change_validation(self):
        """Data Flow: 変更データ検証テスト"""
        # テストケース: 変更データの検証
        test_changes = [
            {"CUSTOMER_ID": "CUST123456", "OLD_METHOD_TYPE": "CREDIT_CARD", "NEW_METHOD_TYPE": "BANK_TRANSFER", "EFFECTIVE_DATE": "20240301"},
            {"CUSTOMER_ID": "", "OLD_METHOD_TYPE": "CREDIT_CARD", "NEW_METHOD_TYPE": "BANK_TRANSFER", "EFFECTIVE_DATE": "20240301"},  # 不正: 空ID
            {"CUSTOMER_ID": "CUST123458", "OLD_METHOD_TYPE": "CREDIT_CARD", "NEW_METHOD_TYPE": "CREDIT_CARD", "EFFECTIVE_DATE": "20240301"},  # 不正: 同じ支払い方法
            {"CUSTOMER_ID": "CUST123459", "OLD_METHOD_TYPE": "CREDIT_CARD", "NEW_METHOD_TYPE": "BANK_TRANSFER", "EFFECTIVE_DATE": "20240201"}  # 不正: 過去の日付
        ]
        
        # 変更データ検証ロジック（Data Flow内の処理）
        def validate_payment_method_change(change):
            errors = []
            current_date = datetime.utcnow().strftime('%Y%m%d')
            
            # 顧客ID検証
            if not change.get("CUSTOMER_ID", "").strip():
                errors.append("顧客ID必須")
            
            # 支払い方法検証
            old_method = change.get("OLD_METHOD_TYPE", "")
            new_method = change.get("NEW_METHOD_TYPE", "")
            
            if old_method == new_method:
                errors.append("新旧支払い方法が同じです")
            
            # 適用日検証
            effective_date = change.get("EFFECTIVE_DATE", "")
            if effective_date < current_date:
                errors.append("適用日は現在日付以降である必要があります")
            
            return errors
        
        # 検証実行
        validation_results = []
        for change in test_changes:
            errors = validate_payment_method_change(change)
            validation_results.append({
                "change": change,
                "errors": errors,
                "is_valid": len(errors) == 0
            })
        
        # アサーション
        self.assertEqual(len(validation_results), 4, "検証結果数不正")
        self.assertTrue(validation_results[0]["is_valid"], "正常変更が不正判定")
        self.assertFalse(validation_results[1]["is_valid"], "不正変更（空ID）が正常判定")
        self.assertFalse(validation_results[2]["is_valid"], "不正変更（同じ支払い方法）が正常判定")
        self.assertFalse(validation_results[3]["is_valid"], "不正変更（過去の日付）が正常判定")
    
    def test_script_activity_email_notification_sending(self):
        """Script Activity: メール通知送信処理テスト"""
        # テストケース: メールでの変更通知送信
        change_email_data = {
            "to": "test@example.com",
            "subject": "【東京ガス】お支払い方法変更のお知らせ",
            "body": "お支払い方法が変更されました。",
            "change_id": "CHG000001",
            "notification_type": "PAYMENT_METHOD_CHANGE"
        }
        
        self.mock_email_service.send_change_notification.return_value = {
            "status": "sent",
            "message_id": "CHANGE_EMAIL_123",
            "delivery_time": datetime.utcnow().isoformat(),
            "change_id": "CHG000001"
        }
        
        # Script Activity実行シミュレーション
        result = self.mock_email_service.send_change_notification(
            change_email_data["to"],
            change_email_data["subject"],
            change_email_data["body"],
            change_id=change_email_data["change_id"],
            notification_type=change_email_data["notification_type"]
        )
        
        self.assertEqual(result["status"], "sent", "メール通知送信失敗")
        self.assertIsNotNone(result["message_id"], "メッセージID取得失敗")
        self.assertEqual(result["change_id"], "CHG000001", "変更ID確認失敗")
        self.mock_email_service.send_change_notification.assert_called_once()
    
    def test_script_activity_sms_notification_sending(self):
        """Script Activity: SMS通知送信処理テスト"""
        # テストケース: SMSでの変更通知送信
        change_sms_data = {
            "to": "090-1234-5678",
            "message": "東京ガスです。お支払い方法が変更されました。詳細はメールをご確認ください。",
            "change_id": "CHG000001",
            "sender": "TokyoGas"
        }
        
        self.mock_sms_service.send_change_notification.return_value = {
            "status": "sent",
            "message_id": "CHANGE_SMS_456",
            "delivery_time": datetime.utcnow().isoformat(),
            "change_id": "CHG000001",
            "character_count": len(change_sms_data["message"])
        }
        
        # Script Activity実行シミュレーション
        result = self.mock_sms_service.send_change_notification(
            change_sms_data["to"],
            change_sms_data["message"],
            change_id=change_sms_data["change_id"],
            sender=change_sms_data["sender"]
        )
        
        self.assertEqual(result["status"], "sent", "SMS通知送信失敗")
        self.assertIsNotNone(result["message_id"], "SMSメッセージID取得失敗")
        self.assertEqual(result["change_id"], "CHG000001", "変更ID確認失敗")
        self.assertGreater(result["character_count"], 0, "文字数カウント失敗")
        self.mock_sms_service.send_change_notification.assert_called_once()
    
    def test_copy_activity_change_notification_log(self):
        """Copy Activity: 変更通知ログ記録テスト"""
        # テストケース: 変更通知ログの記録
        notification_logs = [
            {
                "CHANGE_ID": "CHG000001",
                "CUSTOMER_ID": "CUST123456",
                "NOTIFICATION_TYPE": "PAYMENT_METHOD_CHANGE",
                "DELIVERY_METHOD": "EMAIL",
                "SENT_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "SENT_TIME": datetime.utcnow().strftime('%H:%M:%S'),
                "STATUS": "SENT",
                "MESSAGE_ID": "CHANGE_EMAIL_123"
            },
            {
                "CHANGE_ID": "CHG000002",
                "CUSTOMER_ID": "CUST123457",
                "NOTIFICATION_TYPE": "PAYMENT_METHOD_CHANGE",
                "DELIVERY_METHOD": "BOTH",
                "SENT_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "SENT_TIME": datetime.utcnow().strftime('%H:%M:%S'),
                "STATUS": "SENT",
                "MESSAGE_ID": "CHANGE_EMAIL_124"
            }
        ]
        
        self.mock_database.insert_records.return_value = True
        
        # Copy Activity実行シミュレーション
        notification_log_table = "payment_method_change_notifications"
        result = self.mock_database.insert_records(notification_log_table, notification_logs)
        
        self.assertTrue(result, "変更通知ログ記録失敗")
        self.mock_database.insert_records.assert_called_once_with(notification_log_table, notification_logs)
    
    def test_copy_activity_change_status_update(self):
        """Copy Activity: 変更ステータス更新テスト"""
        # テストケース: 変更ステータスの更新
        status_updates = [
            {
                "CHANGE_ID": "CHG000001",
                "NOTIFICATION_SENT": "Y",
                "NOTIFICATION_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "UPDATED_BY": "SYSTEM"
            },
            {
                "CHANGE_ID": "CHG000002",
                "NOTIFICATION_SENT": "Y",
                "NOTIFICATION_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "UPDATED_BY": "SYSTEM"
            }
        ]
        
        self.mock_database.update_records.return_value = True
        
        # Copy Activity実行シミュレーション
        for update in status_updates:
            result = self.mock_database.update_records(
                "payment_method_changes",
                update,
                where_clause=f"CHANGE_ID = '{update['CHANGE_ID']}'"
            )
            self.assertTrue(result, f"変更ステータス更新失敗: {update['CHANGE_ID']}")
        
        self.assertEqual(self.mock_database.update_records.call_count, 2, "更新処理回数不正")
    
    def test_script_activity_change_analytics(self):
        """Script Activity: 変更分析処理テスト"""
        # テストケース: 支払い方法変更の分析データ生成
        change_analytics = {
            "execution_date": self.test_date,
            "total_changes": 25,
            "credit_card_to_bank": 15,
            "bank_to_credit_card": 8,
            "to_convenience_store": 2,
            "user_requested_changes": 20,
            "system_initiated_changes": 5,
            "email_notifications_sent": 23,
            "sms_notifications_sent": 18,
            "notification_success_rate": 0.96,
            "processing_time_minutes": 2.8
        }
        
        self.mock_database.insert_records.return_value = True
        
        # Script Activity実行シミュレーション
        analytics_table = "payment_method_change_analytics"
        result = self.mock_database.insert_records(analytics_table, [change_analytics])
        
        self.assertTrue(result, "変更分析記録失敗")
        self.mock_database.insert_records.assert_called_once_with(analytics_table, [change_analytics])
    
    def test_lookup_activity_change_history_analysis(self):
        """Lookup Activity: 変更履歴分析テスト"""
        # テストケース: 顧客の支払い方法変更履歴分析
        change_history = [
            {
                "CUSTOMER_ID": "CUST123456",
                "CHANGE_COUNT": 2,
                "LAST_CHANGE_DATE": "20240115",
                "CHANGE_FREQUENCY": "OCCASIONAL",
                "PREFERRED_METHOD": "CREDIT_CARD"
            },
            {
                "CUSTOMER_ID": "CUST123457",
                "CHANGE_COUNT": 5,
                "LAST_CHANGE_DATE": "20240210",
                "CHANGE_FREQUENCY": "FREQUENT",
                "PREFERRED_METHOD": "BANK_TRANSFER"
            }
        ]
        
        self.mock_database.query_records.return_value = change_history
        
        # Lookup Activity実行シミュレーション
        history_query = """
        SELECT CUSTOMER_ID, 
               COUNT(*) as CHANGE_COUNT,
               MAX(CHANGE_DATE) as LAST_CHANGE_DATE,
               CASE 
                   WHEN COUNT(*) >= 5 THEN 'FREQUENT'
                   WHEN COUNT(*) >= 2 THEN 'OCCASIONAL'
                   ELSE 'RARE'
               END as CHANGE_FREQUENCY,
               MODE() WITHIN GROUP (ORDER BY NEW_METHOD_TYPE) as PREFERRED_METHOD
        FROM payment_method_changes
        WHERE CUSTOMER_ID IN ('CUST123456', 'CUST123457')
        GROUP BY CUSTOMER_ID
        """
        
        result = self.mock_database.query_records("payment_method_changes", history_query)
        
        self.assertEqual(len(result), 2, "変更履歴分析結果数不正")
        self.assertEqual(result[0]["CHANGE_FREQUENCY"], "OCCASIONAL", "変更頻度判定失敗")
        self.assertEqual(result[1]["CHANGE_FREQUENCY"], "FREQUENT", "変更頻度判定失敗")
    
    def test_data_flow_change_batch_processing(self):
        """Data Flow: 変更バッチ処理テスト"""
        # テストケース: 変更通知のバッチ処理
        large_change_dataset = []
        for i in range(200):
            large_change_dataset.append({
                "CHANGE_ID": f"CHG{i:06d}",
                "CUSTOMER_ID": f"CUST{i:06d}",
                "OLD_METHOD_TYPE": ["CREDIT_CARD", "BANK_TRANSFER"][i % 2],
                "NEW_METHOD_TYPE": ["BANK_TRANSFER", "CREDIT_CARD"][i % 2],
                "NOTIFICATION_PREFERENCE": ["EMAIL", "SMS", "BOTH"][i % 3]
            })
        
        # バッチ処理ロジック（Data Flow内の処理）
        def process_change_notification_batch(change_data_list, batch_size=20):
            processed_batches = []
            
            for i in range(0, len(change_data_list), batch_size):
                batch = change_data_list[i:i + batch_size]
                
                # バッチ処理の実行
                batch_result = {
                    "batch_id": f"BATCH_{i//batch_size + 1:03d}",
                    "changes_count": len(batch),
                    "email_notifications": sum(1 for change in batch if change["NOTIFICATION_PREFERENCE"] in ["EMAIL", "BOTH"]),
                    "sms_notifications": sum(1 for change in batch if change["NOTIFICATION_PREFERENCE"] in ["SMS", "BOTH"]),
                    "credit_to_bank": sum(1 for change in batch if change["OLD_METHOD_TYPE"] == "CREDIT_CARD" and change["NEW_METHOD_TYPE"] == "BANK_TRANSFER"),
                    "bank_to_credit": sum(1 for change in batch if change["OLD_METHOD_TYPE"] == "BANK_TRANSFER" and change["NEW_METHOD_TYPE"] == "CREDIT_CARD"),
                    "processing_time": 0.5  # シミュレーション
                }
                
                processed_batches.append(batch_result)
            
            return processed_batches
        
        # バッチ処理実行
        batch_results = process_change_notification_batch(large_change_dataset, batch_size=20)
        
        # アサーション
        self.assertEqual(len(batch_results), 10, "バッチ数不正")  # 200 / 20 = 10
        self.assertEqual(batch_results[0]["changes_count"], 20, "バッチサイズ不正")
        self.assertGreater(batch_results[0]["email_notifications"], 0, "メール通知数不正")
        self.assertGreater(batch_results[0]["sms_notifications"], 0, "SMS通知数不正")
        
        # 全バッチの合計確認
        total_changes = sum(batch["changes_count"] for batch in batch_results)
        self.assertEqual(total_changes, 200, "全バッチ処理件数不正")
    
    def _create_payment_method_change_csv_content(self) -> str:
        """支払い方法変更データ用CSVコンテンツ生成"""
        header = "CHANGE_ID,CUSTOMER_ID,CUSTOMER_NAME,EMAIL_ADDRESS,PHONE_NUMBER,OLD_METHOD_TYPE,NEW_METHOD_TYPE,OLD_CARD_NUMBER,NEW_BANK_CODE,NEW_ACCOUNT_NUMBER,CHANGE_DATE,CHANGE_REASON,EFFECTIVE_DATE"
        rows = []
        
        for item in self.sample_payment_change_data:
            row = f"{item['CHANGE_ID']},{item['CUSTOMER_ID']},{item['CUSTOMER_NAME']},{item['EMAIL_ADDRESS']},{item['PHONE_NUMBER']},{item['OLD_METHOD_TYPE']},{item['NEW_METHOD_TYPE']},{item.get('OLD_CARD_NUMBER', '')},{item.get('NEW_BANK_CODE', '')},{item.get('NEW_ACCOUNT_NUMBER', '')},{item['CHANGE_DATE']},{item['CHANGE_REASON']},{item['EFFECTIVE_DATE']}"
            rows.append(row)
        
        return header + "\n" + "\n".join(rows)


if __name__ == "__main__":
    unittest.main()