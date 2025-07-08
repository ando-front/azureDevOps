"""
pi_Send_PaymentAlert パイプラインのユニットテスト

支払いアラート送信パイプラインの個別コンポーネントテスト
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any


class TestPaymentAlertUnit(unittest.TestCase):
    """支払いアラートパイプラインユニットテスト"""
    
    def setUp(self):
        """テストセットアップ"""
        self.pipeline_name = "pi_Send_PaymentAlert"
        self.domain = "smc"
        
        # モックサービス
        self.mock_blob_storage = Mock()
        self.mock_database = Mock()
        self.mock_email_service = Mock()
        self.mock_sms_service = Mock()
        
        # テストデータ
        self.test_date = datetime.utcnow().strftime('%Y%m%d')
        self.test_file_path = f"PaymentAlert/{self.test_date}/payment_alert_data.csv"
        
        # 基本的な支払いアラートデータ
        self.sample_payment_alert_data = [
            {
                "ALERT_ID": "ALERT000001",
                "CUSTOMER_ID": "CUST123456",
                "CUSTOMER_NAME": "テストユーザー1",
                "EMAIL_ADDRESS": "test1@example.com",
                "PHONE_NUMBER": "090-1234-5678",
                "BILL_ID": "BILL000001",
                "BILL_AMOUNT": 12500.0,
                "DUE_DATE": "20240228",
                "DAYS_OVERDUE": 3,
                "ALERT_TYPE": "OVERDUE",
                "ALERT_LEVEL": "WARNING"
            },
            {
                "ALERT_ID": "ALERT000002",
                "CUSTOMER_ID": "CUST123457",
                "CUSTOMER_NAME": "テストユーザー2",
                "EMAIL_ADDRESS": "test2@example.com",
                "PHONE_NUMBER": "090-2345-6789",
                "BILL_ID": "BILL000002",
                "BILL_AMOUNT": 8900.0,
                "DUE_DATE": "20240225",
                "DAYS_OVERDUE": 6,
                "ALERT_TYPE": "OVERDUE",
                "ALERT_LEVEL": "URGENT"
            }
        ]
    
    def test_lookup_activity_overdue_bill_detection(self):
        """Lookup Activity: 延滞請求書検出テスト"""
        # テストケース: 延滞請求書の検出
        current_date = datetime.utcnow().strftime('%Y%m%d')
        
        mock_overdue_bills = [
            {
                "BILL_ID": "BILL000001",
                "CUSTOMER_ID": "CUST123456",
                "BILL_AMOUNT": 12500.0,
                "DUE_DATE": "20240228",
                "PAYMENT_STATUS": "UNPAID",
                "DAYS_OVERDUE": 3
            },
            {
                "BILL_ID": "BILL000002",
                "CUSTOMER_ID": "CUST123457",
                "BILL_AMOUNT": 8900.0,
                "DUE_DATE": "20240225",
                "PAYMENT_STATUS": "UNPAID",
                "DAYS_OVERDUE": 6
            }
        ]
        
        self.mock_database.query_records.return_value = mock_overdue_bills
        
        # Lookup Activity実行シミュレーション
        overdue_query = f"""
        SELECT BILL_ID, CUSTOMER_ID, BILL_AMOUNT, DUE_DATE, PAYMENT_STATUS,
               DATEDIFF(day, DUE_DATE, '{current_date}') as DAYS_OVERDUE
        FROM billing_data
        WHERE DUE_DATE < '{current_date}' AND PAYMENT_STATUS = 'UNPAID'
        """
        
        result = self.mock_database.query_records("billing_data", overdue_query)
        
        self.assertEqual(len(result), 2, "延滞請求書検出件数不正")
        self.assertEqual(result[0]["BILL_ID"], "BILL000001", "請求書ID確認失敗")
        self.assertEqual(result[0]["PAYMENT_STATUS"], "UNPAID", "支払いステータス確認失敗")
        self.assertEqual(result[0]["DAYS_OVERDUE"], 3, "延滞日数確認失敗")
    
    def test_lookup_activity_customer_alert_preferences(self):
        """Lookup Activity: 顧客アラート設定取得テスト"""
        # テストケース: 顧客のアラート設定取得
        mock_alert_preferences = [
            {
                "CUSTOMER_ID": "CUST123456",
                "ALERT_EMAIL": "Y",
                "ALERT_SMS": "N",
                "ALERT_FREQUENCY": "IMMEDIATE",
                "PREFERRED_CONTACT_TIME": "09:00-18:00"
            },
            {
                "CUSTOMER_ID": "CUST123457",
                "ALERT_EMAIL": "Y",
                "ALERT_SMS": "Y",
                "ALERT_FREQUENCY": "DAILY",
                "PREFERRED_CONTACT_TIME": "10:00-20:00"
            }
        ]
        
        self.mock_database.query_records.return_value = mock_alert_preferences
        
        # Lookup Activity実行シミュレーション
        preferences_query = """
        SELECT CUSTOMER_ID, ALERT_EMAIL, ALERT_SMS, ALERT_FREQUENCY, PREFERRED_CONTACT_TIME
        FROM customer_alert_preferences
        WHERE CUSTOMER_ID IN ('CUST123456', 'CUST123457')
        """
        
        result = self.mock_database.query_records("customer_alert_preferences", preferences_query)
        
        self.assertEqual(len(result), 2, "顧客アラート設定取得件数不正")
        self.assertEqual(result[0]["ALERT_EMAIL"], "Y", "メールアラート設定確認失敗")
        self.assertEqual(result[0]["ALERT_FREQUENCY"], "IMMEDIATE", "アラート頻度確認失敗")
    
    def test_data_flow_alert_level_determination(self):
        """Data Flow: アラートレベル判定テスト"""
        # テストケース: アラートレベルの判定
        payment_scenarios = [
            {"DAYS_OVERDUE": 1, "BILL_AMOUNT": 5000.0, "CUSTOMER_HISTORY": "GOOD"},
            {"DAYS_OVERDUE": 7, "BILL_AMOUNT": 15000.0, "CUSTOMER_HISTORY": "AVERAGE"},
            {"DAYS_OVERDUE": 15, "BILL_AMOUNT": 25000.0, "CUSTOMER_HISTORY": "POOR"}
        ]
        
        # アラートレベル判定ロジック（Data Flow内の処理）
        def determine_alert_level(payment_data):
            days_overdue = payment_data["DAYS_OVERDUE"]
            bill_amount = payment_data["BILL_AMOUNT"]
            customer_history = payment_data["CUSTOMER_HISTORY"]
            
            # 基本レベル判定
            if days_overdue <= 3:
                base_level = "INFO"
            elif days_overdue <= 7:
                base_level = "WARNING"
            else:
                base_level = "URGENT"
            
            # 請求金額による調整
            if bill_amount >= 20000:
                if base_level == "INFO":
                    base_level = "WARNING"
                elif base_level == "WARNING":
                    base_level = "URGENT"
            
            # 顧客履歴による調整
            if customer_history == "POOR" and base_level != "URGENT":
                base_level = "WARNING"
            elif customer_history == "GOOD" and base_level == "WARNING":
                base_level = "INFO"
            
            return base_level
        
        # 期待される結果
        expected_levels = ["INFO", "WARNING", "URGENT"]
        
        # 各シナリオでのレベル判定
        for i, scenario in enumerate(payment_scenarios):
            level = determine_alert_level(scenario)
            self.assertEqual(level, expected_levels[i], f"シナリオ{i+1}のアラートレベル判定失敗")
    
    def test_data_flow_alert_message_generation(self):
        """Data Flow: アラートメッセージ生成テスト"""
        # テストケース: アラートメッセージの生成
        alert_data = {
            "CUSTOMER_NAME": "テストユーザー1",
            "BILL_AMOUNT": 12500.0,
            "DUE_DATE": "20240228",
            "DAYS_OVERDUE": 3,
            "ALERT_LEVEL": "WARNING",
            "PAYMENT_METHOD": "口座振替"
        }
        
        # アラートメッセージ生成ロジック（Data Flow内の処理）
        def generate_alert_message(alert_data):
            customer_name = alert_data["CUSTOMER_NAME"]
            bill_amount = alert_data["BILL_AMOUNT"]
            due_date = alert_data["DUE_DATE"]
            days_overdue = alert_data["DAYS_OVERDUE"]
            alert_level = alert_data["ALERT_LEVEL"]
            payment_method = alert_data["PAYMENT_METHOD"]
            
            # 期限のフォーマット
            formatted_due_date = f"{due_date[:4]}年{due_date[4:6]}月{due_date[6:8]}日"
            
            # アラートレベル別のメッセージ調整
            if alert_level == "INFO":
                urgency_msg = "お支払いのご案内"
                tone = "お知らせいたします"
            elif alert_level == "WARNING":
                urgency_msg = "お支払いのお願い"
                tone = "お願いいたします"
            else:  # URGENT
                urgency_msg = "緊急：お支払いのお願い"
                tone = "至急お支払いください"
            
            alert_message = f"""
            {customer_name}様
            
            {urgency_msg}
            
            お支払い期限を{days_overdue}日過ぎたお支払いがございます。
            
            請求金額：{bill_amount:,.0f}円
            お支払い期限：{formatted_due_date}
            お支払い方法：{payment_method}
            
            {tone}。
            
            東京ガス株式会社
            """
            
            return alert_message.strip()
        
        # メッセージ生成実行
        message = generate_alert_message(alert_data)
        
        # アサーション
        self.assertIn("テストユーザー1様", message, "顧客名挿入失敗")
        self.assertIn("お支払いのお願い", message, "アラートレベル反映失敗")
        self.assertIn("12,500円", message, "請求金額挿入失敗")
        self.assertIn("2024年02月28日", message, "期限フォーマット失敗")
        self.assertIn("3日過ぎた", message, "延滞日数挿入失敗")
        self.assertIn("口座振替", message, "支払い方法挿入失敗")
    
    def test_data_flow_alert_frequency_control(self):
        """Data Flow: アラート送信頻度制御テスト"""
        # テストケース: アラート送信頻度の制御
        alert_history = [
            {"CUSTOMER_ID": "CUST123456", "LAST_ALERT_DATE": "20240301", "ALERT_COUNT": 1},
            {"CUSTOMER_ID": "CUST123457", "LAST_ALERT_DATE": "20240228", "ALERT_COUNT": 3}
        ]
        
        # アラート頻度制御ロジック（Data Flow内の処理）
        def should_send_alert(customer_id, alert_history, current_date, alert_frequency):
            customer_history = next((h for h in alert_history if h["CUSTOMER_ID"] == customer_id), None)
            
            if not customer_history:
                return True  # 初回アラート
            
            last_alert_date = datetime.strptime(customer_history["LAST_ALERT_DATE"], "%Y%m%d")
            current_dt = datetime.strptime(current_date, "%Y%m%d")
            days_since_last = (current_dt - last_alert_date).days
            alert_count = customer_history["ALERT_COUNT"]
            
            # 頻度設定による制御
            if alert_frequency == "IMMEDIATE":
                return True
            elif alert_frequency == "DAILY":
                return days_since_last >= 1
            elif alert_frequency == "WEEKLY":
                return days_since_last >= 7
            
            # アラート回数による制御
            if alert_count >= 5:
                return days_since_last >= 7  # 5回以上は週1回に制限
            elif alert_count >= 3:
                return days_since_last >= 3  # 3回以上は3日に1回
            else:
                return days_since_last >= 1  # 通常は1日に1回
        
        # 各顧客の送信判定
        current_date = "20240303"
        
        # CUST123456: 前回送信から2日後、回数1回
        should_send_1 = should_send_alert("CUST123456", alert_history, current_date, "DAILY")
        self.assertTrue(should_send_1, "顧客1のアラート送信判定失敗")
        
        # CUST123457: 前回送信から3日後、回数3回
        should_send_2 = should_send_alert("CUST123457", alert_history, current_date, "DAILY")
        self.assertTrue(should_send_2, "顧客2のアラート送信判定失敗")
        
        # 同日再送信テスト
        same_day_send = should_send_alert("CUST123456", alert_history, "20240301", "DAILY")
        self.assertFalse(same_day_send, "同日再送信制御失敗")
    
    def test_script_activity_email_alert_sending(self):
        """Script Activity: メールアラート送信処理テスト"""
        # テストケース: メールでのアラート送信
        alert_email_data = {
            "to": "test@example.com",
            "subject": "【東京ガス】お支払いのお願い",
            "body": "お支払い期限を過ぎたお支払いがございます。",
            "priority": "high",
            "alert_level": "WARNING"
        }
        
        self.mock_email_service.send_alert_email.return_value = {
            "status": "sent",
            "message_id": "ALERT_EMAIL_123",
            "delivery_time": datetime.utcnow().isoformat(),
            "priority_used": "high"
        }
        
        # Script Activity実行シミュレーション
        result = self.mock_email_service.send_alert_email(
            alert_email_data["to"],
            alert_email_data["subject"],
            alert_email_data["body"],
            priority=alert_email_data["priority"],
            alert_level=alert_email_data["alert_level"]
        )
        
        self.assertEqual(result["status"], "sent", "メールアラート送信失敗")
        self.assertIsNotNone(result["message_id"], "メッセージID取得失敗")
        self.assertEqual(result["priority_used"], "high", "優先度確認失敗")
        self.mock_email_service.send_alert_email.assert_called_once()
    
    def test_script_activity_sms_alert_sending(self):
        """Script Activity: SMSアラート送信処理テスト"""
        # テストケース: SMSでのアラート送信
        alert_sms_data = {
            "to": "090-1234-5678",
            "message": "東京ガスです。お支払い期限を過ぎたお支払いがございます。詳細はメールをご確認ください。",
            "sender": "TokyoGas",
            "alert_level": "WARNING"
        }
        
        self.mock_sms_service.send_alert_sms.return_value = {
            "status": "sent",
            "message_id": "ALERT_SMS_456",
            "delivery_time": datetime.utcnow().isoformat(),
            "character_count": len(alert_sms_data["message"])
        }
        
        # Script Activity実行シミュレーション
        result = self.mock_sms_service.send_alert_sms(
            alert_sms_data["to"],
            alert_sms_data["message"],
            sender=alert_sms_data["sender"],
            alert_level=alert_sms_data["alert_level"]
        )
        
        self.assertEqual(result["status"], "sent", "SMSアラート送信失敗")
        self.assertIsNotNone(result["message_id"], "SMSメッセージID取得失敗")
        self.assertGreater(result["character_count"], 0, "文字数カウント失敗")
        self.mock_sms_service.send_alert_sms.assert_called_once()
    
    def test_copy_activity_alert_log_recording(self):
        """Copy Activity: アラートログ記録テスト"""
        # テストケース: アラート送信ログの記録
        alert_logs = [
            {
                "ALERT_ID": "ALERT000001",
                "CUSTOMER_ID": "CUST123456",
                "ALERT_TYPE": "PAYMENT_OVERDUE",
                "ALERT_LEVEL": "WARNING",
                "SENT_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "SENT_TIME": datetime.utcnow().strftime('%H:%M:%S'),
                "DELIVERY_METHOD": "EMAIL",
                "STATUS": "SENT"
            },
            {
                "ALERT_ID": "ALERT000002",
                "CUSTOMER_ID": "CUST123457",
                "ALERT_TYPE": "PAYMENT_OVERDUE",
                "ALERT_LEVEL": "URGENT",
                "SENT_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "SENT_TIME": datetime.utcnow().strftime('%H:%M:%S'),
                "DELIVERY_METHOD": "BOTH",
                "STATUS": "SENT"
            }
        ]
        
        self.mock_database.insert_records.return_value = True
        
        # Copy Activity実行シミュレーション
        alert_log_table = "payment_alert_logs"
        result = self.mock_database.insert_records(alert_log_table, alert_logs)
        
        self.assertTrue(result, "アラートログ記録失敗")
        self.mock_database.insert_records.assert_called_once_with(alert_log_table, alert_logs)
    
    def test_copy_activity_alert_escalation(self):
        """Copy Activity: アラートエスカレーション処理テスト"""
        # テストケース: アラートのエスカレーション処理
        escalation_data = [
            {
                "CUSTOMER_ID": "CUST123456",
                "BILL_ID": "BILL000001",
                "CURRENT_ALERT_LEVEL": "WARNING",
                "NEXT_ALERT_LEVEL": "URGENT",
                "ESCALATION_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "ESCALATION_REASON": "NO_RESPONSE_7_DAYS"
            }
        ]
        
        self.mock_database.insert_records.return_value = True
        
        # Copy Activity実行シミュレーション
        escalation_table = "alert_escalations"
        result = self.mock_database.insert_records(escalation_table, escalation_data)
        
        self.assertTrue(result, "アラートエスカレーション処理失敗")
        self.mock_database.insert_records.assert_called_once_with(escalation_table, escalation_data)
    
    def test_script_activity_alert_analytics(self):
        """Script Activity: アラート分析処理テスト"""
        # テストケース: アラート送信の分析データ生成
        alert_analytics = {
            "execution_date": self.test_date,
            "total_overdue_customers": 150,
            "info_alerts_sent": 50,
            "warning_alerts_sent": 80,
            "urgent_alerts_sent": 20,
            "email_alerts_sent": 120,
            "sms_alerts_sent": 90,
            "alert_success_rate": 0.97,
            "average_days_overdue": 5.2,
            "total_overdue_amount": 1250000.0,
            "processing_time_minutes": 3.8
        }
        
        self.mock_database.insert_records.return_value = True
        
        # Script Activity実行シミュレーション
        analytics_table = "payment_alert_analytics"
        result = self.mock_database.insert_records(analytics_table, [alert_analytics])
        
        self.assertTrue(result, "アラート分析記録失敗")
        self.mock_database.insert_records.assert_called_once_with(analytics_table, [alert_analytics])
    
    def test_data_flow_alert_validation(self):
        """Data Flow: アラートデータ検証テスト"""
        # テストケース: アラートデータの検証
        test_alerts = [
            {"CUSTOMER_ID": "CUST123456", "BILL_AMOUNT": 12500.0, "DAYS_OVERDUE": 3, "EMAIL_ADDRESS": "test@example.com"},
            {"CUSTOMER_ID": "", "BILL_AMOUNT": 8900.0, "DAYS_OVERDUE": 5, "EMAIL_ADDRESS": "test2@example.com"},  # 不正: 空ID
            {"CUSTOMER_ID": "CUST123458", "BILL_AMOUNT": -1000.0, "DAYS_OVERDUE": 2, "EMAIL_ADDRESS": "test3@example.com"},  # 不正: 負の金額
            {"CUSTOMER_ID": "CUST123459", "BILL_AMOUNT": 5000.0, "DAYS_OVERDUE": 0, "EMAIL_ADDRESS": "invalid-email"}  # 不正: 延滞日数、メール形式
        ]
        
        # アラートデータ検証ロジック（Data Flow内の処理）
        def validate_alert_data(alert):
            errors = []
            
            # 顧客ID検証
            if not alert.get("CUSTOMER_ID", "").strip():
                errors.append("顧客ID必須")
            
            # 請求金額検証
            bill_amount = alert.get("BILL_AMOUNT", 0)
            if bill_amount <= 0:
                errors.append("請求金額は正の値である必要があります")
            
            # 延滞日数検証
            days_overdue = alert.get("DAYS_OVERDUE", 0)
            if days_overdue <= 0:
                errors.append("延滞日数は正の値である必要があります")
            
            # メールアドレス検証
            email = alert.get("EMAIL_ADDRESS", "")
            if not email or "@" not in email:
                errors.append("メールアドレス形式不正")
            
            return errors
        
        # 検証実行
        validation_results = []
        for alert in test_alerts:
            errors = validate_alert_data(alert)
            validation_results.append({
                "alert": alert,
                "errors": errors,
                "is_valid": len(errors) == 0
            })
        
        # アサーション
        self.assertEqual(len(validation_results), 4, "検証結果数不正")
        self.assertTrue(validation_results[0]["is_valid"], "正常アラートが不正判定")
        self.assertFalse(validation_results[1]["is_valid"], "不正アラート（空ID）が正常判定")
        self.assertFalse(validation_results[2]["is_valid"], "不正アラート（負の金額）が正常判定")
        self.assertFalse(validation_results[3]["is_valid"], "不正アラート（延滞日数、メール形式）が正常判定")
    
    def test_lookup_activity_payment_history_analysis(self):
        """Lookup Activity: 支払い履歴分析テスト"""
        # テストケース: 顧客の支払い履歴分析
        payment_history = [
            {
                "CUSTOMER_ID": "CUST123456",
                "AVERAGE_DAYS_LATE": 2.5,
                "LATE_PAYMENT_COUNT": 3,
                "TOTAL_PAYMENTS": 12,
                "PAYMENT_RELIABILITY": "GOOD"
            },
            {
                "CUSTOMER_ID": "CUST123457",
                "AVERAGE_DAYS_LATE": 8.2,
                "LATE_PAYMENT_COUNT": 8,
                "TOTAL_PAYMENTS": 12,
                "PAYMENT_RELIABILITY": "POOR"
            }
        ]
        
        self.mock_database.query_records.return_value = payment_history
        
        # Lookup Activity実行シミュレーション
        history_query = """
        SELECT CUSTOMER_ID, 
               AVG(DAYS_LATE) as AVERAGE_DAYS_LATE,
               COUNT(*) as LATE_PAYMENT_COUNT,
               CASE 
                   WHEN AVG(DAYS_LATE) <= 3 THEN 'GOOD'
                   WHEN AVG(DAYS_LATE) <= 7 THEN 'AVERAGE'
                   ELSE 'POOR'
               END as PAYMENT_RELIABILITY
        FROM payment_history
        WHERE CUSTOMER_ID IN ('CUST123456', 'CUST123457')
        GROUP BY CUSTOMER_ID
        """
        
        result = self.mock_database.query_records("payment_history", history_query)
        
        self.assertEqual(len(result), 2, "支払い履歴分析結果数不正")
        self.assertEqual(result[0]["PAYMENT_RELIABILITY"], "GOOD", "支払い信頼度判定失敗")
        self.assertEqual(result[1]["PAYMENT_RELIABILITY"], "POOR", "支払い信頼度判定失敗")
    
    def _create_payment_alert_csv_content(self) -> str:
        """支払いアラートデータ用CSVコンテンツ生成"""
        header = "ALERT_ID,CUSTOMER_ID,CUSTOMER_NAME,EMAIL_ADDRESS,PHONE_NUMBER,BILL_ID,BILL_AMOUNT,DUE_DATE,DAYS_OVERDUE,ALERT_TYPE,ALERT_LEVEL"
        rows = []
        
        for item in self.sample_payment_alert_data:
            row = f"{item['ALERT_ID']},{item['CUSTOMER_ID']},{item['CUSTOMER_NAME']},{item['EMAIL_ADDRESS']},{item['PHONE_NUMBER']},{item['BILL_ID']},{item['BILL_AMOUNT']},{item['DUE_DATE']},{item['DAYS_OVERDUE']},{item['ALERT_TYPE']},{item['ALERT_LEVEL']}"
            rows.append(row)
        
        return header + "\n" + "\n".join(rows)


if __name__ == "__main__":
    unittest.main()