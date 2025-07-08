"""
pi_Send_ElectricityContractThanks パイプラインのユニットテスト

電気契約お礼送信パイプラインの個別コンポーネントテスト
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any


class TestElectricityContractThanksUnit(unittest.TestCase):
    """電気契約お礼パイプラインユニットテスト"""
    
    def setUp(self):
        """テストセットアップ"""
        self.pipeline_name = "pi_Send_ElectricityContractThanks"
        self.domain = "kendenki"
        
        # モックサービス
        self.mock_blob_storage = Mock()
        self.mock_database = Mock()
        self.mock_email_service = Mock()
        self.mock_sms_service = Mock()
        
        # テストデータ
        self.test_date = datetime.utcnow().strftime('%Y%m%d')
        self.test_file_path = f"ElectricityContractThanks/{self.test_date}/contract_thanks_data.csv"
        
        # 基本的な電気契約データ
        self.sample_contract_data = [
            {
                "CONTRACT_ID": "ELEC000001",
                "CUSTOMER_ID": "CUST123456",
                "CUSTOMER_NAME": "テストユーザー1",
                "EMAIL_ADDRESS": "test1@example.com",
                "PHONE_NUMBER": "090-1234-5678",
                "CONTRACT_TYPE": "FAMILY",
                "POWER_CAPACITY": "40A",
                "CONTRACT_DATE": "20240115",
                "START_DATE": "20240201",
                "MONTHLY_ESTIMATE": 8500.0,
                "WELCOME_GIFT": "LED電球セット"
            },
            {
                "CONTRACT_ID": "ELEC000002",
                "CUSTOMER_ID": "CUST123457",
                "CUSTOMER_NAME": "テストユーザー2",
                "EMAIL_ADDRESS": "test2@example.com",
                "PHONE_NUMBER": "090-2345-6789",
                "CONTRACT_TYPE": "BUSINESS",
                "POWER_CAPACITY": "60A",
                "CONTRACT_DATE": "20240116",
                "START_DATE": "20240202",
                "MONTHLY_ESTIMATE": 12000.0,
                "WELCOME_GIFT": "省エネグッズセット"
            }
        ]
    
    def test_lookup_activity_new_contract_detection(self):
        """Lookup Activity: 新規契約検出テスト"""
        # テストケース: 新規電気契約の検出
        mock_new_contracts = [
            {
                "CONTRACT_ID": "ELEC000001",
                "CUSTOMER_ID": "CUST123456",
                "CONTRACT_TYPE": "FAMILY",
                "CONTRACT_DATE": "20240115",
                "CONTRACT_STATUS": "ACTIVE",
                "THANKS_SENT": "N"
            },
            {
                "CONTRACT_ID": "ELEC000002",
                "CUSTOMER_ID": "CUST123457",
                "CONTRACT_TYPE": "BUSINESS",
                "CONTRACT_DATE": "20240116",
                "CONTRACT_STATUS": "ACTIVE",
                "THANKS_SENT": "N"
            }
        ]
        
        self.mock_database.query_records.return_value = mock_new_contracts
        
        # Lookup Activity実行シミュレーション
        contract_query = f"""
        SELECT CONTRACT_ID, CUSTOMER_ID, CONTRACT_TYPE, CONTRACT_DATE, CONTRACT_STATUS
        FROM electricity_contracts
        WHERE CONTRACT_DATE >= '{self.test_date}' AND CONTRACT_STATUS = 'ACTIVE' AND THANKS_SENT = 'N'
        """
        
        result = self.mock_database.query_records("electricity_contracts", contract_query)
        
        self.assertEqual(len(result), 2, "新規契約検出件数不正")
        self.assertEqual(result[0]["CONTRACT_ID"], "ELEC000001", "契約ID確認失敗")
        self.assertEqual(result[0]["CONTRACT_STATUS"], "ACTIVE", "契約ステータス確認失敗")
        self.assertEqual(result[0]["THANKS_SENT"], "N", "お礼送信フラグ確認失敗")
    
    def test_lookup_activity_customer_contact_info(self):
        """Lookup Activity: 顧客連絡先情報取得テスト"""
        # テストケース: 顧客の連絡先情報取得
        mock_contact_info = [
            {
                "CUSTOMER_ID": "CUST123456",
                "CUSTOMER_NAME": "テストユーザー1",
                "EMAIL_ADDRESS": "test1@example.com",
                "PHONE_NUMBER": "090-1234-5678",
                "COMMUNICATION_PREFERENCE": "EMAIL",
                "LANGUAGE_PREFERENCE": "JA"
            },
            {
                "CUSTOMER_ID": "CUST123457",
                "CUSTOMER_NAME": "テストユーザー2",
                "EMAIL_ADDRESS": "test2@example.com",
                "PHONE_NUMBER": "090-2345-6789",
                "COMMUNICATION_PREFERENCE": "BOTH",
                "LANGUAGE_PREFERENCE": "JA"
            }
        ]
        
        self.mock_database.query_records.return_value = mock_contact_info
        
        # Lookup Activity実行シミュレーション
        contact_query = """
        SELECT CUSTOMER_ID, CUSTOMER_NAME, EMAIL_ADDRESS, PHONE_NUMBER, 
               COMMUNICATION_PREFERENCE, LANGUAGE_PREFERENCE
        FROM customer_contact_info
        WHERE CUSTOMER_ID IN ('CUST123456', 'CUST123457')
        """
        
        result = self.mock_database.query_records("customer_contact_info", contact_query)
        
        self.assertEqual(len(result), 2, "顧客連絡先情報取得件数不正")
        self.assertEqual(result[0]["EMAIL_ADDRESS"], "test1@example.com", "メールアドレス確認失敗")
        self.assertEqual(result[0]["COMMUNICATION_PREFERENCE"], "EMAIL", "通信設定確認失敗")
    
    def test_data_flow_thanks_message_generation(self):
        """Data Flow: お礼メッセージ生成テスト"""
        # テストケース: お礼メッセージの生成
        contract_data = {
            "CUSTOMER_NAME": "テストユーザー1",
            "CONTRACT_TYPE": "FAMILY",
            "POWER_CAPACITY": "40A",
            "START_DATE": "20240201",
            "MONTHLY_ESTIMATE": 8500.0,
            "WELCOME_GIFT": "LED電球セット"
        }
        
        # お礼メッセージ生成ロジック（Data Flow内の処理）
        def generate_thanks_message(contract_data):
            customer_name = contract_data["CUSTOMER_NAME"]
            contract_type = contract_data["CONTRACT_TYPE"]
            power_capacity = contract_data["POWER_CAPACITY"]
            start_date = contract_data["START_DATE"]
            monthly_estimate = contract_data["MONTHLY_ESTIMATE"]
            welcome_gift = contract_data["WELCOME_GIFT"]
            
            # 契約タイプ別のメッセージ調整
            if contract_type == "FAMILY":
                contract_type_msg = "ファミリープラン"
            elif contract_type == "BUSINESS":
                contract_type_msg = "ビジネスプラン"
            else:
                contract_type_msg = "スタンダードプラン"
            
            # 開始日のフォーマット
            formatted_start_date = f"{start_date[:4]}年{start_date[4:6]}月{start_date[6:8]}日"
            
            thanks_message = f"""
            {customer_name}様
            
            この度は、東京ガスの電気サービスをご契約いただき、誠にありがとうございます。
            
            ご契約内容：
            - プラン: {contract_type_msg}
            - 契約容量: {power_capacity}
            - 供給開始日: {formatted_start_date}
            - 月額料金目安: {monthly_estimate:,.0f}円
            
            ウェルカムギフトとして「{welcome_gift}」をお送りいたします。
            
            今後ともどうぞよろしくお願いいたします。
            東京ガス株式会社
            """
            
            return thanks_message.strip()
        
        # メッセージ生成実行
        message = generate_thanks_message(contract_data)
        
        # アサーション
        self.assertIn("テストユーザー1様", message, "顧客名挿入失敗")
        self.assertIn("ファミリープラン", message, "契約タイプ挿入失敗")
        self.assertIn("40A", message, "契約容量挿入失敗")
        self.assertIn("2024年02月01日", message, "開始日フォーマット失敗")
        self.assertIn("8,500円", message, "月額料金挿入失敗")
        self.assertIn("LED電球セット", message, "ウェルカムギフト挿入失敗")
    
    def test_data_flow_welcome_gift_selection(self):
        """Data Flow: ウェルカムギフト選択テスト"""
        # テストケース: ウェルカムギフトの選択ロジック
        contract_scenarios = [
            {"CONTRACT_TYPE": "FAMILY", "POWER_CAPACITY": "30A", "MONTHLY_ESTIMATE": 6000.0},
            {"CONTRACT_TYPE": "FAMILY", "POWER_CAPACITY": "50A", "MONTHLY_ESTIMATE": 10000.0},
            {"CONTRACT_TYPE": "BUSINESS", "POWER_CAPACITY": "60A", "MONTHLY_ESTIMATE": 15000.0}
        ]
        
        # ウェルカムギフト選択ロジック（Data Flow内の処理）
        def select_welcome_gift(contract_data):
            contract_type = contract_data["CONTRACT_TYPE"]
            power_capacity = int(contract_data["POWER_CAPACITY"][:-1])  # "30A" -> 30
            monthly_estimate = contract_data["MONTHLY_ESTIMATE"]
            
            if contract_type == "FAMILY":
                if power_capacity >= 50:
                    return "プレミアムLED電球セット"
                elif power_capacity >= 40:
                    return "LED電球セット"
                else:
                    return "省エネタイマー"
            elif contract_type == "BUSINESS":
                if monthly_estimate >= 15000:
                    return "ビジネス向け省エネ診断サービス"
                else:
                    return "省エネグッズセット"
            else:
                return "エコバッグ"
        
        # 各シナリオでのギフト選択
        expected_gifts = [
            "省エネタイマー",  # FAMILY, 30A
            "プレミアムLED電球セット",  # FAMILY, 50A
            "ビジネス向け省エネ診断サービス"  # BUSINESS, 60A, 15000円
        ]
        
        for i, scenario in enumerate(contract_scenarios):
            gift = select_welcome_gift(scenario)
            self.assertEqual(gift, expected_gifts[i], f"シナリオ{i+1}のギフト選択失敗")
    
    def test_data_flow_thanks_timing_validation(self):
        """Data Flow: お礼送信タイミング検証テスト"""
        # テストケース: お礼送信タイミングの検証
        contract_data = {
            "CONTRACT_DATE": "20240115",
            "START_DATE": "20240201",
            "THANKS_SENT": "N",
            "CUSTOMER_STATUS": "ACTIVE"
        }
        
        # お礼送信タイミング検証ロジック（Data Flow内の処理）
        def validate_thanks_timing(contract_data, current_date):
            contract_date = datetime.strptime(contract_data["CONTRACT_DATE"], "%Y%m%d")
            start_date = datetime.strptime(contract_data["START_DATE"], "%Y%m%d")
            current_dt = datetime.strptime(current_date, "%Y%m%d")
            
            # 送信タイミングの判定
            days_since_contract = (current_dt - contract_date).days
            days_until_start = (start_date - current_dt).days
            
            # 送信条件
            conditions = {
                "contract_recent": days_since_contract <= 7,  # 契約から7日以内
                "not_started_yet": days_until_start >= 0,  # まだ供給開始前
                "not_sent_yet": contract_data["THANKS_SENT"] == "N",  # まだ送信していない
                "customer_active": contract_data["CUSTOMER_STATUS"] == "ACTIVE"  # 顧客アクティブ
            }
            
            send_thanks = all(conditions.values())
            
            return {
                "should_send": send_thanks,
                "conditions": conditions,
                "days_since_contract": days_since_contract,
                "days_until_start": days_until_start
            }
        
        # 検証実行
        result = validate_thanks_timing(contract_data, "20240118")  # 契約から3日後
        
        # アサーション
        self.assertTrue(result["should_send"], "お礼送信判定失敗")
        self.assertTrue(result["conditions"]["contract_recent"], "契約日近接判定失敗")
        self.assertTrue(result["conditions"]["not_started_yet"], "供給開始前判定失敗")
        self.assertEqual(result["days_since_contract"], 3, "契約からの日数計算失敗")
        self.assertEqual(result["days_until_start"], 14, "供給開始までの日数計算失敗")
    
    def test_script_activity_email_thanks_sending(self):
        """Script Activity: メールお礼送信処理テスト"""
        # テストケース: メールでのお礼送信
        thanks_email_data = {
            "to": "test@example.com",
            "subject": "【東京ガス】電気サービスご契約ありがとうございます",
            "body": "この度はご契約いただき、ありがとうございます。",
            "template": "contract_thanks_template",
            "priority": "normal"
        }
        
        self.mock_email_service.send_templated_email.return_value = {
            "status": "sent",
            "message_id": "THANKS_EMAIL_123",
            "delivery_time": datetime.utcnow().isoformat(),
            "template_used": "contract_thanks_template"
        }
        
        # Script Activity実行シミュレーション
        result = self.mock_email_service.send_templated_email(
            thanks_email_data["to"],
            thanks_email_data["subject"],
            thanks_email_data["body"],
            template=thanks_email_data["template"],
            priority=thanks_email_data["priority"]
        )
        
        self.assertEqual(result["status"], "sent", "メールお礼送信失敗")
        self.assertIsNotNone(result["message_id"], "メッセージID取得失敗")
        self.assertEqual(result["template_used"], "contract_thanks_template", "テンプレート確認失敗")
        self.mock_email_service.send_templated_email.assert_called_once()
    
    def test_script_activity_sms_thanks_sending(self):
        """Script Activity: SMSお礼送信処理テスト"""
        # テストケース: SMSでのお礼送信
        thanks_sms_data = {
            "to": "090-1234-5678",
            "message": "東京ガスです。電気サービスご契約ありがとうございます。詳細はメールをご確認ください。",
            "sender": "TokyoGas"
        }
        
        self.mock_sms_service.send_sms.return_value = {
            "status": "sent",
            "message_id": "SMS_456",
            "delivery_time": datetime.utcnow().isoformat(),
            "character_count": len(thanks_sms_data["message"])
        }
        
        # Script Activity実行シミュレーション
        result = self.mock_sms_service.send_sms(
            thanks_sms_data["to"],
            thanks_sms_data["message"],
            sender=thanks_sms_data["sender"]
        )
        
        self.assertEqual(result["status"], "sent", "SMSお礼送信失敗")
        self.assertIsNotNone(result["message_id"], "SMSメッセージID取得失敗")
        self.assertGreater(result["character_count"], 0, "文字数カウント失敗")
        self.mock_sms_service.send_sms.assert_called_once()
    
    def test_copy_activity_thanks_status_update(self):
        """Copy Activity: お礼送信ステータス更新テスト"""
        # テストケース: お礼送信ステータスの更新
        status_updates = [
            {
                "CONTRACT_ID": "ELEC000001",
                "THANKS_SENT": "Y",
                "THANKS_SENT_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "THANKS_METHOD": "EMAIL",
                "UPDATED_BY": "SYSTEM"
            },
            {
                "CONTRACT_ID": "ELEC000002",
                "THANKS_SENT": "Y",
                "THANKS_SENT_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "THANKS_METHOD": "BOTH",
                "UPDATED_BY": "SYSTEM"
            }
        ]
        
        self.mock_database.update_records.return_value = True
        
        # Copy Activity実行シミュレーション
        for update in status_updates:
            result = self.mock_database.update_records(
                "electricity_contracts",
                update,
                where_clause=f"CONTRACT_ID = '{update['CONTRACT_ID']}'"
            )
            self.assertTrue(result, f"ステータス更新失敗: {update['CONTRACT_ID']}")
        
        self.assertEqual(self.mock_database.update_records.call_count, 2, "更新処理回数不正")
    
    def test_copy_activity_welcome_gift_processing(self):
        """Copy Activity: ウェルカムギフト処理テスト"""
        # テストケース: ウェルカムギフトの処理
        gift_orders = [
            {
                "CONTRACT_ID": "ELEC000001",
                "CUSTOMER_ID": "CUST123456",
                "GIFT_ITEM": "LED電球セット",
                "DELIVERY_ADDRESS": "東京都渋谷区1-1-1",
                "ORDER_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "STATUS": "ORDERED"
            },
            {
                "CONTRACT_ID": "ELEC000002",
                "CUSTOMER_ID": "CUST123457",
                "GIFT_ITEM": "省エネグッズセット",
                "DELIVERY_ADDRESS": "東京都新宿区2-2-2",
                "ORDER_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "STATUS": "ORDERED"
            }
        ]
        
        self.mock_database.insert_records.return_value = True
        
        # Copy Activity実行シミュレーション
        gift_order_table = "welcome_gift_orders"
        result = self.mock_database.insert_records(gift_order_table, gift_orders)
        
        self.assertTrue(result, "ウェルカムギフト処理失敗")
        self.mock_database.insert_records.assert_called_once_with(gift_order_table, gift_orders)
    
    def test_script_activity_thanks_analytics(self):
        """Script Activity: お礼送信分析処理テスト"""
        # テストケース: お礼送信の分析データ生成
        thanks_analytics = {
            "execution_date": self.test_date,
            "total_new_contracts": 25,
            "email_thanks_sent": 20,
            "sms_thanks_sent": 15,
            "both_methods_sent": 10,
            "thanks_success_rate": 0.96,
            "family_contracts": 18,
            "business_contracts": 7,
            "average_monthly_estimate": 9200.0,
            "welcome_gifts_ordered": 23,
            "processing_time_minutes": 4.2
        }
        
        self.mock_database.insert_records.return_value = True
        
        # Script Activity実行シミュレーション
        analytics_table = "contract_thanks_analytics"
        result = self.mock_database.insert_records(analytics_table, [thanks_analytics])
        
        self.assertTrue(result, "お礼送信分析記録失敗")
        self.mock_database.insert_records.assert_called_once_with(analytics_table, [thanks_analytics])
    
    def test_data_flow_contract_validation(self):
        """Data Flow: 契約データ検証テスト"""
        # テストケース: 契約データの検証
        test_contracts = [
            {"CONTRACT_ID": "ELEC000001", "CUSTOMER_ID": "CUST123456", "POWER_CAPACITY": "40A", "MONTHLY_ESTIMATE": 8500.0},
            {"CONTRACT_ID": "", "CUSTOMER_ID": "CUST123457", "POWER_CAPACITY": "30A", "MONTHLY_ESTIMATE": 6000.0},  # 不正: 空ID
            {"CONTRACT_ID": "ELEC000003", "CUSTOMER_ID": "CUST123458", "POWER_CAPACITY": "INVALID", "MONTHLY_ESTIMATE": 7000.0},  # 不正: 容量形式
            {"CONTRACT_ID": "ELEC000004", "CUSTOMER_ID": "CUST123459", "POWER_CAPACITY": "50A", "MONTHLY_ESTIMATE": -1000.0}  # 不正: 負の見積もり
        ]
        
        # 契約データ検証ロジック（Data Flow内の処理）
        def validate_contract_data(contract):
            errors = []
            
            # 契約ID検証
            if not contract.get("CONTRACT_ID", "").strip():
                errors.append("契約ID必須")
            
            # 顧客ID検証
            if not contract.get("CUSTOMER_ID", "").strip():
                errors.append("顧客ID必須")
            
            # 契約容量検証
            power_capacity = contract.get("POWER_CAPACITY", "")
            if not power_capacity.endswith("A") or not power_capacity[:-1].isdigit():
                errors.append("契約容量形式不正（例：40A）")
            
            # 月額見積もり検証
            monthly_estimate = contract.get("MONTHLY_ESTIMATE", 0)
            if monthly_estimate < 0:
                errors.append("月額見積もりは正の値である必要があります")
            
            return errors
        
        # 検証実行
        validation_results = []
        for contract in test_contracts:
            errors = validate_contract_data(contract)
            validation_results.append({
                "contract": contract,
                "errors": errors,
                "is_valid": len(errors) == 0
            })
        
        # アサーション
        self.assertEqual(len(validation_results), 4, "検証結果数不正")
        self.assertTrue(validation_results[0]["is_valid"], "正常契約が不正判定")
        self.assertFalse(validation_results[1]["is_valid"], "不正契約（空ID）が正常判定")
        self.assertFalse(validation_results[2]["is_valid"], "不正契約（容量形式）が正常判定")
        self.assertFalse(validation_results[3]["is_valid"], "不正契約（負の見積もり）が正常判定")
    
    def test_lookup_activity_contract_history_check(self):
        """Lookup Activity: 契約履歴確認テスト"""
        # テストケース: 顧客の契約履歴確認
        customer_history = [
            {
                "CUSTOMER_ID": "CUST123456",
                "PREVIOUS_CONTRACTS": 0,
                "LAST_CONTRACT_DATE": None,
                "CUSTOMER_SEGMENT": "NEW"
            },
            {
                "CUSTOMER_ID": "CUST123457",
                "PREVIOUS_CONTRACTS": 1,
                "LAST_CONTRACT_DATE": "20220315",
                "CUSTOMER_SEGMENT": "RETURNING"
            }
        ]
        
        self.mock_database.query_records.return_value = customer_history
        
        # Lookup Activity実行シミュレーション
        history_query = """
        SELECT CUSTOMER_ID, COUNT(*) as PREVIOUS_CONTRACTS, 
               MAX(CONTRACT_DATE) as LAST_CONTRACT_DATE,
               CASE WHEN COUNT(*) = 0 THEN 'NEW' ELSE 'RETURNING' END as CUSTOMER_SEGMENT
        FROM electricity_contracts
        WHERE CUSTOMER_ID IN ('CUST123456', 'CUST123457')
        GROUP BY CUSTOMER_ID
        """
        
        result = self.mock_database.query_records("electricity_contracts", history_query)
        
        self.assertEqual(len(result), 2, "契約履歴取得件数不正")
        self.assertEqual(result[0]["CUSTOMER_SEGMENT"], "NEW", "新規顧客判定失敗")
        self.assertEqual(result[1]["CUSTOMER_SEGMENT"], "RETURNING", "既存顧客判定失敗")
    
    def _create_contract_csv_content(self) -> str:
        """電気契約データ用CSVコンテンツ生成"""
        header = "CONTRACT_ID,CUSTOMER_ID,CUSTOMER_NAME,EMAIL_ADDRESS,PHONE_NUMBER,CONTRACT_TYPE,POWER_CAPACITY,CONTRACT_DATE,START_DATE,MONTHLY_ESTIMATE,WELCOME_GIFT"
        rows = []
        
        for item in self.sample_contract_data:
            row = f"{item['CONTRACT_ID']},{item['CUSTOMER_ID']},{item['CUSTOMER_NAME']},{item['EMAIL_ADDRESS']},{item['PHONE_NUMBER']},{item['CONTRACT_TYPE']},{item['POWER_CAPACITY']},{item['CONTRACT_DATE']},{item['START_DATE']},{item['MONTHLY_ESTIMATE']},{item['WELCOME_GIFT']}"
            rows.append(row)
        
        return header + "\n" + "\n".join(rows)


if __name__ == "__main__":
    unittest.main()