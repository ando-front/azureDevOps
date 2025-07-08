"""
pi_CustmNoRegistComp パイプラインのユニットテスト

顧客番号登録完了通知送信パイプラインの個別コンポーネントテスト
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any


class TestCustomerRegistrationCompletionUnit(unittest.TestCase):
    """顧客番号登録完了通知パイプラインユニットテスト"""
    
    def setUp(self):
        """テストセットアップ"""
        self.pipeline_name = "pi_CustmNoRegistComp"
        self.domain = "smc"
        
        # モックサービス
        self.mock_blob_storage = Mock()
        self.mock_database = Mock()
        self.mock_email_service = Mock()
        self.mock_sms_service = Mock()
        
        # テストデータ
        self.test_date = datetime.utcnow().strftime('%Y%m%d')
        self.test_file_path = f"CustomerRegistrationCompletion/{self.test_date}/customer_registration_completion.csv"
        
        # 基本的な顧客番号登録完了データ
        self.sample_registration_completion_data = [
            {
                "COMPLETION_ID": "COMP000001",
                "CUSTOMER_ID": "CUST123456",
                "CUSTOMER_NAME": "テストユーザー1",
                "EMAIL_ADDRESS": "test1@example.com",
                "PHONE_NUMBER": "090-1234-5678",
                "CUSTOMER_NUMBER": "12345678",
                "REGISTRATION_DATE": "20240215",
                "COMPLETION_DATE": "20240216",
                "REGISTRATION_TYPE": "ONLINE",
                "SERVICE_TYPE": "GAS",
                "CONTRACT_STATUS": "ACTIVE",
                "WELCOME_PACKAGE_SENT": "N"
            },
            {
                "COMPLETION_ID": "COMP000002",
                "CUSTOMER_ID": "CUST123457",
                "CUSTOMER_NAME": "テストユーザー2",
                "EMAIL_ADDRESS": "test2@example.com",
                "PHONE_NUMBER": "090-2345-6789",
                "CUSTOMER_NUMBER": "12345679",
                "REGISTRATION_DATE": "20240216",
                "COMPLETION_DATE": "20240217",
                "REGISTRATION_TYPE": "PHONE",
                "SERVICE_TYPE": "GAS_ELECTRIC",
                "CONTRACT_STATUS": "ACTIVE",
                "WELCOME_PACKAGE_SENT": "N"
            }
        ]
    
    def test_lookup_activity_registration_completion_detection(self):
        """Lookup Activity: 登録完了検出テスト"""
        # テストケース: 顧客番号登録完了の検出
        mock_completions = [
            {
                "COMPLETION_ID": "COMP000001",
                "CUSTOMER_ID": "CUST123456",
                "CUSTOMER_NUMBER": "12345678",
                "REGISTRATION_DATE": "20240215",
                "COMPLETION_DATE": "20240216",
                "REGISTRATION_TYPE": "ONLINE",
                "CONTRACT_STATUS": "ACTIVE",
                "WELCOME_PACKAGE_SENT": "N"
            },
            {
                "COMPLETION_ID": "COMP000002",
                "CUSTOMER_ID": "CUST123457",
                "CUSTOMER_NUMBER": "12345679",
                "REGISTRATION_DATE": "20240216",
                "COMPLETION_DATE": "20240217",
                "REGISTRATION_TYPE": "PHONE",
                "CONTRACT_STATUS": "ACTIVE",
                "WELCOME_PACKAGE_SENT": "N"
            }
        ]
        
        self.mock_database.query_records.return_value = mock_completions
        
        # Lookup Activity実行シミュレーション
        completion_query = f"""
        SELECT COMPLETION_ID, CUSTOMER_ID, CUSTOMER_NUMBER, REGISTRATION_DATE, 
               COMPLETION_DATE, REGISTRATION_TYPE, CONTRACT_STATUS, WELCOME_PACKAGE_SENT
        FROM customer_registration_completions
        WHERE COMPLETION_DATE >= '{self.test_date}' AND CONTRACT_STATUS = 'ACTIVE' 
        AND WELCOME_PACKAGE_SENT = 'N'
        """
        
        result = self.mock_database.query_records("customer_registration_completions", completion_query)
        
        self.assertEqual(len(result), 2, "登録完了検出件数不正")
        self.assertEqual(result[0]["COMPLETION_ID"], "COMP000001", "完了ID確認失敗")
        self.assertEqual(result[0]["CONTRACT_STATUS"], "ACTIVE", "契約ステータス確認失敗")
        self.assertEqual(result[0]["WELCOME_PACKAGE_SENT"], "N", "ウェルカムパッケージ送信フラグ確認失敗")
    
    def test_lookup_activity_customer_service_details(self):
        """Lookup Activity: 顧客サービス詳細取得テスト"""
        # テストケース: 顧客のサービス詳細取得
        mock_service_details = [
            {
                "CUSTOMER_ID": "CUST123456",
                "SERVICE_TYPE": "GAS",
                "SERVICE_ADDRESS": "東京都渋谷区1-1-1",
                "SERVICE_START_DATE": "20240220",
                "METER_NUMBER": "M1234567890",
                "TARIFF_PLAN": "STANDARD"
            },
            {
                "CUSTOMER_ID": "CUST123457",
                "SERVICE_TYPE": "GAS_ELECTRIC",
                "SERVICE_ADDRESS": "東京都新宿区2-2-2",
                "SERVICE_START_DATE": "20240221",
                "METER_NUMBER": "M2345678901",
                "TARIFF_PLAN": "PREMIUM"
            }
        ]
        
        self.mock_database.query_records.return_value = mock_service_details
        
        # Lookup Activity実行シミュレーション
        service_query = """
        SELECT CUSTOMER_ID, SERVICE_TYPE, SERVICE_ADDRESS, SERVICE_START_DATE, 
               METER_NUMBER, TARIFF_PLAN
        FROM customer_service_details
        WHERE CUSTOMER_ID IN ('CUST123456', 'CUST123457')
        """
        
        result = self.mock_database.query_records("customer_service_details", service_query)
        
        self.assertEqual(len(result), 2, "顧客サービス詳細取得件数不正")
        self.assertEqual(result[0]["SERVICE_TYPE"], "GAS", "サービスタイプ確認失敗")
        self.assertEqual(result[0]["TARIFF_PLAN"], "STANDARD", "料金プラン確認失敗")
    
    def test_data_flow_welcome_message_generation(self):
        """Data Flow: ウェルカムメッセージ生成テスト"""
        # テストケース: ウェルカムメッセージの生成
        completion_data = {
            "CUSTOMER_NAME": "テストユーザー1",
            "CUSTOMER_NUMBER": "12345678",
            "REGISTRATION_DATE": "20240215",
            "COMPLETION_DATE": "20240216",
            "REGISTRATION_TYPE": "ONLINE",
            "SERVICE_TYPE": "GAS",
            "SERVICE_ADDRESS": "東京都渋谷区1-1-1",
            "SERVICE_START_DATE": "20240220",
            "METER_NUMBER": "M1234567890"
        }
        
        # ウェルカムメッセージ生成ロジック（Data Flow内の処理）
        def generate_welcome_message(completion_data):
            customer_name = completion_data["CUSTOMER_NAME"]
            customer_number = completion_data["CUSTOMER_NUMBER"]
            registration_date = completion_data["REGISTRATION_DATE"]
            completion_date = completion_data["COMPLETION_DATE"]
            registration_type = completion_data["REGISTRATION_TYPE"]
            service_type = completion_data["SERVICE_TYPE"]
            service_address = completion_data["SERVICE_ADDRESS"]
            service_start_date = completion_data["SERVICE_START_DATE"]
            meter_number = completion_data["METER_NUMBER"]
            
            # 日付のフォーマット
            formatted_registration_date = f"{registration_date[:4]}年{registration_date[4:6]}月{registration_date[6:8]}日"
            formatted_completion_date = f"{completion_date[:4]}年{completion_date[4:6]}月{completion_date[6:8]}日"
            formatted_service_start_date = f"{service_start_date[:4]}年{service_start_date[4:6]}月{service_start_date[6:8]}日"
            
            # 登録タイプ別のメッセージ
            if registration_type == "ONLINE":
                registration_msg = "オンライン"
            elif registration_type == "PHONE":
                registration_msg = "お電話"
            else:
                registration_msg = "窓口"
            
            # サービスタイプ別のメッセージ
            if service_type == "GAS":
                service_msg = "ガス"
            elif service_type == "ELECTRIC":
                service_msg = "電気"
            else:
                service_msg = "ガス・電気"
            
            welcome_message = f"""
            {customer_name}様
            
            東京ガスへのご登録ありがとうございます
            
            {registration_msg}でのお申し込みをいただき、顧客番号の登録が完了いたしました。
            
            ご登録情報：
            - 顧客番号：{customer_number}
            - 申込日：{formatted_registration_date}
            - 登録完了日：{formatted_completion_date}
            - サービス内容：{service_msg}
            - サービス開始日：{formatted_service_start_date}
            - サービス住所：{service_address}
            - メーター番号：{meter_number}
            
            今後、各種お手続きの際には顧客番号をご利用ください。
            
            ご不明な点がございましたら、お気軽にお問い合わせください。
            
            東京ガス株式会社
            """
            
            return welcome_message.strip()
        
        # メッセージ生成実行
        message = generate_welcome_message(completion_data)
        
        # アサーション
        self.assertIn("テストユーザー1様", message, "顧客名挿入失敗")
        self.assertIn("東京ガスへのご登録ありがとうございます", message, "タイトル挿入失敗")
        self.assertIn("12345678", message, "顧客番号挿入失敗")
        self.assertIn("2024年02月15日", message, "申込日フォーマット失敗")
        self.assertIn("2024年02月16日", message, "登録完了日フォーマット失敗")
        self.assertIn("オンライン", message, "登録タイプ挿入失敗")
        self.assertIn("ガス", message, "サービスタイプ挿入失敗")
        self.assertIn("東京都渋谷区1-1-1", message, "サービス住所挿入失敗")
        self.assertIn("M1234567890", message, "メーター番号挿入失敗")
    
    def test_data_flow_welcome_package_preparation(self):
        """Data Flow: ウェルカムパッケージ準備テスト"""
        # テストケース: ウェルカムパッケージの準備
        package_scenarios = [
            {
                "SERVICE_TYPE": "GAS",
                "REGISTRATION_TYPE": "ONLINE",
                "CUSTOMER_TIER": "STANDARD",
                "EXPECTED_ITEMS": ["ガス使用開始ガイド", "料金プラン説明書", "安全使用ガイド", "オンライン会員特典案内"]
            },
            {
                "SERVICE_TYPE": "GAS_ELECTRIC",
                "REGISTRATION_TYPE": "PHONE",
                "CUSTOMER_TIER": "PREMIUM",
                "EXPECTED_ITEMS": ["総合サービスガイド", "プレミアムプラン案内", "安全使用ガイド", "カスタマーサポート案内"]
            }
        ]
        
        # ウェルカムパッケージ準備ロジック（Data Flow内の処理）
        def prepare_welcome_package(package_data):
            service_type = package_data["SERVICE_TYPE"]
            registration_type = package_data["REGISTRATION_TYPE"]
            customer_tier = package_data["CUSTOMER_TIER"]
            
            package_items = []
            
            # サービスタイプ別の基本アイテム
            if service_type == "GAS":
                package_items.extend(["ガス使用開始ガイド", "料金プラン説明書", "安全使用ガイド"])
            elif service_type == "ELECTRIC":
                package_items.extend(["電気使用開始ガイド", "電気料金プラン説明書", "省エネガイド"])
            else:  # GAS_ELECTRIC
                package_items.extend(["総合サービスガイド", "統合料金プラン説明書", "安全使用ガイド"])
            
            # 登録タイプ別の追加アイテム
            if registration_type == "ONLINE":
                package_items.append("オンライン会員特典案内")
            elif registration_type == "PHONE":
                package_items.append("電話サポート案内")
            
            # 顧客ティア別の追加アイテム
            if customer_tier == "PREMIUM":
                package_items.extend(["プレミアムプラン案内", "優待特典案内"])
            elif customer_tier == "BUSINESS":
                package_items.extend(["法人向けサービス案内", "業務効率化ガイド"])
            
            # 共通アイテム
            package_items.append("カスタマーサポート案内")
            
            return {
                "package_items": package_items,
                "total_items": len(package_items),
                "estimated_delivery_days": 3 if customer_tier == "PREMIUM" else 5
            }
        
        # 各シナリオでのパッケージ準備
        for scenario in package_scenarios:
            package_result = prepare_welcome_package(scenario)
            
            # 期待されるアイテムが含まれているか確認
            for expected_item in scenario["EXPECTED_ITEMS"]:
                self.assertIn(expected_item, package_result["package_items"], 
                             f"期待アイテム不足: {expected_item} in {scenario}")
    
    def test_data_flow_customer_onboarding_schedule(self):
        """Data Flow: 顧客オンボーディングスケジュール処理テスト"""
        # テストケース: 顧客オンボーディングスケジュール
        onboarding_data = {
            "CUSTOMER_ID": "CUST123456",
            "COMPLETION_DATE": "20240216",
            "SERVICE_START_DATE": "20240220",
            "REGISTRATION_TYPE": "ONLINE",
            "SERVICE_TYPE": "GAS"
        }
        
        # オンボーディングスケジュール生成ロジック（Data Flow内の処理）
        def generate_onboarding_schedule(onboarding_data):
            customer_id = onboarding_data["CUSTOMER_ID"]
            completion_date = datetime.strptime(onboarding_data["COMPLETION_DATE"], "%Y%m%d")
            service_start_date = datetime.strptime(onboarding_data["SERVICE_START_DATE"], "%Y%m%d")
            registration_type = onboarding_data["REGISTRATION_TYPE"]
            service_type = onboarding_data["SERVICE_TYPE"]
            
            schedule_items = []
            
            # Day 1: 登録完了通知
            schedule_items.append({
                "day": 1,
                "date": completion_date.strftime("%Y%m%d"),
                "action": "登録完了通知送信",
                "status": "PENDING"
            })
            
            # Day 2: ウェルカムパッケージ発送
            package_date = completion_date + timedelta(days=1)
            schedule_items.append({
                "day": 2,
                "date": package_date.strftime("%Y%m%d"),
                "action": "ウェルカムパッケージ発送",
                "status": "PENDING"
            })
            
            # Day 3: オンライン会員登録案内（オンライン登録の場合）
            if registration_type == "ONLINE":
                online_guide_date = completion_date + timedelta(days=2)
                schedule_items.append({
                    "day": 3,
                    "date": online_guide_date.strftime("%Y%m%d"),
                    "action": "オンライン会員登録案内",
                    "status": "PENDING"
                })
            
            # サービス開始日: 使用開始確認
            schedule_items.append({
                "day": (service_start_date - completion_date).days + 1,
                "date": service_start_date.strftime("%Y%m%d"),
                "action": "サービス使用開始確認",
                "status": "PENDING"
            })
            
            # サービス開始後1週間: 満足度調査
            satisfaction_date = service_start_date + timedelta(days=7)
            schedule_items.append({
                "day": (satisfaction_date - completion_date).days + 1,
                "date": satisfaction_date.strftime("%Y%m%d"),
                "action": "満足度調査送信",
                "status": "PENDING"
            })
            
            return {
                "customer_id": customer_id,
                "schedule_items": schedule_items,
                "total_actions": len(schedule_items)
            }
        
        # オンボーディングスケジュール生成
        schedule = generate_onboarding_schedule(onboarding_data)
        
        # アサーション
        self.assertEqual(schedule["customer_id"], "CUST123456", "顧客ID確認失敗")
        self.assertGreater(schedule["total_actions"], 0, "スケジュールアクション数不正")
        self.assertEqual(schedule["schedule_items"][0]["action"], "登録完了通知送信", "最初のアクション確認失敗")
        
        # オンライン登録特有のアクションが含まれているか確認
        online_actions = [item for item in schedule["schedule_items"] if "オンライン会員登録案内" in item["action"]]
        self.assertEqual(len(online_actions), 1, "オンライン会員登録案内アクション確認失敗")
    
    def test_script_activity_welcome_email_sending(self):
        """Script Activity: ウェルカムメール送信処理テスト"""
        # テストケース: ウェルカムメールの送信
        welcome_email_data = {
            "to": "test@example.com",
            "subject": "【東京ガス】ご登録完了のお知らせ",
            "body": "東京ガスへのご登録ありがとうございます。",
            "completion_id": "COMP000001",
            "template": "welcome_template"
        }
        
        self.mock_email_service.send_welcome_email.return_value = {
            "status": "sent",
            "message_id": "WELCOME_EMAIL_123",
            "delivery_time": datetime.utcnow().isoformat(),
            "completion_id": "COMP000001"
        }
        
        # Script Activity実行シミュレーション
        result = self.mock_email_service.send_welcome_email(
            welcome_email_data["to"],
            welcome_email_data["subject"],
            welcome_email_data["body"],
            completion_id=welcome_email_data["completion_id"],
            template=welcome_email_data["template"]
        )
        
        self.assertEqual(result["status"], "sent", "ウェルカムメール送信失敗")
        self.assertIsNotNone(result["message_id"], "メッセージID取得失敗")
        self.assertEqual(result["completion_id"], "COMP000001", "完了ID確認失敗")
        self.mock_email_service.send_welcome_email.assert_called_once()
    
    def test_script_activity_sms_notification_sending(self):
        """Script Activity: SMS通知送信処理テスト"""
        # テストケース: SMS通知の送信
        sms_data = {
            "to": "090-1234-5678",
            "message": "東京ガスです。顧客番号登録が完了しました。顧客番号：12345678。詳細はメールをご確認ください。",
            "completion_id": "COMP000001",
            "sender": "TokyoGas"
        }
        
        self.mock_sms_service.send_welcome_sms.return_value = {
            "status": "sent",
            "message_id": "WELCOME_SMS_456",
            "delivery_time": datetime.utcnow().isoformat(),
            "completion_id": "COMP000001",
            "character_count": len(sms_data["message"])
        }
        
        # Script Activity実行シミュレーション
        result = self.mock_sms_service.send_welcome_sms(
            sms_data["to"],
            sms_data["message"],
            completion_id=sms_data["completion_id"],
            sender=sms_data["sender"]
        )
        
        self.assertEqual(result["status"], "sent", "SMS通知送信失敗")
        self.assertIsNotNone(result["message_id"], "SMSメッセージID取得失敗")
        self.assertEqual(result["completion_id"], "COMP000001", "完了ID確認失敗")
        self.assertGreater(result["character_count"], 0, "文字数カウント失敗")
        self.mock_sms_service.send_welcome_sms.assert_called_once()
    
    def test_copy_activity_welcome_package_log(self):
        """Copy Activity: ウェルカムパッケージログ記録テスト"""
        # テストケース: ウェルカムパッケージログの記録
        package_logs = [
            {
                "COMPLETION_ID": "COMP000001",
                "CUSTOMER_ID": "CUST123456",
                "PACKAGE_TYPE": "STANDARD",
                "SENT_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "SENT_TIME": datetime.utcnow().strftime('%H:%M:%S'),
                "DELIVERY_METHOD": "EMAIL",
                "STATUS": "SENT",
                "MESSAGE_ID": "WELCOME_EMAIL_123"
            },
            {
                "COMPLETION_ID": "COMP000002",
                "CUSTOMER_ID": "CUST123457",
                "PACKAGE_TYPE": "PREMIUM",
                "SENT_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "SENT_TIME": datetime.utcnow().strftime('%H:%M:%S'),
                "DELIVERY_METHOD": "BOTH",
                "STATUS": "SENT",
                "MESSAGE_ID": "WELCOME_EMAIL_124"
            }
        ]
        
        self.mock_database.insert_records.return_value = True
        
        # Copy Activity実行シミュレーション
        package_log_table = "welcome_package_logs"
        result = self.mock_database.insert_records(package_log_table, package_logs)
        
        self.assertTrue(result, "ウェルカムパッケージログ記録失敗")
        self.mock_database.insert_records.assert_called_once_with(package_log_table, package_logs)
    
    def test_copy_activity_completion_status_update(self):
        """Copy Activity: 完了ステータス更新テスト"""
        # テストケース: 完了ステータスの更新
        status_updates = [
            {
                "COMPLETION_ID": "COMP000001",
                "WELCOME_PACKAGE_SENT": "Y",
                "WELCOME_PACKAGE_SENT_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "UPDATED_BY": "SYSTEM"
            },
            {
                "COMPLETION_ID": "COMP000002",
                "WELCOME_PACKAGE_SENT": "Y",
                "WELCOME_PACKAGE_SENT_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "UPDATED_BY": "SYSTEM"
            }
        ]
        
        self.mock_database.update_records.return_value = True
        
        # Copy Activity実行シミュレーション
        for update in status_updates:
            result = self.mock_database.update_records(
                "customer_registration_completions",
                update,
                where_clause=f"COMPLETION_ID = '{update['COMPLETION_ID']}'"
            )
            self.assertTrue(result, f"完了ステータス更新失敗: {update['COMPLETION_ID']}")
        
        self.assertEqual(self.mock_database.update_records.call_count, 2, "更新処理回数不正")
    
    def test_script_activity_onboarding_analytics(self):
        """Script Activity: オンボーディング分析処理テスト"""
        # テストケース: オンボーディング分析データ生成
        onboarding_analytics = {
            "execution_date": self.test_date,
            "total_completions": 65,
            "online_registrations": 45,
            "phone_registrations": 15,
            "office_registrations": 5,
            "gas_only_services": 35,
            "electric_only_services": 10,
            "combined_services": 20,
            "welcome_emails_sent": 62,
            "welcome_sms_sent": 40,
            "package_delivery_success_rate": 0.95,
            "average_completion_time_hours": 18.5,
            "processing_time_minutes": 4.2
        }
        
        self.mock_database.insert_records.return_value = True
        
        # Script Activity実行シミュレーション
        analytics_table = "customer_onboarding_analytics"
        result = self.mock_database.insert_records(analytics_table, [onboarding_analytics])
        
        self.assertTrue(result, "オンボーディング分析記録失敗")
        self.mock_database.insert_records.assert_called_once_with(analytics_table, [onboarding_analytics])
    
    def test_data_flow_completion_validation(self):
        """Data Flow: 完了データ検証テスト"""
        # テストケース: 完了データの検証
        test_completions = [
            {"CUSTOMER_ID": "CUST123456", "CUSTOMER_NUMBER": "12345678", "REGISTRATION_DATE": "20240215", "COMPLETION_DATE": "20240216"},
            {"CUSTOMER_ID": "", "CUSTOMER_NUMBER": "12345679", "REGISTRATION_DATE": "20240216", "COMPLETION_DATE": "20240217"},  # 不正: 空ID
            {"CUSTOMER_ID": "CUST123458", "CUSTOMER_NUMBER": "", "REGISTRATION_DATE": "20240217", "COMPLETION_DATE": "20240218"},  # 不正: 空顧客番号
            {"CUSTOMER_ID": "CUST123459", "CUSTOMER_NUMBER": "12345680", "REGISTRATION_DATE": "20240220", "COMPLETION_DATE": "20240218"}  # 不正: 完了日が申込日より前
        ]
        
        # 完了データ検証ロジック（Data Flow内の処理）
        def validate_completion_data(completion):
            errors = []
            
            # 顧客ID検証
            if not completion.get("CUSTOMER_ID", "").strip():
                errors.append("顧客ID必須")
            
            # 顧客番号検証
            customer_number = completion.get("CUSTOMER_NUMBER", "")
            if not customer_number.strip():
                errors.append("顧客番号必須")
            elif len(customer_number) != 8:
                errors.append("顧客番号は8桁である必要があります")
            
            # 日付検証
            registration_date = completion.get("REGISTRATION_DATE", "")
            completion_date = completion.get("COMPLETION_DATE", "")
            
            if completion_date <= registration_date:
                errors.append("完了日は申込日より後である必要があります")
            
            return errors
        
        # 検証実行
        validation_results = []
        for completion in test_completions:
            errors = validate_completion_data(completion)
            validation_results.append({
                "completion": completion,
                "errors": errors,
                "is_valid": len(errors) == 0
            })
        
        # アサーション
        self.assertEqual(len(validation_results), 4, "検証結果数不正")
        self.assertTrue(validation_results[0]["is_valid"], "正常完了が不正判定")
        self.assertFalse(validation_results[1]["is_valid"], "不正完了（空ID）が正常判定")
        self.assertFalse(validation_results[2]["is_valid"], "不正完了（空顧客番号）が正常判定")
        self.assertFalse(validation_results[3]["is_valid"], "不正完了（日付不正）が正常判定")
    
    def test_lookup_activity_completion_statistics(self):
        """Lookup Activity: 完了統計取得テスト"""
        # テストケース: 完了の統計情報取得
        completion_statistics = [
            {
                "REGISTRATION_TYPE": "ONLINE",
                "COMPLETION_COUNT": 45,
                "AVERAGE_COMPLETION_TIME": 16.2,
                "SUCCESS_RATE": 0.95
            },
            {
                "REGISTRATION_TYPE": "PHONE",
                "COMPLETION_COUNT": 15,
                "AVERAGE_COMPLETION_TIME": 24.8,
                "SUCCESS_RATE": 0.92
            }
        ]
        
        self.mock_database.query_records.return_value = completion_statistics
        
        # Lookup Activity実行シミュレーション
        statistics_query = f"""
        SELECT REGISTRATION_TYPE, 
               COUNT(*) as COMPLETION_COUNT,
               AVG(DATEDIFF(hour, REGISTRATION_DATE, COMPLETION_DATE)) as AVERAGE_COMPLETION_TIME,
               CAST(COUNT(*) AS FLOAT) / (SELECT COUNT(*) FROM customer_registrations WHERE REGISTRATION_DATE >= '{self.test_date}') as SUCCESS_RATE
        FROM customer_registration_completions
        WHERE COMPLETION_DATE >= '{self.test_date}'
        GROUP BY REGISTRATION_TYPE
        """
        
        result = self.mock_database.query_records("customer_registration_completions", statistics_query)
        
        self.assertEqual(len(result), 2, "完了統計取得件数不正")
        self.assertEqual(result[0]["REGISTRATION_TYPE"], "ONLINE", "登録タイプ確認失敗")
        self.assertEqual(result[0]["COMPLETION_COUNT"], 45, "完了件数確認失敗")
        self.assertEqual(result[0]["AVERAGE_COMPLETION_TIME"], 16.2, "平均完了時間確認失敗")
    
    def _create_customer_registration_completion_csv_content(self) -> str:
        """顧客番号登録完了データ用CSVコンテンツ生成"""
        header = "COMPLETION_ID,CUSTOMER_ID,CUSTOMER_NAME,EMAIL_ADDRESS,PHONE_NUMBER,CUSTOMER_NUMBER,REGISTRATION_DATE,COMPLETION_DATE,REGISTRATION_TYPE,SERVICE_TYPE,CONTRACT_STATUS,WELCOME_PACKAGE_SENT"
        rows = []
        
        for item in self.sample_registration_completion_data:
            row = f"{item['COMPLETION_ID']},{item['CUSTOMER_ID']},{item['CUSTOMER_NAME']},{item['EMAIL_ADDRESS']},{item['PHONE_NUMBER']},{item['CUSTOMER_NUMBER']},{item['REGISTRATION_DATE']},{item['COMPLETION_DATE']},{item['REGISTRATION_TYPE']},{item['SERVICE_TYPE']},{item['CONTRACT_STATUS']},{item['WELCOME_PACKAGE_SENT']}"
            rows.append(row)
        
        return header + "\n" + "\n".join(rows)


if __name__ == "__main__":
    unittest.main()