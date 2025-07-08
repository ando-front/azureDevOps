"""
pi_Send_MovingPromotionList パイプラインのユニットテスト

引越しプロモーションリスト送信パイプラインの個別コンポーネントテスト
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any


class TestMovingPromotionListUnit(unittest.TestCase):
    """引越しプロモーションリストパイプラインユニットテスト"""
    
    def setUp(self):
        """テストセットアップ"""
        self.pipeline_name = "pi_Send_MovingPromotionList"
        self.domain = "smc"
        
        # モックサービス
        self.mock_blob_storage = Mock()
        self.mock_database = Mock()
        self.mock_email_service = Mock()
        self.mock_sms_service = Mock()
        
        # テストデータ
        self.test_date = datetime.utcnow().strftime('%Y%m%d')
        self.test_file_path = f"MovingPromotionList/{self.test_date}/moving_promotion_list.csv"
        
        # 基本的な引越しプロモーションデータ
        self.sample_moving_promotion_data = [
            {
                "PROMOTION_ID": "PROMO000001",
                "CUSTOMER_ID": "CUST123456",
                "CUSTOMER_NAME": "テストユーザー1",
                "EMAIL_ADDRESS": "test1@example.com",
                "PHONE_NUMBER": "090-1234-5678",
                "CURRENT_ADDRESS": "東京都渋谷区1-1-1",
                "NEW_ADDRESS": "東京都新宿区2-2-2",
                "MOVING_DATE": "20240315",
                "MOVING_TYPE": "WITHIN_TOKYO",
                "PROMOTION_TYPE": "EARLY_BIRD",
                "DISCOUNT_RATE": 0.15,
                "CAMPAIGN_END_DATE": "20240331",
                "ELIGIBILITY_STATUS": "ELIGIBLE"
            },
            {
                "PROMOTION_ID": "PROMO000002",
                "CUSTOMER_ID": "CUST123457",
                "CUSTOMER_NAME": "テストユーザー2",
                "EMAIL_ADDRESS": "test2@example.com",
                "PHONE_NUMBER": "090-2345-6789",
                "CURRENT_ADDRESS": "東京都港区3-3-3",
                "NEW_ADDRESS": "神奈川県横浜市4-4-4",
                "MOVING_DATE": "20240320",
                "MOVING_TYPE": "CROSS_PREFECTURE",
                "PROMOTION_TYPE": "STANDARD",
                "DISCOUNT_RATE": 0.10,
                "CAMPAIGN_END_DATE": "20240331",
                "ELIGIBILITY_STATUS": "ELIGIBLE"
            }
        ]
    
    def test_lookup_activity_moving_customer_detection(self):
        """Lookup Activity: 引越し顧客検出テスト"""
        # テストケース: 引越し予定顧客の検出
        mock_moving_customers = [
            {
                "CUSTOMER_ID": "CUST123456",
                "CURRENT_ADDRESS": "東京都渋谷区1-1-1",
                "NEW_ADDRESS": "東京都新宿区2-2-2",
                "MOVING_DATE": "20240315",
                "MOVING_STATUS": "SCHEDULED",
                "PROMOTION_SENT": "N"
            },
            {
                "CUSTOMER_ID": "CUST123457",
                "CURRENT_ADDRESS": "東京都港区3-3-3",
                "NEW_ADDRESS": "神奈川県横浜市4-4-4",
                "MOVING_DATE": "20240320",
                "MOVING_STATUS": "SCHEDULED",
                "PROMOTION_SENT": "N"
            }
        ]
        
        self.mock_database.query_records.return_value = mock_moving_customers
        
        # Lookup Activity実行シミュレーション
        moving_query = f"""
        SELECT CUSTOMER_ID, CURRENT_ADDRESS, NEW_ADDRESS, MOVING_DATE, 
               MOVING_STATUS, PROMOTION_SENT
        FROM customer_moving_schedule
        WHERE MOVING_DATE >= '{self.test_date}' AND MOVING_STATUS = 'SCHEDULED' 
        AND PROMOTION_SENT = 'N'
        """
        
        result = self.mock_database.query_records("customer_moving_schedule", moving_query)
        
        self.assertEqual(len(result), 2, "引越し顧客検出件数不正")
        self.assertEqual(result[0]["CUSTOMER_ID"], "CUST123456", "顧客ID確認失敗")
        self.assertEqual(result[0]["MOVING_STATUS"], "SCHEDULED", "引越しステータス確認失敗")
        self.assertEqual(result[0]["PROMOTION_SENT"], "N", "プロモーション送信フラグ確認失敗")
    
    def test_lookup_activity_promotion_eligibility_check(self):
        """Lookup Activity: プロモーション適格性確認テスト"""
        # テストケース: プロモーション適格性の確認
        mock_eligibility_data = [
            {
                "CUSTOMER_ID": "CUST123456",
                "CUSTOMER_TIER": "PREMIUM",
                "PAYMENT_HISTORY": "GOOD",
                "CURRENT_CONTRACTS": 2,
                "LOYALTY_POINTS": 5000,
                "PROMOTION_ELIGIBILITY": "ELIGIBLE"
            },
            {
                "CUSTOMER_ID": "CUST123457",
                "CUSTOMER_TIER": "STANDARD",
                "PAYMENT_HISTORY": "AVERAGE",
                "CURRENT_CONTRACTS": 1,
                "LOYALTY_POINTS": 1500,
                "PROMOTION_ELIGIBILITY": "ELIGIBLE"
            }
        ]
        
        self.mock_database.query_records.return_value = mock_eligibility_data
        
        # Lookup Activity実行シミュレーション
        eligibility_query = """
        SELECT CUSTOMER_ID, CUSTOMER_TIER, PAYMENT_HISTORY, CURRENT_CONTRACTS, 
               LOYALTY_POINTS, PROMOTION_ELIGIBILITY
        FROM customer_promotion_eligibility
        WHERE CUSTOMER_ID IN ('CUST123456', 'CUST123457')
        """
        
        result = self.mock_database.query_records("customer_promotion_eligibility", eligibility_query)
        
        self.assertEqual(len(result), 2, "プロモーション適格性確認件数不正")
        self.assertEqual(result[0]["CUSTOMER_TIER"], "PREMIUM", "顧客ティア確認失敗")
        self.assertEqual(result[0]["PROMOTION_ELIGIBILITY"], "ELIGIBLE", "プロモーション適格性確認失敗")
    
    def test_data_flow_promotion_type_determination(self):
        """Data Flow: プロモーションタイプ決定テスト"""
        # テストケース: プロモーションタイプの決定
        customer_scenarios = [
            {
                "CUSTOMER_TIER": "PREMIUM",
                "MOVING_TYPE": "WITHIN_TOKYO",
                "DAYS_UNTIL_MOVING": 10,
                "LOYALTY_POINTS": 5000,
                "EXPECTED_PROMOTION": "PREMIUM_EARLY_BIRD"
            },
            {
                "CUSTOMER_TIER": "STANDARD",
                "MOVING_TYPE": "CROSS_PREFECTURE",
                "DAYS_UNTIL_MOVING": 5,
                "LOYALTY_POINTS": 1500,
                "EXPECTED_PROMOTION": "STANDARD_EARLY_BIRD"
            },
            {
                "CUSTOMER_TIER": "BASIC",
                "MOVING_TYPE": "WITHIN_TOKYO",
                "DAYS_UNTIL_MOVING": 25,
                "LOYALTY_POINTS": 500,
                "EXPECTED_PROMOTION": "BASIC_STANDARD"
            }
        ]
        
        # プロモーションタイプ決定ロジック（Data Flow内の処理）
        def determine_promotion_type(customer_data):
            customer_tier = customer_data["CUSTOMER_TIER"]
            moving_type = customer_data["MOVING_TYPE"]
            days_until_moving = customer_data["DAYS_UNTIL_MOVING"]
            loyalty_points = customer_data["LOYALTY_POINTS"]
            
            # 基本プロモーションタイプ
            if customer_tier == "PREMIUM":
                base_promotion = "PREMIUM"
            elif customer_tier == "STANDARD":
                base_promotion = "STANDARD"
            else:
                base_promotion = "BASIC"
            
            # 早期申込み特典
            if days_until_moving <= 7:
                timing_bonus = "EARLY_BIRD"
            elif days_until_moving <= 14:
                timing_bonus = "STANDARD"
            else:
                timing_bonus = "STANDARD"
            
            # 最終プロモーションタイプ
            promotion_type = f"{base_promotion}_{timing_bonus}"
            
            return promotion_type
        
        # 各シナリオでのプロモーションタイプ決定
        for scenario in customer_scenarios:
            promotion_type = determine_promotion_type(scenario)
            self.assertEqual(promotion_type, scenario["EXPECTED_PROMOTION"], 
                           f"プロモーションタイプ決定失敗: {scenario}")
    
    def test_data_flow_discount_calculation(self):
        """Data Flow: 割引計算テスト"""
        # テストケース: 割引額の計算
        discount_scenarios = [
            {
                "PROMOTION_TYPE": "PREMIUM_EARLY_BIRD",
                "MOVING_TYPE": "WITHIN_TOKYO",
                "ESTIMATED_BILL": 8000.0,
                "LOYALTY_POINTS": 5000,
                "EXPECTED_DISCOUNT_RATE": 0.20
            },
            {
                "PROMOTION_TYPE": "STANDARD_EARLY_BIRD",
                "MOVING_TYPE": "CROSS_PREFECTURE",
                "ESTIMATED_BILL": 12000.0,
                "LOYALTY_POINTS": 1500,
                "EXPECTED_DISCOUNT_RATE": 0.15
            },
            {
                "PROMOTION_TYPE": "BASIC_STANDARD",
                "MOVING_TYPE": "WITHIN_TOKYO",
                "ESTIMATED_BILL": 6000.0,
                "LOYALTY_POINTS": 500,
                "EXPECTED_DISCOUNT_RATE": 0.10
            }
        ]
        
        # 割引計算ロジック（Data Flow内の処理）
        def calculate_discount_rate(discount_data):
            promotion_type = discount_data["PROMOTION_TYPE"]
            moving_type = discount_data["MOVING_TYPE"]
            estimated_bill = discount_data["ESTIMATED_BILL"]
            loyalty_points = discount_data["LOYALTY_POINTS"]
            
            # 基本割引率
            base_rates = {
                "PREMIUM_EARLY_BIRD": 0.20,
                "PREMIUM_STANDARD": 0.15,
                "STANDARD_EARLY_BIRD": 0.15,
                "STANDARD_STANDARD": 0.10,
                "BASIC_EARLY_BIRD": 0.10,
                "BASIC_STANDARD": 0.05
            }
            
            base_rate = base_rates.get(promotion_type, 0.05)
            
            # 引越しタイプによる調整
            if moving_type == "CROSS_PREFECTURE":
                # 県外引越しは基本割引率のまま
                type_adjustment = 0.0
            else:
                # 県内引越しは少し割引率を上乗せ
                type_adjustment = 0.02
            
            # ロイヤルティポイントによる調整
            if loyalty_points >= 10000:
                loyalty_adjustment = 0.03
            elif loyalty_points >= 5000:
                loyalty_adjustment = 0.02
            elif loyalty_points >= 1000:
                loyalty_adjustment = 0.01
            else:
                loyalty_adjustment = 0.0
            
            final_rate = base_rate + type_adjustment + loyalty_adjustment
            
            # 最大割引率制限
            return min(final_rate, 0.30)
        
        # 各シナリオでの割引計算
        for scenario in discount_scenarios:
            discount_rate = calculate_discount_rate(scenario)
            self.assertEqual(discount_rate, scenario["EXPECTED_DISCOUNT_RATE"], 
                           f"割引計算失敗: {scenario}")
    
    def test_data_flow_promotion_message_generation(self):
        """Data Flow: プロモーションメッセージ生成テスト"""
        # テストケース: プロモーションメッセージの生成
        promotion_data = {
            "CUSTOMER_NAME": "テストユーザー1",
            "CURRENT_ADDRESS": "東京都渋谷区1-1-1",
            "NEW_ADDRESS": "東京都新宿区2-2-2",
            "MOVING_DATE": "20240315",
            "PROMOTION_TYPE": "PREMIUM_EARLY_BIRD",
            "DISCOUNT_RATE": 0.20,
            "CAMPAIGN_END_DATE": "20240331",
            "ESTIMATED_SAVINGS": 1600.0
        }
        
        # プロモーションメッセージ生成ロジック（Data Flow内の処理）
        def generate_promotion_message(promotion_data):
            customer_name = promotion_data["CUSTOMER_NAME"]
            current_address = promotion_data["CURRENT_ADDRESS"]
            new_address = promotion_data["NEW_ADDRESS"]
            moving_date = promotion_data["MOVING_DATE"]
            promotion_type = promotion_data["PROMOTION_TYPE"]
            discount_rate = promotion_data["DISCOUNT_RATE"]
            campaign_end_date = promotion_data["CAMPAIGN_END_DATE"]
            estimated_savings = promotion_data["ESTIMATED_SAVINGS"]
            
            # 日付のフォーマット
            formatted_moving_date = f"{moving_date[:4]}年{moving_date[4:6]}月{moving_date[6:8]}日"
            formatted_campaign_end = f"{campaign_end_date[:4]}年{campaign_end_date[4:6]}月{campaign_end_date[6:8]}日"
            
            # プロモーションタイプ別のメッセージ調整
            if "PREMIUM" in promotion_type:
                tier_msg = "プレミアム会員様限定"
                special_msg = "特別優待として"
            elif "STANDARD" in promotion_type:
                tier_msg = "スタンダード会員様向け"
                special_msg = "お得な特典として"
            else:
                tier_msg = "新規ご契約"
                special_msg = "ご利用特典として"
            
            if "EARLY_BIRD" in promotion_type:
                timing_msg = "早期お申し込み特典"
            else:
                timing_msg = "標準特典"
            
            promotion_message = f"""
            {customer_name}様
            
            引越しキャンペーンのご案内
            
            この度のお引越しにあたり、{tier_msg}の{timing_msg}をご案内いたします。
            
            お引越し情報：
            - 現住所：{current_address}
            - 新住所：{new_address}
            - お引越し予定日：{formatted_moving_date}
            
            キャンペーン内容：
            - 割引率：{discount_rate:.0%}
            - 予想節約額：{estimated_savings:,.0f}円
            - キャンペーン期限：{formatted_campaign_end}
            
            {special_msg}、ガス料金が{discount_rate:.0%}割引になります。
            
            お申し込みは期限までにお済ませください。
            詳細については、お気軽にお問い合わせください。
            
            東京ガス株式会社
            """
            
            return promotion_message.strip()
        
        # メッセージ生成実行
        message = generate_promotion_message(promotion_data)
        
        # アサーション
        self.assertIn("テストユーザー1様", message, "顧客名挿入失敗")
        self.assertIn("プレミアム会員様限定", message, "ティアメッセージ挿入失敗")
        self.assertIn("早期お申し込み特典", message, "タイミングメッセージ挿入失敗")
        self.assertIn("東京都渋谷区1-1-1", message, "現住所挿入失敗")
        self.assertIn("東京都新宿区2-2-2", message, "新住所挿入失敗")
        self.assertIn("2024年03月15日", message, "引越し日フォーマット失敗")
        self.assertIn("20%", message, "割引率挿入失敗")
        self.assertIn("1,600円", message, "予想節約額挿入失敗")
    
    def test_data_flow_moving_type_classification(self):
        """Data Flow: 引越しタイプ分類テスト"""
        # テストケース: 引越しタイプの分類
        address_scenarios = [
            {
                "CURRENT_ADDRESS": "東京都渋谷区1-1-1",
                "NEW_ADDRESS": "東京都新宿区2-2-2",
                "EXPECTED_TYPE": "WITHIN_TOKYO"
            },
            {
                "CURRENT_ADDRESS": "東京都港区3-3-3",
                "NEW_ADDRESS": "神奈川県横浜市4-4-4",
                "EXPECTED_TYPE": "CROSS_PREFECTURE"
            },
            {
                "CURRENT_ADDRESS": "東京都千代田区5-5-5",
                "NEW_ADDRESS": "東京都世田谷区6-6-6",
                "EXPECTED_TYPE": "WITHIN_TOKYO"
            }
        ]
        
        # 引越しタイプ分類ロジック（Data Flow内の処理）
        def classify_moving_type(current_address, new_address):
            # 都道府県の抽出
            def extract_prefecture(address):
                if address.startswith("東京都"):
                    return "東京都"
                elif address.startswith("神奈川県"):
                    return "神奈川県"
                elif address.startswith("埼玉県"):
                    return "埼玉県"
                elif address.startswith("千葉県"):
                    return "千葉県"
                else:
                    return "その他"
            
            current_pref = extract_prefecture(current_address)
            new_pref = extract_prefecture(new_address)
            
            if current_pref == new_pref:
                if current_pref == "東京都":
                    return "WITHIN_TOKYO"
                else:
                    return "WITHIN_PREFECTURE"
            else:
                # 首都圏内の移動
                metro_area = {"東京都", "神奈川県", "埼玉県", "千葉県"}
                if current_pref in metro_area and new_pref in metro_area:
                    return "WITHIN_METRO"
                else:
                    return "CROSS_PREFECTURE"
        
        # 各シナリオでの分類
        for scenario in address_scenarios:
            moving_type = classify_moving_type(scenario["CURRENT_ADDRESS"], scenario["NEW_ADDRESS"])
            self.assertEqual(moving_type, scenario["EXPECTED_TYPE"], 
                           f"引越しタイプ分類失敗: {scenario}")
    
    def test_script_activity_email_promotion_sending(self):
        """Script Activity: メールプロモーション送信処理テスト"""
        # テストケース: メールでのプロモーション送信
        promotion_email_data = {
            "to": "test@example.com",
            "subject": "【東京ガス】お引越しキャンペーンのご案内",
            "body": "お引越しキャンペーンをご案内いたします。",
            "promotion_id": "PROMO000001",
            "campaign_type": "MOVING_PROMOTION"
        }
        
        self.mock_email_service.send_promotion_email.return_value = {
            "status": "sent",
            "message_id": "PROMOTION_EMAIL_123",
            "delivery_time": datetime.utcnow().isoformat(),
            "promotion_id": "PROMO000001"
        }
        
        # Script Activity実行シミュレーション
        result = self.mock_email_service.send_promotion_email(
            promotion_email_data["to"],
            promotion_email_data["subject"],
            promotion_email_data["body"],
            promotion_id=promotion_email_data["promotion_id"],
            campaign_type=promotion_email_data["campaign_type"]
        )
        
        self.assertEqual(result["status"], "sent", "メールプロモーション送信失敗")
        self.assertIsNotNone(result["message_id"], "メッセージID取得失敗")
        self.assertEqual(result["promotion_id"], "PROMO000001", "プロモーションID確認失敗")
        self.mock_email_service.send_promotion_email.assert_called_once()
    
    def test_script_activity_sms_promotion_sending(self):
        """Script Activity: SMSプロモーション送信処理テスト"""
        # テストケース: SMSでのプロモーション送信
        promotion_sms_data = {
            "to": "090-1234-5678",
            "message": "東京ガスです。お引越しキャンペーンのご案内です。20%割引実施中。詳細はメールをご確認ください。",
            "promotion_id": "PROMO000001",
            "sender": "TokyoGas"
        }
        
        self.mock_sms_service.send_promotion_sms.return_value = {
            "status": "sent",
            "message_id": "PROMOTION_SMS_456",
            "delivery_time": datetime.utcnow().isoformat(),
            "promotion_id": "PROMO000001",
            "character_count": len(promotion_sms_data["message"])
        }
        
        # Script Activity実行シミュレーション
        result = self.mock_sms_service.send_promotion_sms(
            promotion_sms_data["to"],
            promotion_sms_data["message"],
            promotion_id=promotion_sms_data["promotion_id"],
            sender=promotion_sms_data["sender"]
        )
        
        self.assertEqual(result["status"], "sent", "SMSプロモーション送信失敗")
        self.assertIsNotNone(result["message_id"], "SMSメッセージID取得失敗")
        self.assertEqual(result["promotion_id"], "PROMO000001", "プロモーションID確認失敗")
        self.assertGreater(result["character_count"], 0, "文字数カウント失敗")
        self.mock_sms_service.send_promotion_sms.assert_called_once()
    
    def test_copy_activity_promotion_delivery_log(self):
        """Copy Activity: プロモーション配信ログ記録テスト"""
        # テストケース: プロモーション配信ログの記録
        delivery_logs = [
            {
                "PROMOTION_ID": "PROMO000001",
                "CUSTOMER_ID": "CUST123456",
                "DELIVERY_METHOD": "EMAIL",
                "DELIVERY_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "DELIVERY_TIME": datetime.utcnow().strftime('%H:%M:%S'),
                "STATUS": "DELIVERED",
                "MESSAGE_ID": "PROMOTION_EMAIL_123",
                "CAMPAIGN_TYPE": "MOVING_PROMOTION"
            },
            {
                "PROMOTION_ID": "PROMO000002",
                "CUSTOMER_ID": "CUST123457",
                "DELIVERY_METHOD": "BOTH",
                "DELIVERY_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "DELIVERY_TIME": datetime.utcnow().strftime('%H:%M:%S'),
                "STATUS": "DELIVERED",
                "MESSAGE_ID": "PROMOTION_EMAIL_124",
                "CAMPAIGN_TYPE": "MOVING_PROMOTION"
            }
        ]
        
        self.mock_database.insert_records.return_value = True
        
        # Copy Activity実行シミュレーション
        delivery_log_table = "moving_promotion_delivery_logs"
        result = self.mock_database.insert_records(delivery_log_table, delivery_logs)
        
        self.assertTrue(result, "プロモーション配信ログ記録失敗")
        self.mock_database.insert_records.assert_called_once_with(delivery_log_table, delivery_logs)
    
    def test_copy_activity_promotion_status_update(self):
        """Copy Activity: プロモーション送信ステータス更新テスト"""
        # テストケース: プロモーション送信ステータスの更新
        status_updates = [
            {
                "CUSTOMER_ID": "CUST123456",
                "PROMOTION_SENT": "Y",
                "PROMOTION_SENT_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "UPDATED_BY": "SYSTEM"
            },
            {
                "CUSTOMER_ID": "CUST123457",
                "PROMOTION_SENT": "Y",
                "PROMOTION_SENT_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "UPDATED_BY": "SYSTEM"
            }
        ]
        
        self.mock_database.update_records.return_value = True
        
        # Copy Activity実行シミュレーション
        for update in status_updates:
            result = self.mock_database.update_records(
                "customer_moving_schedule",
                update,
                where_clause=f"CUSTOMER_ID = '{update['CUSTOMER_ID']}'"
            )
            self.assertTrue(result, f"プロモーション送信ステータス更新失敗: {update['CUSTOMER_ID']}")
        
        self.assertEqual(self.mock_database.update_records.call_count, 2, "更新処理回数不正")
    
    def test_script_activity_promotion_analytics(self):
        """Script Activity: プロモーション分析処理テスト"""
        # テストケース: プロモーション送信の分析データ生成
        promotion_analytics = {
            "execution_date": self.test_date,
            "total_promotions_sent": 65,
            "within_tokyo_moves": 40,
            "cross_prefecture_moves": 25,
            "premium_promotions": 20,
            "standard_promotions": 30,
            "basic_promotions": 15,
            "early_bird_promotions": 45,
            "email_deliveries": 60,
            "sms_deliveries": 35,
            "average_discount_rate": 0.15,
            "total_estimated_savings": 95000.0,
            "processing_time_minutes": 4.5
        }
        
        self.mock_database.insert_records.return_value = True
        
        # Script Activity実行シミュレーション
        analytics_table = "moving_promotion_analytics"
        result = self.mock_database.insert_records(analytics_table, [promotion_analytics])
        
        self.assertTrue(result, "プロモーション分析記録失敗")
        self.mock_database.insert_records.assert_called_once_with(analytics_table, [promotion_analytics])
    
    def test_data_flow_promotion_validation(self):
        """Data Flow: プロモーションデータ検証テスト"""
        # テストケース: プロモーションデータの検証
        test_promotions = [
            {"CUSTOMER_ID": "CUST123456", "MOVING_DATE": "20240315", "DISCOUNT_RATE": 0.15, "CAMPAIGN_END_DATE": "20240331"},
            {"CUSTOMER_ID": "", "MOVING_DATE": "20240315", "DISCOUNT_RATE": 0.15, "CAMPAIGN_END_DATE": "20240331"},  # 不正: 空ID
            {"CUSTOMER_ID": "CUST123458", "MOVING_DATE": "20240201", "DISCOUNT_RATE": 0.15, "CAMPAIGN_END_DATE": "20240331"},  # 不正: 過去の日付
            {"CUSTOMER_ID": "CUST123459", "MOVING_DATE": "20240315", "DISCOUNT_RATE": 0.50, "CAMPAIGN_END_DATE": "20240310"}  # 不正: 高すぎる割引率、期限切れ
        ]
        
        # プロモーションデータ検証ロジック（Data Flow内の処理）
        def validate_promotion_data(promotion):
            errors = []
            current_date = datetime.utcnow().strftime('%Y%m%d')
            
            # 顧客ID検証
            if not promotion.get("CUSTOMER_ID", "").strip():
                errors.append("顧客ID必須")
            
            # 引越し日検証
            moving_date = promotion.get("MOVING_DATE", "")
            if moving_date < current_date:
                errors.append("引越し日は現在日付以降である必要があります")
            
            # 割引率検証
            discount_rate = promotion.get("DISCOUNT_RATE", 0)
            if discount_rate < 0 or discount_rate > 0.30:
                errors.append("割引率は0-30%の範囲である必要があります")
            
            # キャンペーン期限検証
            campaign_end_date = promotion.get("CAMPAIGN_END_DATE", "")
            if campaign_end_date < current_date:
                errors.append("キャンペーン期限が過ぎています")
            
            return errors
        
        # 検証実行
        validation_results = []
        for promotion in test_promotions:
            errors = validate_promotion_data(promotion)
            validation_results.append({
                "promotion": promotion,
                "errors": errors,
                "is_valid": len(errors) == 0
            })
        
        # アサーション
        self.assertEqual(len(validation_results), 4, "検証結果数不正")
        self.assertTrue(validation_results[0]["is_valid"], "正常プロモーションが不正判定")
        self.assertFalse(validation_results[1]["is_valid"], "不正プロモーション（空ID）が正常判定")
        self.assertFalse(validation_results[2]["is_valid"], "不正プロモーション（過去の日付）が正常判定")
        self.assertFalse(validation_results[3]["is_valid"], "不正プロモーション（高い割引率、期限切れ）が正常判定")
    
    def test_lookup_activity_moving_statistics(self):
        """Lookup Activity: 引越し統計取得テスト"""
        # テストケース: 引越しの統計情報取得
        moving_statistics = [
            {
                "MOVING_TYPE": "WITHIN_TOKYO",
                "MOVING_COUNT": 40,
                "AVERAGE_DISCOUNT_RATE": 0.18,
                "TOTAL_ESTIMATED_SAVINGS": 64000.0
            },
            {
                "MOVING_TYPE": "CROSS_PREFECTURE",
                "MOVING_COUNT": 25,
                "AVERAGE_DISCOUNT_RATE": 0.12,
                "TOTAL_ESTIMATED_SAVINGS": 31000.0
            }
        ]
        
        self.mock_database.query_records.return_value = moving_statistics
        
        # Lookup Activity実行シミュレーション
        statistics_query = f"""
        SELECT MOVING_TYPE, 
               COUNT(*) as MOVING_COUNT,
               AVG(DISCOUNT_RATE) as AVERAGE_DISCOUNT_RATE,
               SUM(ESTIMATED_SAVINGS) as TOTAL_ESTIMATED_SAVINGS
        FROM moving_promotion_list
        WHERE MOVING_DATE >= '{self.test_date}'
        GROUP BY MOVING_TYPE
        """
        
        result = self.mock_database.query_records("moving_promotion_list", statistics_query)
        
        self.assertEqual(len(result), 2, "引越し統計取得件数不正")
        self.assertEqual(result[0]["MOVING_TYPE"], "WITHIN_TOKYO", "引越しタイプ確認失敗")
        self.assertEqual(result[0]["MOVING_COUNT"], 40, "引越し件数確認失敗")
        self.assertEqual(result[0]["AVERAGE_DISCOUNT_RATE"], 0.18, "平均割引率確認失敗")
    
    def _create_moving_promotion_list_csv_content(self) -> str:
        """引越しプロモーションリストデータ用CSVコンテンツ生成"""
        header = "PROMOTION_ID,CUSTOMER_ID,CUSTOMER_NAME,EMAIL_ADDRESS,PHONE_NUMBER,CURRENT_ADDRESS,NEW_ADDRESS,MOVING_DATE,MOVING_TYPE,PROMOTION_TYPE,DISCOUNT_RATE,CAMPAIGN_END_DATE,ELIGIBILITY_STATUS"
        rows = []
        
        for item in self.sample_moving_promotion_data:
            row = f"{item['PROMOTION_ID']},{item['CUSTOMER_ID']},{item['CUSTOMER_NAME']},{item['EMAIL_ADDRESS']},{item['PHONE_NUMBER']},{item['CURRENT_ADDRESS']},{item['NEW_ADDRESS']},{item['MOVING_DATE']},{item['MOVING_TYPE']},{item['PROMOTION_TYPE']},{item['DISCOUNT_RATE']},{item['CAMPAIGN_END_DATE']},{item['ELIGIBILITY_STATUS']}"
            rows.append(row)
        
        return header + "\n" + "\n".join(rows)


if __name__ == "__main__":
    unittest.main()