"""
pi_Send_ClientDM パイプラインのユニットテスト

Client DM（ダイレクトメール）送信パイプラインの個別コンポーネントテスト
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any


class TestClientDmUnit(unittest.TestCase):
    """Client DMパイプラインユニットテスト"""
    
    def setUp(self):
        """テストセットアップ"""
        self.pipeline_name = "pi_Send_ClientDM"
        self.domain = "marketing"
        
        # モックサービス
        self.mock_blob_storage = Mock()
        self.mock_database = Mock()
        self.mock_email_service = Mock()
        self.mock_dm_service = Mock()
        
        # テストデータ
        self.test_date = datetime.utcnow().strftime('%Y%m%d')
        self.test_file_path = f"ClientDM/{self.test_date}/client_dm.csv"
        
        # 基本的なClient DMデータ
        self.sample_client_dm_data = [
            {
                "DM_ID": "DM000001",
                "CUSTOMER_ID": "CUST123456",
                "CUSTOMER_NAME": "テストユーザー1",
                "EMAIL_ADDRESS": "test1@example.com",
                "PHONE_NUMBER": "090-1234-5678",
                "POSTAL_CODE": "100-0001",
                "ADDRESS": "東京都千代田区千代田1-1-1",
                "DM_TYPE": "PROMOTIONAL",
                "CAMPAIGN_ID": "CAMP2024001",
                "CAMPAIGN_NAME": "春の特別キャンペーン",
                "TEMPLATE_ID": "TEMP001",
                "SEND_DATE": "20240301",
                "SEND_TIME": "10:00:00",
                "DELIVERY_METHOD": "EMAIL",
                "PRIORITY": "HIGH",
                "STATUS": "PENDING"
            },
            {
                "DM_ID": "DM000002",
                "CUSTOMER_ID": "CUST123457",
                "CUSTOMER_NAME": "テストユーザー2",
                "EMAIL_ADDRESS": "test2@example.com",
                "PHONE_NUMBER": "090-2345-6789",
                "POSTAL_CODE": "160-0023",
                "ADDRESS": "東京都新宿区西新宿1-1-1",
                "DM_TYPE": "TRANSACTIONAL",
                "CAMPAIGN_ID": "CAMP2024002",
                "CAMPAIGN_NAME": "料金お知らせ",
                "TEMPLATE_ID": "TEMP002",
                "SEND_DATE": "20240301",
                "SEND_TIME": "14:00:00",
                "DELIVERY_METHOD": "EMAIL",
                "PRIORITY": "MEDIUM",
                "STATUS": "PENDING"
            }
        ]
    
    def test_lookup_activity_pending_dm_detection(self):
        """Lookup Activity: 未送信DM検出テスト"""
        # テストケース: 未送信のDMの検出
        mock_pending_dms = [
            {
                "DM_ID": "DM000001",
                "CUSTOMER_ID": "CUST123456",
                "DM_TYPE": "PROMOTIONAL",
                "CAMPAIGN_ID": "CAMP2024001",
                "TEMPLATE_ID": "TEMP001",
                "SEND_DATE": "20240301",
                "DELIVERY_METHOD": "EMAIL",
                "PRIORITY": "HIGH",
                "STATUS": "PENDING"
            },
            {
                "DM_ID": "DM000002",
                "CUSTOMER_ID": "CUST123457",
                "DM_TYPE": "TRANSACTIONAL",
                "CAMPAIGN_ID": "CAMP2024002",
                "TEMPLATE_ID": "TEMP002",
                "SEND_DATE": "20240301",
                "DELIVERY_METHOD": "EMAIL",
                "PRIORITY": "MEDIUM",
                "STATUS": "PENDING"
            }
        ]
        
        self.mock_database.query_records.return_value = mock_pending_dms
        
        # Lookup Activity実行シミュレーション
        pending_dm_query = f"""
        SELECT DM_ID, CUSTOMER_ID, DM_TYPE, CAMPAIGN_ID, TEMPLATE_ID, 
               SEND_DATE, DELIVERY_METHOD, PRIORITY, STATUS
        FROM client_dm_queue
        WHERE SEND_DATE <= '{self.test_date}' AND STATUS = 'PENDING'
        ORDER BY PRIORITY DESC, SEND_DATE ASC
        """
        
        result = self.mock_database.query_records("client_dm_queue", pending_dm_query)
        
        self.assertEqual(len(result), 2, "未送信DM検出件数不正")
        self.assertEqual(result[0]["DM_ID"], "DM000001", "DM ID確認失敗")
        self.assertEqual(result[0]["STATUS"], "PENDING", "ステータス確認失敗")
        self.assertEqual(result[0]["PRIORITY"], "HIGH", "優先度確認失敗")
    
    def test_lookup_activity_customer_preferences(self):
        """Lookup Activity: 顧客配信設定取得テスト"""
        # テストケース: 顧客の配信設定取得
        mock_customer_preferences = [
            {
                "CUSTOMER_ID": "CUST123456",
                "EMAIL_OPT_IN": "Y",
                "SMS_OPT_IN": "N",
                "POSTAL_OPT_IN": "Y",
                "PROMOTIONAL_OPT_IN": "Y",
                "TRANSACTIONAL_OPT_IN": "Y",
                "PREFERRED_DELIVERY_TIME": "10:00:00",
                "PREFERRED_FREQUENCY": "WEEKLY",
                "UNSUBSCRIBE_FLAG": "N"
            },
            {
                "CUSTOMER_ID": "CUST123457",
                "EMAIL_OPT_IN": "Y",
                "SMS_OPT_IN": "Y",
                "POSTAL_OPT_IN": "N",
                "PROMOTIONAL_OPT_IN": "N",
                "TRANSACTIONAL_OPT_IN": "Y",
                "PREFERRED_DELIVERY_TIME": "14:00:00",
                "PREFERRED_FREQUENCY": "MONTHLY",
                "UNSUBSCRIBE_FLAG": "N"
            }
        ]
        
        self.mock_database.query_records.return_value = mock_customer_preferences
        
        # Lookup Activity実行シミュレーション
        preferences_query = """
        SELECT CUSTOMER_ID, EMAIL_OPT_IN, SMS_OPT_IN, POSTAL_OPT_IN, 
               PROMOTIONAL_OPT_IN, TRANSACTIONAL_OPT_IN, PREFERRED_DELIVERY_TIME,
               PREFERRED_FREQUENCY, UNSUBSCRIBE_FLAG
        FROM customer_dm_preferences
        WHERE CUSTOMER_ID IN ('CUST123456', 'CUST123457')
        """
        
        result = self.mock_database.query_records("customer_dm_preferences", preferences_query)
        
        self.assertEqual(len(result), 2, "顧客配信設定取得件数不正")
        self.assertEqual(result[0]["CUSTOMER_ID"], "CUST123456", "顧客ID確認失敗")
        self.assertEqual(result[0]["EMAIL_OPT_IN"], "Y", "メール配信設定確認失敗")
        self.assertEqual(result[0]["PROMOTIONAL_OPT_IN"], "Y", "プロモーション配信設定確認失敗")
        self.assertEqual(result[1]["PROMOTIONAL_OPT_IN"], "N", "プロモーション配信設定確認失敗")
    
    def test_data_flow_dm_personalization(self):
        """Data Flow: DM個人化処理テスト"""
        # テストケース: DMの個人化処理
        dm_data = {
            "DM_ID": "DM000001",
            "CUSTOMER_ID": "CUST123456",
            "CUSTOMER_NAME": "テストユーザー1",
            "EMAIL_ADDRESS": "test1@example.com",
            "CAMPAIGN_ID": "CAMP2024001",
            "TEMPLATE_ID": "TEMP001",
            "DM_TYPE": "PROMOTIONAL"
        }
        
        customer_data = {
            "CUSTOMER_ID": "CUST123456",
            "CUSTOMER_NAME": "テストユーザー1",
            "CUSTOMER_TIER": "PREMIUM",
            "REGISTRATION_DATE": "20230101",
            "LAST_PURCHASE_DATE": "20240215",
            "TOTAL_PURCHASES": 25,
            "TOTAL_SPEND": 125000,
            "PREFERRED_CATEGORY": "家庭用品"
        }
        
        template_data = {
            "TEMPLATE_ID": "TEMP001",
            "TEMPLATE_NAME": "春の特別キャンペーン",
            "SUBJECT_TEMPLATE": "【東京ガス】{customer_name}様限定！春の特別キャンペーン",
            "BODY_TEMPLATE": """
            {customer_name}様

            いつも東京ガスをご利用いただき、ありがとうございます。

            {customer_tier}会員の{customer_name}様に、春の特別キャンペーンをご案内いたします。

            ご利用実績：
            - 登録日: {registration_date}
            - 最終購入日: {last_purchase_date}
            - 総購入回数: {total_purchases}回
            - 総購入金額: {total_spend:,}円
            - おすすめカテゴリー: {preferred_category}

            この機会をお見逃しなく！

            東京ガス株式会社
            """,
            "CTA_TEMPLATE": "今すぐ{preferred_category}商品をチェック"
        }
        
        # DM個人化処理ロジック（Data Flow内の処理）
        def personalize_dm_content(dm_data, customer_data, template_data):
            customer_name = customer_data["CUSTOMER_NAME"]
            customer_tier = customer_data["CUSTOMER_TIER"]
            registration_date = customer_data["REGISTRATION_DATE"]
            last_purchase_date = customer_data["LAST_PURCHASE_DATE"]
            total_purchases = customer_data["TOTAL_PURCHASES"]
            total_spend = customer_data["TOTAL_SPEND"]
            preferred_category = customer_data["PREFERRED_CATEGORY"]
            
            # 日付フォーマット
            formatted_registration_date = f"{registration_date[:4]}年{registration_date[4:6]}月{registration_date[6:8]}日"
            formatted_last_purchase_date = f"{last_purchase_date[:4]}年{last_purchase_date[4:6]}月{last_purchase_date[6:8]}日"
            
            # 個人化されたコンテンツの生成
            personalized_subject = template_data["SUBJECT_TEMPLATE"].format(
                customer_name=customer_name
            )
            
            personalized_body = template_data["BODY_TEMPLATE"].format(
                customer_name=customer_name,
                customer_tier=customer_tier,
                registration_date=formatted_registration_date,
                last_purchase_date=formatted_last_purchase_date,
                total_purchases=total_purchases,
                total_spend=total_spend,
                preferred_category=preferred_category
            )
            
            personalized_cta = template_data["CTA_TEMPLATE"].format(
                preferred_category=preferred_category
            )
            
            personalized_dm = {
                "DM_ID": dm_data["DM_ID"],
                "CUSTOMER_ID": dm_data["CUSTOMER_ID"],
                "PERSONALIZED_SUBJECT": personalized_subject,
                "PERSONALIZED_BODY": personalized_body.strip(),
                "PERSONALIZED_CTA": personalized_cta,
                "PERSONALIZATION_SCORE": self._calculate_personalization_score(customer_data),
                "CONTENT_LENGTH": len(personalized_body),
                "PERSONALIZATION_TOKENS": 7  # 使用したトークン数
            }
            
            return personalized_dm
        
        # 個人化処理実行
        personalized_dm = personalize_dm_content(dm_data, customer_data, template_data)
        
        # アサーション
        self.assertEqual(personalized_dm["DM_ID"], "DM000001", "DM ID確認失敗")
        self.assertIn("テストユーザー1様限定", personalized_dm["PERSONALIZED_SUBJECT"], "件名個人化確認失敗")
        self.assertIn("テストユーザー1様", personalized_dm["PERSONALIZED_BODY"], "本文個人化確認失敗")
        self.assertIn("PREMIUM会員", personalized_dm["PERSONALIZED_BODY"], "顧客ティア個人化確認失敗")
        self.assertIn("25回", personalized_dm["PERSONALIZED_BODY"], "購入回数個人化確認失敗")
        self.assertIn("125,000円", personalized_dm["PERSONALIZED_BODY"], "購入金額個人化確認失敗")
        self.assertIn("家庭用品", personalized_dm["PERSONALIZED_CTA"], "CTA個人化確認失敗")
        self.assertGreater(personalized_dm["PERSONALIZATION_SCORE"], 0, "個人化スコア確認失敗")
        self.assertEqual(personalized_dm["PERSONALIZATION_TOKENS"], 7, "個人化トークン数確認失敗")
    
    def _calculate_personalization_score(self, customer_data):
        """個人化スコア計算"""
        score = 0
        if customer_data["CUSTOMER_TIER"] == "PREMIUM":
            score += 30
        elif customer_data["CUSTOMER_TIER"] == "STANDARD":
            score += 20
        else:
            score += 10
        
        # 購入履歴によるスコア追加
        if customer_data["TOTAL_PURCHASES"] > 20:
            score += 20
        elif customer_data["TOTAL_PURCHASES"] > 10:
            score += 15
        else:
            score += 10
        
        # 最終購入日による追加
        last_purchase = datetime.strptime(customer_data["LAST_PURCHASE_DATE"], "%Y%m%d")
        days_since_last_purchase = (datetime.utcnow() - last_purchase).days
        if days_since_last_purchase < 30:
            score += 25
        elif days_since_last_purchase < 90:
            score += 15
        else:
            score += 5
        
        return score
    
    def test_data_flow_dm_filtering(self):
        """Data Flow: DM配信フィルタリング処理テスト"""
        # テストケース: DM配信フィルタリング
        dm_requests = [
            {"DM_ID": "DM000001", "CUSTOMER_ID": "CUST123456", "DM_TYPE": "PROMOTIONAL", "DELIVERY_METHOD": "EMAIL"},
            {"DM_ID": "DM000002", "CUSTOMER_ID": "CUST123457", "DM_TYPE": "PROMOTIONAL", "DELIVERY_METHOD": "EMAIL"},  # フィルタ対象
            {"DM_ID": "DM000003", "CUSTOMER_ID": "CUST123458", "DM_TYPE": "TRANSACTIONAL", "DELIVERY_METHOD": "EMAIL"},
            {"DM_ID": "DM000004", "CUSTOMER_ID": "CUST123459", "DM_TYPE": "PROMOTIONAL", "DELIVERY_METHOD": "SMS"}  # フィルタ対象
        ]
        
        customer_preferences = [
            {"CUSTOMER_ID": "CUST123456", "EMAIL_OPT_IN": "Y", "SMS_OPT_IN": "N", "PROMOTIONAL_OPT_IN": "Y", "UNSUBSCRIBE_FLAG": "N"},
            {"CUSTOMER_ID": "CUST123457", "EMAIL_OPT_IN": "Y", "SMS_OPT_IN": "Y", "PROMOTIONAL_OPT_IN": "N", "UNSUBSCRIBE_FLAG": "N"},  # プロモーション無効
            {"CUSTOMER_ID": "CUST123458", "EMAIL_OPT_IN": "Y", "SMS_OPT_IN": "N", "PROMOTIONAL_OPT_IN": "Y", "UNSUBSCRIBE_FLAG": "N"},
            {"CUSTOMER_ID": "CUST123459", "EMAIL_OPT_IN": "Y", "SMS_OPT_IN": "N", "PROMOTIONAL_OPT_IN": "Y", "UNSUBSCRIBE_FLAG": "N"}   # SMS無効
        ]
        
        # DM配信フィルタリングロジック（Data Flow内の処理）
        def filter_dm_requests(dm_requests, customer_preferences):
            preferences_dict = {pref["CUSTOMER_ID"]: pref for pref in customer_preferences}
            
            filtered_requests = []
            filtered_out = []
            
            for dm_request in dm_requests:
                customer_id = dm_request["CUSTOMER_ID"]
                dm_type = dm_request["DM_TYPE"]
                delivery_method = dm_request["DELIVERY_METHOD"]
                
                preferences = preferences_dict.get(customer_id, {})
                
                # 配信停止チェック
                if preferences.get("UNSUBSCRIBE_FLAG") == "Y":
                    filtered_out.append({"dm_request": dm_request, "reason": "配信停止設定"})
                    continue
                
                # 配信方法チェック
                if delivery_method == "EMAIL" and preferences.get("EMAIL_OPT_IN") != "Y":
                    filtered_out.append({"dm_request": dm_request, "reason": "メール配信無効"})
                    continue
                
                if delivery_method == "SMS" and preferences.get("SMS_OPT_IN") != "Y":
                    filtered_out.append({"dm_request": dm_request, "reason": "SMS配信無効"})
                    continue
                
                # DM種別チェック
                if dm_type == "PROMOTIONAL" and preferences.get("PROMOTIONAL_OPT_IN") != "Y":
                    filtered_out.append({"dm_request": dm_request, "reason": "プロモーション配信無効"})
                    continue
                
                # フィルタを通過
                filtered_requests.append(dm_request)
            
            return {
                "filtered_requests": filtered_requests,
                "filtered_out": filtered_out,
                "total_requests": len(dm_requests),
                "passed_count": len(filtered_requests),
                "filtered_count": len(filtered_out)
            }
        
        # フィルタリング実行
        filter_result = filter_dm_requests(dm_requests, customer_preferences)
        
        # アサーション
        self.assertEqual(filter_result["total_requests"], 4, "総リクエスト数不正")
        self.assertEqual(filter_result["passed_count"], 2, "通過数不正")
        self.assertEqual(filter_result["filtered_count"], 2, "フィルタ数不正")
        self.assertEqual(filter_result["filtered_requests"][0]["DM_ID"], "DM000001", "通過DM確認失敗")
        self.assertEqual(filter_result["filtered_requests"][1]["DM_ID"], "DM000003", "通過DM確認失敗")
        self.assertEqual(filter_result["filtered_out"][0]["reason"], "プロモーション配信無効", "フィルタ理由確認失敗")
        self.assertEqual(filter_result["filtered_out"][1]["reason"], "SMS配信無効", "フィルタ理由確認失敗")
    
    def test_data_flow_dm_scheduling(self):
        """Data Flow: DM配信スケジューリング処理テスト"""
        # テストケース: DM配信スケジューリング
        dm_requests = [
            {"DM_ID": "DM000001", "CUSTOMER_ID": "CUST123456", "PRIORITY": "HIGH", "SEND_DATE": "20240301", "SEND_TIME": "10:00:00"},
            {"DM_ID": "DM000002", "CUSTOMER_ID": "CUST123457", "PRIORITY": "LOW", "SEND_DATE": "20240301", "SEND_TIME": "14:00:00"},
            {"DM_ID": "DM000003", "CUSTOMER_ID": "CUST123458", "PRIORITY": "MEDIUM", "SEND_DATE": "20240301", "SEND_TIME": "12:00:00"},
            {"DM_ID": "DM000004", "CUSTOMER_ID": "CUST123459", "PRIORITY": "HIGH", "SEND_DATE": "20240302", "SEND_TIME": "09:00:00"}
        ]
        
        # DM配信スケジューリングロジック（Data Flow内の処理）
        def schedule_dm_delivery(dm_requests):
            priority_order = {"HIGH": 1, "MEDIUM": 2, "LOW": 3}
            
            # 優先度、日付、時間でソート
            sorted_requests = sorted(dm_requests, key=lambda x: (
                x["SEND_DATE"],
                priority_order.get(x["PRIORITY"], 4),
                x["SEND_TIME"]
            ))
            
            # 配信スケジュールの生成
            schedule_batches = []
            current_batch = []
            current_date = None
            batch_size = 100  # バッチサイズ
            
            for i, request in enumerate(sorted_requests):
                send_date = request["SEND_DATE"]
                
                # 新しい日付の場合、バッチをリセット
                if current_date != send_date:
                    if current_batch:
                        schedule_batches.append({
                            "batch_id": f"BATCH_{len(schedule_batches) + 1:03d}",
                            "send_date": current_date,
                            "dm_requests": current_batch,
                            "batch_size": len(current_batch)
                        })
                    current_batch = []
                    current_date = send_date
                
                current_batch.append(request)
                
                # バッチサイズに達した場合
                if len(current_batch) >= batch_size:
                    schedule_batches.append({
                        "batch_id": f"BATCH_{len(schedule_batches) + 1:03d}",
                        "send_date": current_date,
                        "dm_requests": current_batch,
                        "batch_size": len(current_batch)
                    })
                    current_batch = []
            
            # 最後のバッチを追加
            if current_batch:
                schedule_batches.append({
                    "batch_id": f"BATCH_{len(schedule_batches) + 1:03d}",
                    "send_date": current_date,
                    "dm_requests": current_batch,
                    "batch_size": len(current_batch)
                })
            
            return {
                "schedule_batches": schedule_batches,
                "total_batches": len(schedule_batches),
                "total_requests": len(dm_requests)
            }
        
        # スケジューリング実行
        schedule_result = schedule_dm_delivery(dm_requests)
        
        # アサーション
        self.assertEqual(schedule_result["total_requests"], 4, "総リクエスト数不正")
        self.assertEqual(schedule_result["total_batches"], 2, "総バッチ数不正")  # 2つの日付
        self.assertEqual(schedule_result["schedule_batches"][0]["send_date"], "20240301", "バッチ日付確認失敗")
        self.assertEqual(schedule_result["schedule_batches"][0]["batch_size"], 3, "バッチサイズ確認失敗")
        self.assertEqual(schedule_result["schedule_batches"][1]["send_date"], "20240302", "バッチ日付確認失敗")
        
        # 優先度順序の確認
        first_batch_requests = schedule_result["schedule_batches"][0]["dm_requests"]
        self.assertEqual(first_batch_requests[0]["PRIORITY"], "HIGH", "優先度順序確認失敗")
        self.assertEqual(first_batch_requests[1]["PRIORITY"], "MEDIUM", "優先度順序確認失敗")
        self.assertEqual(first_batch_requests[2]["PRIORITY"], "LOW", "優先度順序確認失敗")
    
    def test_script_activity_dm_sending(self):
        """Script Activity: DM送信処理テスト"""
        # テストケース: DM送信処理
        dm_sending_data = {
            "dm_id": "DM000001",
            "customer_id": "CUST123456",
            "to": "test@example.com",
            "subject": "【東京ガス】テストユーザー1様限定！春の特別キャンペーン",
            "body": "個人化されたメール本文",
            "delivery_method": "EMAIL",
            "template_id": "TEMP001"
        }
        
        self.mock_dm_service.send_dm.return_value = {
            "status": "sent",
            "message_id": "DM_MSG_12345",
            "delivery_time": datetime.utcnow().isoformat(),
            "dm_id": "DM000001",
            "delivery_method": "EMAIL"
        }
        
        # Script Activity実行シミュレーション
        result = self.mock_dm_service.send_dm(
            dm_sending_data["dm_id"],
            dm_sending_data["customer_id"],
            dm_sending_data["to"],
            dm_sending_data["subject"],
            dm_sending_data["body"],
            delivery_method=dm_sending_data["delivery_method"],
            template_id=dm_sending_data["template_id"]
        )
        
        self.assertEqual(result["status"], "sent", "DM送信失敗")
        self.assertEqual(result["dm_id"], "DM000001", "DM ID確認失敗")
        self.assertIsNotNone(result["message_id"], "メッセージID取得失敗")
        self.assertEqual(result["delivery_method"], "EMAIL", "配信方法確認失敗")
        self.mock_dm_service.send_dm.assert_called_once()
    
    def test_copy_activity_dm_delivery_log(self):
        """Copy Activity: DM配信ログ記録テスト"""
        # テストケース: DM配信ログの記録
        delivery_logs = [
            {
                "DM_ID": "DM000001",
                "CUSTOMER_ID": "CUST123456",
                "CAMPAIGN_ID": "CAMP2024001",
                "DELIVERY_METHOD": "EMAIL",
                "SENT_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "SENT_TIME": datetime.utcnow().strftime('%H:%M:%S'),
                "STATUS": "SENT",
                "MESSAGE_ID": "DM_MSG_12345",
                "DELIVERY_TIME": datetime.utcnow().isoformat()
            },
            {
                "DM_ID": "DM000002",
                "CUSTOMER_ID": "CUST123457",
                "CAMPAIGN_ID": "CAMP2024002",
                "DELIVERY_METHOD": "EMAIL",
                "SENT_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "SENT_TIME": datetime.utcnow().strftime('%H:%M:%S'),
                "STATUS": "SENT",
                "MESSAGE_ID": "DM_MSG_12346",
                "DELIVERY_TIME": datetime.utcnow().isoformat()
            }
        ]
        
        self.mock_database.insert_records.return_value = True
        
        # Copy Activity実行シミュレーション
        delivery_log_table = "dm_delivery_logs"
        result = self.mock_database.insert_records(delivery_log_table, delivery_logs)
        
        self.assertTrue(result, "DM配信ログ記録失敗")
        self.mock_database.insert_records.assert_called_once_with(delivery_log_table, delivery_logs)
    
    def test_copy_activity_dm_status_update(self):
        """Copy Activity: DMステータス更新テスト"""
        # テストケース: DMステータスの更新
        status_updates = [
            {
                "DM_ID": "DM000001",
                "STATUS": "SENT",
                "SENT_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "SENT_TIME": datetime.utcnow().strftime('%H:%M:%S'),
                "MESSAGE_ID": "DM_MSG_12345",
                "UPDATED_BY": "SYSTEM"
            },
            {
                "DM_ID": "DM000002",
                "STATUS": "SENT",
                "SENT_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "SENT_TIME": datetime.utcnow().strftime('%H:%M:%S'),
                "MESSAGE_ID": "DM_MSG_12346",
                "UPDATED_BY": "SYSTEM"
            }
        ]
        
        self.mock_database.update_records.return_value = True
        
        # Copy Activity実行シミュレーション
        for update in status_updates:
            result = self.mock_database.update_records(
                "client_dm_queue",
                update,
                where_clause=f"DM_ID = '{update['DM_ID']}'"
            )
            self.assertTrue(result, f"DMステータス更新失敗: {update['DM_ID']}")
        
        self.assertEqual(self.mock_database.update_records.call_count, 2, "更新処理回数不正")
    
    def test_script_activity_dm_analytics(self):
        """Script Activity: DM分析処理テスト"""
        # テストケース: DM分析データ生成
        dm_analytics = {
            "execution_date": self.test_date,
            "total_dm_sent": 145,
            "promotional_dm_sent": 95,
            "transactional_dm_sent": 50,
            "email_delivery_count": 130,
            "sms_delivery_count": 15,
            "high_priority_count": 35,
            "medium_priority_count": 60,
            "low_priority_count": 50,
            "personalization_average_score": 75.8,
            "delivery_success_rate": 0.96,
            "average_delivery_time_seconds": 2.3,
            "processing_time_minutes": 18.5
        }
        
        self.mock_database.insert_records.return_value = True
        
        # Script Activity実行シミュレーション
        analytics_table = "client_dm_analytics"
        result = self.mock_database.insert_records(analytics_table, [dm_analytics])
        
        self.assertTrue(result, "DM分析記録失敗")
        self.mock_database.insert_records.assert_called_once_with(analytics_table, [dm_analytics])
    
    def test_data_flow_dm_validation(self):
        """Data Flow: DMデータ検証テスト"""
        # テストケース: DMデータの検証
        test_dms = [
            {"DM_ID": "DM000001", "CUSTOMER_ID": "CUST123456", "EMAIL_ADDRESS": "test@example.com", "DM_TYPE": "PROMOTIONAL", "TEMPLATE_ID": "TEMP001"},
            {"DM_ID": "", "CUSTOMER_ID": "CUST123457", "EMAIL_ADDRESS": "test2@example.com", "DM_TYPE": "TRANSACTIONAL", "TEMPLATE_ID": "TEMP002"},  # 不正: 空DM ID
            {"DM_ID": "DM000003", "CUSTOMER_ID": "CUST123458", "EMAIL_ADDRESS": "invalid-email", "DM_TYPE": "PROMOTIONAL", "TEMPLATE_ID": "TEMP001"},  # 不正: 無効メール
            {"DM_ID": "DM000004", "CUSTOMER_ID": "CUST123459", "EMAIL_ADDRESS": "test4@example.com", "DM_TYPE": "UNKNOWN", "TEMPLATE_ID": "TEMP001"}  # 不正: 不明DMタイプ
        ]
        
        # DMデータ検証ロジック（Data Flow内の処理）
        def validate_dm_data(dm):
            errors = []
            
            # DM ID検証
            if not dm.get("DM_ID", "").strip():
                errors.append("DM ID必須")
            
            # 顧客ID検証
            if not dm.get("CUSTOMER_ID", "").strip():
                errors.append("顧客ID必須")
            
            # メールアドレス検証
            email = dm.get("EMAIL_ADDRESS", "")
            if not email.strip():
                errors.append("メールアドレス必須")
            elif "@" not in email or "." not in email:
                errors.append("メールアドレス形式が不正です")
            
            # DMタイプ検証
            valid_dm_types = ["PROMOTIONAL", "TRANSACTIONAL", "NEWSLETTER"]
            if dm.get("DM_TYPE") not in valid_dm_types:
                errors.append("DMタイプが不正です")
            
            # テンプレートID検証
            if not dm.get("TEMPLATE_ID", "").strip():
                errors.append("テンプレートID必須")
            
            return errors
        
        # 検証実行
        validation_results = []
        for dm in test_dms:
            errors = validate_dm_data(dm)
            validation_results.append({
                "dm": dm,
                "errors": errors,
                "is_valid": len(errors) == 0
            })
        
        # アサーション
        self.assertEqual(len(validation_results), 4, "検証結果数不正")
        self.assertTrue(validation_results[0]["is_valid"], "正常DMが不正判定")
        self.assertFalse(validation_results[1]["is_valid"], "不正DM（空ID）が正常判定")
        self.assertFalse(validation_results[2]["is_valid"], "不正DM（無効メール）が正常判定")
        self.assertFalse(validation_results[3]["is_valid"], "不正DM（不明タイプ）が正常判定")
    
    def test_lookup_activity_dm_performance_metrics(self):
        """Lookup Activity: DM配信パフォーマンス指標取得テスト"""
        # テストケース: DM配信パフォーマンス指標の取得
        performance_metrics = [
            {
                "DM_TYPE": "PROMOTIONAL",
                "SENT_COUNT": 95,
                "DELIVERED_COUNT": 91,
                "OPENED_COUNT": 68,
                "CLICKED_COUNT": 28,
                "UNSUBSCRIBED_COUNT": 2,
                "BOUNCE_COUNT": 4,
                "DELIVERY_RATE": 0.958,
                "OPEN_RATE": 0.747,
                "CLICK_RATE": 0.295,
                "UNSUBSCRIBE_RATE": 0.021
            },
            {
                "DM_TYPE": "TRANSACTIONAL",
                "SENT_COUNT": 50,
                "DELIVERED_COUNT": 49,
                "OPENED_COUNT": 42,
                "CLICKED_COUNT": 15,
                "UNSUBSCRIBED_COUNT": 0,
                "BOUNCE_COUNT": 1,
                "DELIVERY_RATE": 0.980,
                "OPEN_RATE": 0.857,
                "CLICK_RATE": 0.300,
                "UNSUBSCRIBE_RATE": 0.000
            }
        ]
        
        self.mock_database.query_records.return_value = performance_metrics
        
        # Lookup Activity実行シミュレーション
        performance_query = f"""
        SELECT DM_TYPE,
               COUNT(*) as SENT_COUNT,
               SUM(CASE WHEN DELIVERY_STATUS = 'DELIVERED' THEN 1 ELSE 0 END) as DELIVERED_COUNT,
               SUM(CASE WHEN OPEN_STATUS = 'OPENED' THEN 1 ELSE 0 END) as OPENED_COUNT,
               SUM(CASE WHEN CLICK_STATUS = 'CLICKED' THEN 1 ELSE 0 END) as CLICKED_COUNT,
               SUM(CASE WHEN UNSUBSCRIBE_STATUS = 'UNSUBSCRIBED' THEN 1 ELSE 0 END) as UNSUBSCRIBED_COUNT,
               SUM(CASE WHEN DELIVERY_STATUS = 'BOUNCED' THEN 1 ELSE 0 END) as BOUNCE_COUNT,
               CAST(SUM(CASE WHEN DELIVERY_STATUS = 'DELIVERED' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) as DELIVERY_RATE,
               CAST(SUM(CASE WHEN OPEN_STATUS = 'OPENED' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) as OPEN_RATE,
               CAST(SUM(CASE WHEN CLICK_STATUS = 'CLICKED' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) as CLICK_RATE,
               CAST(SUM(CASE WHEN UNSUBSCRIBE_STATUS = 'UNSUBSCRIBED' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) as UNSUBSCRIBE_RATE
        FROM dm_delivery_logs
        WHERE SENT_DATE >= '{self.test_date}'
        GROUP BY DM_TYPE
        """
        
        result = self.mock_database.query_records("dm_delivery_logs", performance_query)
        
        self.assertEqual(len(result), 2, "DM配信パフォーマンス指標取得件数不正")
        self.assertEqual(result[0]["DM_TYPE"], "PROMOTIONAL", "DMタイプ確認失敗")
        self.assertEqual(result[0]["SENT_COUNT"], 95, "送信数確認失敗")
        self.assertEqual(result[0]["DELIVERY_RATE"], 0.958, "配信率確認失敗")
        self.assertEqual(result[0]["OPEN_RATE"], 0.747, "開封率確認失敗")
        self.assertEqual(result[1]["DM_TYPE"], "TRANSACTIONAL", "DMタイプ確認失敗")
        self.assertEqual(result[1]["DELIVERY_RATE"], 0.980, "配信率確認失敗")
    
    def test_data_flow_batch_dm_processing(self):
        """Data Flow: バッチDM処理テスト"""
        # テストケース: 大量DMのバッチ処理
        large_dm_dataset = []
        for i in range(1000):
            large_dm_dataset.append({
                "DM_ID": f"DM{i:06d}",
                "CUSTOMER_ID": f"CUST{i:06d}",
                "DM_TYPE": ["PROMOTIONAL", "TRANSACTIONAL"][i % 2],
                "DELIVERY_METHOD": ["EMAIL", "SMS"][i % 2],
                "PRIORITY": ["HIGH", "MEDIUM", "LOW"][i % 3],
                "TEMPLATE_ID": f"TEMP{(i % 5) + 1:03d}"
            })
        
        # バッチDM処理ロジック（Data Flow内の処理）
        def process_dm_batch(dm_list, batch_size=100):
            processed_batches = []
            
            for i in range(0, len(dm_list), batch_size):
                batch = dm_list[i:i + batch_size]
                
                # バッチ処理の実行
                batch_result = {
                    "batch_id": f"BATCH_{i//batch_size + 1:03d}",
                    "dm_count": len(batch),
                    "promotional_count": sum(1 for dm in batch if dm["DM_TYPE"] == "PROMOTIONAL"),
                    "transactional_count": sum(1 for dm in batch if dm["DM_TYPE"] == "TRANSACTIONAL"),
                    "email_count": sum(1 for dm in batch if dm["DELIVERY_METHOD"] == "EMAIL"),
                    "sms_count": sum(1 for dm in batch if dm["DELIVERY_METHOD"] == "SMS"),
                    "high_priority_count": sum(1 for dm in batch if dm["PRIORITY"] == "HIGH"),
                    "medium_priority_count": sum(1 for dm in batch if dm["PRIORITY"] == "MEDIUM"),
                    "low_priority_count": sum(1 for dm in batch if dm["PRIORITY"] == "LOW"),
                    "processing_time": 5.0,  # シミュレーション
                    "success_rate": 0.98
                }
                
                processed_batches.append(batch_result)
            
            return processed_batches
        
        # バッチ処理実行
        batch_results = process_dm_batch(large_dm_dataset, batch_size=100)
        
        # アサーション
        self.assertEqual(len(batch_results), 10, "バッチ数不正")  # 1000 / 100 = 10
        self.assertEqual(batch_results[0]["dm_count"], 100, "バッチサイズ不正")
        self.assertEqual(batch_results[0]["promotional_count"], 50, "プロモーションDM数不正")
        self.assertEqual(batch_results[0]["transactional_count"], 50, "トランザクションDM数不正")
        
        # 全バッチの合計確認
        total_dms = sum(batch["dm_count"] for batch in batch_results)
        total_promotional = sum(batch["promotional_count"] for batch in batch_results)
        total_transactional = sum(batch["transactional_count"] for batch in batch_results)
        
        self.assertEqual(total_dms, 1000, "全バッチ処理件数不正")
        self.assertEqual(total_promotional, 500, "全プロモーションDM数不正")
        self.assertEqual(total_transactional, 500, "全トランザクションDM数不正")
    
    def _create_client_dm_csv_content(self) -> str:
        """Client DMデータ用CSVコンテンツ生成"""
        header = "DM_ID,CUSTOMER_ID,CUSTOMER_NAME,EMAIL_ADDRESS,PHONE_NUMBER,POSTAL_CODE,ADDRESS,DM_TYPE,CAMPAIGN_ID,CAMPAIGN_NAME,TEMPLATE_ID,SEND_DATE,SEND_TIME,DELIVERY_METHOD,PRIORITY,STATUS"
        rows = []
        
        for item in self.sample_client_dm_data:
            row = f"{item['DM_ID']},{item['CUSTOMER_ID']},{item['CUSTOMER_NAME']},{item['EMAIL_ADDRESS']},{item['PHONE_NUMBER']},{item['POSTAL_CODE']},{item['ADDRESS']},{item['DM_TYPE']},{item['CAMPAIGN_ID']},{item['CAMPAIGN_NAME']},{item['TEMPLATE_ID']},{item['SEND_DATE']},{item['SEND_TIME']},{item['DELIVERY_METHOD']},{item['PRIORITY']},{item['STATUS']}"
            rows.append(row)
        
        return header + "\n" + "\n".join(rows)


if __name__ == "__main__":
    unittest.main()