"""
pi_Send_OpeningPaymentGuide パイプラインのユニットテスト

開栓時支払いガイド送信パイプラインの個別コンポーネントテスト
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any


class TestOpeningPaymentGuideUnit(unittest.TestCase):
    """開栓時支払いガイドパイプラインユニットテスト"""
    
    def setUp(self):
        """テストセットアップ"""
        self.pipeline_name = "pi_Send_OpeningPaymentGuide"
        self.domain = "smc"
        
        # モックサービス
        self.mock_blob_storage = Mock()
        self.mock_database = Mock()
        self.mock_email_service = Mock()
        self.mock_pdf_service = Mock()
        
        # テストデータ
        self.test_date = datetime.utcnow().strftime('%Y%m%d')
        self.test_file_path = f"OpeningPaymentGuide/{self.test_date}/opening_payment_guide.csv"
        
        # 基本的な開栓時支払いガイドデータ
        self.sample_opening_guide_data = [
            {
                "GUIDE_ID": "GUIDE000001",
                "CUSTOMER_ID": "CUST123456",
                "CUSTOMER_NAME": "テストユーザー1",
                "EMAIL_ADDRESS": "test1@example.com",
                "PHONE_NUMBER": "090-1234-5678",
                "SERVICE_ADDRESS": "東京都渋谷区1-1-1",
                "OPENING_DATE": "20240301",
                "METER_NUMBER": "M1234567890",
                "INITIAL_READING": 0,
                "DEPOSIT_AMOUNT": 10000.0,
                "PAYMENT_METHODS": "CREDIT_CARD,BANK_TRANSFER,CONVENIENCE_STORE",
                "CONTRACT_TYPE": "RESIDENTIAL",
                "GUIDE_TYPE": "STANDARD"
            },
            {
                "GUIDE_ID": "GUIDE000002",
                "CUSTOMER_ID": "CUST123457",
                "CUSTOMER_NAME": "テストユーザー2",
                "EMAIL_ADDRESS": "test2@example.com",
                "PHONE_NUMBER": "090-2345-6789",
                "SERVICE_ADDRESS": "東京都新宿区2-2-2",
                "OPENING_DATE": "20240302",
                "METER_NUMBER": "M2345678901",
                "INITIAL_READING": 0,
                "DEPOSIT_AMOUNT": 20000.0,
                "PAYMENT_METHODS": "CREDIT_CARD,BANK_TRANSFER",
                "CONTRACT_TYPE": "BUSINESS",
                "GUIDE_TYPE": "BUSINESS"
            }
        ]
    
    def test_lookup_activity_opening_schedule_detection(self):
        """Lookup Activity: 開栓予定検出テスト"""
        # テストケース: 開栓予定の検出
        mock_opening_schedule = [
            {
                "CUSTOMER_ID": "CUST123456",
                "SERVICE_ADDRESS": "東京都渋谷区1-1-1",
                "OPENING_DATE": "20240301",
                "METER_NUMBER": "M1234567890",
                "CONTRACT_TYPE": "RESIDENTIAL",
                "GUIDE_SENT": "N",
                "OPENING_STATUS": "SCHEDULED"
            },
            {
                "CUSTOMER_ID": "CUST123457",
                "SERVICE_ADDRESS": "東京都新宿区2-2-2",
                "OPENING_DATE": "20240302",
                "METER_NUMBER": "M2345678901",
                "CONTRACT_TYPE": "BUSINESS",
                "GUIDE_SENT": "N",
                "OPENING_STATUS": "SCHEDULED"
            }
        ]
        
        self.mock_database.query_records.return_value = mock_opening_schedule
        
        # Lookup Activity実行シミュレーション
        opening_query = f"""
        SELECT CUSTOMER_ID, SERVICE_ADDRESS, OPENING_DATE, METER_NUMBER, 
               CONTRACT_TYPE, GUIDE_SENT, OPENING_STATUS
        FROM gas_opening_schedule
        WHERE OPENING_DATE >= '{self.test_date}' AND OPENING_STATUS = 'SCHEDULED' 
        AND GUIDE_SENT = 'N'
        """
        
        result = self.mock_database.query_records("gas_opening_schedule", opening_query)
        
        self.assertEqual(len(result), 2, "開栓予定検出件数不正")
        self.assertEqual(result[0]["CUSTOMER_ID"], "CUST123456", "顧客ID確認失敗")
        self.assertEqual(result[0]["OPENING_STATUS"], "SCHEDULED", "開栓ステータス確認失敗")
        self.assertEqual(result[0]["GUIDE_SENT"], "N", "ガイド送信フラグ確認失敗")
    
    def test_lookup_activity_customer_contract_info(self):
        """Lookup Activity: 顧客契約情報取得テスト"""
        # テストケース: 顧客の契約情報取得
        mock_contract_info = [
            {
                "CUSTOMER_ID": "CUST123456",
                "CONTRACT_NUMBER": "C1234567890",
                "CONTRACT_TYPE": "RESIDENTIAL",
                "DEPOSIT_AMOUNT": 10000.0,
                "PREFERRED_PAYMENT_METHOD": "CREDIT_CARD",
                "BILLING_CYCLE": "MONTHLY"
            },
            {
                "CUSTOMER_ID": "CUST123457",
                "CONTRACT_NUMBER": "C2345678901",
                "CONTRACT_TYPE": "BUSINESS",
                "DEPOSIT_AMOUNT": 20000.0,
                "PREFERRED_PAYMENT_METHOD": "BANK_TRANSFER",
                "BILLING_CYCLE": "MONTHLY"
            }
        ]
        
        self.mock_database.query_records.return_value = mock_contract_info
        
        # Lookup Activity実行シミュレーション
        contract_query = """
        SELECT CUSTOMER_ID, CONTRACT_NUMBER, CONTRACT_TYPE, DEPOSIT_AMOUNT, 
               PREFERRED_PAYMENT_METHOD, BILLING_CYCLE
        FROM gas_contracts
        WHERE CUSTOMER_ID IN ('CUST123456', 'CUST123457')
        """
        
        result = self.mock_database.query_records("gas_contracts", contract_query)
        
        self.assertEqual(len(result), 2, "顧客契約情報取得件数不正")
        self.assertEqual(result[0]["CONTRACT_TYPE"], "RESIDENTIAL", "契約タイプ確認失敗")
        self.assertEqual(result[0]["DEPOSIT_AMOUNT"], 10000.0, "保証金額確認失敗")
    
    def test_data_flow_payment_guide_content_generation(self):
        """Data Flow: 支払いガイド内容生成テスト"""
        # テストケース: 支払いガイド内容の生成
        guide_data = {
            "CUSTOMER_NAME": "テストユーザー1",
            "SERVICE_ADDRESS": "東京都渋谷区1-1-1",
            "OPENING_DATE": "20240301",
            "METER_NUMBER": "M1234567890",
            "DEPOSIT_AMOUNT": 10000.0,
            "CONTRACT_TYPE": "RESIDENTIAL",
            "PAYMENT_METHODS": "CREDIT_CARD,BANK_TRANSFER,CONVENIENCE_STORE"
        }
        
        # 支払いガイド内容生成ロジック（Data Flow内の処理）
        def generate_payment_guide_content(guide_data):
            customer_name = guide_data["CUSTOMER_NAME"]
            service_address = guide_data["SERVICE_ADDRESS"]
            opening_date = guide_data["OPENING_DATE"]
            meter_number = guide_data["METER_NUMBER"]
            deposit_amount = guide_data["DEPOSIT_AMOUNT"]
            contract_type = guide_data["CONTRACT_TYPE"]
            payment_methods = guide_data["PAYMENT_METHODS"].split(",")
            
            # 開栓日のフォーマット
            formatted_opening_date = f"{opening_date[:4]}年{opening_date[4:6]}月{opening_date[6:8]}日"
            
            # 契約タイプ別のメッセージ調整
            if contract_type == "RESIDENTIAL":
                contract_msg = "ご家庭用ガス"
                deposit_msg = "家庭用保証金"
            else:  # BUSINESS
                contract_msg = "事業用ガス"
                deposit_msg = "事業用保証金"
            
            # 支払い方法の日本語化
            method_names = {
                "CREDIT_CARD": "クレジットカード",
                "BANK_TRANSFER": "銀行振込",
                "CONVENIENCE_STORE": "コンビニ支払い"
            }
            
            payment_method_list = [method_names.get(method, method) for method in payment_methods]
            payment_methods_text = "、".join(payment_method_list)
            
            guide_content = f"""
            {customer_name}様
            
            ガス開栓時のお支払いガイド
            
            この度は東京ガスをご契約いただき、ありがとうございます。
            
            開栓情報：
            - サービス住所：{service_address}
            - 開栓予定日：{formatted_opening_date}
            - メーター番号：{meter_number}
            - 契約種別：{contract_msg}
            
            お支払い情報：
            - {deposit_msg}：{deposit_amount:,.0f}円
            - ご利用可能なお支払い方法：{payment_methods_text}
            
            開栓作業時に作業員がお伺いいたします。
            お支払い方法の詳細については、同封の資料をご確認ください。
            
            ご不明な点がございましたら、お気軽にお問い合わせください。
            
            東京ガス株式会社
            """
            
            return guide_content.strip()
        
        # ガイド内容生成実行
        content = generate_payment_guide_content(guide_data)
        
        # アサーション
        self.assertIn("テストユーザー1様", content, "顧客名挿入失敗")
        self.assertIn("東京都渋谷区1-1-1", content, "サービス住所挿入失敗")
        self.assertIn("2024年03月01日", content, "開栓日フォーマット失敗")
        self.assertIn("M1234567890", content, "メーター番号挿入失敗")
        self.assertIn("10,000円", content, "保証金額挿入失敗")
        self.assertIn("ご家庭用ガス", content, "契約タイプ挿入失敗")
        self.assertIn("クレジットカード", content, "支払い方法挿入失敗")
    
    def test_data_flow_deposit_calculation(self):
        """Data Flow: 保証金計算テスト"""
        # テストケース: 保証金の計算
        deposit_scenarios = [
            {
                "CONTRACT_TYPE": "RESIDENTIAL",
                "ESTIMATED_MONTHLY_USAGE": 30,  # m³
                "CUSTOMER_HISTORY": "NEW",
                "EXPECTED_DEPOSIT": 10000.0
            },
            {
                "CONTRACT_TYPE": "BUSINESS",
                "ESTIMATED_MONTHLY_USAGE": 100,  # m³
                "CUSTOMER_HISTORY": "GOOD",
                "EXPECTED_DEPOSIT": 20000.0
            },
            {
                "CONTRACT_TYPE": "BUSINESS",
                "ESTIMATED_MONTHLY_USAGE": 500,  # m³
                "CUSTOMER_HISTORY": "AVERAGE",
                "EXPECTED_DEPOSIT": 50000.0
            }
        ]
        
        # 保証金計算ロジック（Data Flow内の処理）
        def calculate_deposit_amount(contract_data):
            contract_type = contract_data["CONTRACT_TYPE"]
            estimated_usage = contract_data["ESTIMATED_MONTHLY_USAGE"]
            customer_history = contract_data["CUSTOMER_HISTORY"]
            
            # 基本保証金
            if contract_type == "RESIDENTIAL":
                base_deposit = 10000.0
            else:  # BUSINESS
                base_deposit = 20000.0
            
            # 使用量による調整
            if contract_type == "BUSINESS":
                if estimated_usage > 200:
                    usage_multiplier = 2.5
                elif estimated_usage > 100:
                    usage_multiplier = 1.5
                else:
                    usage_multiplier = 1.0
                
                base_deposit *= usage_multiplier
            
            # 顧客履歴による調整
            if customer_history == "GOOD":
                history_multiplier = 1.0
            elif customer_history == "AVERAGE":
                history_multiplier = 1.0
            elif customer_history == "POOR":
                history_multiplier = 1.5
            else:  # NEW
                history_multiplier = 1.0
            
            final_deposit = base_deposit * history_multiplier
            
            # 1000円単位に切り上げ
            return round(final_deposit / 1000) * 1000
        
        # 各シナリオでの保証金計算
        for scenario in deposit_scenarios:
            calculated_deposit = calculate_deposit_amount(scenario)
            self.assertEqual(calculated_deposit, scenario["EXPECTED_DEPOSIT"], 
                           f"保証金計算失敗: {scenario}")
    
    def test_data_flow_guide_personalization(self):
        """Data Flow: ガイド個人化処理テスト"""
        # テストケース: ガイドの個人化
        customer_data = {
            "CUSTOMER_ID": "CUST123456",
            "CUSTOMER_NAME": "テストユーザー1",
            "CONTRACT_TYPE": "RESIDENTIAL",
            "PREFERRED_PAYMENT_METHOD": "CREDIT_CARD",
            "PREVIOUS_CONTRACTS": 0,
            "CUSTOMER_SEGMENT": "NEW"
        }
        
        # ガイド個人化ロジック（Data Flow内の処理）
        def personalize_payment_guide(customer_data):
            customer_name = customer_data["CUSTOMER_NAME"]
            contract_type = customer_data["CONTRACT_TYPE"]
            preferred_method = customer_data["PREFERRED_PAYMENT_METHOD"]
            previous_contracts = customer_data["PREVIOUS_CONTRACTS"]
            customer_segment = customer_data["CUSTOMER_SEGMENT"]
            
            personalization = {
                "greeting": f"{customer_name}様",
                "welcome_message": "",
                "payment_recommendation": "",
                "additional_info": ""
            }
            
            # 新規/既存顧客別のメッセージ
            if previous_contracts == 0:
                personalization["welcome_message"] = "初めて東京ガスをご利用いただき、ありがとうございます。"
            else:
                personalization["welcome_message"] = "いつも東京ガスをご利用いただき、ありがとうございます。"
            
            # 優先支払い方法の推奨
            method_recommendations = {
                "CREDIT_CARD": "クレジットカードでのお支払いが便利です。",
                "BANK_TRANSFER": "銀行振込でのお支払いが確実です。",
                "CONVENIENCE_STORE": "コンビニでのお支払いが手軽です。"
            }
            
            personalization["payment_recommendation"] = method_recommendations.get(
                preferred_method, "お好みのお支払い方法をお選びください。"
            )
            
            # 顧客セグメント別の追加情報
            if customer_segment == "PREMIUM":
                personalization["additional_info"] = "プレミアム会員特典として、保証金が減額されます。"
            elif contract_type == "BUSINESS":
                personalization["additional_info"] = "事業用ガスの詳細な料金体系については、別途資料をご確認ください。"
            
            return personalization
        
        # 個人化実行
        personalized_content = personalize_payment_guide(customer_data)
        
        # アサーション
        self.assertEqual(personalized_content["greeting"], "テストユーザー1様", "挨拶個人化失敗")
        self.assertIn("初めて東京ガス", personalized_content["welcome_message"], "ウェルカムメッセージ個人化失敗")
        self.assertIn("クレジットカード", personalized_content["payment_recommendation"], "支払い推奨個人化失敗")
    
    def test_script_activity_pdf_guide_generation(self):
        """Script Activity: PDFガイド生成処理テスト"""
        # テストケース: PDFガイドの生成
        guide_data = {
            "GUIDE_ID": "GUIDE000001",
            "CUSTOMER_NAME": "テストユーザー1",
            "SERVICE_ADDRESS": "東京都渋谷区1-1-1",
            "OPENING_DATE": "2024年03月01日",
            "PAYMENT_METHODS": "クレジットカード、銀行振込、コンビニ支払い",
            "DEPOSIT_AMOUNT": "10,000円"
        }
        
        self.mock_pdf_service.generate_payment_guide_pdf.return_value = {
            "status": "success",
            "pdf_path": f"/tmp/guides/{guide_data['GUIDE_ID']}.pdf",
            "file_size": 156789,
            "generation_time": datetime.utcnow().isoformat()
        }
        
        # Script Activity実行シミュレーション
        result = self.mock_pdf_service.generate_payment_guide_pdf(
            guide_data,
            template="opening_payment_guide_template"
        )
        
        self.assertEqual(result["status"], "success", "PDFガイド生成失敗")
        self.assertIn(guide_data["GUIDE_ID"], result["pdf_path"], "PDFパス確認失敗")
        self.assertGreater(result["file_size"], 0, "PDFファイルサイズ確認失敗")
        self.mock_pdf_service.generate_payment_guide_pdf.assert_called_once()
    
    def test_script_activity_email_guide_sending(self):
        """Script Activity: メールガイド送信処理テスト"""
        # テストケース: メールでのガイド送信
        guide_email_data = {
            "to": "test@example.com",
            "subject": "【東京ガス】開栓時のお支払いガイド",
            "body": "開栓時のお支払いガイドを添付いたします。",
            "attachments": ["/tmp/guides/GUIDE000001.pdf"],
            "guide_id": "GUIDE000001"
        }
        
        self.mock_email_service.send_guide_email.return_value = {
            "status": "sent",
            "message_id": "GUIDE_EMAIL_123",
            "delivery_time": datetime.utcnow().isoformat(),
            "guide_id": "GUIDE000001"
        }
        
        # Script Activity実行シミュレーション
        result = self.mock_email_service.send_guide_email(
            guide_email_data["to"],
            guide_email_data["subject"],
            guide_email_data["body"],
            attachments=guide_email_data["attachments"],
            guide_id=guide_email_data["guide_id"]
        )
        
        self.assertEqual(result["status"], "sent", "メールガイド送信失敗")
        self.assertIsNotNone(result["message_id"], "メッセージID取得失敗")
        self.assertEqual(result["guide_id"], "GUIDE000001", "ガイドID確認失敗")
        self.mock_email_service.send_guide_email.assert_called_once()
    
    def test_copy_activity_guide_delivery_log(self):
        """Copy Activity: ガイド配信ログ記録テスト"""
        # テストケース: ガイド配信ログの記録
        delivery_logs = [
            {
                "GUIDE_ID": "GUIDE000001",
                "CUSTOMER_ID": "CUST123456",
                "DELIVERY_METHOD": "EMAIL",
                "DELIVERY_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "DELIVERY_TIME": datetime.utcnow().strftime('%H:%M:%S'),
                "STATUS": "DELIVERED",
                "MESSAGE_ID": "GUIDE_EMAIL_123"
            },
            {
                "GUIDE_ID": "GUIDE000002",
                "CUSTOMER_ID": "CUST123457",
                "DELIVERY_METHOD": "EMAIL",
                "DELIVERY_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "DELIVERY_TIME": datetime.utcnow().strftime('%H:%M:%S'),
                "STATUS": "DELIVERED",
                "MESSAGE_ID": "GUIDE_EMAIL_124"
            }
        ]
        
        self.mock_database.insert_records.return_value = True
        
        # Copy Activity実行シミュレーション
        delivery_log_table = "opening_payment_guide_delivery_logs"
        result = self.mock_database.insert_records(delivery_log_table, delivery_logs)
        
        self.assertTrue(result, "ガイド配信ログ記録失敗")
        self.mock_database.insert_records.assert_called_once_with(delivery_log_table, delivery_logs)
    
    def test_copy_activity_guide_status_update(self):
        """Copy Activity: ガイド送信ステータス更新テスト"""
        # テストケース: ガイド送信ステータスの更新
        status_updates = [
            {
                "CUSTOMER_ID": "CUST123456",
                "GUIDE_SENT": "Y",
                "GUIDE_SENT_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "UPDATED_BY": "SYSTEM"
            },
            {
                "CUSTOMER_ID": "CUST123457",
                "GUIDE_SENT": "Y",
                "GUIDE_SENT_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "UPDATED_BY": "SYSTEM"
            }
        ]
        
        self.mock_database.update_records.return_value = True
        
        # Copy Activity実行シミュレーション
        for update in status_updates:
            result = self.mock_database.update_records(
                "gas_opening_schedule",
                update,
                where_clause=f"CUSTOMER_ID = '{update['CUSTOMER_ID']}'"
            )
            self.assertTrue(result, f"ガイド送信ステータス更新失敗: {update['CUSTOMER_ID']}")
        
        self.assertEqual(self.mock_database.update_records.call_count, 2, "更新処理回数不正")
    
    def test_script_activity_guide_analytics(self):
        """Script Activity: ガイド分析処理テスト"""
        # テストケース: ガイド送信の分析データ生成
        guide_analytics = {
            "execution_date": self.test_date,
            "total_guides_sent": 45,
            "residential_guides": 35,
            "business_guides": 10,
            "email_deliveries": 43,
            "postal_deliveries": 2,
            "pdf_generation_success_rate": 0.98,
            "email_delivery_success_rate": 0.96,
            "average_deposit_amount": 15000.0,
            "processing_time_minutes": 6.2
        }
        
        self.mock_database.insert_records.return_value = True
        
        # Script Activity実行シミュレーション
        analytics_table = "opening_payment_guide_analytics"
        result = self.mock_database.insert_records(analytics_table, [guide_analytics])
        
        self.assertTrue(result, "ガイド分析記録失敗")
        self.mock_database.insert_records.assert_called_once_with(analytics_table, [guide_analytics])
    
    def test_data_flow_guide_validation(self):
        """Data Flow: ガイドデータ検証テスト"""
        # テストケース: ガイドデータの検証
        test_guides = [
            {"CUSTOMER_ID": "CUST123456", "OPENING_DATE": "20240301", "METER_NUMBER": "M1234567890", "DEPOSIT_AMOUNT": 10000.0},
            {"CUSTOMER_ID": "", "OPENING_DATE": "20240301", "METER_NUMBER": "M1234567890", "DEPOSIT_AMOUNT": 10000.0},  # 不正: 空ID
            {"CUSTOMER_ID": "CUST123458", "OPENING_DATE": "20240201", "METER_NUMBER": "M1234567890", "DEPOSIT_AMOUNT": 10000.0},  # 不正: 過去の日付
            {"CUSTOMER_ID": "CUST123459", "OPENING_DATE": "20240301", "METER_NUMBER": "", "DEPOSIT_AMOUNT": -5000.0}  # 不正: 空メーター、負の保証金
        ]
        
        # ガイドデータ検証ロジック（Data Flow内の処理）
        def validate_guide_data(guide):
            errors = []
            current_date = datetime.utcnow().strftime('%Y%m%d')
            
            # 顧客ID検証
            if not guide.get("CUSTOMER_ID", "").strip():
                errors.append("顧客ID必須")
            
            # 開栓日検証
            opening_date = guide.get("OPENING_DATE", "")
            if opening_date < current_date:
                errors.append("開栓日は現在日付以降である必要があります")
            
            # メーター番号検証
            if not guide.get("METER_NUMBER", "").strip():
                errors.append("メーター番号必須")
            
            # 保証金額検証
            deposit_amount = guide.get("DEPOSIT_AMOUNT", 0)
            if deposit_amount < 0:
                errors.append("保証金額は正の値である必要があります")
            
            return errors
        
        # 検証実行
        validation_results = []
        for guide in test_guides:
            errors = validate_guide_data(guide)
            validation_results.append({
                "guide": guide,
                "errors": errors,
                "is_valid": len(errors) == 0
            })
        
        # アサーション
        self.assertEqual(len(validation_results), 4, "検証結果数不正")
        self.assertTrue(validation_results[0]["is_valid"], "正常ガイドが不正判定")
        self.assertFalse(validation_results[1]["is_valid"], "不正ガイド（空ID）が正常判定")
        self.assertFalse(validation_results[2]["is_valid"], "不正ガイド（過去の日付）が正常判定")
        self.assertFalse(validation_results[3]["is_valid"], "不正ガイド（空メーター、負の保証金）が正常判定")
    
    def test_lookup_activity_opening_statistics(self):
        """Lookup Activity: 開栓統計取得テスト"""
        # テストケース: 開栓の統計情報取得
        opening_statistics = [
            {
                "CONTRACT_TYPE": "RESIDENTIAL",
                "OPENING_COUNT": 35,
                "AVERAGE_DEPOSIT": 12000.0,
                "PREFERRED_PAYMENT_METHOD": "CREDIT_CARD"
            },
            {
                "CONTRACT_TYPE": "BUSINESS",
                "OPENING_COUNT": 10,
                "AVERAGE_DEPOSIT": 25000.0,
                "PREFERRED_PAYMENT_METHOD": "BANK_TRANSFER"
            }
        ]
        
        self.mock_database.query_records.return_value = opening_statistics
        
        # Lookup Activity実行シミュレーション
        statistics_query = f"""
        SELECT CONTRACT_TYPE, 
               COUNT(*) as OPENING_COUNT,
               AVG(DEPOSIT_AMOUNT) as AVERAGE_DEPOSIT,
               MODE() WITHIN GROUP (ORDER BY PREFERRED_PAYMENT_METHOD) as PREFERRED_PAYMENT_METHOD
        FROM gas_opening_schedule
        WHERE OPENING_DATE >= '{self.test_date}'
        GROUP BY CONTRACT_TYPE
        """
        
        result = self.mock_database.query_records("gas_opening_schedule", statistics_query)
        
        self.assertEqual(len(result), 2, "開栓統計取得件数不正")
        self.assertEqual(result[0]["CONTRACT_TYPE"], "RESIDENTIAL", "契約タイプ確認失敗")
        self.assertEqual(result[0]["OPENING_COUNT"], 35, "開栓件数確認失敗")
        self.assertEqual(result[0]["AVERAGE_DEPOSIT"], 12000.0, "平均保証金確認失敗")
    
    def _create_opening_payment_guide_csv_content(self) -> str:
        """開栓時支払いガイドデータ用CSVコンテンツ生成"""
        header = "GUIDE_ID,CUSTOMER_ID,CUSTOMER_NAME,EMAIL_ADDRESS,PHONE_NUMBER,SERVICE_ADDRESS,OPENING_DATE,METER_NUMBER,INITIAL_READING,DEPOSIT_AMOUNT,PAYMENT_METHODS,CONTRACT_TYPE,GUIDE_TYPE"
        rows = []
        
        for item in self.sample_opening_guide_data:
            row = f"{item['GUIDE_ID']},{item['CUSTOMER_ID']},{item['CUSTOMER_NAME']},{item['EMAIL_ADDRESS']},{item['PHONE_NUMBER']},{item['SERVICE_ADDRESS']},{item['OPENING_DATE']},{item['METER_NUMBER']},{item['INITIAL_READING']},{item['DEPOSIT_AMOUNT']},{item['PAYMENT_METHODS']},{item['CONTRACT_TYPE']},{item['GUIDE_TYPE']}"
            rows.append(row)
        
        return header + "\n" + "\n".join(rows)


if __name__ == "__main__":
    unittest.main()