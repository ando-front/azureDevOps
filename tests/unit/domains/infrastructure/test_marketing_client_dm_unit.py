"""
pi_Copy_marketing_client_dm パイプラインのユニットテスト（Infrastructure）

インフラストラクチャマーケティングClient DMパイプラインの個別コンポーネントテスト
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any


class TestInfrastructureMarketingClientDmUnit(unittest.TestCase):
    """インフラストラクチャマーケティングClient DMパイプラインユニットテスト"""
    
    def setUp(self):
        """テストセットアップ"""
        self.pipeline_name = "pi_Copy_marketing_client_dm"
        self.domain = "infrastructure"
        
        # モックサービス
        self.mock_blob_storage = Mock()
        self.mock_database = Mock()
        self.mock_synapse_service = Mock()
        self.mock_data_lake_service = Mock()
        
        # テストデータ
        self.test_date = datetime.utcnow().strftime('%Y%m%d')
        self.source_file_path = f"infrastructure/marketing_client_dm/source/{self.test_date}/marketing_client_dm_source.csv"
        self.target_file_path = f"infrastructure/marketing_client_dm/target/{self.test_date}/marketing_client_dm_target.csv"
        
        # 基本的なインフラストラクチャマーケティングClient DMデータ
        self.sample_infrastructure_marketing_dm_data = [
            {
                "INFRA_ID": "INFRA000001",
                "SYSTEM_SOURCE": "TGCRM",
                "EXTRACTION_TIMESTAMP": "2024-03-01T10:00:00Z",
                "CUSTOMER_ID": "CUST123456",
                "CUSTOMER_NAME": "テストユーザー1",
                "EMAIL_ADDRESS": "test1@example.com",
                "PHONE_NUMBER": "090-1234-5678",
                "CUSTOMER_SEGMENT": "PREMIUM",
                "REGISTRATION_DATE": "20230101",
                "LAST_ACTIVITY_DATE": "20240228",
                "LIFECYCLE_STAGE": "ACTIVE",
                "MARKETING_PREFERENCES": {
                    "email_opt_in": True,
                    "sms_opt_in": False,
                    "postal_opt_in": True,
                    "promotional_opt_in": True
                },
                "SYSTEM_METADATA": {
                    "record_version": "1.0",
                    "data_quality_score": 0.95,
                    "last_sync_timestamp": "2024-03-01T09:55:00Z"
                },
                "PROCESSING_STATUS": "PENDING",
                "BATCH_ID": "BATCH_20240301_001"
            },
            {
                "INFRA_ID": "INFRA000002",
                "SYSTEM_SOURCE": "SYNAPSE",
                "EXTRACTION_TIMESTAMP": "2024-03-01T10:00:00Z",
                "CUSTOMER_ID": "CUST123457",
                "CUSTOMER_NAME": "テストユーザー2",
                "EMAIL_ADDRESS": "test2@example.com",
                "PHONE_NUMBER": "090-2345-6789",
                "CUSTOMER_SEGMENT": "STANDARD",
                "REGISTRATION_DATE": "20230201",
                "LAST_ACTIVITY_DATE": "20240225",
                "LIFECYCLE_STAGE": "ACTIVE",
                "MARKETING_PREFERENCES": {
                    "email_opt_in": True,
                    "sms_opt_in": True,
                    "postal_opt_in": False,
                    "promotional_opt_in": False
                },
                "SYSTEM_METADATA": {
                    "record_version": "1.0",
                    "data_quality_score": 0.88,
                    "last_sync_timestamp": "2024-03-01T09:55:00Z"
                },
                "PROCESSING_STATUS": "PENDING",
                "BATCH_ID": "BATCH_20240301_001"
            }
        ]
    
    def test_lookup_activity_infrastructure_source_systems(self):
        """Lookup Activity: インフラストラクチャソースシステム検出テスト"""
        # テストケース: インフラストラクチャソースシステムの検出
        mock_source_systems = [
            {
                "SYSTEM_ID": "TGCRM",
                "SYSTEM_NAME": "Tokyo Gas CRM",
                "SYSTEM_TYPE": "CRM",
                "CONNECTION_STRING": "Server=tgcrm-prod;Database=CRM;",
                "LAST_SYNC_TIME": "2024-03-01T09:55:00Z",
                "SYNC_STATUS": "ACTIVE",
                "RECORD_COUNT": 50000,
                "DATA_QUALITY_THRESHOLD": 0.90
            },
            {
                "SYSTEM_ID": "SYNAPSE",
                "SYSTEM_NAME": "Azure Synapse Analytics",
                "SYSTEM_TYPE": "DATA_WAREHOUSE",
                "CONNECTION_STRING": "Server=synapse-prod.sql.azuresynapse.net;Database=Marketing;",
                "LAST_SYNC_TIME": "2024-03-01T09:55:00Z",
                "SYNC_STATUS": "ACTIVE",
                "RECORD_COUNT": 75000,
                "DATA_QUALITY_THRESHOLD": 0.85
            }
        ]
        
        self.mock_database.query_records.return_value = mock_source_systems
        
        # Lookup Activity実行シミュレーション
        source_systems_query = f"""
        SELECT SYSTEM_ID, SYSTEM_NAME, SYSTEM_TYPE, CONNECTION_STRING, 
               LAST_SYNC_TIME, SYNC_STATUS, RECORD_COUNT, DATA_QUALITY_THRESHOLD
        FROM infrastructure_source_systems
        WHERE SYNC_STATUS = 'ACTIVE' 
        AND LAST_SYNC_TIME >= DATEADD(hour, -1, GETDATE())
        ORDER BY LAST_SYNC_TIME DESC
        """
        
        result = self.mock_database.query_records("infrastructure_source_systems", source_systems_query)
        
        self.assertEqual(len(result), 2, "インフラストラクチャソースシステム検出件数不正")
        self.assertEqual(result[0]["SYSTEM_ID"], "TGCRM", "システムID確認失敗")
        self.assertEqual(result[0]["SYNC_STATUS"], "ACTIVE", "同期ステータス確認失敗")
        self.assertEqual(result[0]["RECORD_COUNT"], 50000, "レコード数確認失敗")
        self.assertEqual(result[1]["SYSTEM_ID"], "SYNAPSE", "システムID確認失敗")
        self.assertEqual(result[1]["SYSTEM_TYPE"], "DATA_WAREHOUSE", "システムタイプ確認失敗")
    
    def test_lookup_activity_data_lineage_tracking(self):
        """Lookup Activity: データ系譜追跡テスト"""
        # テストケース: データ系譜の追跡
        mock_data_lineage = [
            {
                "LINEAGE_ID": "LINEAGE_001",
                "SOURCE_SYSTEM": "TGCRM",
                "SOURCE_TABLE": "customers",
                "TARGET_SYSTEM": "SYNAPSE",
                "TARGET_TABLE": "marketing_client_dm_target",
                "TRANSFORMATION_PIPELINE": "pi_Copy_marketing_client_dm",
                "TRANSFORMATION_RULES": [
                    "CUSTOMER_SEGMENT: PREMIUM -> PREMIUM_MARKETING",
                    "PHONE_NUMBER: FORMAT(090-XXXX-XXXX)",
                    "EMAIL_ADDRESS: VALIDATE_EMAIL_FORMAT"
                ],
                "DATA_FLOW_TIMESTAMP": "2024-03-01T10:00:00Z",
                "RECORD_COUNT_SOURCE": 50000,
                "RECORD_COUNT_TARGET": 49500,
                "DATA_QUALITY_IMPACT": 0.01
            },
            {
                "LINEAGE_ID": "LINEAGE_002",
                "SOURCE_SYSTEM": "SYNAPSE",
                "SOURCE_TABLE": "customer_marketing_preferences",
                "TARGET_SYSTEM": "SYNAPSE",
                "TARGET_TABLE": "marketing_client_dm_target",
                "TRANSFORMATION_PIPELINE": "pi_Copy_marketing_client_dm",
                "TRANSFORMATION_RULES": [
                    "MARKETING_PREFERENCES: JSON_EXTRACT_PREFERENCES",
                    "LIFECYCLE_STAGE: DERIVE_FROM_ACTIVITY_DATE"
                ],
                "DATA_FLOW_TIMESTAMP": "2024-03-01T10:00:00Z",
                "RECORD_COUNT_SOURCE": 75000,
                "RECORD_COUNT_TARGET": 74250,
                "DATA_QUALITY_IMPACT": 0.01
            }
        ]
        
        self.mock_database.query_records.return_value = mock_data_lineage
        
        # Lookup Activity実行シミュレーション
        lineage_query = f"""
        SELECT LINEAGE_ID, SOURCE_SYSTEM, SOURCE_TABLE, TARGET_SYSTEM, TARGET_TABLE,
               TRANSFORMATION_PIPELINE, TRANSFORMATION_RULES, DATA_FLOW_TIMESTAMP,
               RECORD_COUNT_SOURCE, RECORD_COUNT_TARGET, DATA_QUALITY_IMPACT
        FROM data_lineage_tracking
        WHERE TRANSFORMATION_PIPELINE = 'pi_Copy_marketing_client_dm'
        AND DATA_FLOW_TIMESTAMP >= '{self.test_date}'
        ORDER BY DATA_FLOW_TIMESTAMP DESC
        """
        
        result = self.mock_database.query_records("data_lineage_tracking", lineage_query)
        
        self.assertEqual(len(result), 2, "データ系譜追跡件数不正")
        self.assertEqual(result[0]["LINEAGE_ID"], "LINEAGE_001", "系譜ID確認失敗")
        self.assertEqual(result[0]["SOURCE_SYSTEM"], "TGCRM", "ソースシステム確認失敗")
        self.assertEqual(result[0]["TARGET_SYSTEM"], "SYNAPSE", "ターゲットシステム確認失敗")
        self.assertEqual(result[0]["RECORD_COUNT_SOURCE"], 50000, "ソースレコード数確認失敗")
        self.assertEqual(result[0]["RECORD_COUNT_TARGET"], 49500, "ターゲットレコード数確認失敗")
    
    def test_data_flow_infrastructure_data_transformation(self):
        """Data Flow: インフラストラクチャデータ変換処理テスト"""
        # テストケース: インフラストラクチャデータ変換処理
        source_data = {
            "INFRA_ID": "INFRA000001",
            "SYSTEM_SOURCE": "TGCRM",
            "CUSTOMER_ID": "CUST123456",
            "CUSTOMER_NAME": "テストユーザー1",
            "EMAIL_ADDRESS": "test1@example.com",
            "PHONE_NUMBER": "090-1234-5678",
            "CUSTOMER_SEGMENT": "PREMIUM",
            "REGISTRATION_DATE": "20230101",
            "LAST_ACTIVITY_DATE": "20240228",
            "MARKETING_PREFERENCES": {
                "email_opt_in": True,
                "sms_opt_in": False,
                "postal_opt_in": True,
                "promotional_opt_in": True
            },
            "SYSTEM_METADATA": {
                "record_version": "1.0",
                "data_quality_score": 0.95,
                "last_sync_timestamp": "2024-03-01T09:55:00Z"
            }
        }
        
        # インフラストラクチャデータ変換処理ロジック（Data Flow内の処理）
        def transform_infrastructure_marketing_dm_data(source_data):
            # インフラストラクチャターゲットID生成
            target_id = f"INFRA_TGT_{source_data['CUSTOMER_ID'][4:]}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
            
            # システムソース別の変換ルール
            if source_data["SYSTEM_SOURCE"] == "TGCRM":
                system_priority = "HIGH"
                data_confidence = 0.95
            elif source_data["SYSTEM_SOURCE"] == "SYNAPSE":
                system_priority = "MEDIUM"
                data_confidence = 0.88
            else:
                system_priority = "LOW"
                data_confidence = 0.70
            
            # マーケティングセグメント変換
            segment_mapping = {
                "PREMIUM": "INFRA_PREMIUM",
                "STANDARD": "INFRA_STANDARD",
                "BASIC": "INFRA_BASIC",
                "VIP": "INFRA_VIP"
            }
            infrastructure_segment = segment_mapping.get(source_data["CUSTOMER_SEGMENT"], "INFRA_UNKNOWN")
            
            # マーケティング設定統合
            marketing_preferences = source_data.get("MARKETING_PREFERENCES", {})
            preference_score = sum([
                1 if marketing_preferences.get("email_opt_in", False) else 0,
                1 if marketing_preferences.get("sms_opt_in", False) else 0,
                1 if marketing_preferences.get("postal_opt_in", False) else 0,
                1 if marketing_preferences.get("promotional_opt_in", False) else 0
            ])
            
            # ライフサイクルステージ判定
            last_activity_date = datetime.strptime(source_data["LAST_ACTIVITY_DATE"], "%Y%m%d")
            days_since_last_activity = (datetime.utcnow() - last_activity_date).days
            
            if days_since_last_activity <= 30:
                lifecycle_stage = "HIGHLY_ACTIVE"
            elif days_since_last_activity <= 90:
                lifecycle_stage = "ACTIVE"
            elif days_since_last_activity <= 180:
                lifecycle_stage = "MODERATE"
            else:
                lifecycle_stage = "INACTIVE"
            
            # データ品質評価
            system_metadata = source_data.get("SYSTEM_METADATA", {})
            data_quality_score = system_metadata.get("data_quality_score", 0.0)
            
            # 変換されたデータ
            transformed_data = {
                "INFRASTRUCTURE_TARGET_ID": target_id,
                "ORIGINAL_INFRA_ID": source_data["INFRA_ID"],
                "SYSTEM_SOURCE": source_data["SYSTEM_SOURCE"],
                "SYSTEM_PRIORITY": system_priority,
                "DATA_CONFIDENCE": data_confidence,
                "CUSTOMER_ID": source_data["CUSTOMER_ID"],
                "CUSTOMER_NAME": source_data["CUSTOMER_NAME"],
                "EMAIL_ADDRESS": source_data["EMAIL_ADDRESS"],
                "PHONE_NUMBER": source_data["PHONE_NUMBER"],
                "INFRASTRUCTURE_SEGMENT": infrastructure_segment,
                "REGISTRATION_DATE": source_data["REGISTRATION_DATE"],
                "LAST_ACTIVITY_DATE": source_data["LAST_ACTIVITY_DATE"],
                "DAYS_SINCE_LAST_ACTIVITY": days_since_last_activity,
                "LIFECYCLE_STAGE": lifecycle_stage,
                "MARKETING_PREFERENCE_SCORE": preference_score,
                "EMAIL_OPT_IN": marketing_preferences.get("email_opt_in", False),
                "SMS_OPT_IN": marketing_preferences.get("sms_opt_in", False),
                "POSTAL_OPT_IN": marketing_preferences.get("postal_opt_in", False),
                "PROMOTIONAL_OPT_IN": marketing_preferences.get("promotional_opt_in", False),
                "DATA_QUALITY_SCORE": data_quality_score,
                "RECORD_VERSION": system_metadata.get("record_version", "1.0"),
                "TRANSFORMATION_TIMESTAMP": datetime.utcnow().isoformat(),
                "TRANSFORMATION_PIPELINE": "pi_Copy_marketing_client_dm",
                "INFRASTRUCTURE_PROCESSING_STATUS": "TRANSFORMED"
            }
            
            return transformed_data
        
        # インフラストラクチャデータ変換実行
        transformed_data = transform_infrastructure_marketing_dm_data(source_data)
        
        # アサーション
        self.assertIsNotNone(transformed_data["INFRASTRUCTURE_TARGET_ID"], "インフラストラクチャターゲットID生成失敗")
        self.assertTrue(transformed_data["INFRASTRUCTURE_TARGET_ID"].startswith("INFRA_TGT_123456"), "ターゲットID形式確認失敗")
        self.assertEqual(transformed_data["SYSTEM_SOURCE"], "TGCRM", "システムソース引き継ぎ失敗")
        self.assertEqual(transformed_data["SYSTEM_PRIORITY"], "HIGH", "システム優先度設定失敗")
        self.assertEqual(transformed_data["DATA_CONFIDENCE"], 0.95, "データ信頼度設定失敗")
        self.assertEqual(transformed_data["INFRASTRUCTURE_SEGMENT"], "INFRA_PREMIUM", "インフラストラクチャセグメント変換失敗")
        self.assertEqual(transformed_data["LIFECYCLE_STAGE"], "HIGHLY_ACTIVE", "ライフサイクルステージ判定失敗")
        self.assertEqual(transformed_data["MARKETING_PREFERENCE_SCORE"], 3, "マーケティング設定スコア計算失敗")
        self.assertTrue(transformed_data["EMAIL_OPT_IN"], "メールオプトイン確認失敗")
        self.assertFalse(transformed_data["SMS_OPT_IN"], "SMSオプトイン確認失敗")
        self.assertEqual(transformed_data["DATA_QUALITY_SCORE"], 0.95, "データ品質スコア引き継ぎ失敗")
        self.assertEqual(transformed_data["INFRASTRUCTURE_PROCESSING_STATUS"], "TRANSFORMED", "処理ステータス確認失敗")
    
    def test_data_flow_infrastructure_data_quality_assessment(self):
        """Data Flow: インフラストラクチャデータ品質評価テスト"""
        # テストケース: インフラストラクチャデータ品質評価
        test_infrastructure_data = [
            {
                "INFRA_ID": "INFRA000001",
                "CUSTOMER_ID": "CUST123456",
                "CUSTOMER_NAME": "テストユーザー1",
                "EMAIL_ADDRESS": "test1@example.com",
                "PHONE_NUMBER": "090-1234-5678",
                "SYSTEM_SOURCE": "TGCRM",
                "DATA_QUALITY_SCORE": 0.95
            },
            {
                "INFRA_ID": "",
                "CUSTOMER_ID": "CUST123457",
                "CUSTOMER_NAME": "テストユーザー2",
                "EMAIL_ADDRESS": "test2@example.com",
                "PHONE_NUMBER": "090-2345-6789",
                "SYSTEM_SOURCE": "SYNAPSE",
                "DATA_QUALITY_SCORE": 0.88
            },  # 不正: 空インフラID
            {
                "INFRA_ID": "INFRA000003",
                "CUSTOMER_ID": "CUST123458",
                "CUSTOMER_NAME": "",
                "EMAIL_ADDRESS": "invalid-email",
                "PHONE_NUMBER": "090-3456-7890",
                "SYSTEM_SOURCE": "TGCRM",
                "DATA_QUALITY_SCORE": 0.60
            },  # 不正: 空顧客名、無効メール
            {
                "INFRA_ID": "INFRA000004",
                "CUSTOMER_ID": "CUST123459",
                "CUSTOMER_NAME": "テストユーザー4",
                "EMAIL_ADDRESS": "test4@example.com",
                "PHONE_NUMBER": "090-4567-8901",
                "SYSTEM_SOURCE": "UNKNOWN",
                "DATA_QUALITY_SCORE": 0.45
            }  # 不正: 不明システムソース、低品質スコア
        ]
        
        # インフラストラクチャデータ品質評価ロジック（Data Flow内の処理）
        def assess_infrastructure_data_quality(record):
            quality_checks = {
                "infra_id_present": bool(record.get("INFRA_ID", "").strip()),
                "customer_id_present": bool(record.get("CUSTOMER_ID", "").strip()),
                "customer_name_present": bool(record.get("CUSTOMER_NAME", "").strip()),
                "email_valid": self._is_valid_email(record.get("EMAIL_ADDRESS", "")),
                "phone_valid": self._is_valid_phone(record.get("PHONE_NUMBER", "")),
                "system_source_known": record.get("SYSTEM_SOURCE") in ["TGCRM", "SYNAPSE", "DATA_LAKE"],
                "quality_score_acceptable": record.get("DATA_QUALITY_SCORE", 0.0) >= 0.80
            }
            
            # 品質スコア計算
            passed_checks = sum(quality_checks.values())
            total_checks = len(quality_checks)
            calculated_quality_score = passed_checks / total_checks
            
            # 品質レベル判定
            if calculated_quality_score >= 0.95:
                quality_level = "EXCELLENT"
            elif calculated_quality_score >= 0.85:
                quality_level = "GOOD"
            elif calculated_quality_score >= 0.70:
                quality_level = "ACCEPTABLE"
            elif calculated_quality_score >= 0.50:
                quality_level = "POOR"
            else:
                quality_level = "UNACCEPTABLE"
            
            # 処理可否判定
            processing_recommendation = "PROCESS" if calculated_quality_score >= 0.70 else "REJECT"
            
            return {
                "record": record,
                "quality_checks": quality_checks,
                "calculated_quality_score": calculated_quality_score,
                "quality_level": quality_level,
                "processing_recommendation": processing_recommendation,
                "failed_checks": [check for check, passed in quality_checks.items() if not passed]
            }
        
        # 品質評価実行
        quality_assessments = []
        for record in test_infrastructure_data:
            assessment = assess_infrastructure_data_quality(record)
            quality_assessments.append(assessment)
        
        # アサーション
        self.assertEqual(len(quality_assessments), 4, "品質評価結果数不正")
        self.assertEqual(quality_assessments[0]["quality_level"], "EXCELLENT", "品質レベル確認失敗")
        self.assertEqual(quality_assessments[0]["processing_recommendation"], "PROCESS", "処理推奨確認失敗")
        self.assertEqual(quality_assessments[1]["quality_level"], "GOOD", "品質レベル確認失敗")
        self.assertEqual(quality_assessments[1]["processing_recommendation"], "PROCESS", "処理推奨確認失敗")
        self.assertEqual(quality_assessments[2]["quality_level"], "ACCEPTABLE", "品質レベル確認失敗")
        self.assertEqual(quality_assessments[2]["processing_recommendation"], "PROCESS", "処理推奨確認失敗")
        self.assertEqual(quality_assessments[3]["quality_level"], "POOR", "品質レベル確認失敗")
        self.assertEqual(quality_assessments[3]["processing_recommendation"], "REJECT", "処理推奨確認失敗")
        
        # 失敗チェック確認
        self.assertEqual(len(quality_assessments[1]["failed_checks"]), 1, "失敗チェック数不正")
        self.assertIn("infra_id_present", quality_assessments[1]["failed_checks"], "失敗チェック確認失敗")
        self.assertEqual(len(quality_assessments[2]["failed_checks"]), 3, "失敗チェック数不正")
        self.assertIn("customer_name_present", quality_assessments[2]["failed_checks"], "失敗チェック確認失敗")
        self.assertIn("email_valid", quality_assessments[2]["failed_checks"], "失敗チェック確認失敗")
    
    def _is_valid_email(self, email):
        """メールアドレス検証"""
        return "@" in email and "." in email and len(email) > 5
    
    def _is_valid_phone(self, phone):
        """電話番号検証"""
        return len(phone) >= 10 and phone.replace("-", "").replace(" ", "").isdigit()
    
    def test_data_flow_infrastructure_data_lineage_creation(self):
        """Data Flow: インフラストラクチャデータ系譜作成テスト"""
        # テストケース: インフラストラクチャデータ系譜作成
        transformation_context = {
            "pipeline_name": "pi_Copy_marketing_client_dm",
            "execution_id": "EXEC_20240301_001",
            "source_systems": ["TGCRM", "SYNAPSE"],
            "target_system": "SYNAPSE",
            "transformation_timestamp": "2024-03-01T10:00:00Z",
            "processed_records": 1000,
            "transformed_records": 950,
            "failed_records": 50
        }
        
        # インフラストラクチャデータ系譜作成ロジック（Data Flow内の処理）
        def create_infrastructure_data_lineage(context):
            lineage_entries = []
            
            for source_system in context["source_systems"]:
                lineage_entry = {
                    "LINEAGE_ID": f"LINEAGE_{source_system}_{context['execution_id']}",
                    "PIPELINE_NAME": context["pipeline_name"],
                    "EXECUTION_ID": context["execution_id"],
                    "SOURCE_SYSTEM": source_system,
                    "TARGET_SYSTEM": context["target_system"],
                    "TRANSFORMATION_TIMESTAMP": context["transformation_timestamp"],
                    "TRANSFORMATION_TYPE": "COPY_WITH_TRANSFORMATION",
                    "DATA_FLOW_DIRECTION": "SOURCE_TO_TARGET",
                    "TRANSFORMATION_RULES": self._get_transformation_rules(source_system),
                    "DATA_QUALITY_IMPACT": self._calculate_quality_impact(source_system),
                    "PROCESSING_METRICS": {
                        "processed_records": context["processed_records"],
                        "transformed_records": context["transformed_records"],
                        "failed_records": context["failed_records"],
                        "success_rate": context["transformed_records"] / context["processed_records"] if context["processed_records"] > 0 else 0
                    },
                    "METADATA": {
                        "created_by": "SYSTEM",
                        "creation_timestamp": datetime.utcnow().isoformat(),
                        "retention_period_days": 365
                    }
                }
                lineage_entries.append(lineage_entry)
            
            return lineage_entries
        
        # データ系譜作成実行
        lineage_entries = create_infrastructure_data_lineage(transformation_context)
        
        # アサーション
        self.assertEqual(len(lineage_entries), 2, "データ系譜エントリ数不正")
        self.assertEqual(lineage_entries[0]["SOURCE_SYSTEM"], "TGCRM", "ソースシステム確認失敗")
        self.assertEqual(lineage_entries[0]["TARGET_SYSTEM"], "SYNAPSE", "ターゲットシステム確認失敗")
        self.assertEqual(lineage_entries[0]["TRANSFORMATION_TYPE"], "COPY_WITH_TRANSFORMATION", "変換タイプ確認失敗")
        self.assertEqual(lineage_entries[0]["PROCESSING_METRICS"]["success_rate"], 0.95, "成功率確認失敗")
        self.assertEqual(lineage_entries[1]["SOURCE_SYSTEM"], "SYNAPSE", "ソースシステム確認失敗")
        self.assertIsNotNone(lineage_entries[0]["TRANSFORMATION_RULES"], "変換ルール確認失敗")
        self.assertIsNotNone(lineage_entries[0]["DATA_QUALITY_IMPACT"], "データ品質影響確認失敗")
    
    def _get_transformation_rules(self, source_system):
        """変換ルール取得"""
        rules = {
            "TGCRM": [
                "CUSTOMER_SEGMENT: PREMIUM -> INFRA_PREMIUM",
                "PHONE_NUMBER: FORMAT(090-XXXX-XXXX)",
                "MARKETING_PREFERENCES: JSON_EXTRACT"
            ],
            "SYNAPSE": [
                "LIFECYCLE_STAGE: DERIVE_FROM_ACTIVITY",
                "DATA_QUALITY_SCORE: NORMALIZE_SCALE"
            ]
        }
        return rules.get(source_system, [])
    
    def _calculate_quality_impact(self, source_system):
        """データ品質影響計算"""
        impact = {
            "TGCRM": 0.02,  # 2%の品質影響
            "SYNAPSE": 0.01  # 1%の品質影響
        }
        return impact.get(source_system, 0.05)
    
    def test_copy_activity_infrastructure_data_transfer(self):
        """Copy Activity: インフラストラクチャデータ転送テスト"""
        # テストケース: インフラストラクチャデータ転送
        source_data = self.sample_infrastructure_marketing_dm_data
        
        # データ転送処理（Copy Activity内の処理）
        def transfer_infrastructure_data(source_data):
            # インフラストラクチャターゲット形式に変換
            target_data = []
            for record in source_data:
                target_record = {
                    "INFRASTRUCTURE_TARGET_ID": f"INFRA_TGT_{record['CUSTOMER_ID'][4:]}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
                    "ORIGINAL_INFRA_ID": record["INFRA_ID"],
                    "SYSTEM_SOURCE": record["SYSTEM_SOURCE"],
                    "CUSTOMER_ID": record["CUSTOMER_ID"],
                    "CUSTOMER_NAME": record["CUSTOMER_NAME"],
                    "EMAIL_ADDRESS": record["EMAIL_ADDRESS"],
                    "PHONE_NUMBER": record["PHONE_NUMBER"],
                    "CUSTOMER_SEGMENT": record["CUSTOMER_SEGMENT"],
                    "REGISTRATION_DATE": record["REGISTRATION_DATE"],
                    "LAST_ACTIVITY_DATE": record["LAST_ACTIVITY_DATE"],
                    "LIFECYCLE_STAGE": record["LIFECYCLE_STAGE"],
                    "MARKETING_PREFERENCES": record["MARKETING_PREFERENCES"],
                    "SYSTEM_METADATA": record["SYSTEM_METADATA"],
                    "PROCESSING_STATUS": "TRANSFERRED",
                    "BATCH_ID": record["BATCH_ID"],
                    "TRANSFER_TIMESTAMP": datetime.utcnow().isoformat()
                }
                target_data.append(target_record)
            
            return target_data
        
        # データ転送実行
        target_data = transfer_infrastructure_data(source_data)
        
        self.mock_synapse_service.insert_records.return_value = True
        
        # Copy Activity実行シミュレーション
        target_table = "infrastructure_marketing_client_dm_target"
        result = self.mock_synapse_service.insert_records(target_table, target_data)
        
        self.assertTrue(result, "インフラストラクチャデータ転送失敗")
        self.assertEqual(len(target_data), 2, "転送データ件数不正")
        self.assertIsNotNone(target_data[0]["INFRASTRUCTURE_TARGET_ID"], "インフラストラクチャターゲットID確認失敗")
        self.assertEqual(target_data[0]["SYSTEM_SOURCE"], "TGCRM", "システムソース確認失敗")
        self.assertEqual(target_data[0]["PROCESSING_STATUS"], "TRANSFERRED", "処理ステータス確認失敗")
        self.assertEqual(target_data[1]["SYSTEM_SOURCE"], "SYNAPSE", "システムソース確認失敗")
        self.mock_synapse_service.insert_records.assert_called_once_with(target_table, target_data)
    
    def test_copy_activity_infrastructure_processing_log(self):
        """Copy Activity: インフラストラクチャ処理ログ記録テスト"""
        # テストケース: インフラストラクチャ処理ログの記録
        processing_logs = [
            {
                "INFRASTRUCTURE_PROCESS_ID": "INFRA_PROC_001",
                "PIPELINE_NAME": "pi_Copy_marketing_client_dm",
                "EXECUTION_ID": "EXEC_20240301_001",
                "EXECUTION_DATE": datetime.utcnow().strftime('%Y%m%d'),
                "EXECUTION_TIME": datetime.utcnow().strftime('%H:%M:%S'),
                "SOURCE_SYSTEMS": ["TGCRM", "SYNAPSE"],
                "TARGET_SYSTEM": "SYNAPSE",
                "RECORDS_PROCESSED": 1000,
                "RECORDS_TRANSFORMED": 950,
                "RECORDS_INSERTED": 950,
                "RECORDS_FAILED": 50,
                "DATA_QUALITY_SCORE": 0.92,
                "PROCESSING_STATUS": "SUCCESS",
                "PROCESSING_TIME_SECONDS": 125.8,
                "MEMORY_USAGE_MB": 512.0,
                "ERROR_DETAILS": None,
                "INFRASTRUCTURE_METADATA": {
                    "pipeline_version": "1.0",
                    "infrastructure_environment": "PRODUCTION",
                    "resource_utilization": 0.75
                }
            }
        ]
        
        self.mock_database.insert_records.return_value = True
        
        # Copy Activity実行シミュレーション
        processing_log_table = "infrastructure_processing_logs"
        result = self.mock_database.insert_records(processing_log_table, processing_logs)
        
        self.assertTrue(result, "インフラストラクチャ処理ログ記録失敗")
        self.mock_database.insert_records.assert_called_once_with(processing_log_table, processing_logs)
    
    def test_script_activity_infrastructure_monitoring(self):
        """Script Activity: インフラストラクチャ監視処理テスト"""
        # テストケース: インフラストラクチャ監視
        monitoring_data = {
            "execution_date": self.test_date,
            "pipeline_name": "pi_Copy_marketing_client_dm",
            "infrastructure_metrics": {
                "total_records_processed": 1000,
                "processing_time_seconds": 125.8,
                "memory_usage_mb": 512.0,
                "cpu_usage_percent": 75.0,
                "disk_io_mb": 1024.0,
                "network_io_mb": 256.0
            },
            "system_health": {
                "tgcrm_connection_status": "HEALTHY",
                "synapse_connection_status": "HEALTHY",
                "data_lake_connection_status": "HEALTHY"
            },
            "data_quality_metrics": {
                "overall_quality_score": 0.92,
                "tgcrm_quality_score": 0.95,
                "synapse_quality_score": 0.88
            }
        }
        
        self.mock_data_lake_service.record_infrastructure_metrics.return_value = {
            "status": "recorded",
            "metrics_id": "METRICS_20240301_001",
            "recording_timestamp": datetime.utcnow().isoformat()
        }
        
        # Script Activity実行シミュレーション
        result = self.mock_data_lake_service.record_infrastructure_metrics(
            monitoring_data["execution_date"],
            monitoring_data["pipeline_name"],
            monitoring_data["infrastructure_metrics"],
            monitoring_data["system_health"],
            monitoring_data["data_quality_metrics"]
        )
        
        self.assertEqual(result["status"], "recorded", "インフラストラクチャ監視記録失敗")
        self.assertEqual(result["metrics_id"], "METRICS_20240301_001", "メトリクスID確認失敗")
        self.assertIsNotNone(result["recording_timestamp"], "記録タイムスタンプ確認失敗")
        self.mock_data_lake_service.record_infrastructure_metrics.assert_called_once()
    
    def test_script_activity_infrastructure_analytics(self):
        """Script Activity: インフラストラクチャ分析処理テスト"""
        # テストケース: インフラストラクチャ分析データ生成
        infrastructure_analytics = {
            "execution_date": self.test_date,
            "pipeline_name": "pi_Copy_marketing_client_dm",
            "total_records_processed": 1000,
            "tgcrm_records_processed": 500,
            "synapse_records_processed": 500,
            "transformation_success_rate": 0.95,
            "data_quality_score": 0.92,
            "processing_time_seconds": 125.8,
            "memory_usage_mb": 512.0,
            "cpu_usage_percent": 75.0,
            "infrastructure_efficiency": 0.88,
            "system_health_score": 0.96,
            "data_lineage_entries_created": 2,
            "quality_assessments_performed": 1000,
            "infrastructure_alerts_triggered": 0
        }
        
        self.mock_database.insert_records.return_value = True
        
        # Script Activity実行シミュレーション
        analytics_table = "infrastructure_marketing_analytics"
        result = self.mock_database.insert_records(analytics_table, [infrastructure_analytics])
        
        self.assertTrue(result, "インフラストラクチャ分析記録失敗")
        self.mock_database.insert_records.assert_called_once_with(analytics_table, [infrastructure_analytics])
    
    def test_data_flow_infrastructure_batch_processing(self):
        """Data Flow: インフラストラクチャバッチ処理テスト"""
        # テストケース: インフラストラクチャ大量データのバッチ処理
        large_infrastructure_dataset = []
        for i in range(5000):
            large_infrastructure_dataset.append({
                "INFRA_ID": f"INFRA{i:06d}",
                "SYSTEM_SOURCE": ["TGCRM", "SYNAPSE", "DATA_LAKE"][i % 3],
                "CUSTOMER_ID": f"CUST{i:06d}",
                "CUSTOMER_SEGMENT": ["PREMIUM", "STANDARD", "BASIC"][i % 3],
                "LIFECYCLE_STAGE": ["ACTIVE", "INACTIVE", "MODERATE"][i % 3],
                "DATA_QUALITY_SCORE": 0.5 + (i % 50) / 100
            })
        
        # インフラストラクチャバッチ処理ロジック（Data Flow内の処理）
        def process_infrastructure_batch(data_list, batch_size=500):
            processed_batches = []
            
            for i in range(0, len(data_list), batch_size):
                batch = data_list[i:i + batch_size]
                
                # バッチ処理の実行
                batch_result = {
                    "batch_id": f"INFRA_BATCH_{i//batch_size + 1:03d}",
                    "record_count": len(batch),
                    "tgcrm_records": sum(1 for record in batch if record["SYSTEM_SOURCE"] == "TGCRM"),
                    "synapse_records": sum(1 for record in batch if record["SYSTEM_SOURCE"] == "SYNAPSE"),
                    "data_lake_records": sum(1 for record in batch if record["SYSTEM_SOURCE"] == "DATA_LAKE"),
                    "premium_customers": sum(1 for record in batch if record["CUSTOMER_SEGMENT"] == "PREMIUM"),
                    "standard_customers": sum(1 for record in batch if record["CUSTOMER_SEGMENT"] == "STANDARD"),
                    "basic_customers": sum(1 for record in batch if record["CUSTOMER_SEGMENT"] == "BASIC"),
                    "active_customers": sum(1 for record in batch if record["LIFECYCLE_STAGE"] == "ACTIVE"),
                    "average_quality_score": sum(record["DATA_QUALITY_SCORE"] for record in batch) / len(batch),
                    "high_quality_records": sum(1 for record in batch if record["DATA_QUALITY_SCORE"] >= 0.80),
                    "processing_time": 25.0,  # シミュレーション
                    "infrastructure_success_rate": 0.98
                }
                
                processed_batches.append(batch_result)
            
            return processed_batches
        
        # インフラストラクチャバッチ処理実行
        batch_results = process_infrastructure_batch(large_infrastructure_dataset, batch_size=500)
        
        # アサーション
        self.assertEqual(len(batch_results), 10, "バッチ数不正")  # 5000 / 500 = 10
        self.assertEqual(batch_results[0]["record_count"], 500, "バッチサイズ不正")
        self.assertGreater(batch_results[0]["average_quality_score"], 0, "平均品質スコア確認失敗")
        
        # 全バッチの合計確認
        total_records = sum(batch["record_count"] for batch in batch_results)
        total_tgcrm = sum(batch["tgcrm_records"] for batch in batch_results)
        total_synapse = sum(batch["synapse_records"] for batch in batch_results)
        total_data_lake = sum(batch["data_lake_records"] for batch in batch_results)
        
        self.assertEqual(total_records, 5000, "全バッチ処理件数不正")
        self.assertAlmostEqual(total_tgcrm, 1667, delta=1, msg="TGCRM レコード数不正")
        self.assertAlmostEqual(total_synapse, 1667, delta=1, msg="Synapse レコード数不正")
        self.assertAlmostEqual(total_data_lake, 1666, delta=1, msg="Data Lake レコード数不正")
    
    def _create_infrastructure_marketing_client_dm_csv_content(self) -> str:
        """インフラストラクチャマーケティングClient DMデータ用CSVコンテンツ生成"""
        header = "INFRA_ID,SYSTEM_SOURCE,EXTRACTION_TIMESTAMP,CUSTOMER_ID,CUSTOMER_NAME,EMAIL_ADDRESS,PHONE_NUMBER,CUSTOMER_SEGMENT,REGISTRATION_DATE,LAST_ACTIVITY_DATE,LIFECYCLE_STAGE,MARKETING_PREFERENCES,SYSTEM_METADATA,PROCESSING_STATUS,BATCH_ID"
        rows = []
        
        for item in self.sample_infrastructure_marketing_dm_data:
            marketing_prefs = str(item['MARKETING_PREFERENCES']).replace(',', ';')
            system_metadata = str(item['SYSTEM_METADATA']).replace(',', ';')
            row = f"{item['INFRA_ID']},{item['SYSTEM_SOURCE']},{item['EXTRACTION_TIMESTAMP']},{item['CUSTOMER_ID']},{item['CUSTOMER_NAME']},{item['EMAIL_ADDRESS']},{item['PHONE_NUMBER']},{item['CUSTOMER_SEGMENT']},{item['REGISTRATION_DATE']},{item['LAST_ACTIVITY_DATE']},{item['LIFECYCLE_STAGE']},{marketing_prefs},{system_metadata},{item['PROCESSING_STATUS']},{item['BATCH_ID']}"
            rows.append(row)
        
        return header + "\n" + "\n".join(rows)


if __name__ == "__main__":
    unittest.main()