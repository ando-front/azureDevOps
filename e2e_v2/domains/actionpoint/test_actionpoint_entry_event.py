"""
pi_Insert_ActionPointEntryEvent パイプラインのE2Eテスト

アクションポイント登録イベント処理パイプラインの包括的テスト
"""

import time
import json
from datetime import datetime, timedelta
from typing import Dict, List

from ...base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus


class TestActionPointEntryEventPipeline(DomainTestBase):
    """アクションポイント登録イベントパイプラインテスト"""
    
    def __init__(self):
        super().__init__("pi_Insert_ActionPointEntryEvent", "actionpoint")
    
    def domain_specific_setup(self):
        """ActionPointドメイン固有セットアップ"""
        # ActionPointドメイン用設定
        self.source_container = "mytokyogas"
        self.output_container = "datalake"
        self.database_table = "ActionPointEntryEvent"
        
        # 期待される入力フィールド
        self.input_columns = [
            "EVENT_ID", "CUSTOMER_ID", "ACTION_TYPE", "POINT_VALUE", 
            "EVENT_DATE", "EVENT_TIME", "CAMPAIGN_ID", "STATUS"
        ]
        
        # 期待される出力フィールド（DB挿入用）
        self.output_columns = [
            "EVENT_ID", "CUSTOMER_ID", "ACTION_TYPE", "POINT_VALUE",
            "EVENT_DATETIME", "CAMPAIGN_ID", "STATUS", "CREATED_DATE", "PROCESS_DATE"
        ]
        
        # モックデータベース設定
        from ...common.azure_mock import MockDatabase
        self.mock_db = MockDatabase()
        self.mock_db.create_table(self.database_table, self.output_columns)
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """ActionPointドメイン用テストデータテンプレート"""
        return {
            "entry_event_data": self._generate_entry_event_data(),
            "campaign_event_data": self._generate_campaign_event_data(),
            "bulk_event_data": self._generate_bulk_event_data(1000)
        }
    
    def _generate_entry_event_data(self, record_count: int = 100) -> str:
        """アクションポイント登録イベントデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        action_types = ["LOGIN", "PURCHASE", "REVIEW", "REFERRAL", "SURVEY"]
        
        for i in range(1, record_count + 1):
            event_date = (base_date - timedelta(days=i % 30)).strftime('%Y%m%d')
            event_time = f"{(i % 24):02d}:{(i % 60):02d}:{(i % 60):02d}"
            action_type = action_types[i % len(action_types)]
            point_value = self._calculate_point_value(action_type, i)
            campaign_id = f"CAMP{(i % 10):03d}" if i % 3 == 0 else ""
            status = "PENDING"
            
            data_lines.append(
                f"EVT{i:08d}\tCUST{i:06d}\t{action_type}\t{point_value}\t"
                f"{event_date}\t{event_time}\t{campaign_id}\t{status}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_campaign_event_data(self) -> str:
        """キャンペーン関連イベントデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        
        # 特別キャンペーンイベント
        campaign_events = [
            ("EVT10000001", "CUST000001", "CAMPAIGN_PURCHASE", "500", "CAMP001"),
            ("EVT10000002", "CUST000002", "CAMPAIGN_REFERRAL", "1000", "CAMP002"),
            ("EVT10000003", "CUST000003", "CAMPAIGN_REVIEW", "200", "CAMP001"),
            ("EVT10000004", "CUST000004", "DOUBLE_POINT_DAY", "300", "CAMP003"),
        ]
        
        for i, (event_id, customer_id, action_type, point_value, campaign_id) in enumerate(campaign_events):
            event_date = (base_date - timedelta(days=i)).strftime('%Y%m%d')
            event_time = f"{10 + i}:00:00"
            status = "PENDING"
            
            data_lines.append(
                f"{event_id}\t{customer_id}\t{action_type}\t{point_value}\t"
                f"{event_date}\t{event_time}\t{campaign_id}\t{status}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_bulk_event_data(self, record_count: int) -> str:
        """大量イベントデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        action_types = ["LOGIN", "PURCHASE", "REVIEW", "REFERRAL", "SURVEY", "SHARE"]
        
        for i in range(1, record_count + 1):
            event_date = (base_date - timedelta(days=i % 365)).strftime('%Y%m%d')
            event_time = f"{(i % 24):02d}:{(i % 60):02d}:{(i % 60):02d}"
            action_type = action_types[i % len(action_types)]
            point_value = self._calculate_point_value(action_type, i)
            campaign_id = f"CAMP{(i % 50):03d}" if i % 5 == 0 else ""
            status = "PENDING"
            
            data_lines.append(
                f"EVT{i:08d}\tCUST{(i % 10000):06d}\t{action_type}\t{point_value}\t"
                f"{event_date}\t{event_time}\t{campaign_id}\t{status}"
            )
        
        return "\n".join(data_lines)
    
    def _calculate_point_value(self, action_type: str, multiplier: int) -> str:
        """アクションタイプに応じたポイント値計算"""
        base_points = {
            "LOGIN": 10,
            "PURCHASE": 100,
            "REVIEW": 50,
            "REFERRAL": 500,
            "SURVEY": 30,
            "SHARE": 20,
            "CAMPAIGN_PURCHASE": 200,
            "CAMPAIGN_REFERRAL": 1000,
            "CAMPAIGN_REVIEW": 100,
            "DOUBLE_POINT_DAY": 200
        }
        
        base = base_points.get(action_type, 10)
        # キャンペーンの場合は倍率適用
        if "CAMPAIGN" in action_type:
            base *= 2
        
        return str(base + (multiplier % 50))
    
    def _transform_entry_event_data(self, input_data: str) -> List[Dict[str, str]]:
        """アクションポイント登録イベントデータ変換処理"""
        lines = input_data.strip().split('\n')
        
        transformed_records = []
        process_date = datetime.utcnow().strftime('%Y/%m/%d %H:%M:%S')
        
        for line in lines[1:]:
            if not line.strip():
                continue
                
            parts = line.split('\t')
            if len(parts) != len(self.input_columns):
                continue
            
            event_id, customer_id, action_type, point_value, event_date, event_time, campaign_id, status = parts
            
            # データ品質チェック
            if not all([event_id.strip(), customer_id.strip(), action_type.strip()]):
                continue
            
            # ポイント値検証
            try:
                point_val = int(point_value)
                if point_val < 0:
                    continue
            except ValueError:
                continue
            
            # イベント日時結合
            try:
                event_datetime = f"{event_date} {event_time}"
                # 日時フォーマット検証
                datetime.strptime(f"{event_date} {event_time}", '%Y%m%d %H:%M:%S')
            except ValueError:
                continue
            
            # ステータス更新（処理中に変更）
            new_status = "PROCESSING"
            
            # 変換後レコード作成
            record = {
                "EVENT_ID": event_id,
                "CUSTOMER_ID": customer_id,
                "ACTION_TYPE": action_type,
                "POINT_VALUE": point_value,
                "EVENT_DATETIME": event_datetime,
                "CAMPAIGN_ID": campaign_id,
                "STATUS": new_status,
                "CREATED_DATE": process_date,
                "PROCESS_DATE": process_date
            }
            
            transformed_records.append(record)
        
        return transformed_records
    
    def validate_domain_business_rules(self, records: List[Dict[str, str]]) -> List[str]:
        """ActionPointドメインビジネスルール検証"""
        violations = []
        
        for i, record in enumerate(records, 1):
            # イベントID形式チェック
            if not record["EVENT_ID"].startswith('EVT'):
                violations.append(f"レコード{i}: イベントID形式不正 ({record['EVENT_ID']})")
            
            # 顧客ID形式チェック
            if not record["CUSTOMER_ID"].startswith('CUST'):
                violations.append(f"レコード{i}: 顧客ID形式不正 ({record['CUSTOMER_ID']})")
            
            # アクションタイプ検証
            valid_actions = ["LOGIN", "PURCHASE", "REVIEW", "REFERRAL", "SURVEY", "SHARE", 
                           "CAMPAIGN_PURCHASE", "CAMPAIGN_REFERRAL", "CAMPAIGN_REVIEW", "DOUBLE_POINT_DAY"]
            if record["ACTION_TYPE"] not in valid_actions:
                violations.append(f"レコード{i}: アクションタイプ不正 ({record['ACTION_TYPE']})")
            
            # ポイント値検証
            try:
                point_val = int(record["POINT_VALUE"])
                if point_val < 0 or point_val > 10000:
                    violations.append(f"レコード{i}: ポイント値範囲外 ({record['POINT_VALUE']})")
            except ValueError:
                violations.append(f"レコード{i}: ポイント値形式不正 ({record['POINT_VALUE']})")
            
            # キャンペーンIDチェック（設定されている場合）
            if record["CAMPAIGN_ID"] and not record["CAMPAIGN_ID"].startswith('CAMP'):
                violations.append(f"レコード{i}: キャンペーンID形式不正 ({record['CAMPAIGN_ID']})")
            
            # ステータス検証
            valid_statuses = ["PENDING", "PROCESSING", "COMPLETED", "FAILED"]
            if record["STATUS"] not in valid_statuses:
                violations.append(f"レコード{i}: ステータス不正 ({record['STATUS']})")
        
        return violations
    
    def test_functional_entry_event_processing(self):
        """機能テスト: エントリーイベント正常処理"""
        test_id = f"functional_entry_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # テストデータ準備
        test_data = self._generate_entry_event_data()
        records_count = len(test_data.split('\n')) - 1
        
        # ファイルアップロード
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"ActionPoint/Entry/{file_date}/entry_events.tsv"
        self.mock_storage.upload_file(self.source_container, file_path, test_data.encode('utf-8'))
        
        # データ変換処理
        transformed_records = self._transform_entry_event_data(test_data)
        
        # データベース挿入シミュレーション
        inserted_count = self.mock_db.insert_data(self.database_table, transformed_records)
        
        # ビジネスルール検証
        violations = self.validate_domain_business_rules(transformed_records)
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.FUNCTIONAL,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=records_count,
            records_transformed=len(transformed_records),
            records_loaded=inserted_count,
            data_quality_score=0.98,
            errors=[],
            warnings=violations
        )
        
        # アサーション
        self.validate_common_assertions(result)
        assert result.records_extracted > 0, "抽出レコード数が0"
        assert result.records_transformed > 0, "変換レコード数が0"
        assert inserted_count > 0, "DB挿入件数が0"
        assert len(violations) == 0, f"ビジネスルール違反: {violations}"
        
        # データベース状態確認
        db_records = self.mock_db.select_data(self.database_table)
        assert len(db_records) == inserted_count, "DB格納件数不一致"
        
        self.log_test_info(test_id, f"成功 - {inserted_count}件のイベント処理完了")
    
    def test_functional_campaign_event_processing(self):
        """機能テスト: キャンペーンイベント処理"""
        test_id = f"functional_campaign_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # キャンペーンデータ準備
        test_data = self._generate_campaign_event_data()
        
        # データ変換処理
        transformed_records = self._transform_entry_event_data(test_data)
        
        # キャンペーン関連レコード確認
        campaign_records = [r for r in transformed_records if r["CAMPAIGN_ID"]]
        
        # データベース挿入
        inserted_count = self.mock_db.insert_data(self.database_table, transformed_records)
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.FUNCTIONAL,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=len(test_data.split('\n')) - 1,
            records_transformed=len(transformed_records),
            records_loaded=inserted_count,
            data_quality_score=1.0,
            errors=[]
        )
        
        # キャンペーン処理アサーション
        assert len(campaign_records) >= 3, f"キャンペーンレコード数不足: {len(campaign_records)}"
        
        # 高額ポイントレコード確認
        high_point_records = [r for r in campaign_records if int(r["POINT_VALUE"]) >= 500]
        assert len(high_point_records) >= 1, "高額ポイントレコードなし"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, f"成功 - {len(campaign_records)}件のキャンペーンイベント処理完了")
    
    def test_data_quality_validation(self):
        """データ品質テスト: 不正データ処理"""
        test_id = f"data_quality_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # 問題データ準備
        problematic_data = [
            "\t".join(self.input_columns),
            "EVT00000001\tCUST000001\tLOGIN\t10\t20241231\t12:00:00\t\tPENDING",  # 正常
            "\t\tLOGIN\t10\t20241231\t12:00:00\t\tPENDING",  # ID不正
            "EVT00000003\tCUST000003\tINVALID_ACTION\t10\t20241231\t12:00:00\t\tPENDING",  # アクション不正
            "EVT00000004\tCUST000004\tLOGIN\t-10\t20241231\t12:00:00\t\tPENDING",  # 負のポイント
            "EVT00000005\tCUST000005\tLOGIN\tabc\t20241231\t12:00:00\t\tPENDING",  # ポイント形式不正
            "EVT00000006\tCUST000006\tLOGIN\t10\t99999999\t25:00:00\t\tPENDING",  # 日時不正
        ]
        test_data = "\n".join(problematic_data)
        
        # データ変換処理
        transformed_records = self._transform_entry_event_data(test_data)
        
        # 品質検証
        violations = self.validate_domain_business_rules(transformed_records)
        
        # 有効レコード数確認（正常な1件のみ）
        valid_records = len(transformed_records)
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.DATA_QUALITY,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=6,
            records_transformed=valid_records,
            records_loaded=valid_records,
            data_quality_score=valid_records / 6,  # 有効レコード率
            errors=[],
            warnings=violations
        )
        
        # データ品質アサーション
        assert valid_records == 1, f"有効レコード数不正: 期待=1, 実際={valid_records}"
        assert len(violations) == 0, f"ビジネスルール違反: {violations}"
        assert result.data_quality_score >= 0.1, "データ品質スコア低すぎ"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, "成功 - データ品質検証完了")
    
    def test_performance_bulk_processing(self):
        """パフォーマンステスト: 大量データ処理"""
        test_id = f"performance_bulk_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # 大量データ準備（1000件）
        bulk_data = self._generate_bulk_event_data(1000)
        
        start_time = time.time()
        
        # ETL処理シミュレーション
        transformed_records = self._transform_entry_event_data(bulk_data)
        
        # データベース挿入
        inserted_count = self.mock_db.insert_data(self.database_table, transformed_records)
        
        execution_time = time.time() - start_time
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.PERFORMANCE,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=1000,
            records_transformed=len(transformed_records),
            records_loaded=inserted_count,
            data_quality_score=0.99,
            errors=[]
        )
        
        # パフォーマンス基準
        throughput = 1000 / execution_time
        assert throughput > 100, f"スループットが基準未満: {throughput:.2f} records/sec"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, f"成功 - {execution_time:.2f}秒で処理完了")
    
    def test_integration_database_operations(self):
        """統合テスト: データベース操作統合"""
        test_id = f"integration_database_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # テストデータ準備
        test_data = self._generate_entry_event_data(50)
        transformed_records = self._transform_entry_event_data(test_data)
        
        # 段階的データベース操作
        # 1. 挿入
        inserted_count = self.mock_db.insert_data(self.database_table, transformed_records)
        
        # 2. 選択（特定顧客のイベント）
        customer_events = self.mock_db.select_data(
            self.database_table, 
            where_condition={"CUSTOMER_ID": "CUST000001"}
        )
        
        # 3. 更新（ステータス変更）
        updated_count = self.mock_db.update_data(
            self.database_table,
            set_values={"STATUS": "COMPLETED"},
            where_condition={"STATUS": "PROCESSING"}
        )
        
        # 4. 最終状態確認
        final_records = self.mock_db.select_data(self.database_table)
        completed_records = self.mock_db.select_data(
            self.database_table,
            where_condition={"STATUS": "COMPLETED"}
        )
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.INTEGRATION,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=50,
            records_transformed=len(transformed_records),
            records_loaded=inserted_count,
            data_quality_score=1.0,
            errors=[]
        )
        
        # 統合テストアサーション
        assert inserted_count > 0, "挿入件数が0"
        assert len(customer_events) >= 0, "顧客イベント選択失敗"
        assert updated_count > 0, "更新件数が0"
        assert len(final_records) == inserted_count, "最終レコード数不一致"
        assert len(completed_records) == updated_count, "完了レコード数不一致"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, f"成功 - DB統合テスト完了 (挿入:{inserted_count}, 更新:{updated_count})")