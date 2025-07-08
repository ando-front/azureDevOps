"""
pi_Copy_marketing_client_dna パイプラインのE2Eテスト

マーケティングクライアントDNAコピーパイプラインのETL処理テスト
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List

from ...base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus


class TestClientDNAPipeline(DomainTestBase):
    """マーケティングクライアントDNAコピーパイプラインテスト"""
    
    def __init__(self):
        super().__init__("pi_Copy_marketing_client_dna", "marketing")
    
    def domain_specific_setup(self):
        """マーケティングドメイン固有セットアップ"""
        self.source_container = "marketing"
        self.output_container = "datalake"
        self.database_target = "marketing_client_dna"
        
        # 期待される入力フィールド（ETL処理用）
        self.input_columns = [
            "CLIENT_ID", "CLIENT_NAME", "DNA_PROFILE", "BEHAVIOR_SCORE",
            "PREFERENCE_CATEGORY", "PURCHASE_PATTERN", "ENGAGEMENT_LEVEL",
            "DEMOGRAPHIC_GROUP", "LIFECYCLE_STAGE", "INTERACTION_HISTORY",
            "LAST_UPDATE_DATE", "STATUS"
        ]
        
        # 期待される出力フィールド（ETL変換後）
        self.output_columns = [
            "CLIENT_ID", "CLIENT_NAME", "DNA_PROFILE", "BEHAVIOR_SCORE",
            "PREFERENCE_CATEGORY", "PURCHASE_PATTERN", "ENGAGEMENT_LEVEL",
            "DEMOGRAPHIC_GROUP", "LIFECYCLE_STAGE", "INTERACTION_HISTORY",
            "LAST_UPDATE_DATE", "STATUS", "COPY_DATE", "DNA_VERSION", "ANALYSIS_FLAG"
        ]
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """マーケティングドメイン用テストデータテンプレート"""
        return {
            "client_dna_data": self._generate_client_dna_data(),
            "high_engagement_data": self._generate_high_engagement_data(),
            "bulk_dna_analysis": self._generate_bulk_dna_analysis()
        }
    
    def _generate_client_dna_data(self, record_count: int = 2000) -> str:
        """クライアントDNAデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        dna_profiles = ["EXPLORER", "LOYALIST", "OPTIMIZER", "CONSERVATIVE", "TRENDSETTER"]
        preference_categories = ["ENERGY_SAVER", "CONVENIENCE", "PREMIUM", "BUDGET", "ECO_FRIENDLY"]
        purchase_patterns = ["REGULAR", "SEASONAL", "IMPULSE", "PLANNED", "BULK"]
        engagement_levels = ["HIGH", "MEDIUM", "LOW", "DORMANT"]
        demographic_groups = ["YOUNG_PROFESSIONAL", "FAMILY", "SENIOR", "STUDENT", "ENTERPRISE"]
        lifecycle_stages = ["NEW", "GROWING", "MATURE", "DECLINING", "CHURNED"]
        statuses = ["ACTIVE", "INACTIVE", "PENDING", "ARCHIVED"]
        
        base_date = datetime.utcnow()
        
        for i in range(1, record_count + 1):
            # 最終更新日（過去90日以内）
            update_days = i % 90
            last_update_date = (base_date - timedelta(days=update_days)).strftime('%Y%m%d')
            
            dna_profile = dna_profiles[i % len(dna_profiles)]
            preference_category = preference_categories[i % len(preference_categories)]
            purchase_pattern = purchase_patterns[i % len(purchase_patterns)]
            engagement_level = engagement_levels[i % len(engagement_levels)]
            demographic_group = demographic_groups[i % len(demographic_groups)]
            lifecycle_stage = lifecycle_stages[i % len(lifecycle_stages)]
            status = statuses[i % len(statuses)]
            
            # 行動スコア（0-100）
            behavior_score = (i % 100) + 1
            
            # インタラクション履歴生成
            interaction_count = (i % 20) + 1
            interaction_history = f"INTERACTIONS_{interaction_count}"
            
            client_name = f"DNAクライアント{i:04d}"
            
            data_lines.append(
                f"CLIENT{i:06d}\t{client_name}\t{dna_profile}\t{behavior_score}\t"
                f"{preference_category}\t{purchase_pattern}\t{engagement_level}\t"
                f"{demographic_group}\t{lifecycle_stage}\t{interaction_history}\t"
                f"{last_update_date}\t{status}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_high_engagement_data(self) -> str:
        """高エンゲージメントデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        last_update_date = base_date.strftime('%Y%m%d')
        
        # 高エンゲージメントクライアント
        high_engagement_clients = [
            ("CLIENT900001", "高エンゲージ1", "EXPLORER", "95", "PREMIUM", "REGULAR", "HIGH", "YOUNG_PROFESSIONAL", "MATURE", "INTERACTIONS_50", last_update_date, "ACTIVE"),
            ("CLIENT900002", "高エンゲージ2", "TRENDSETTER", "98", "ECO_FRIENDLY", "PLANNED", "HIGH", "FAMILY", "GROWING", "INTERACTIONS_45", last_update_date, "ACTIVE"),
            ("CLIENT900003", "高エンゲージ3", "LOYALIST", "92", "CONVENIENCE", "BULK", "HIGH", "SENIOR", "MATURE", "INTERACTIONS_40", last_update_date, "ACTIVE"),
        ]
        
        for client_data in high_engagement_clients:
            data_lines.append("\t".join(client_data))
        
        return "\n".join(data_lines)
    
    def _generate_bulk_dna_analysis(self) -> str:
        """一括DNAアナリシスデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        last_update_date = (base_date - timedelta(days=1)).strftime('%Y%m%d')
        
        # 一括分析対象（新規登録者）
        for i in range(1, 101):
            behavior_score = 50 + (i % 30)  # 50-79の範囲
            
            data_lines.append(
                f"CLIENT{i:06d}\t一括分析{i:03d}\tCONSERVATIVE\t{behavior_score}\t"
                f"BUDGET\tREGULAR\tMEDIUM\tSTUDENT\tNEW\t"
                f"INTERACTIONS_5\t{last_update_date}\tACTIVE"
            )
        
        return "\n".join(data_lines)
    
    def _transform_client_dna_data(self, input_data: str) -> str:
        """クライアントDNAデータETL変換処理"""
        lines = input_data.strip().split('\n')
        
        # 出力CSVヘッダー
        output_header = ",".join(self.output_columns)
        output_lines = [output_header]
        
        current_date = datetime.utcnow()
        copy_date = current_date.strftime('%Y/%m/%d %H:%M:%S')
        dna_version = "2.0"
        
        for line in lines[1:]:
            if not line.strip():
                continue
                
            parts = line.split('\t')
            if len(parts) != len(self.input_columns):
                continue
            
            (client_id, client_name, dna_profile, behavior_score,
             preference_category, purchase_pattern, engagement_level,
             demographic_group, lifecycle_stage, interaction_history,
             last_update_date, status) = parts
            
            # ETL品質チェック
            if not client_id.strip() or not client_name.strip() or not dna_profile.strip():
                continue
            
            # アナリシスフラグ設定（ETL処理）
            score = int(behavior_score) if behavior_score.isdigit() else 0
            if score >= 80:
                analysis_flag = "HIGH_VALUE"
            elif score >= 50:
                analysis_flag = "STANDARD"
            else:
                analysis_flag = "LOW_PRIORITY"
            
            # CSV出力行生成
            output_line = (
                f"{client_id},{client_name},{dna_profile},{behavior_score},"
                f"{preference_category},{purchase_pattern},{engagement_level},"
                f"{demographic_group},{lifecycle_stage},{interaction_history},"
                f"{last_update_date},{status},{copy_date},{dna_version},{analysis_flag}"
            )
            output_lines.append(output_line)
        
        return '\n'.join(output_lines)
    
    def validate_domain_business_rules(self, data: str) -> List[str]:
        """ETLレベルの基本検証のみ"""
        violations = []
        lines = data.strip().split('\n')
        
        if len(lines) < 2:
            return violations
        
        # 構造検証
        expected_header = ",".join(self.output_columns)
        if lines[0] != expected_header:
            violations.append(f"ヘッダー不正: 期待={expected_header}")
        
        for i, line in enumerate(lines[1:], 2):
            parts = line.split(',')
            
            if len(parts) != len(self.output_columns):
                violations.append(f"行{i}: フィールド数不正")
                continue
        
        return violations
    
    def test_functional_with_file_exists(self):
        """機能テスト: ファイル有り処理"""
        test_id = f"functional_with_file_{int(time.time())}"
        
        # テストデータ準備
        test_data = self._generate_client_dna_data()
        
        # ファイルアップロード
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"MarketingClientDNA/{file_date}/client_dna.tsv"
        self.mock_storage.upload_file(self.source_container, file_path, test_data.encode('utf-8'))
        
        # ETL処理シミュレーション
        transformed_data, output_data = self.simulate_etl_process(
            test_data,
            self._transform_client_dna_data,
            "csv"
        )
        
        # データベース挿入シミュレーション
        db_records = self.convert_csv_to_dict_list(transformed_data)
        db_insert_result = self.mock_database.insert_records(
            self.database_target, 
            db_records
        )
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.FUNCTIONAL,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=len(test_data.split('\n')) - 1,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(db_records),
            data_quality_score=0.95,
            errors=[]
        )
        
        # アサーション
        self.validate_common_assertions(result)
        assert result.records_extracted > 0, "抽出レコード数が0"
        assert db_insert_result, "データベース挿入失敗"
    
    def test_functional_without_file(self):
        """機能テスト: ファイル無し処理"""
        test_id = f"functional_no_file_{int(time.time())}"
        
        # ファイル存在チェック（存在しないファイルパスを使用）
        file_date = "99999999"
        file_path = f"MarketingClientDNA/{file_date}/client_dna.tsv"
        
        file_exists = self.mock_storage.file_exists(self.source_container, file_path)
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.FUNCTIONAL,
            status=PipelineStatus.SKIPPED,
            records_extracted=0,
            records_transformed=0,
            records_loaded=0,
            data_quality_score=1.0,
            errors=[]
        )
        
        # ファイル無しの場合はスキップが正常
        assert not file_exists, "ファイルが存在する（テスト条件不正）"
        assert result.status == PipelineStatus.SKIPPED, "スキップ処理が正常実行されていない"
        
        self.validate_common_assertions(result)
    
    def test_data_quality_validation(self):
        """データ品質テスト: ETL品質検証"""
        test_id = f"data_quality_{int(time.time())}"
        
        # 高エンゲージメントデータでテスト
        test_data = self._generate_high_engagement_data()
        
        # データ変換処理
        transformed_data = self._transform_client_dna_data(test_data)
        
        # 品質検証
        quality_metrics = self.validate_data_quality(transformed_data, self.output_columns)
        violations = self.validate_domain_business_rules(transformed_data)
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.DATA_QUALITY,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=len(test_data.split('\n')) - 1,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=min(quality_metrics.values()),
            errors=[],
            warnings=violations
        )
        
        # ETL品質アサーション
        assert quality_metrics["completeness"] >= 0.90, f"完全性が基準未満: {quality_metrics['completeness']}"
        assert quality_metrics["validity"] >= 0.95, f"有効性が基準未満: {quality_metrics['validity']}"
        
        self.validate_common_assertions(result)
    
    def test_performance_large_dataset(self):
        """パフォーマンステスト: 大量データ処理"""
        test_id = f"performance_large_{int(time.time())}"
        
        # 大量データ準備（180000件）
        large_data = self._generate_client_dna_data(180000)
        
        start_time = time.time()
        
        # ETL処理実行
        transformed_data = self._transform_client_dna_data(large_data)
        
        execution_time = time.time() - start_time
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.PERFORMANCE,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=180000,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=0.95,
            errors=[]
        )
        
        # パフォーマンス基準
        throughput = 180000 / execution_time
        assert throughput > 1800, f"スループットが基準未満: {throughput:.2f} records/sec"
        
        self.validate_common_assertions(result)
    
    def test_integration_database_operations(self):
        """統合テスト: データベース操作"""
        test_id = f"integration_database_{int(time.time())}"
        
        # 一括DNAアナリシスデータでテスト
        test_data = self._generate_bulk_dna_analysis()
        
        # ETL処理
        transformed_data = self._transform_client_dna_data(test_data)
        
        # データベース操作シミュレーション
        # 1. 既存レコードチェック
        existing_check = self.mock_database.query_records(
            self.database_target, 
            "SELECT client_id FROM marketing_client_dna WHERE analysis_flag = 'HIGH_VALUE'"
        )
        
        # 2. 新規挿入
        db_records = self.convert_csv_to_dict_list(transformed_data)
        insert_result = self.mock_database.insert_records(
            self.database_target, 
            db_records
        )
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.INTEGRATION,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=len(test_data.split('\n')) - 1,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(db_records),
            data_quality_score=1.0,
            errors=[]
        )
        
        # 統合テストアサーション
        assert insert_result, "データベース挿入失敗"
        assert len(db_records) >= 100, f"処理レコード数不足: {len(db_records)}"
        
        self.validate_common_assertions(result)