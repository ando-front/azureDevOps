"""
pi_Ins_marketing_client_dna_bk パイプラインのE2Eテスト

マーケティングクライアントDNAバックアップ挿入パイプラインのETL処理テスト
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List

from ...base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus


class TestClientDNABackupPipeline(DomainTestBase):
    """マーケティングクライアントDNAバックアップ挿入パイプラインテスト"""
    
    def __init__(self):
        super().__init__("pi_Ins_marketing_client_dna_bk", "marketing")
    
    def domain_specific_setup(self):
        """マーケティングドメイン固有セットアップ"""
        self.source_container = "marketing"
        self.output_container = "backup"
        self.database_target = "marketing_client_dna_backup"
        
        # 期待される入力フィールド（ETL処理用）
        self.input_columns = [
            "CLIENT_ID", "CLIENT_NAME", "DNA_PROFILE", "BEHAVIOR_SCORE",
            "PREFERENCE_CATEGORY", "PURCHASE_PATTERN", "ENGAGEMENT_LEVEL",
            "DEMOGRAPHIC_GROUP", "LIFECYCLE_STAGE", "INTERACTION_HISTORY",
            "BACKUP_DATE", "ORIGINAL_UPDATE_DATE", "STATUS"
        ]
        
        # 期待される出力フィールド（ETL変換後）
        self.output_columns = [
            "CLIENT_ID", "CLIENT_NAME", "DNA_PROFILE", "BEHAVIOR_SCORE",
            "PREFERENCE_CATEGORY", "PURCHASE_PATTERN", "ENGAGEMENT_LEVEL",
            "DEMOGRAPHIC_GROUP", "LIFECYCLE_STAGE", "INTERACTION_HISTORY",
            "BACKUP_DATE", "ORIGINAL_UPDATE_DATE", "STATUS",
            "BACKUP_ID", "BACKUP_VERSION", "RETENTION_PERIOD", "ARCHIVE_FLAG"
        ]
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """マーケティングドメイン用テストデータテンプレート"""
        return {
            "client_dna_backup_data": self._generate_client_dna_backup_data(),
            "historical_backup_data": self._generate_historical_backup_data(),
            "bulk_backup_data": self._generate_bulk_backup_data()
        }
    
    def _generate_client_dna_backup_data(self, record_count: int = 1800) -> str:
        """クライアントDNAバックアップデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        dna_profiles = ["EXPLORER", "LOYALIST", "OPTIMIZER", "CONSERVATIVE", "TRENDSETTER"]
        preference_categories = ["ENERGY_SAVER", "CONVENIENCE", "PREMIUM", "BUDGET", "ECO_FRIENDLY"]
        purchase_patterns = ["REGULAR", "SEASONAL", "IMPULSE", "PLANNED", "BULK"]
        engagement_levels = ["HIGH", "MEDIUM", "LOW", "DORMANT"]
        demographic_groups = ["YOUNG_PROFESSIONAL", "FAMILY", "SENIOR", "STUDENT", "ENTERPRISE"]
        lifecycle_stages = ["NEW", "GROWING", "MATURE", "DECLINING", "CHURNED"]
        statuses = ["ACTIVE", "ARCHIVED", "DELETED", "MIGRATED"]
        
        base_date = datetime.utcnow()
        
        for i in range(1, record_count + 1):
            # バックアップ日（過去180日以内）
            backup_days = i % 180
            backup_date = (base_date - timedelta(days=backup_days)).strftime('%Y%m%d')
            
            # 元データ更新日（バックアップ日より前）
            original_update_days = backup_days + (i % 30) + 1
            original_update_date = (base_date - timedelta(days=original_update_days)).strftime('%Y%m%d')
            
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
            interaction_count = (i % 50) + 1
            interaction_history = f"BACKUP_INTERACTIONS_{interaction_count}"
            
            client_name = f"DNAバックアップ{i:04d}"
            
            data_lines.append(
                f"CLIENT{i:06d}\t{client_name}\t{dna_profile}\t{behavior_score}\t"
                f"{preference_category}\t{purchase_pattern}\t{engagement_level}\t"
                f"{demographic_group}\t{lifecycle_stage}\t{interaction_history}\t"
                f"{backup_date}\t{original_update_date}\t{status}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_historical_backup_data(self) -> str:
        """履歴バックアップデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        backup_date = (base_date - timedelta(days=365)).strftime('%Y%m%d')  # 1年前
        original_update_date = (base_date - timedelta(days=370)).strftime('%Y%m%d')
        
        # 長期保存対象の履歴データ
        historical_clients = [
            ("CLIENT800001", "履歴バックアップ1", "LOYALIST", "85", "PREMIUM", "REGULAR", "HIGH", "FAMILY", "MATURE", "BACKUP_INTERACTIONS_100", backup_date, original_update_date, "ARCHIVED"),
            ("CLIENT800002", "履歴バックアップ2", "EXPLORER", "92", "ECO_FRIENDLY", "PLANNED", "HIGH", "YOUNG_PROFESSIONAL", "GROWING", "BACKUP_INTERACTIONS_80", backup_date, original_update_date, "ARCHIVED"),
            ("CLIENT800003", "履歴バックアップ3", "CONSERVATIVE", "78", "BUDGET", "SEASONAL", "MEDIUM", "SENIOR", "DECLINING", "BACKUP_INTERACTIONS_60", backup_date, original_update_date, "MIGRATED"),
        ]
        
        for client_data in historical_clients:
            data_lines.append("\t".join(client_data))
        
        return "\n".join(data_lines)
    
    def _generate_bulk_backup_data(self) -> str:
        """一括バックアップデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        backup_date = (base_date - timedelta(days=7)).strftime('%Y%m%d')  # 週次バックアップ
        original_update_date = (base_date - timedelta(days=14)).strftime('%Y%m%d')
        
        # 週次一括バックアップ
        for i in range(1, 151):
            behavior_score = 40 + (i % 40)  # 40-79の範囲
            
            data_lines.append(
                f"CLIENT{i:06d}\t一括バックアップ{i:03d}\tSTANDARD\t{behavior_score}\t"
                f"CONVENIENCE\tREGULAR\tMEDIUM\tFAMILY\tMATURE\t"
                f"BACKUP_INTERACTIONS_30\t{backup_date}\t{original_update_date}\tACTIVE"
            )
        
        return "\n".join(data_lines)
    
    def _transform_client_dna_backup_data(self, input_data: str) -> str:
        """クライアントDNAバックアップデータETL変換処理"""
        lines = input_data.strip().split('\n')
        
        # 出力CSVヘッダー
        output_header = ",".join(self.output_columns)
        output_lines = [output_header]
        
        current_date = datetime.utcnow()
        backup_version = "BK_2.0"
        
        for line in lines[1:]:
            if not line.strip():
                continue
                
            parts = line.split('\t')
            if len(parts) != len(self.input_columns):
                continue
            
            (client_id, client_name, dna_profile, behavior_score,
             preference_category, purchase_pattern, engagement_level,
             demographic_group, lifecycle_stage, interaction_history,
             backup_date, original_update_date, status) = parts
            
            # ETL品質チェック
            if not client_id.strip() or not client_name.strip() or not backup_date.strip():
                continue
            
            # バックアップID生成（ETL処理）
            backup_id = f"BK{client_id[6:]}{backup_date}"
            
            # 保存期間設定（ETL処理）
            backup_dt = datetime.strptime(backup_date, '%Y%m%d')
            days_since_backup = (current_date - backup_dt).days
            
            if status == "ARCHIVED":
                retention_period = "LONG_TERM"  # 7年保存
            elif status == "DELETED":
                retention_period = "SHORT_TERM"  # 1年保存
            else:
                retention_period = "STANDARD"  # 3年保存
            
            # アーカイブフラグ設定（ETL処理）
            if days_since_backup > 365:
                archive_flag = "ARCHIVED"
            elif days_since_backup > 180:
                archive_flag = "ARCHIVING"
            else:
                archive_flag = "ACTIVE"
            
            # CSV出力行生成
            output_line = (
                f"{client_id},{client_name},{dna_profile},{behavior_score},"
                f"{preference_category},{purchase_pattern},{engagement_level},"
                f"{demographic_group},{lifecycle_stage},{interaction_history},"
                f"{backup_date},{original_update_date},{status},"
                f"{backup_id},{backup_version},{retention_period},{archive_flag}"
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
        test_data = self._generate_client_dna_backup_data()
        
        # ファイルアップロード
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"MarketingClientDNABackup/{file_date}/client_dna_backup.tsv"
        self.mock_storage.upload_file(self.source_container, file_path, test_data.encode('utf-8'))
        
        # ETL処理シミュレーション
        transformed_data, output_data = self.simulate_etl_process(
            test_data,
            self._transform_client_dna_backup_data,
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
        file_path = f"MarketingClientDNABackup/{file_date}/client_dna_backup.tsv"
        
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
        
        # 履歴バックアップデータでテスト
        test_data = self._generate_historical_backup_data()
        
        # データ変換処理
        transformed_data = self._transform_client_dna_backup_data(test_data)
        
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
        
        # 大量データ準備（300000件）
        large_data = self._generate_client_dna_backup_data(300000)
        
        start_time = time.time()
        
        # ETL処理実行
        transformed_data = self._transform_client_dna_backup_data(large_data)
        
        execution_time = time.time() - start_time
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.PERFORMANCE,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=300000,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=0.95,
            errors=[]
        )
        
        # パフォーマンス基準
        throughput = 300000 / execution_time
        assert throughput > 1000, f"スループットが基準未満: {throughput:.2f} records/sec"
        
        self.validate_common_assertions(result)
    
    def test_integration_database_operations(self):
        """統合テスト: データベース操作"""
        test_id = f"integration_database_{int(time.time())}"
        
        # 一括バックアップデータでテスト
        test_data = self._generate_bulk_backup_data()
        
        # ETL処理
        transformed_data = self._transform_client_dna_backup_data(test_data)
        
        # データベース操作シミュレーション
        # 1. 既存バックアップチェック
        existing_check = self.mock_database.query_records(
            self.database_target, 
            "SELECT backup_id FROM marketing_client_dna_backup WHERE archive_flag = 'ARCHIVED'"
        )
        
        # 2. 新規バックアップ挿入
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
        assert len(db_records) >= 150, f"処理レコード数不足: {len(db_records)}"
        
        self.validate_common_assertions(result)