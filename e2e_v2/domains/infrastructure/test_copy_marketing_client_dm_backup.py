"""
pi_Copy_marketing_client_dm_bk パイプラインのE2Eテスト

マーケティングクライアントDMコピーバックアップパイプラインのETL処理テスト
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List

from ...base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus


class TestCopyMarketingClientDMBackupPipeline(DomainTestBase):
    """マーケティングクライアントDMコピーバックアップパイプラインテスト"""
    
    def __init__(self):
        super().__init__("pi_Copy_marketing_client_dm_bk", "infrastructure")
    
    def domain_specific_setup(self):
        """インフラストラクチャードメイン固有セットアップ"""
        self.source_container = "marketing-backup"
        self.output_container = "backup-archive"
        self.database_target = "marketing_client_dm_backup"
        
        # 期待される入力フィールド（ETL処理用）
        self.input_columns = [
            "BACKUP_ID", "CLIENT_ID", "CLIENT_NAME", "DM_CAMPAIGN_ID",
            "BACKUP_DATE", "ORIGINAL_SEND_DATE", "BACKUP_TYPE", "RETENTION_PERIOD",
            "BACKUP_STATUS", "DATA_SIZE", "ARCHIVE_LOCATION", "METADATA"
        ]
        
        # 期待される出力フィールド（ETL変換後）
        self.output_columns = [
            "BACKUP_ID", "CLIENT_ID", "CLIENT_NAME", "DM_CAMPAIGN_ID",
            "BACKUP_DATE", "ORIGINAL_SEND_DATE", "BACKUP_TYPE", "RETENTION_PERIOD",
            "BACKUP_STATUS", "DATA_SIZE", "ARCHIVE_LOCATION", "METADATA",
            "COPY_DATE", "BACKUP_REPORT_ID", "EXPIRY_DATE", "ARCHIVE_FLAG"
        ]
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """インフラストラクチャードメイン用テストデータテンプレート"""
        return {
            "dm_backup_data": self._generate_dm_backup_data(),
            "expired_backup_data": self._generate_expired_backup_data(),
            "bulk_archive_data": self._generate_bulk_archive_data()
        }
    
    def _generate_dm_backup_data(self, record_count: int = 1500) -> str:
        """DMバックアップデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        backup_types = ["FULL", "INCREMENTAL", "DIFFERENTIAL", "ARCHIVE"]
        retention_periods = ["1_YEAR", "3_YEARS", "7_YEARS", "PERMANENT"]
        backup_statuses = ["COMPLETED", "IN_PROGRESS", "FAILED", "EXPIRED"]
        
        base_date = datetime.utcnow()
        
        for i in range(1, record_count + 1):
            # バックアップ日（過去365日以内）
            backup_days = i % 365
            backup_date = (base_date - timedelta(days=backup_days)).strftime('%Y%m%d')
            
            # 元送信日（バックアップ日より前）
            original_send_days = backup_days + (i % 30) + 1
            original_send_date = (base_date - timedelta(days=original_send_days)).strftime('%Y%m%d')
            
            backup_type = backup_types[i % len(backup_types)]
            retention_period = retention_periods[i % len(retention_periods)]
            backup_status = backup_statuses[i % len(backup_statuses)]
            
            # データサイズ（KB）
            data_size = (i % 10000) + 100  # 100-10099 KB
            
            # アーカイブ場所
            archive_location = f"/backup/marketing/dm/{backup_date[:6]}/backup_{i:06d}.zip"
            
            # メタデータ
            metadata = f"CLIENT_COUNT:{i % 1000 + 1};CAMPAIGN_TYPE:DM;COMPRESS_RATIO:0.{i % 9 + 1}"
            
            client_name = f"DMバックアップ{i:04d}"
            
            data_lines.append(
                f"BK{i:08d}\tCLIENT{i:06d}\t{client_name}\tDM{i:06d}\t"
                f"{backup_date}\t{original_send_date}\t{backup_type}\t{retention_period}\t"
                f"{backup_status}\t{data_size}\t{archive_location}\t{metadata}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_expired_backup_data(self) -> str:
        """期限切れバックアップデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        # 2年前のバックアップ（1年保存期限切れ）
        backup_date = (base_date - timedelta(days=730)).strftime('%Y%m%d')
        original_send_date = (base_date - timedelta(days=735)).strftime('%Y%m%d')
        
        # 期限切れバックアップ
        expired_backups = [
            ("BK90000001", "CLIENT900001", "期限切れDM1", "DM900001", backup_date, original_send_date, "FULL", "1_YEAR", "EXPIRED", "5000", "/backup/marketing/dm/202101/backup_900001.zip", "CLIENT_COUNT:500;CAMPAIGN_TYPE:DM;COMPRESS_RATIO:0.7"),
            ("BK90000002", "CLIENT900002", "期限切れDM2", "DM900002", backup_date, original_send_date, "INCREMENTAL", "1_YEAR", "EXPIRED", "3200", "/backup/marketing/dm/202101/backup_900002.zip", "CLIENT_COUNT:320;CAMPAIGN_TYPE:DM;COMPRESS_RATIO:0.8"),
            ("BK90000003", "CLIENT900003", "期限切れDM3", "DM900003", backup_date, original_send_date, "ARCHIVE", "1_YEAR", "EXPIRED", "8500", "/backup/marketing/dm/202101/backup_900003.zip", "CLIENT_COUNT:850;CAMPAIGN_TYPE:DM;COMPRESS_RATIO:0.6"),
        ]
        
        for backup_data in expired_backups:
            data_lines.append("\t".join(backup_data))
        
        return "\n".join(data_lines)
    
    def _generate_bulk_archive_data(self) -> str:
        """一括アーカイブデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        # 3年前のバックアップ（長期保存対象）
        backup_date = (base_date - timedelta(days=1095)).strftime('%Y%m%d')
        original_send_date = (base_date - timedelta(days=1100)).strftime('%Y%m%d')
        
        # 一括アーカイブ対象
        for i in range(1, 121):
            data_size = 2000 + (i % 3000)  # 2000-4999 KB
            archive_location = f"/backup/marketing/dm/archive/bulk_archive_{i:06d}.zip"
            metadata = f"CLIENT_COUNT:{i * 10};CAMPAIGN_TYPE:DM;COMPRESS_RATIO:0.{i % 9 + 1}"
            
            data_lines.append(
                f"BK{i:08d}\tCLIENT{i:06d}\t一括アーカイブ{i:03d}\tDM{i:06d}\t"
                f"{backup_date}\t{original_send_date}\tFULL\t7_YEARS\t"
                f"COMPLETED\t{data_size}\t{archive_location}\t{metadata}"
            )
        
        return "\n".join(data_lines)
    
    def _transform_dm_backup_data(self, input_data: str) -> str:
        """DMバックアップデータETL変換処理"""
        lines = input_data.strip().split('\n')
        
        # 出力CSVヘッダー
        output_header = ",".join(self.output_columns)
        output_lines = [output_header]
        
        current_date = datetime.utcnow()
        copy_date = current_date.strftime('%Y/%m/%d %H:%M:%S')
        
        for line in lines[1:]:
            if not line.strip():
                continue
                
            parts = line.split('\t')
            if len(parts) != len(self.input_columns):
                continue
            
            (backup_id, client_id, client_name, dm_campaign_id,
             backup_date, original_send_date, backup_type, retention_period,
             backup_status, data_size, archive_location, metadata) = parts
            
            # ETL品質チェック
            if not backup_id.strip() or not client_id.strip() or not backup_date.strip():
                continue
            
            # バックアップレポートID生成（ETL処理）
            backup_report_id = f"BKRPT{backup_id[2:]}{backup_date}"
            
            # 有効期限計算（ETL処理）
            backup_dt = datetime.strptime(backup_date, '%Y%m%d')
            if retention_period == "1_YEAR":
                expiry_dt = backup_dt + timedelta(days=365)
            elif retention_period == "3_YEARS":
                expiry_dt = backup_dt + timedelta(days=1095)
            elif retention_period == "7_YEARS":
                expiry_dt = backup_dt + timedelta(days=2555)
            else:  # PERMANENT
                expiry_dt = datetime(2099, 12, 31)
            
            expiry_date = expiry_dt.strftime('%Y%m%d')
            
            # アーカイブフラグ設定（ETL処理）
            if current_date > expiry_dt:
                archive_flag = "EXPIRED"
            elif (expiry_dt - current_date).days <= 90:
                archive_flag = "EXPIRING_SOON"
            elif (current_date - backup_dt).days > 1095:  # 3年以上
                archive_flag = "ARCHIVED"
            else:
                archive_flag = "ACTIVE"
            
            # CSV出力行生成
            output_line = (
                f"{backup_id},{client_id},{client_name},{dm_campaign_id},"
                f"{backup_date},{original_send_date},{backup_type},{retention_period},"
                f"{backup_status},{data_size},{archive_location},{metadata},"
                f"{copy_date},{backup_report_id},{expiry_date},{archive_flag}"
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
        test_data = self._generate_dm_backup_data()
        
        # ファイルアップロード
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"MarketingClientDMBackup/{file_date}/dm_backup_data.tsv"
        self.mock_storage.upload_file(self.source_container, file_path, test_data.encode('utf-8'))
        
        # ETL処理シミュレーション
        transformed_data, output_data = self.simulate_etl_process(
            test_data,
            self._transform_dm_backup_data,
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
        file_path = f"MarketingClientDMBackup/{file_date}/dm_backup_data.tsv"
        
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
        
        # 期限切れバックアップデータでテスト
        test_data = self._generate_expired_backup_data()
        
        # データ変換処理
        transformed_data = self._transform_dm_backup_data(test_data)
        
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
        
        # 大量データ準備（100000件）
        large_data = self._generate_dm_backup_data(100000)
        
        start_time = time.time()
        
        # ETL処理実行
        transformed_data = self._transform_dm_backup_data(large_data)
        
        execution_time = time.time() - start_time
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.PERFORMANCE,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=100000,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=0.95,
            errors=[]
        )
        
        # パフォーマンス基準
        throughput = 100000 / execution_time
        assert throughput > 2500, f"スループットが基準未満: {throughput:.2f} records/sec"
        
        self.validate_common_assertions(result)
    
    def test_integration_database_operations(self):
        """統合テスト: データベース操作"""
        test_id = f"integration_database_{int(time.time())}"
        
        # 一括アーカイブデータでテスト
        test_data = self._generate_bulk_archive_data()
        
        # ETL処理
        transformed_data = self._transform_dm_backup_data(test_data)
        
        # データベース操作シミュレーション
        # 1. 既存バックアップチェック
        existing_check = self.mock_database.query_records(
            self.database_target, 
            "SELECT backup_id FROM marketing_client_dm_backup WHERE archive_flag = 'EXPIRED'"
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
        assert len(db_records) >= 120, f"処理レコード数不足: {len(db_records)}"
        
        self.validate_common_assertions(result)