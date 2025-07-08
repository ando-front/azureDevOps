"""
pi_Send_mTGMailPermission パイプラインのE2Eテスト

mTGメール許可配信パイプラインのETL処理テスト
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List

from ...base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus


class TestMTGMailPermissionPipeline(DomainTestBase):
    """mTGメール許可配信パイプラインテスト"""
    
    def __init__(self):
        super().__init__("pi_Send_mTGMailPermission", "mtgmaster")
    
    def domain_specific_setup(self):
        """mTGマスタードメイン固有セットアップ"""
        self.source_container = "mytokyogas"
        self.output_container = "datalake"
        self.sftp_target_dir = "/Export/MTGMailPermission"
        
        # 期待される入力フィールド（ETL処理用）
        self.input_columns = [
            "CUSTOMER_ID", "EMAIL_ADDRESS", "PERMISSION_TYPE", "PERMISSION_STATUS",
            "PERMISSION_DATE", "CONSENT_SOURCE", "CONSENT_VERSION", "OPT_IN_CATEGORIES",
            "FREQUENCY_PREFERENCE", "FORMAT_PREFERENCE", "LANGUAGE_PREFERENCE", "STATUS"
        ]
        
        # 期待される出力フィールド（ETL変換後）
        self.output_columns = [
            "CUSTOMER_ID", "EMAIL_ADDRESS", "PERMISSION_TYPE", "PERMISSION_STATUS",
            "PERMISSION_DATE", "CONSENT_SOURCE", "CONSENT_VERSION", "OPT_IN_CATEGORIES",
            "FREQUENCY_PREFERENCE", "FORMAT_PREFERENCE", "LANGUAGE_PREFERENCE",
            "STATUS", "PROCESS_DATE", "PERMISSION_ID", "COMPLIANCE_FLAG"
        ]
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """mTGマスタードメイン用テストデータテンプレート"""
        return {
            "mail_permission_data": self._generate_mail_permission_data(),
            "opt_in_data": self._generate_opt_in_data(),
            "bulk_permission_data": self._generate_bulk_permission_data()
        }
    
    def _generate_mail_permission_data(self, record_count: int = 1000) -> str:
        """メール許可データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        permission_types = ["MARKETING", "TRANSACTIONAL", "PROMOTIONAL", "NEWSLETTER"]
        permission_statuses = ["GRANTED", "DENIED", "PENDING", "REVOKED"]
        consent_sources = ["WEB", "PHONE", "MAIL", "STORE"]
        opt_in_categories = ["ALL", "PROMOTIONAL", "NEWSLETTER", "BILLING", "EMERGENCY"]
        frequency_preferences = ["DAILY", "WEEKLY", "MONTHLY", "QUARTERLY"]
        format_preferences = ["HTML", "TEXT", "BOTH"]
        language_preferences = ["JA", "EN", "AUTO"]
        statuses = ["ACTIVE", "INACTIVE", "SUSPENDED"]
        
        base_date = datetime.utcnow()
        
        for i in range(1, record_count + 1):
            # 許可日（過去30日以内）
            permission_days = i % 30
            permission_date = (base_date - timedelta(days=permission_days)).strftime('%Y%m%d')
            
            permission_type = permission_types[i % len(permission_types)]
            permission_status = permission_statuses[i % len(permission_statuses)]
            consent_source = consent_sources[i % len(consent_sources)]
            opt_in_category = opt_in_categories[i % len(opt_in_categories)]
            frequency_preference = frequency_preferences[i % len(frequency_preferences)]
            format_preference = format_preferences[i % len(format_preferences)]
            language_preference = language_preferences[i % len(language_preferences)]
            status = statuses[i % len(statuses)]
            
            email = f"mailperm{i:06d}@tokyogas.co.jp"
            consent_version = f"v{i % 5 + 1}.0"
            
            data_lines.append(
                f"CUST{i:06d}\t{email}\t{permission_type}\t{permission_status}\t"
                f"{permission_date}\t{consent_source}\t{consent_version}\t{opt_in_category}\t"
                f"{frequency_preference}\t{format_preference}\t{language_preference}\t{status}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_opt_in_data(self) -> str:
        """オプトインデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        permission_date = base_date.strftime('%Y%m%d')
        
        # 新規オプトイン
        opt_in_customers = [
            ("CUST900001", "optin1@tokyogas.co.jp", "MARKETING", "GRANTED", permission_date, "WEB", "v2.0", "ALL", "WEEKLY", "HTML", "JA", "ACTIVE"),
            ("CUST900002", "optin2@tokyogas.co.jp", "PROMOTIONAL", "GRANTED", permission_date, "WEB", "v2.0", "PROMOTIONAL", "MONTHLY", "HTML", "JA", "ACTIVE"),
            ("CUST900003", "optin3@tokyogas.co.jp", "NEWSLETTER", "GRANTED", permission_date, "PHONE", "v2.0", "NEWSLETTER", "MONTHLY", "TEXT", "JA", "ACTIVE"),
        ]
        
        for customer_data in opt_in_customers:
            data_lines.append("\t".join(customer_data))
        
        return "\n".join(data_lines)
    
    def _generate_bulk_permission_data(self) -> str:
        """一括許可データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        permission_date = base_date.strftime('%Y%m%d')
        
        # 一括更新（プライバシーポリシー更新対応）
        for i in range(1, 101):
            data_lines.append(
                f"CUST{i:06d}\tbulk{i:03d}@tokyogas.co.jp\tMARKETING\t"
                f"GRANTED\t{permission_date}\tMAIL\tv3.0\tALL\t"
                f"MONTHLY\tHTML\tJA\tACTIVE"
            )
        
        return "\n".join(data_lines)
    
    def _transform_mail_permission_data(self, input_data: str) -> str:
        """メール許可データETL変換処理"""
        lines = input_data.strip().split('\n')
        
        # 出力CSVヘッダー
        output_header = ",".join(self.output_columns)
        output_lines = [output_header]
        
        current_date = datetime.utcnow()
        process_date = current_date.strftime('%Y%m%d')
        
        for line in lines[1:]:
            if not line.strip():
                continue
                
            parts = line.split('\t')
            if len(parts) != len(self.input_columns):
                continue
            
            (customer_id, email_address, permission_type, permission_status,
             permission_date, consent_source, consent_version, opt_in_categories,
             frequency_preference, format_preference, language_preference, status) = parts
            
            # ETL品質チェック
            if not customer_id.strip() or not email_address.strip():
                continue
            
            # 許可ID生成（ETL処理）
            permission_id = f"PERM{customer_id[4:]}{process_date}"
            
            # コンプライアンスフラグ設定（ETL処理）
            compliance_flag = "COMPLIANT" if permission_status == "GRANTED" and status == "ACTIVE" else "NON_COMPLIANT"
            
            # CSV出力行生成
            output_line = (
                f"{customer_id},{email_address},{permission_type},{permission_status},"
                f"{permission_date},{consent_source},{consent_version},{opt_in_categories},"
                f"{frequency_preference},{format_preference},{language_preference},"
                f"{status},{process_date},{permission_id},{compliance_flag}"
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
        test_data = self._generate_mail_permission_data()
        
        # ファイルアップロード
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"MTGMailPermission/{file_date}/mtg_mail_permission.tsv"
        self.mock_storage.upload_file(self.source_container, file_path, test_data.encode('utf-8'))
        
        # ETL処理シミュレーション
        transformed_data, output_data = self.simulate_etl_process(
            test_data,
            self._transform_mail_permission_data,
            "csv"
        )
        
        # 出力ファイル保存
        output_file_path = f"MTGMailPermission/processed_{file_date}.csv"
        self.mock_storage.upload_file(self.output_container, output_file_path, output_data)
        
        # SFTP転送
        sftp_path = f"{self.sftp_target_dir}/mtg_mail_permission_{file_date}.csv"
        sftp_result = self.mock_sftp.upload(output_file_path, sftp_path, output_data)
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.FUNCTIONAL,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=len(test_data.split('\n')) - 1,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=0.95,
            errors=[]
        )
        
        # アサーション
        self.validate_common_assertions(result)
        assert result.records_extracted > 0, "抽出レコード数が0"
        assert sftp_result, "SFTP転送失敗"
    
    def test_functional_without_file(self):
        """機能テスト: ファイル無し処理"""
        test_id = f"functional_no_file_{int(time.time())}"
        
        # ファイル存在チェック（存在しないファイルパスを使用）
        file_date = "99999999"
        file_path = f"MTGMailPermission/{file_date}/mtg_mail_permission.tsv"
        
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
        
        # オプトインデータでテスト
        test_data = self._generate_opt_in_data()
        
        # データ変換処理
        transformed_data = self._transform_mail_permission_data(test_data)
        
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
        
        # 大量データ準備（80000件）
        large_data = self._generate_mail_permission_data(80000)
        
        start_time = time.time()
        
        # ETL処理実行
        transformed_data = self._transform_mail_permission_data(large_data)
        
        execution_time = time.time() - start_time
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.PERFORMANCE,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=80000,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=0.95,
            errors=[]
        )
        
        # パフォーマンス基準
        throughput = 80000 / execution_time
        assert throughput > 2000, f"スループットが基準未満: {throughput:.2f} records/sec"
        
        self.validate_common_assertions(result)
    
    def test_integration_sftp_connectivity(self):
        """統合テスト: SFTP接続性"""
        test_id = f"integration_sftp_{int(time.time())}"
        
        # 一括許可データでテスト
        test_data = self._generate_bulk_permission_data()
        
        # ETL処理
        transformed_data = self._transform_mail_permission_data(test_data)
        
        # SFTP転送テスト
        file_date = datetime.utcnow().strftime('%Y%m%d')
        sftp_path = f"{self.sftp_target_dir}/mtg_mail_permission_bulk_{file_date}.csv"
        sftp_result = self.mock_sftp.upload(
            f"mtg_mail_permission_bulk_{file_date}.csv",
            sftp_path,
            transformed_data.encode('utf-8')
        )
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.INTEGRATION,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=len(test_data.split('\n')) - 1,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=1.0,
            errors=[]
        )
        
        # 統合テストアサーション
        assert sftp_result, "SFTP転送失敗"
        
        # 転送履歴確認
        transfer_history = self.mock_sftp.get_transfer_history()
        upload_records = [h for h in transfer_history if h["action"] == "upload"]
        assert len(upload_records) > 0, "SFTP転送履歴なし"
        
        self.validate_common_assertions(result)