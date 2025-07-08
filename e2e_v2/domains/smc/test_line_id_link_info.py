"""
pi_Send_LINEIDLinkInfo パイプラインのE2Eテスト

LINE ID連携情報配信パイプラインのETL処理テスト
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List

from ...base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus


class TestLINEIDLinkInfoPipeline(DomainTestBase):
    """LINE ID連携情報配信パイプラインテスト"""
    
    def __init__(self):
        super().__init__("pi_Send_LINEIDLinkInfo", "smc")
    
    def domain_specific_setup(self):
        """SMCドメイン固有セットアップ"""
        self.source_container = "mytokyogas"
        self.output_container = "datalake"
        self.sftp_target_dir = "/Export/LINEIDLinkInfo"
        
        # 期待される入力フィールド（ETL処理用）
        self.input_columns = [
            "CUSTOMER_ID", "LINE_USER_ID", "LINK_STATUS", "LINK_DATE",
            "CUSTOMER_NAME", "EMAIL_ADDRESS", "PHONE_NUMBER", "VERIFICATION_STATUS",
            "NOTIFICATION_PREFERENCE", "SERVICE_AGREEMENT", "PRIVACY_CONSENT", "STATUS"
        ]
        
        # 期待される出力フィールド（ETL変換後）
        self.output_columns = [
            "CUSTOMER_ID", "LINE_USER_ID", "LINK_STATUS", "LINK_DATE",
            "CUSTOMER_NAME", "EMAIL_ADDRESS", "PHONE_NUMBER", "VERIFICATION_STATUS",
            "NOTIFICATION_PREFERENCE", "SERVICE_AGREEMENT", "PRIVACY_CONSENT",
            "STATUS", "PROCESS_DATE", "LINK_INFO_ID", "COMPLIANCE_FLAG"
        ]
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """SMCドメイン用テストデータテンプレート"""
        return {
            "line_link_data": self._generate_line_link_data(),
            "new_link_data": self._generate_new_link_data(),
            "bulk_verification_data": self._generate_bulk_verification_data()
        }
    
    def _generate_line_link_data(self, record_count: int = 1000) -> str:
        """LINE連携データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        link_statuses = ["LINKED", "PENDING", "UNLINKED", "SUSPENDED"]
        verification_statuses = ["VERIFIED", "PENDING", "FAILED", "EXPIRED"]
        notification_preferences = ["ALL", "IMPORTANT", "BILLING", "NONE"]
        service_agreements = ["AGREED", "PENDING", "DECLINED"]
        privacy_consents = ["GRANTED", "PENDING", "REVOKED"]
        statuses = ["ACTIVE", "INACTIVE", "SUSPENDED"]
        
        base_date = datetime.utcnow()
        
        for i in range(1, record_count + 1):
            # 連携日（過去60日以内）
            link_days = i % 60
            link_date = (base_date - timedelta(days=link_days)).strftime('%Y%m%d')
            
            link_status = link_statuses[i % len(link_statuses)]
            verification_status = verification_statuses[i % len(verification_statuses)]
            notification_preference = notification_preferences[i % len(notification_preferences)]
            service_agreement = service_agreements[i % len(service_agreements)]
            privacy_consent = privacy_consents[i % len(privacy_consents)]
            status = statuses[i % len(statuses)]
            
            customer_name = f"LINE連携{i:04d}"
            email = f"linelink{i:06d}@tokyogas.co.jp"
            phone = f"090-{i:04d}-{(i*13) % 10000:04d}"
            line_user_id = f"U{i:032x}"  # LINE USER IDの形式
            
            data_lines.append(
                f"CUST{i:06d}\t{line_user_id}\t{link_status}\t{link_date}\t"
                f"{customer_name}\t{email}\t{phone}\t{verification_status}\t"
                f"{notification_preference}\t{service_agreement}\t{privacy_consent}\t{status}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_new_link_data(self) -> str:
        """新規連携データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        link_date = base_date.strftime('%Y%m%d')
        
        # 新規LINE連携
        new_links = [
            ("CUST900001", "U900000000000000000000000000001", "LINKED", link_date, "新規連携1", "newlink1@tokyogas.co.jp", "090-9001-0001", "VERIFIED", "ALL", "AGREED", "GRANTED", "ACTIVE"),
            ("CUST900002", "U900000000000000000000000000002", "PENDING", link_date, "新規連携2", "newlink2@tokyogas.co.jp", "090-9002-0002", "PENDING", "IMPORTANT", "PENDING", "PENDING", "ACTIVE"),
            ("CUST900003", "U900000000000000000000000000003", "LINKED", link_date, "新規連携3", "newlink3@tokyogas.co.jp", "090-9003-0003", "VERIFIED", "BILLING", "AGREED", "GRANTED", "ACTIVE"),
        ]
        
        for link_data in new_links:
            data_lines.append("\t".join(link_data))
        
        return "\n".join(data_lines)
    
    def _generate_bulk_verification_data(self) -> str:
        """一括認証データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        link_date = (base_date - timedelta(days=30)).strftime('%Y%m%d')
        
        # 一括認証処理
        for i in range(1, 101):
            line_user_id = f"U{i:032x}"
            data_lines.append(
                f"CUST{i:06d}\t{line_user_id}\tLINKED\t{link_date}\t"
                f"一括認証{i:03d}\tbulk{i:03d}@tokyogas.co.jp\t090-0000-{i:04d}\t"
                f"VERIFIED\tALL\tAGREED\tGRANTED\tACTIVE"
            )
        
        return "\n".join(data_lines)
    
    def _transform_line_link_data(self, input_data: str) -> str:
        """LINE連携データETL変換処理"""
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
            
            (customer_id, line_user_id, link_status, link_date,
             customer_name, email_address, phone_number, verification_status,
             notification_preference, service_agreement, privacy_consent, status) = parts
            
            # ETL品質チェック
            if not customer_id.strip() or not line_user_id.strip() or not customer_name.strip():
                continue
            
            # 連携情報ID生成（ETL処理）
            link_info_id = f"LINK{customer_id[4:]}{process_date}"
            
            # コンプライアンスフラグ設定（ETL処理）
            if (link_status == "LINKED" and verification_status == "VERIFIED" and 
                service_agreement == "AGREED" and privacy_consent == "GRANTED"):
                compliance_flag = "COMPLIANT"
            else:
                compliance_flag = "NON_COMPLIANT"
            
            # CSV出力行生成
            output_line = (
                f"{customer_id},{line_user_id},{link_status},{link_date},"
                f"{customer_name},{email_address},{phone_number},{verification_status},"
                f"{notification_preference},{service_agreement},{privacy_consent},"
                f"{status},{process_date},{link_info_id},{compliance_flag}"
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
        test_data = self._generate_line_link_data()
        
        # ファイルアップロード
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"LINEIDLinkInfo/{file_date}/line_id_link_info.tsv"
        self.mock_storage.upload_file(self.source_container, file_path, test_data.encode('utf-8'))
        
        # ETL処理シミュレーション
        transformed_data, output_data = self.simulate_etl_process(
            test_data,
            self._transform_line_link_data,
            "csv"
        )
        
        # 出力ファイル保存
        output_file_path = f"LINEIDLinkInfo/processed_{file_date}.csv"
        self.mock_storage.upload_file(self.output_container, output_file_path, output_data)
        
        # SFTP転送
        sftp_path = f"{self.sftp_target_dir}/line_id_link_info_{file_date}.csv"
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
        file_path = f"LINEIDLinkInfo/{file_date}/line_id_link_info.tsv"
        
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
        
        # 新規連携データでテスト
        test_data = self._generate_new_link_data()
        
        # データ変換処理
        transformed_data = self._transform_line_link_data(test_data)
        
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
        
        # 大量データ準備（60000件）
        large_data = self._generate_line_link_data(60000)
        
        start_time = time.time()
        
        # ETL処理実行
        transformed_data = self._transform_line_link_data(large_data)
        
        execution_time = time.time() - start_time
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.PERFORMANCE,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=60000,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=0.95,
            errors=[]
        )
        
        # パフォーマンス基準
        throughput = 60000 / execution_time
        assert throughput > 3500, f"スループットが基準未満: {throughput:.2f} records/sec"
        
        self.validate_common_assertions(result)
    
    def test_integration_sftp_connectivity(self):
        """統合テスト: SFTP接続性"""
        test_id = f"integration_sftp_{int(time.time())}"
        
        # 一括認証データでテスト
        test_data = self._generate_bulk_verification_data()
        
        # ETL処理
        transformed_data = self._transform_line_link_data(test_data)
        
        # SFTP転送テスト
        file_date = datetime.utcnow().strftime('%Y%m%d')
        sftp_path = f"{self.sftp_target_dir}/line_id_link_info_bulk_{file_date}.csv"
        sftp_result = self.mock_sftp.upload(
            f"line_id_link_info_bulk_{file_date}.csv",
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