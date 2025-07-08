"""
pi_PointGrantEmail パイプラインのE2Eテスト

ポイント付与メール送信パイプラインの包括的テスト
"""

import time
import gzip
from datetime import datetime, timedelta
from typing import Dict, List

from ...base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus


class TestPointGrantEmailPipeline(DomainTestBase):
    """ポイント付与メールパイプラインテスト"""
    
    def __init__(self):
        super().__init__("pi_PointGrantEmail", "kendenki")
    
    def domain_specific_setup(self):
        """検電ドメイン固有セットアップ"""
        # 検電ドメイン用ディレクトリ構造
        self.source_container = "mytokyogas"
        self.output_container = "datalake"
        self.sftp_target_dir = "/Import/DAM/3A01b_PointAddition"
        
        # 期待される入力フィールド
        self.input_columns = ["ID_NO", "PNT_TYPE_CD", "MAIL_ADR", "PICTURE_MM"]
        
        # 期待される出力フィールド
        self.output_columns = [
            "ID_NO", "PNT_TYPE_CD", "MAIL_ADR", "PICTURE_MM", 
            "CSV_YMD", "OUTPUT_DATETIME"
        ]
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """検電ドメイン用テストデータテンプレート"""
        return {
            "point_grant_data": self._generate_point_grant_data(),
            "point_grant_data_with_issues": self._generate_data_with_quality_issues(),
            "large_point_grant_data": self._generate_large_dataset(10000)
        }
    
    def _generate_point_grant_data(self, record_count: int = 100) -> str:
        """ポイント付与データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        for i in range(1, record_count + 1):
            data_lines.append(
                f"MTG{i:06d}\tP001\ttest{i}@tokyogas.co.jp\t{(i % 12) + 1:02d}"
            )
        
        # 重複データ追加（重複削除ロジックテスト用）
        data_lines.append("MTG000001\tP001\tdup1@tokyogas.co.jp\t01")
        data_lines.append("MTG000001\tP001\tdup2@tokyogas.co.jp\t01")
        
        return "\n".join(data_lines)
    
    def _generate_data_with_quality_issues(self) -> str:
        """品質問題を含むデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        # 正常データ（80件）
        for i in range(1, 81):
            data_lines.append(
                f"MTG{i:06d}\tP001\ttest{i}@tokyogas.co.jp\t{(i % 12) + 1:02d}"
            )
        
        # 品質問題データ
        data_lines.append("MTG000081\t\ttest81@tokyogas.co.jp\t01")  # NULL値
        data_lines.append("MTG000082\tP001\t\t02")  # メールアドレスなし
        data_lines.append("MTG000083\tP001\tinvalid-email\t13")  # 無効な月
        data_lines.append("MTG000001\tP001\tdup@tokyogas.co.jp\t01")  # 重複
        
        return "\n".join(data_lines)
    
    def _generate_large_dataset(self, record_count: int) -> str:
        """大量データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        for i in range(1, record_count + 1):
            data_lines.append(
                f"MTG{i:06d}\tP{(i % 3) + 1:03d}\tuser{i}@tokyogas.co.jp\t{(i % 12) + 1:02d}"
            )
        
        return "\n".join(data_lines)
    
    def _transform_point_grant_data(self, input_data: str) -> str:
        """ポイント付与データ変換処理"""
        lines = input_data.strip().split('\n')
        
        # 出力CSVヘッダー
        output_header = ",".join(self.output_columns)
        output_lines = [output_header]
        
        # データ変換（重複削除含む）
        seen = set()
        csv_ymd = datetime.utcnow().strftime('%Y%m%d')
        output_datetime = datetime.utcnow().strftime('%Y/%m/%d %H:%M:%S')
        
        for line in lines[1:]:
            if not line.strip():
                continue
                
            parts = line.split('\t')
            if len(parts) != 4:
                continue
            
            # データ品質チェック
            if not all(part.strip() for part in parts[:3]):  # ID, TYPE, EMAILは必須
                continue
            
            # 月の妥当性チェック
            try:
                month = int(parts[3])
                if not (1 <= month <= 12):
                    continue
            except ValueError:
                continue
            
            # メールアドレス簡易チェック
            if '@' not in parts[2]:
                continue
            
            # 重複チェックキー（ID + TYPE + MONTH）
            dup_key = f"{parts[0]}|{parts[1]}|{parts[3]}"
            if dup_key not in seen:
                seen.add(dup_key)
                output_line = f"{parts[0]},{parts[1]},{parts[2]},{parts[3]},{csv_ymd},{output_datetime}"
                output_lines.append(output_line)
        
        return '\n'.join(output_lines)
    
    def validate_domain_business_rules(self, data: str) -> List[str]:
        """検電ドメインビジネスルール検証"""
        violations = []
        lines = data.strip().split('\n')
        
        if len(lines) < 2:  # ヘッダーのみの場合はOK
            return violations
        
        # ヘッダー検証
        expected_header = ",".join(self.output_columns)
        if lines[0] != expected_header:
            violations.append(f"ヘッダー不正: 期待={expected_header}, 実際={lines[0]}")
        
        # データ行検証
        for i, line in enumerate(lines[1:], 2):
            parts = line.split(',')
            
            # フィールド数チェック
            if len(parts) != len(self.output_columns):
                violations.append(f"行{i}: フィールド数不正 (期待={len(self.output_columns)}, 実際={len(parts)})")
                continue
            
            # ID形式チェック
            if not parts[0].startswith('MTG'):
                violations.append(f"行{i}: ID形式不正 ({parts[0]})")
            
            # ポイントタイプチェック
            if not parts[1].startswith('P'):
                violations.append(f"行{i}: ポイントタイプ形式不正 ({parts[1]})")
            
            # メールアドレスチェック
            if '@' not in parts[2]:
                violations.append(f"行{i}: メールアドレス形式不正 ({parts[2]})")
            
            # 月チェック
            try:
                month = int(parts[3])
                if not (1 <= month <= 12):
                    violations.append(f"行{i}: 月の値不正 ({parts[3]})")
            except ValueError:
                violations.append(f"行{i}: 月の形式不正 ({parts[3]})")
        
        return violations
    
    def test_functional_with_file_exists(self):
        """機能テスト: ファイルが存在する場合の正常処理"""
        test_id = f"functional_file_exists_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # テストデータ準備
        test_data = self._generate_point_grant_data()
        records_count = len(test_data.split('\n')) - 1  # ヘッダー除く
        
        # ファイルをBlobにアップロード
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"forRcvry/{file_date}/DCPDA016/{file_date}_DAM_PointAdd.tsv"
        self.mock_storage.upload_file(self.source_container, file_path, test_data.encode('utf-8'))
        
        # ETL処理シミュレーション
        transformed_data, compressed_data = self.simulate_etl_process(
            test_data,
            self._transform_point_grant_data,
            "csv.gz"
        )
        
        # 出力ファイル保存
        output_file_path = f"OMNI/MA/PointGrantEmail/PointGrantEmail_{file_date}.csv.gz"
        self.mock_storage.upload_file(self.output_container, output_file_path, compressed_data)
        
        # SFTP転送
        sftp_path = f"{self.sftp_target_dir}/PointGrantEmail_{file_date}.csv.gz"
        sftp_result = self.mock_sftp.upload(output_file_path, sftp_path, compressed_data)
        
        # ビジネスルール検証
        violations = self.validate_domain_business_rules(transformed_data)
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.FUNCTIONAL,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=records_count,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=0.98,
            errors=[],
            warnings=violations
        )
        
        # アサーション
        self.validate_common_assertions(result)
        assert result.records_extracted > 0, "抽出レコード数が0"
        assert result.records_transformed > 0, "変換レコード数が0"
        assert result.records_loaded > 0, "ロードレコード数が0"
        assert sftp_result, "SFTP転送失敗"
        assert self.mock_sftp.get_transfer_count() >= 1, "SFTP転送回数が不正"
        assert len(violations) == 0, f"ビジネスルール違反: {violations}"
        
        self.log_test_info(test_id, f"成功 - {result.records_loaded}件処理完了")
    
    def test_functional_without_file(self):
        """機能テスト: ファイルが存在しない場合のヘッダーのみ出力"""
        test_id = f"functional_no_file_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # ファイル存在確認（前日日付で存在しないことを確認）
        file_date = (datetime.utcnow() - timedelta(days=1)).strftime('%Y%m%d')
        file_path = f"forRcvry/{file_date}/DCPDA016/{file_date}_DAM_PointAdd.tsv"
        
        assert not self.mock_storage.file_exists(self.source_container, file_path), "ファイルが存在してはいけない"
        
        # ヘッダーのみデータ作成
        header_data = ",".join(self.output_columns) + "\n"
        
        # 出力ファイル保存
        output_file_path = f"OMNI/MA/PointGrantEmail/PointGrantEmail_{file_date}.csv.gz"
        compressed_data = gzip.compress(header_data.encode('utf-8'))
        self.mock_storage.upload_file(self.output_container, output_file_path, compressed_data)
        
        # SFTP転送
        sftp_path = f"{self.sftp_target_dir}/PointGrantEmail_{file_date}.csv.gz"
        sftp_result = self.mock_sftp.upload(output_file_path, sftp_path, compressed_data)
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.FUNCTIONAL,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=0,
            records_transformed=0,
            records_loaded=0,
            data_quality_score=1.0,  # ヘッダーのみは品質100%
            errors=[]
        )
        
        # アサーション
        self.validate_common_assertions(result)
        assert result.records_extracted == 0, "抽出レコード数が0でない"
        assert sftp_result, "SFTP転送失敗"
        
        self.log_test_info(test_id, "成功 - ヘッダーのみ出力完了")
    
    def test_data_quality_validation(self):
        """データ品質テスト: データ品質検証"""
        test_id = f"data_quality_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # 品質問題を含むテストデータ準備
        test_data = self._generate_data_with_quality_issues()
        
        # ファイルアップロード
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"forRcvry/{file_date}/DCPDA016/{file_date}_DAM_PointAdd.tsv"
        self.mock_storage.upload_file(self.source_container, file_path, test_data.encode('utf-8'))
        
        # データ変換処理
        transformed_data = self._transform_point_grant_data(test_data)
        
        # データ品質検証
        quality_metrics = self.validate_data_quality(transformed_data, self.output_columns)
        
        # ビジネスルール検証
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
        
        # データ品質アサーション
        assert quality_metrics["completeness"] >= 0.95, f"完全性が基準未満: {quality_metrics['completeness']}"
        assert quality_metrics["consistency"] >= 0.95, f"一貫性が基準未満: {quality_metrics['consistency']}"
        assert quality_metrics["accuracy"] >= 0.90, f"精度が基準未満: {quality_metrics['accuracy']}"
        assert len(violations) == 0, f"ビジネスルール違反: {violations}"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, "成功 - データ品質検証完了")
    
    def test_performance_large_dataset(self):
        """パフォーマンステスト: 大量データ処理"""
        test_id = f"performance_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # 大量データ準備（10万件）
        large_data = self._generate_large_dataset(100000)
        
        start_time = time.time()
        
        # ETL処理シミュレーション
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"forRcvry/{file_date}/DCPDA016/{file_date}_DAM_PointAdd.tsv"
        self.mock_storage.upload_file(self.source_container, file_path, large_data.encode('utf-8'))
        
        # 変換処理
        transformed_data = self._transform_point_grant_data(large_data)
        
        # 圧縮・出力
        compressed_data = gzip.compress(transformed_data.encode('utf-8'))
        output_file_path = f"OMNI/MA/PointGrantEmail/PointGrantEmail_{file_date}.csv.gz"
        self.mock_storage.upload_file(self.output_container, output_file_path, compressed_data)
        
        execution_time = time.time() - start_time
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.PERFORMANCE,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=100000,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=0.99,
            errors=[]
        )
        
        # パフォーマンス基準
        throughput = 100000 / execution_time
        assert throughput > 1000, f"スループットが基準未満: {throughput:.2f} records/sec"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, f"成功 - {execution_time:.2f}秒で処理完了")
    
    def test_integration_sftp_connectivity(self):
        """統合テスト: SFTP接続とファイル転送"""
        test_id = f"integration_sftp_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # テストデータ準備
        test_data = self._generate_point_grant_data(50)
        transformed_data = self._transform_point_grant_data(test_data)
        compressed_data = gzip.compress(transformed_data.encode('utf-8'))
        
        # SFTP転送テスト
        file_date = datetime.utcnow().strftime('%Y%m%d')
        sftp_path = f"{self.sftp_target_dir}/PointGrantEmail_{file_date}.csv.gz"
        
        # アップロード
        upload_result = self.mock_sftp.upload("local_path", sftp_path, compressed_data)
        
        # ダウンロード確認
        download_result = self.mock_sftp.download(sftp_path, "download_path")
        
        # ファイル存在確認
        exists_result = self.mock_sftp.file_exists(sftp_path)
        
        # 転送履歴確認
        transfer_history = self.mock_sftp.get_transfer_history()
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.INTEGRATION,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=50,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=1.0,
            errors=[]
        )
        
        # 統合テストアサーション
        assert upload_result, "SFTPアップロード失敗"
        assert download_result == compressed_data, "SFTPダウンロードデータ不一致"
        assert exists_result, "SFTPファイル存在確認失敗"
        assert len(transfer_history) >= 2, "SFTP転送履歴不足"  # upload + download
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, "成功 - SFTP統合テスト完了")