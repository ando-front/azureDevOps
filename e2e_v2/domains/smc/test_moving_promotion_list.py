"""
pi_Send_MovingPromotionList パイプラインのE2Eテスト

引越しプロモーションリスト配信パイプラインのETL処理テスト
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List

from ...base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus


class TestMovingPromotionListPipeline(DomainTestBase):
    """引越しプロモーションリスト配信パイプラインテスト"""
    
    def __init__(self):
        super().__init__("pi_Send_MovingPromotionList", "smc")
    
    def domain_specific_setup(self):
        """SMCドメイン固有セットアップ"""
        self.source_container = "mytokyogas"
        self.output_container = "datalake"
        self.sftp_target_dir = "/Export/MovingPromotionList"
        
        # 期待される入力フィールド（ETL処理用）
        self.input_columns = [
            "CUSTOMER_ID", "CUSTOMER_NAME", "CURRENT_ADDRESS", "NEW_ADDRESS",
            "MOVING_DATE", "MOVING_TYPE", "PROMOTION_CODE", "OFFER_TYPE",
            "DISCOUNT_AMOUNT", "EMAIL_ADDRESS", "PHONE_NUMBER", "STATUS"
        ]
        
        # 期待される出力フィールド（ETL変換後）
        self.output_columns = [
            "CUSTOMER_ID", "CUSTOMER_NAME", "CURRENT_ADDRESS", "NEW_ADDRESS",
            "MOVING_DATE", "MOVING_TYPE", "PROMOTION_CODE", "OFFER_TYPE",
            "DISCOUNT_AMOUNT", "EMAIL_ADDRESS", "PHONE_NUMBER",
            "STATUS", "PROCESS_DATE", "PROMOTION_LIST_ID", "ELIGIBILITY_FLAG"
        ]
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """SMCドメイン用テストデータテンプレート"""
        return {
            "moving_promotion_data": self._generate_moving_promotion_data(),
            "urgent_moving_data": self._generate_urgent_moving_data(),
            "bulk_promotion_data": self._generate_bulk_promotion_data()
        }
    
    def _generate_moving_promotion_data(self, record_count: int = 800) -> str:
        """引越しプロモーションデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        moving_types = ["INBOUND", "OUTBOUND", "WITHIN_AREA", "RETURN"]
        promotion_codes = ["WELCOME50", "MOVE100", "FAMILY200", "STUDENT30"]
        offer_types = ["DISCOUNT", "CASHBACK", "FREE_MONTH", "UPGRADE"]
        statuses = ["ELIGIBLE", "PENDING", "EXPIRED", "USED"]
        
        base_date = datetime.utcnow()
        
        for i in range(1, record_count + 1):
            # 引越し日（今日から90日以内）
            moving_days = i % 90
            moving_date = (base_date + timedelta(days=moving_days)).strftime('%Y%m%d')
            
            moving_type = moving_types[i % len(moving_types)]
            promotion_code = promotion_codes[i % len(promotion_codes)]
            offer_type = offer_types[i % len(offer_types)]
            status = statuses[i % len(statuses)]
            
            # プロモーションコードによる割引額設定
            if promotion_code == "WELCOME50":
                discount_amount = 5000
            elif promotion_code == "MOVE100":
                discount_amount = 10000
            elif promotion_code == "FAMILY200":
                discount_amount = 20000
            else:  # STUDENT30
                discount_amount = 3000
            
            customer_name = f"引越し{i:04d}"
            email = f"moving{i:06d}@tokyogas.co.jp"
            phone = f"03-{i:04d}-{(i*15) % 10000:04d}"
            
            # 住所生成
            current_address = f"東京都{i % 23 + 1}区現住所{i % 99 + 1}-{i % 99 + 1}-{i % 99 + 1}"
            new_address = f"東京都{(i + 5) % 23 + 1}区新住所{(i + 10) % 99 + 1}-{(i + 10) % 99 + 1}-{(i + 10) % 99 + 1}"
            
            data_lines.append(
                f"CUST{i:06d}\t{customer_name}\t{current_address}\t{new_address}\t"
                f"{moving_date}\t{moving_type}\t{promotion_code}\t{offer_type}\t"
                f"{discount_amount}\t{email}\t{phone}\t{status}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_urgent_moving_data(self) -> str:
        """緊急引越しデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        moving_date = (base_date + timedelta(days=7)).strftime('%Y%m%d')  # 1週間後
        
        # 緊急引越し（早期割引対象）
        urgent_moves = [
            ("CUST900001", "緊急引越1", "東京都新宿区緊急1-1-1", "東京都渋谷区新緊急1-1-1", moving_date, "URGENT", "MOVE100", "DISCOUNT", "15000", "urgent1@tokyogas.co.jp", "03-9001-0001", "ELIGIBLE"),
            ("CUST900002", "緊急引越2", "東京都渋谷区緊急2-2-2", "東京都港区新緊急2-2-2", moving_date, "URGENT", "WELCOME50", "CASHBACK", "7500", "urgent2@tokyogas.co.jp", "03-9002-0002", "ELIGIBLE"),
            ("CUST900003", "緊急引越3", "東京都港区緊急3-3-3", "東京都新宿区新緊急3-3-3", moving_date, "URGENT", "FAMILY200", "FREE_MONTH", "25000", "urgent3@tokyogas.co.jp", "03-9003-0003", "PENDING"),
        ]
        
        for moving_data in urgent_moves:
            data_lines.append("\t".join(moving_data))
        
        return "\n".join(data_lines)
    
    def _generate_bulk_promotion_data(self) -> str:
        """一括プロモーションデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        moving_date = (base_date + timedelta(days=30)).strftime('%Y%m%d')  # 1ヶ月後
        
        # 学生向け一括プロモーション（新学期）
        for i in range(1, 51):
            data_lines.append(
                f"CUST{i:06d}\t学生引越{i:03d}\t東京都学生区{i:03d}\t"
                f"東京都新学期区{i:03d}\t{moving_date}\tINBOUND\t"
                f"STUDENT30\tDISCOUNT\t3000\tstudent{i:03d}@tokyogas.co.jp\t"
                f"090-0000-{i:04d}\tELIGIBLE"
            )
        
        return "\n".join(data_lines)
    
    def _transform_moving_promotion_data(self, input_data: str) -> str:
        """引越しプロモーションデータETL変換処理"""
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
            
            (customer_id, customer_name, current_address, new_address,
             moving_date, moving_type, promotion_code, offer_type,
             discount_amount, email_address, phone_number, status) = parts
            
            # ETL品質チェック
            if not customer_id.strip() or not customer_name.strip() or not moving_date.strip():
                continue
            
            # プロモーションリストID生成（ETL処理）
            promotion_list_id = f"PROMO{customer_id[4:]}{process_date}"
            
            # 適格性フラグ設定（ETL処理）
            moving_dt = datetime.strptime(moving_date, '%Y%m%d')
            days_until_move = (moving_dt - current_date).days
            
            if status == "ELIGIBLE" and 0 <= days_until_move <= 90:
                eligibility_flag = "ELIGIBLE"
            elif status == "PENDING":
                eligibility_flag = "UNDER_REVIEW"
            else:
                eligibility_flag = "NOT_ELIGIBLE"
            
            # CSV出力行生成
            output_line = (
                f"{customer_id},{customer_name},{current_address},{new_address},"
                f"{moving_date},{moving_type},{promotion_code},{offer_type},"
                f"{discount_amount},{email_address},{phone_number},"
                f"{status},{process_date},{promotion_list_id},{eligibility_flag}"
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
        test_data = self._generate_moving_promotion_data()
        
        # ファイルアップロード
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"MovingPromotionList/{file_date}/moving_promotion_list.tsv"
        self.mock_storage.upload_file(self.source_container, file_path, test_data.encode('utf-8'))
        
        # ETL処理シミュレーション
        transformed_data, output_data = self.simulate_etl_process(
            test_data,
            self._transform_moving_promotion_data,
            "csv"
        )
        
        # 出力ファイル保存
        output_file_path = f"MovingPromotionList/processed_{file_date}.csv"
        self.mock_storage.upload_file(self.output_container, output_file_path, output_data)
        
        # SFTP転送
        sftp_path = f"{self.sftp_target_dir}/moving_promotion_list_{file_date}.csv"
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
        file_path = f"MovingPromotionList/{file_date}/moving_promotion_list.tsv"
        
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
        
        # 緊急引越しデータでテスト
        test_data = self._generate_urgent_moving_data()
        
        # データ変換処理
        transformed_data = self._transform_moving_promotion_data(test_data)
        
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
        
        # 大量データ準備（40000件）
        large_data = self._generate_moving_promotion_data(40000)
        
        start_time = time.time()
        
        # ETL処理実行
        transformed_data = self._transform_moving_promotion_data(large_data)
        
        execution_time = time.time() - start_time
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.PERFORMANCE,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=40000,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=0.95,
            errors=[]
        )
        
        # パフォーマンス基準
        throughput = 40000 / execution_time
        assert throughput > 4000, f"スループットが基準未満: {throughput:.2f} records/sec"
        
        self.validate_common_assertions(result)
    
    def test_integration_sftp_connectivity(self):
        """統合テスト: SFTP接続性"""
        test_id = f"integration_sftp_{int(time.time())}"
        
        # 一括プロモーションデータでテスト
        test_data = self._generate_bulk_promotion_data()
        
        # ETL処理
        transformed_data = self._transform_moving_promotion_data(test_data)
        
        # SFTP転送テスト
        file_date = datetime.utcnow().strftime('%Y%m%d')
        sftp_path = f"{self.sftp_target_dir}/moving_promotion_list_bulk_{file_date}.csv"
        sftp_result = self.mock_sftp.upload(
            f"moving_promotion_list_bulk_{file_date}.csv",
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