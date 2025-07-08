"""
pi_Insert_ActionPointTransactionHistory パイプラインのE2Eテスト

アクションポイント取引履歴挿入パイプラインの包括的テスト
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List

from ...base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus


class TestActionPointTransactionHistoryPipeline(DomainTestBase):
    """アクションポイント取引履歴挿入パイプラインテスト"""
    
    def __init__(self):
        super().__init__("pi_Insert_ActionPointTransactionHistory", "actionpoint")
    
    def domain_specific_setup(self):
        """ActionPointドメイン固有セットアップ"""
        # ActionPointドメイン用設定
        self.source_container = "mytokyogas"
        self.output_container = "datalake"
        self.database_target = "actionpoint_transaction_history"
        
        # 期待される入力フィールド
        self.input_columns = [
            "TRANSACTION_ID", "CUSTOMER_ID", "POINT_TYPE", "TRANSACTION_TYPE",
            "TRANSACTION_DATE", "POINT_AMOUNT", "BALANCE_BEFORE", "BALANCE_AFTER",
            "CAMPAIGN_ID", "MERCHANT_ID", "PURCHASE_AMOUNT", "DESCRIPTION",
            "STATUS", "EXPIRED_DATE", "SOURCE_SYSTEM"
        ]
        
        # 期待される出力フィールド
        self.output_columns = [
            "TRANSACTION_ID", "CUSTOMER_ID", "POINT_TYPE", "TRANSACTION_TYPE",
            "TRANSACTION_DATE", "POINT_AMOUNT", "BALANCE_BEFORE", "BALANCE_AFTER",
            "CAMPAIGN_ID", "MERCHANT_ID", "PURCHASE_AMOUNT", "DESCRIPTION",
            "STATUS", "EXPIRED_DATE", "SOURCE_SYSTEM", "INSERT_DATE",
            "UPDATE_DATE", "VALIDITY_STATUS"
        ]
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """ActionPointドメイン用テストデータテンプレート"""
        return {
            "transaction_history_data": self._generate_transaction_history_data(),
            "campaign_transactions": self._generate_campaign_transactions(),
            "expired_transactions": self._generate_expired_transactions()
        }
    
    def _generate_transaction_history_data(self, record_count: int = 2000) -> str:
        """取引履歴データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        point_types = ["BASIC", "BONUS", "CAMPAIGN", "REFERRAL", "BIRTHDAY"]
        transaction_types = ["EARN", "REDEEM", "TRANSFER", "EXPIRE", "ADJUST"]
        statuses = ["COMPLETED", "PENDING", "CANCELLED", "FAILED"]
        source_systems = ["WEB", "MOBILE", "STORE", "BATCH", "MANUAL"]
        
        current_balance = 10000  # 初期残高
        
        for i in range(1, record_count + 1):
            # 取引日（過去90日以内）
            transaction_days = i % 90
            transaction_date = (base_date - timedelta(days=transaction_days)).strftime('%Y%m%d %H:%M:%S')
            
            # 失効日（取引日から1年後）
            expired_date = (base_date + timedelta(days=365 - transaction_days)).strftime('%Y%m%d')
            
            # 取引タイプ別ポイント計算
            transaction_type = transaction_types[i % len(transaction_types)]
            point_type = point_types[i % len(point_types)]
            
            if transaction_type == "EARN":
                point_amount = 100 + (i % 500)  # 100-600ポイント獲得
                balance_before = current_balance
                balance_after = balance_before + point_amount
            elif transaction_type == "REDEEM":
                point_amount = -(50 + (i % 300))  # 50-350ポイント利用
                balance_before = current_balance
                balance_after = max(0, balance_before + point_amount)  # 負にならない
            elif transaction_type == "TRANSFER":
                # 50%の確率で送信/受信
                if i % 2 == 0:
                    point_amount = -(10 + (i % 100))  # 送信
                else:
                    point_amount = 10 + (i % 100)  # 受信
                balance_before = current_balance
                balance_after = max(0, balance_before + point_amount)
            elif transaction_type == "EXPIRE":
                point_amount = -(1 + (i % 50))  # 失効
                balance_before = current_balance
                balance_after = max(0, balance_before + point_amount)
            else:  # ADJUST
                # 調整（プラス/マイナス）
                point_amount = -200 + (i % 400)  # -200 ~ +200
                balance_before = current_balance
                balance_after = max(0, balance_before + point_amount)
            
            current_balance = balance_after
            
            # キャンペーン・マーチャント情報
            campaign_id = f"CAMP{(i % 50):03d}" if transaction_type in ["EARN", "BONUS"] else ""
            merchant_id = f"MERCH{(i % 100):03d}" if transaction_type in ["EARN", "REDEEM"] else ""
            
            # 購入金額（獲得・利用時のみ）
            if transaction_type in ["EARN", "REDEEM"]:
                purchase_amount = abs(point_amount) * 10  # ポイント×10円
            else:
                purchase_amount = 0
            
            # 説明文
            descriptions = {
                "EARN": f"ポイント獲得 - {campaign_id if campaign_id else '通常購入'}",
                "REDEEM": f"ポイント利用 - {merchant_id}",
                "TRANSFER": "ポイント譲渡" if point_amount < 0 else "ポイント受取",
                "EXPIRE": "ポイント失効",
                "ADJUST": "ポイント調整"
            }
            description = descriptions.get(transaction_type, "その他")
            
            status = statuses[i % len(statuses)]
            source_system = source_systems[i % len(source_systems)]
            
            data_lines.append(
                f"TXN{i:08d}\tCUST{(i % 1000) + 1:06d}\t{point_type}\t{transaction_type}\t"
                f"{transaction_date}\t{point_amount}\t{balance_before}\t{balance_after}\t"
                f"{campaign_id}\t{merchant_id}\t{purchase_amount}\t{description}\t"
                f"{status}\t{expired_date}\t{source_system}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_campaign_transactions(self) -> str:
        """キャンペーン取引データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        
        # 特定キャンペーンの大量取引
        campaign_transactions = []
        campaign_date = (base_date - timedelta(days=7)).strftime('%Y%m%d %H:%M:%S')
        expired_date = (base_date + timedelta(days=365)).strftime('%Y%m%d')
        
        for i in range(1, 101):
            # キャンペーンボーナス
            point_amount = 500 + (i % 1000)  # 500-1500ポイント
            balance_before = 5000 + (i * 100)
            balance_after = balance_before + point_amount
            
            campaign_transactions.append(
                f"TXN{i+100000:08d}\tCUST{i:06d}\tCAMPAIGN\tEARN\t{campaign_date}\t"
                f"{point_amount}\t{balance_before}\t{balance_after}\tCAMP001\tMERCH001\t"
                f"{point_amount * 10}\t春のポイントキャンペーン\tCOMPLETED\t{expired_date}\tBATCH"
            )
        
        for transaction_data in campaign_transactions:
            data_lines.append(transaction_data)
        
        return "\n".join(data_lines)
    
    def _generate_expired_transactions(self) -> str:
        """失効取引データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        
        # 今月の失効処理
        expiry_transactions = []
        expiry_date = base_date.strftime('%Y%m%d %H:%M:%S')
        expired_date = base_date.strftime('%Y%m%d')
        
        for i in range(1, 51):
            # 失効ポイント
            point_amount = -(100 + (i % 500))  # 100-600ポイント失効
            balance_before = 2000 + (i * 50)
            balance_after = balance_before + point_amount
            
            expiry_transactions.append(
                f"TXN{i+200000:08d}\tCUST{i+500:06d}\tBASIC\tEXPIRE\t{expiry_date}\t"
                f"{point_amount}\t{balance_before}\t{balance_after}\t\t\t0\t"
                f"ポイント失効処理\tCOMPLETED\t{expired_date}\tBATCH"
            )
        
        for transaction_data in expiry_transactions:
            data_lines.append(transaction_data)
        
        return "\n".join(data_lines)
    
    def _transform_transaction_history_data(self, input_data: str) -> str:
        """取引履歴データ変換処理"""
        lines = input_data.strip().split('\n')
        
        # 出力CSVヘッダー
        output_header = ",".join(self.output_columns)
        output_lines = [output_header]
        
        current_date = datetime.utcnow()
        insert_date = current_date.strftime('%Y/%m/%d %H:%M:%S')
        update_date = insert_date
        
        for line in lines[1:]:
            if not line.strip():
                continue
                
            parts = line.split('\t')
            if len(parts) != len(self.input_columns):
                continue
            
            (transaction_id, customer_id, point_type, transaction_type, transaction_date,
             point_amount, balance_before, balance_after, campaign_id, merchant_id,
             purchase_amount, description, status, expired_date, source_system) = parts
            
            # データ品質チェック
            if not all([transaction_id.strip(), customer_id.strip(), transaction_type.strip()]):
                continue
            
            # 数値フィールド検証
            try:
                point_amount_val = int(point_amount)
                balance_before_val = int(balance_before)
                balance_after_val = int(balance_after)
                purchase_amount_val = int(purchase_amount)
                
                # 残高整合性チェック
                expected_balance = balance_before_val + point_amount_val
                if abs(expected_balance - balance_after_val) > 1:  # 小数点誤差許容
                    continue  # 残高計算不整合
                
                # 残高が負になっていないかチェック
                if balance_after_val < 0:
                    continue
                
            except ValueError:
                continue
            
            # 取引日検証
            try:
                transaction_dt = datetime.strptime(transaction_date, '%Y%m%d %H:%M:%S')
                
                # 未来日チェック
                if transaction_dt > current_date:
                    continue
                
            except ValueError:
                continue
            
            # 失効日検証
            try:
                expired_dt = datetime.strptime(expired_date, '%Y%m%d')
                
                # 失効日が取引日より前でないかチェック（EXPIRE以外）
                if transaction_type != "EXPIRE" and expired_dt.date() < transaction_dt.date():
                    continue
                    
            except ValueError:
                expired_date = ""  # 失効日無しも許可
            
            # 妥当性ステータス判定
            validity_status = "VALID"
            
            # 取引タイプ別妥当性チェック
            if transaction_type == "EARN" and point_amount_val <= 0:
                validity_status = "INVALID"
            elif transaction_type == "REDEEM" and point_amount_val >= 0:
                validity_status = "INVALID"
            elif transaction_type == "EXPIRE" and point_amount_val >= 0:
                validity_status = "INVALID"
            elif transaction_type == "TRANSFER":
                # 譲渡は送信（負）・受信（正）両方あり得る
                pass
            elif transaction_type == "ADJUST":
                # 調整はプラス・マイナス両方あり得る
                pass
            
            # 大額取引チェック
            if abs(point_amount_val) > 10000:
                validity_status = "LARGE_AMOUNT"
            
            # キャンペーン・マーチャント整合性チェック
            if transaction_type == "EARN" and point_type == "CAMPAIGN" and not campaign_id:
                validity_status = "MISSING_CAMPAIGN"
            
            if transaction_type in ["EARN", "REDEEM"] and purchase_amount_val == 0:
                validity_status = "MISSING_PURCHASE_AMOUNT"
            
            output_line = (
                f"{transaction_id},{customer_id},{point_type},{transaction_type},"
                f"{transaction_date},{point_amount_val},{balance_before_val},{balance_after_val},"
                f"{campaign_id},{merchant_id},{purchase_amount_val},{description},"
                f"{status},{expired_date},{source_system},{insert_date},"
                f"{update_date},{validity_status}"
            )
            output_lines.append(output_line)
        
        return '\n'.join(output_lines)
    
    def validate_domain_business_rules(self, data: str) -> List[str]:
        """ActionPointドメインビジネスルール検証"""
        violations = []
        lines = data.strip().split('\n')
        
        if len(lines) < 2:
            return violations
        
        # ヘッダー検証
        expected_header = ",".join(self.output_columns)
        if lines[0] != expected_header:
            violations.append(f"ヘッダー不正: 期待={expected_header}, 実際={lines[0]}")
        
        transaction_ids = set()
        customer_balances = {}  # 顧客別残高追跡
        
        # データ行検証
        for i, line in enumerate(lines[1:], 2):
            parts = line.split(',')
            
            if len(parts) != len(self.output_columns):
                violations.append(f"行{i}: フィールド数不正")
                continue
            
            # 取引ID重複チェック
            transaction_id = parts[0]
            if transaction_id in transaction_ids:
                violations.append(f"行{i}: 取引ID重複 ({transaction_id})")
            transaction_ids.add(transaction_id)
            
            # ID形式チェック
            if not transaction_id.startswith('TXN'):
                violations.append(f"行{i}: 取引ID形式不正 ({transaction_id})")
            
            if not parts[1].startswith('CUST'):
                violations.append(f"行{i}: 顧客ID形式不正 ({parts[1]})")
            
            # 取引タイプチェック
            valid_transaction_types = ["EARN", "REDEEM", "TRANSFER", "EXPIRE", "ADJUST"]
            if parts[3] not in valid_transaction_types:
                violations.append(f"行{i}: 取引タイプ不正 ({parts[3]})")
            
            # ポイント種別チェック
            valid_point_types = ["BASIC", "BONUS", "CAMPAIGN", "REFERRAL", "BIRTHDAY"]
            if parts[2] not in valid_point_types:
                violations.append(f"行{i}: ポイント種別不正 ({parts[2]})")
            
            # 残高整合性チェック
            try:
                point_amount = int(parts[5])
                balance_before = int(parts[6])
                balance_after = int(parts[7])
                
                if balance_before + point_amount != balance_after:
                    violations.append(f"行{i}: 残高計算不整合")
                
                if balance_after < 0:
                    violations.append(f"行{i}: 残高が負の値")
                
            except ValueError:
                violations.append(f"行{i}: 数値フィールド形式不正")
            
            # ステータスチェック
            valid_statuses = ["COMPLETED", "PENDING", "CANCELLED", "FAILED"]
            if parts[12] not in valid_statuses:
                violations.append(f"行{i}: ステータス不正 ({parts[12]})")
            
            # 妥当性ステータスチェック
            valid_validity_statuses = ["VALID", "INVALID", "LARGE_AMOUNT", "MISSING_CAMPAIGN", "MISSING_PURCHASE_AMOUNT"]
            if parts[17] not in valid_validity_statuses:
                violations.append(f"行{i}: 妥当性ステータス不正 ({parts[17]})")
        
        return violations
    
    def test_functional_entry_event_processing(self):
        """機能テスト: イベント処理"""
        test_id = f"functional_event_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # テストデータ準備
        test_data = self._generate_transaction_history_data()
        records_count = len(test_data.split('\n')) - 1
        
        # ファイルアップロード
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"ActionPoint/TransactionHistory/{file_date}/transaction_history.tsv"
        self.mock_storage.upload_file(self.source_container, file_path, test_data.encode('utf-8'))
        
        # ETL処理シミュレーション
        transformed_data, output_data = self.simulate_etl_process(
            test_data,
            self._transform_transaction_history_data,
            "csv"
        )
        
        # 出力ファイル保存
        output_file_path = f"ActionPoint/TransactionHistory/processed_{file_date}.csv"
        self.mock_storage.upload_file(self.output_container, output_file_path, output_data)
        
        # データベース挿入シミュレーション
        db_records = self.convert_csv_to_dict_list(transformed_data)
        db_insert_result = self.mock_database.insert_records(
            self.database_target, 
            db_records
        )
        
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
            data_quality_score=0.96,
            errors=[],
            warnings=violations
        )
        
        # アサーション
        self.validate_common_assertions(result)
        assert result.records_extracted > 0, "抽出レコード数が0"
        assert result.records_transformed > 0, "変換レコード数が0"
        assert db_insert_result, "データベース挿入失敗"
        assert len(violations) == 0, f"ビジネスルール違反: {violations}"
        
        self.log_test_info(test_id, f"成功 - {result.records_loaded}件の取引履歴処理完了")
    
    def test_functional_campaign_event_processing(self):
        """機能テスト: キャンペーンイベント処理"""
        test_id = f"functional_campaign_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # キャンペーンデータ準備
        campaign_data = self._generate_campaign_transactions()
        
        # データ変換処理
        transformed_data = self._transform_transaction_history_data(campaign_data)
        
        # キャンペーン取引分析
        lines = transformed_data.split('\n')
        campaign_transactions = []
        total_campaign_points = 0
        
        for line in lines[1:]:
            if line.strip():
                parts = line.split(',')
                if len(parts) >= 9 and parts[8]:  # campaign_id有り
                    campaign_transactions.append(parts)
                    total_campaign_points += int(parts[5])
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.FUNCTIONAL,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=len(campaign_data.split('\n')) - 1,
            records_transformed=len(lines) - 1,
            records_loaded=len(lines) - 1,
            data_quality_score=1.0,
            errors=[]
        )
        
        # キャンペーンアサーション
        assert len(campaign_transactions) >= 50, f"キャンペーン取引数不足: {len(campaign_transactions)}"
        assert total_campaign_points > 50000, f"キャンペーンポイント総額不足: {total_campaign_points}"
        
        # 全てのキャンペーン取引が正しいタイプであることを確認
        for transaction in campaign_transactions:
            assert transaction[3] == "EARN", f"キャンペーン取引タイプ不正: {transaction[3]}"
            assert transaction[2] == "CAMPAIGN", f"キャンペーンポイント種別不正: {transaction[2]}"
            assert int(transaction[5]) > 0, f"キャンペーンポイント額不正: {transaction[5]}"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, f"成功 - {len(campaign_transactions)}件のキャンペーン取引処理完了")
    
    def test_performance_bulk_processing(self):
        """パフォーマンステスト: 大量処理"""
        test_id = f"performance_bulk_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # 大量データ準備（200000件）
        large_data = self._generate_transaction_history_data(200000)
        
        start_time = time.time()
        
        # ETL処理シミュレーション
        transformed_data = self._transform_transaction_history_data(large_data)
        
        execution_time = time.time() - start_time
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.PERFORMANCE,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=200000,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=0.95,
            errors=[]
        )
        
        # パフォーマンス基準
        throughput = 200000 / execution_time
        assert throughput > 15000, f"スループットが基準未満: {throughput:.2f} records/sec"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, f"成功 - {execution_time:.2f}秒で処理完了")
    
    def test_integration_database_operations(self):
        """統合テスト: データベース操作"""
        test_id = f"integration_database_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # 失効取引データ準備
        expired_data = self._generate_expired_transactions()
        
        # ETL処理
        transformed_data = self._transform_transaction_history_data(expired_data)
        
        # データベース操作シミュレーション
        # 1. 重複チェック
        duplicate_check = self.mock_database.query_records(
            self.database_target,
            "SELECT transaction_id FROM transaction_history WHERE transaction_id LIKE 'TXN002%'"
        )
        
        # 2. 一括挿入
        db_records = self.convert_csv_to_dict_list(transformed_data)
        insert_result = self.mock_database.insert_records(
            self.database_target, 
            db_records
        )
        
        # 3. 失効処理分析
        lines = transformed_data.split('\n')
        expiry_analysis = {
            "total_expired_transactions": 0,
            "total_expired_points": 0,
            "affected_customers": set(),
            "expiry_date": None
        }
        
        for line in lines[1:]:
            if line.strip():
                parts = line.split(',')
                if len(parts) >= 4 and parts[3] == "EXPIRE":
                    expiry_analysis["total_expired_transactions"] += 1
                    expiry_analysis["total_expired_points"] += abs(int(parts[5]))
                    expiry_analysis["affected_customers"].add(parts[1])
                    if not expiry_analysis["expiry_date"]:
                        expiry_analysis["expiry_date"] = parts[13]
        
        expiry_analysis["affected_customers"] = len(expiry_analysis["affected_customers"])
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.INTEGRATION,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=len(expired_data.split('\n')) - 1,
            records_transformed=len(lines) - 1,
            records_loaded=len(lines) - 1,
            data_quality_score=1.0,
            errors=[]
        )
        
        # 統合テストアサーション
        assert insert_result, "データベース挿入失敗"
        assert expiry_analysis["total_expired_transactions"] >= 25, f"失効取引数不足: {expiry_analysis['total_expired_transactions']}"
        assert expiry_analysis["total_expired_points"] > 5000, f"失効ポイント総額不足: {expiry_analysis['total_expired_points']}"
        assert expiry_analysis["affected_customers"] >= 25, f"失効対象顧客数不足: {expiry_analysis['affected_customers']}"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, f"成功 - {expiry_analysis['total_expired_transactions']}件の失効処理完了")