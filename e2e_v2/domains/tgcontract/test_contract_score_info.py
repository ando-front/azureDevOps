"""
pi_Send_karte_contract_score_info パイプラインのE2Eテスト

契約スコア情報送信パイプラインの包括的テスト
"""

import time
import json
from datetime import datetime, timedelta
from typing import Dict, List

from ...base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus


class TestContractScoreInfoPipeline(DomainTestBase):
    """契約スコア情報パイプラインテスト"""
    
    def __init__(self):
        super().__init__("pi_Send_karte_contract_score_info", "tgcontract")
    
    def domain_specific_setup(self):
        """TGContractドメイン固有セットアップ"""
        # 契約スコアドメイン用設定
        self.source_container = "mytokyogas"
        self.output_container = "datalake"
        self.sftp_target_dir = "/Import/TGContract/ScoreInfo"
        
        # 期待される入力フィールド
        self.input_columns = [
            "CONTRACT_ID", "CUSTOMER_ID", "CONTRACT_TYPE", "CONTRACT_START_DATE",
            "CONTRACT_END_DATE", "MONTHLY_USAGE", "MONTHLY_AMOUNT", "PAYMENT_METHOD",
            "PAYMENT_HISTORY", "SERVICE_USAGE", "COMPLAINT_COUNT", "SATISFACTION_SCORE"
        ]
        
        # 期待される出力フィールド
        self.output_columns = [
            "CONTRACT_ID", "CUSTOMER_ID", "CONTRACT_TYPE", "SCORE_CATEGORY",
            "CREDIT_SCORE", "LOYALTY_SCORE", "RISK_SCORE", "TOTAL_SCORE",
            "SCORE_GRADE", "RECOMMENDATION", "CALCULATED_DATE", "STATUS"
        ]
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """TGContractドメイン用テストデータテンプレート"""
        return {
            "contract_score_data": self._generate_contract_score_data(),
            "high_risk_data": self._generate_high_risk_data(),
            "premium_customer_data": self._generate_premium_customer_data()
        }
    
    def _generate_contract_score_data(self, record_count: int = 100) -> str:
        """契約スコアデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        contract_types = ["RESIDENTIAL", "BUSINESS", "CORPORATE"]
        payment_methods = ["CREDIT_CARD", "BANK_TRANSFER", "CONVENIENCE_STORE", "CASH"]
        
        for i in range(1, record_count + 1):
            # 契約期間（1-10年）
            contract_years = 1 + (i % 10)
            contract_start = (base_date - timedelta(days=contract_years*365)).strftime('%Y%m%d')
            contract_end = (base_date + timedelta(days=(10-contract_years)*365)).strftime('%Y%m%d')
            
            # 月間使用量・金額
            monthly_usage = 100 + (i % 400)  # 100-500
            monthly_amount = monthly_usage * 150  # 使用量×150円
            
            # 支払履歴（0-12ヶ月の延滞回数）
            payment_history = i % 13
            
            # サービス利用（0-10）
            service_usage = i % 11
            
            # 苦情回数（0-5）
            complaint_count = i % 6
            
            # 満足度スコア（1-10）
            satisfaction_score = 1 + (i % 10)
            
            contract_type = contract_types[i % len(contract_types)]
            payment_method = payment_methods[i % len(payment_methods)]
            
            data_lines.append(
                f"CONT{i:06d}\tCUST{i:06d}\t{contract_type}\t{contract_start}\t{contract_end}\t"
                f"{monthly_usage}\t{monthly_amount}\t{payment_method}\t{payment_history}\t"
                f"{service_usage}\t{complaint_count}\t{satisfaction_score}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_high_risk_data(self) -> str:
        """高リスク顧客データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        
        # 高リスク顧客（延滞多数、苦情多数、満足度低）
        high_risk_customers = [
            ("CONT900001", "CUST900001", "RESIDENTIAL", "20200101", "20251231", "80", "12000", "CASH", "8", "1", "4", "2"),
            ("CONT900002", "CUST900002", "BUSINESS", "20190101", "20241231", "200", "30000", "CONVENIENCE_STORE", "12", "0", "5", "3"),
            ("CONT900003", "CUST900003", "RESIDENTIAL", "20210101", "20261231", "50", "7500", "CASH", "6", "2", "3", "4"),
        ]
        
        for customer_data in high_risk_customers:
            data_lines.append("\t".join(customer_data))
        
        return "\n".join(data_lines)
    
    def _generate_premium_customer_data(self) -> str:
        """プレミアム顧客データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        # プレミアム顧客（延滞なし、高使用量、高満足度）
        premium_customers = [
            ("CONT100001", "CUST100001", "CORPORATE", "20150101", "20301231", "1000", "150000", "CREDIT_CARD", "0", "10", "0", "10"),
            ("CONT100002", "CUST100002", "BUSINESS", "20160101", "20291231", "800", "120000", "BANK_TRANSFER", "0", "9", "0", "9"),
            ("CONT100003", "CUST100003", "RESIDENTIAL", "20170101", "20281231", "500", "75000", "CREDIT_CARD", "0", "8", "0", "8"),
        ]
        
        for customer_data in premium_customers:
            data_lines.append("\t".join(customer_data))
        
        return "\n".join(data_lines)
    
    def _transform_contract_score_data(self, input_data: str) -> str:
        """契約スコアデータ変換処理"""
        lines = input_data.strip().split('\n')
        
        # 出力CSVヘッダー
        output_header = ",".join(self.output_columns)
        output_lines = [output_header]
        
        calculated_date = datetime.utcnow().strftime('%Y/%m/%d %H:%M:%S')
        
        for line in lines[1:]:
            if not line.strip():
                continue
                
            parts = line.split('\t')
            if len(parts) != len(self.input_columns):
                continue
            
            (contract_id, customer_id, contract_type, contract_start_date, contract_end_date,
             monthly_usage, monthly_amount, payment_method, payment_history,
             service_usage, complaint_count, satisfaction_score) = parts
            
            # データ品質チェック
            if not all([contract_id.strip(), customer_id.strip(), contract_type.strip()]):
                continue
            
            # 数値フィールド検証
            try:
                monthly_usage_val = int(monthly_usage)
                monthly_amount_val = int(monthly_amount)
                payment_history_val = int(payment_history)
                service_usage_val = int(service_usage)
                complaint_count_val = int(complaint_count)
                satisfaction_score_val = int(satisfaction_score)
                
                if any(val < 0 for val in [monthly_usage_val, monthly_amount_val, payment_history_val, 
                                          service_usage_val, complaint_count_val]):
                    continue
                    
            except ValueError:
                continue
            
            # スコア計算
            credit_score = self._calculate_credit_score(payment_history_val, payment_method)
            loyalty_score = self._calculate_loyalty_score(contract_start_date, service_usage_val, monthly_usage_val)
            risk_score = self._calculate_risk_score(payment_history_val, complaint_count_val, satisfaction_score_val)
            
            # 総合スコア
            total_score = (credit_score + loyalty_score + (100 - risk_score)) / 3
            
            # スコアカテゴリとグレード
            score_category = self._determine_score_category(contract_type, total_score)
            score_grade = self._determine_score_grade(total_score)
            
            # 推奨アクション
            recommendation = self._generate_recommendation(score_grade, risk_score, loyalty_score)
            
            # ステータス
            status = "CALCULATED"
            
            output_line = (
                f"{contract_id},{customer_id},{contract_type},{score_category},"
                f"{credit_score:.1f},{loyalty_score:.1f},{risk_score:.1f},{total_score:.1f},"
                f"{score_grade},{recommendation},{calculated_date},{status}"
            )
            output_lines.append(output_line)
        
        return '\n'.join(output_lines)
    
    def _calculate_credit_score(self, payment_history: int, payment_method: str) -> float:
        """信用スコア計算"""
        # 基本スコア（支払履歴ベース）
        base_score = max(0, 100 - (payment_history * 8))  # 延滞1回につき8点減点
        
        # 支払方法ボーナス
        method_bonus = {
            "CREDIT_CARD": 10,
            "BANK_TRANSFER": 5,
            "CONVENIENCE_STORE": 0,
            "CASH": -5
        }
        
        bonus = method_bonus.get(payment_method, 0)
        return min(100, base_score + bonus)
    
    def _calculate_loyalty_score(self, contract_start_date: str, service_usage: int, monthly_usage: int) -> float:
        """ロイヤルティスコア計算"""
        try:
            start_date = datetime.strptime(contract_start_date, '%Y%m%d')
            contract_years = (datetime.utcnow() - start_date).days / 365.25
            
            # 契約期間スコア（最大40点）
            tenure_score = min(40, contract_years * 4)
            
            # サービス利用スコア（最大30点）
            service_score = min(30, service_usage * 3)
            
            # 使用量スコア（最大30点）
            usage_score = min(30, (monthly_usage - 50) / 20)  # 50以上から加点
            
            return max(0, tenure_score + service_score + usage_score)
            
        except ValueError:
            return 0
    
    def _calculate_risk_score(self, payment_history: int, complaint_count: int, satisfaction_score: int) -> float:
        """リスクスコア計算（高いほどリスク大）"""
        # 支払履歴リスク（最大50点）
        payment_risk = min(50, payment_history * 4)
        
        # 苦情リスク（最大30点）
        complaint_risk = min(30, complaint_count * 6)
        
        # 満足度リスク（最大20点）
        satisfaction_risk = max(0, 20 - (satisfaction_score * 2))
        
        return payment_risk + complaint_risk + satisfaction_risk
    
    def _determine_score_category(self, contract_type: str, total_score: float) -> str:
        """スコアカテゴリ決定"""
        if contract_type == "CORPORATE":
            return "CORPORATE_TIER"
        elif contract_type == "BUSINESS":
            return "BUSINESS_TIER"
        else:
            if total_score >= 80:
                return "PREMIUM_RESIDENTIAL"
            elif total_score >= 60:
                return "STANDARD_RESIDENTIAL"
            else:
                return "BASIC_RESIDENTIAL"
    
    def _determine_score_grade(self, total_score: float) -> str:
        """スコアグレード決定"""
        if total_score >= 90:
            return "A+"
        elif total_score >= 80:
            return "A"
        elif total_score >= 70:
            return "B+"
        elif total_score >= 60:
            return "B"
        elif total_score >= 50:
            return "C+"
        elif total_score >= 40:
            return "C"
        else:
            return "D"
    
    def _generate_recommendation(self, score_grade: str, risk_score: float, loyalty_score: float) -> str:
        """推奨アクション生成"""
        if score_grade in ["A+", "A"]:
            return "PREMIUM_SERVICE_OFFER"
        elif score_grade in ["B+", "B"]:
            if loyalty_score < 50:
                return "LOYALTY_PROGRAM_INVITE"
            else:
                return "SERVICE_UPGRADE_OFFER"
        elif score_grade in ["C+", "C"]:
            if risk_score > 60:
                return "RISK_MONITORING"
            else:
                return "ENGAGEMENT_CAMPAIGN"
        else:
            return "RETENTION_PROGRAM"
    
    def validate_domain_business_rules(self, data: str) -> List[str]:
        """TGContractドメインビジネスルール検証"""
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
            
            # 契約ID形式チェック
            if not parts[0].startswith('CONT'):
                violations.append(f"行{i}: 契約ID形式不正 ({parts[0]})")
            
            # 顧客ID形式チェック
            if not parts[1].startswith('CUST'):
                violations.append(f"行{i}: 顧客ID形式不正 ({parts[1]})")
            
            # スコア値範囲チェック
            for idx, score_name in [(4, "信用スコア"), (5, "ロイヤルティスコア"), (6, "リスクスコア"), (7, "総合スコア")]:
                try:
                    score_value = float(parts[idx])
                    if not (0 <= score_value <= 100):
                        violations.append(f"行{i}: {score_name}が範囲外 ({parts[idx]})")
                except ValueError:
                    violations.append(f"行{i}: {score_name}形式不正 ({parts[idx]})")
            
            # スコアグレードチェック
            valid_grades = ["A+", "A", "B+", "B", "C+", "C", "D"]
            if parts[8] not in valid_grades:
                violations.append(f"行{i}: スコアグレード不正 ({parts[8]})")
            
            # 推奨アクションチェック
            valid_recommendations = ["PREMIUM_SERVICE_OFFER", "LOYALTY_PROGRAM_INVITE", 
                                   "SERVICE_UPGRADE_OFFER", "RISK_MONITORING", 
                                   "ENGAGEMENT_CAMPAIGN", "RETENTION_PROGRAM"]
            if parts[9] not in valid_recommendations:
                violations.append(f"行{i}: 推奨アクション不正 ({parts[9]})")
        
        return violations
    
    def test_functional_contract_score_calculation(self):
        """機能テスト: 契約スコア計算正常処理"""
        test_id = f"functional_score_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # テストデータ準備
        test_data = self._generate_contract_score_data()
        records_count = len(test_data.split('\n')) - 1
        
        # ファイルアップロード
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"TGContract/ScoreInfo/{file_date}/contract_score.tsv"
        self.mock_storage.upload_file(self.source_container, file_path, test_data.encode('utf-8'))
        
        # ETL処理シミュレーション
        transformed_data, output_data = self.simulate_etl_process(
            test_data,
            self._transform_contract_score_data,
            "csv"
        )
        
        # 出力ファイル保存
        output_file_path = f"TGContract/ScoreInfo/calculated_{file_date}.csv"
        self.mock_storage.upload_file(self.output_container, output_file_path, output_data)
        
        # SFTP転送
        sftp_path = f"{self.sftp_target_dir}/contract_scores_{file_date}.csv"
        sftp_result = self.mock_sftp.upload(output_file_path, sftp_path, output_data)
        
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
        assert sftp_result, "SFTP転送失敗"
        assert len(violations) == 0, f"ビジネスルール違反: {violations}"
        
        self.log_test_info(test_id, f"成功 - {result.records_loaded}件のスコア計算完了")
    
    def test_functional_risk_assessment(self):
        """機能テスト: リスク評価処理"""
        test_id = f"functional_risk_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # 高リスクデータ準備
        test_data = self._generate_high_risk_data()
        
        # データ変換処理
        transformed_data = self._transform_contract_score_data(test_data)
        
        # 高リスク顧客抽出
        lines = transformed_data.split('\n')
        high_risk_customers = []
        risk_monitoring_customers = []
        
        for line in lines[1:]:
            if line.strip():
                parts = line.split(',')
                if len(parts) >= 10:
                    risk_score = float(parts[6])  # リスクスコア
                    recommendation = parts[9]     # 推奨アクション
                    
                    if risk_score > 60:
                        high_risk_customers.append(parts)
                    
                    if recommendation == "RISK_MONITORING":
                        risk_monitoring_customers.append(parts)
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.FUNCTIONAL,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=len(test_data.split('\n')) - 1,
            records_transformed=len(lines) - 1,
            records_loaded=len(lines) - 1,
            data_quality_score=1.0,
            errors=[]
        )
        
        # リスク評価アサーション
        assert len(high_risk_customers) >= 2, f"高リスク顧客数不足: {len(high_risk_customers)}"
        assert len(risk_monitoring_customers) >= 1, f"リスク監視対象不足: {len(risk_monitoring_customers)}"
        
        # 全ての高リスク顧客がD グレードであることを確認
        for customer in high_risk_customers:
            score_grade = customer[8]
            assert score_grade in ["C", "D"], f"高リスク顧客のグレード不正: {score_grade}"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, f"成功 - {len(high_risk_customers)}件の高リスク顧客特定完了")
    
    def test_functional_premium_customer_identification(self):
        """機能テスト: プレミアム顧客特定"""
        test_id = f"functional_premium_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # プレミアムデータ準備
        test_data = self._generate_premium_customer_data()
        
        # データ変換処理
        transformed_data = self._transform_contract_score_data(test_data)
        
        # プレミアム顧客抽出
        lines = transformed_data.split('\n')
        premium_customers = []
        premium_offers = []
        
        for line in lines[1:]:
            if line.strip():
                parts = line.split(',')
                if len(parts) >= 10:
                    total_score = float(parts[7])  # 総合スコア
                    score_grade = parts[8]         # スコアグレード
                    recommendation = parts[9]      # 推奨アクション
                    
                    if total_score >= 80:
                        premium_customers.append(parts)
                    
                    if recommendation == "PREMIUM_SERVICE_OFFER":
                        premium_offers.append(parts)
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.FUNCTIONAL,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=len(test_data.split('\n')) - 1,
            records_transformed=len(lines) - 1,
            records_loaded=len(lines) - 1,
            data_quality_score=1.0,
            errors=[]
        )
        
        # プレミアム顧客アサーション
        assert len(premium_customers) >= 3, f"プレミアム顧客数不足: {len(premium_customers)}"
        assert len(premium_offers) >= 2, f"プレミアムオファー対象不足: {len(premium_offers)}"
        
        # 全てのプレミアム顧客がA グレード以上であることを確認
        for customer in premium_customers:
            score_grade = customer[8]
            assert score_grade in ["A+", "A"], f"プレミアム顧客のグレード不正: {score_grade}"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, f"成功 - {len(premium_customers)}件のプレミアム顧客特定完了")
    
    def test_data_quality_score_validation(self):
        """データ品質テスト: スコア値検証"""
        test_id = f"data_quality_score_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # 問題データ準備
        problematic_data = [
            "\t".join(self.input_columns),
            "CONT000001\tCUST000001\tRESIDENTIAL\t20200101\t20251231\t100\t15000\tCREDIT_CARD\t0\t5\t0\t8",  # 正常
            "CONT000002\tCUST000002\tBUSINESS\t20200101\t20251231\t-50\t-7500\tCASH\t-1\t3\t1\t6",  # 負の値
            "CONT000003\tCUST000003\tCORPORATE\t99999999\t20251231\tabc\txyz\tUNKNOWN\tdef\t2\t0\t9",  # 無効データ
            "CONT000004\tCUST000004\tRESIDENTIAL\t20200101\t20251231\t200\t30000\tBANK_TRANSFER\t15\t1\t6\t2",  # 極端値
        ]
        test_data = "\n".join(problematic_data)
        
        # データ変換処理
        transformed_data = self._transform_contract_score_data(test_data)
        
        # 品質検証
        quality_metrics = self.validate_data_quality(transformed_data, self.output_columns)
        violations = self.validate_domain_business_rules(transformed_data)
        
        # 有効レコード数確認（正常な1-2件）
        output_lines = transformed_data.split('\n')
        data_lines = [line for line in output_lines[1:] if line.strip()]
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.DATA_QUALITY,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=4,
            records_transformed=len(data_lines),
            records_loaded=len(data_lines),
            data_quality_score=min(quality_metrics.values()),
            errors=[],
            warnings=violations
        )
        
        # データ品質アサーション
        assert len(data_lines) >= 1, f"有効レコード数不足: {len(data_lines)}"
        assert quality_metrics["completeness"] >= 0.7, f"完全性が基準未満: {quality_metrics['completeness']}"
        assert len(violations) == 0, f"ビジネスルール違反: {violations}"
        
        # スコア値範囲確認
        for line in data_lines:
            parts = line.split(',')
            if len(parts) >= 8:
                for score_idx in [4, 5, 6, 7]:  # 各スコア値
                    score_val = float(parts[score_idx])
                    assert 0 <= score_val <= 100, f"スコア値範囲外: {score_val}"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, "成功 - スコア値データ品質検証完了")
    
    def test_performance_large_contract_portfolio(self):
        """パフォーマンステスト: 大量契約ポートフォリオ処理"""
        test_id = f"performance_portfolio_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # 大量データ準備（20000件）
        large_data = self._generate_contract_score_data(20000)
        
        start_time = time.time()
        
        # ETL処理シミュレーション
        transformed_data = self._transform_contract_score_data(large_data)
        
        execution_time = time.time() - start_time
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.PERFORMANCE,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=20000,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=0.99,
            errors=[]
        )
        
        # パフォーマンス基準
        throughput = 20000 / execution_time
        assert throughput > 1500, f"スループットが基準未満: {throughput:.2f} records/sec"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, f"成功 - {execution_time:.2f}秒で処理完了")
    
    def test_integration_score_based_actions(self):
        """統合テスト: スコアベースアクション連携"""
        test_id = f"integration_actions_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # 混合データ準備
        mixed_data = (
            self._generate_premium_customer_data() + "\n" +
            self._generate_high_risk_data().split('\n', 1)[1]  # ヘッダー除去
        )
        
        # ETL処理
        transformed_data = self._transform_contract_score_data(mixed_data)
        
        # アクション別集計
        lines = transformed_data.split('\n')
        action_summary = {}
        grade_distribution = {}
        
        for line in lines[1:]:
            if line.strip():
                parts = line.split(',')
                if len(parts) >= 10:
                    score_grade = parts[8]         # スコアグレード
                    recommendation = parts[9]      # 推奨アクション
                    
                    action_summary[recommendation] = action_summary.get(recommendation, 0) + 1
                    grade_distribution[score_grade] = grade_distribution.get(score_grade, 0) + 1
        
        # アクション実行シミュレーション
        executed_actions = []
        for action, count in action_summary.items():
            executed_actions.append({
                "action_type": action,
                "target_count": count,
                "execution_date": datetime.utcnow().isoformat(),
                "status": "SCHEDULED"
            })
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.INTEGRATION,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=len(mixed_data.split('\n')) - 1,
            records_transformed=len(lines) - 1,
            records_loaded=len(lines) - 1,
            data_quality_score=1.0,
            errors=[]
        )
        
        # 統合テストアサーション
        assert "PREMIUM_SERVICE_OFFER" in action_summary, "プレミアムサービス提案なし"
        assert "RISK_MONITORING" in action_summary, "リスク監視アクションなし"
        
        assert len(executed_actions) >= 3, f"実行アクション数不足: {len(executed_actions)}"
        
        # グレード分布確認
        assert "A+" in grade_distribution or "A" in grade_distribution, "高グレード顧客なし"
        assert "C" in grade_distribution or "D" in grade_distribution, "低グレード顧客なし"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, f"成功 - {len(executed_actions)}種類のアクション連携完了")