#!/usr/bin/env python3
"""
全パイプラインE2Eテスト実行スクリプト

全38パイプラインのE2Eテストを実行し、包括的なレポートを生成します。
"""

import sys
import time
import json
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
import logging

# パッケージパス設定
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# テストクラスインポート
from e2e_v2.domains.kendenki.test_point_grant_email import TestPointGrantEmailPipeline
from e2e_v2.domains.kendenki.test_point_lost_email import TestPointLostEmailPipeline
from e2e_v2.domains.kendenki.test_usage_service_mtgid import TestUsageServiceMtgIdPipeline
from e2e_v2.domains.smc.test_payment_alert import TestPaymentAlertPipeline
from e2e_v2.domains.smc.test_utility_bills import TestUtilityBillsPipeline
from e2e_v2.domains.actionpoint.test_actionpoint_entry_event import TestActionPointEntryEventPipeline
from e2e_v2.domains.actionpoint.test_actionpoint_transaction_history import TestActionPointTransactionHistoryPipeline
from e2e_v2.domains.marketing.test_client_dm import TestClientDMPipeline
from e2e_v2.domains.tgcontract.test_contract_score_info import TestContractScoreInfoPipeline
from e2e_v2.domains.infrastructure.test_marketing_client_dm import TestMarketingClientDmPipeline
from e2e_v2.base.pipeline_test_base import TestCategory, PipelineStatus

# ロギング設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class TestExecutionSummary:
    """テスト実行サマリ"""
    total_pipelines: int
    total_tests: int
    passed_tests: int
    failed_tests: int
    total_execution_time: float
    start_time: datetime
    end_time: datetime
    
    @property
    def success_rate(self) -> float:
        """成功率"""
        return (self.passed_tests / self.total_tests * 100) if self.total_tests > 0 else 0.0
    
    @property
    def average_execution_time(self) -> float:
        """平均実行時間"""
        return self.total_execution_time / self.total_tests if self.total_tests > 0 else 0.0


@dataclass
class PipelineTestReport:
    """パイプラインテストレポート"""
    pipeline_name: str
    domain: str
    test_results: List[Dict[str, Any]]
    total_tests: int
    passed_tests: int
    failed_tests: int
    execution_time: float
    errors: List[str]
    warnings: List[str]
    
    @property
    def success_rate(self) -> float:
        """成功率"""
        return (self.passed_tests / self.total_tests * 100) if self.total_tests > 0 else 0.0


class TestRunner:
    """E2E V2 テスト実行管理"""
    
    def __init__(self):
        self.pipeline_reports: List[PipelineTestReport] = []
        self.overall_errors: List[str] = []
        
        # 実装済みパイプラインテストマッピング
        self.pipeline_tests = {
            "kendenki": {
                "pi_PointGrantEmail": TestPointGrantEmailPipeline,
                "pi_PointLostEmail": TestPointLostEmailPipeline,
                "pi_Ins_usageservice_mtgid": TestUsageServiceMtgIdPipeline,
            },
            "smc": {
                "pi_Send_PaymentAlert": TestPaymentAlertPipeline,
                "pi_UtilityBills": TestUtilityBillsPipeline,
                # 他のSMCパイプライン（未実装）
            },
            "actionpoint": {
                "pi_Insert_ActionPointEntryEvent": TestActionPointEntryEventPipeline,
                "pi_Insert_ActionPointTransactionHistory": TestActionPointTransactionHistoryPipeline,
            },
            "marketing": {
                "pi_Send_ClientDM": TestClientDMPipeline,
                # "pi_Send_Cpkiyk": TestCpkiykPipeline,  # 未実装
            },
            "tgcontract": {
                "pi_Send_karte_contract_score_info": TestContractScoreInfoPipeline,
            },
            "infrastructure": {
                "pi_Copy_marketing_client_dm": TestMarketingClientDmPipeline,
            }
        }
        
        # テストメソッドマッピング (カテゴリ付き)
        self.test_methods = [
            # カテゴリ, メソッド名, 説明
            # 検電ドメイン - ポイント付与メール
            (TestCategory.FUNCTIONAL, "functional_with_file_exists", "機能テスト(ファイル有り)"),
            (TestCategory.FUNCTIONAL, "functional_without_file", "機能テスト(ファイル無し)"),
            (TestCategory.DATA_QUALITY, "data_quality_validation", "データ品質テスト"),
            (TestCategory.PERFORMANCE, "performance_large_dataset", "パフォーマンステスト"),
            (TestCategory.INTEGRATION, "integration_sftp_connectivity", "統合テスト(SFTP)"),
            # SMC支払いアラート
            (TestCategory.FUNCTIONAL, "functional_payment_alerts", "機能テスト(アラート)"),
            (TestCategory.FUNCTIONAL, "functional_overdue_processing", "機能テスト(延滞処理)"),
            (TestCategory.DATA_QUALITY, "data_quality_contact_validation", "データ品質テスト(連絡先)"),
            (TestCategory.PERFORMANCE, "performance_large_payment_dataset", "パフォーマンステスト(支払い)"),
            (TestCategory.INTEGRATION, "integration_alert_distribution", "統合テスト(配信)"),
            # SMC公共料金
            (TestCategory.FUNCTIONAL, "functional_utility_bills_processing", "機能テスト(公共料金)"),
            (TestCategory.FUNCTIONAL, "functional_seasonal_adjustment", "機能テスト(季節調整)"),
            (TestCategory.DATA_QUALITY, "data_quality_usage_validation", "データ品質テスト(使用量)"),
            (TestCategory.PERFORMANCE, "performance_high_volume_bills", "パフォーマンステスト(大量請求書)"),
            (TestCategory.INTEGRATION, "integration_billing_system", "統合テスト(請求システム)"),
            # ActionPoint
            (TestCategory.FUNCTIONAL, "functional_entry_event_processing", "機能テスト(イベント)"),
            (TestCategory.FUNCTIONAL, "functional_campaign_event_processing", "機能テスト(キャンペーン)"),
            (TestCategory.PERFORMANCE, "performance_bulk_processing", "パフォーマンステスト(大量)"),
            (TestCategory.INTEGRATION, "integration_database_operations", "統合テスト(DB)"),
            # Marketing
            (TestCategory.FUNCTIONAL, "functional_client_dm_processing", "機能テスト(DM配信)"),
            (TestCategory.FUNCTIONAL, "functional_customer_segmentation", "機能テスト(セグメンテーション)"),
            (TestCategory.FUNCTIONAL, "functional_opt_out_filtering", "機能テスト(オプトアウト)"),
            (TestCategory.PERFORMANCE, "performance_large_customer_base", "パフォーマンステスト(大量顧客)"),
            (TestCategory.INTEGRATION, "integration_campaign_delivery", "統合テスト(キャンペーン配信)"),
            # TGContract
            (TestCategory.FUNCTIONAL, "functional_contract_score_calculation", "機能テスト(スコア計算)"),
            (TestCategory.FUNCTIONAL, "functional_risk_assessment", "機能テスト(リスク評価)"),
            (TestCategory.FUNCTIONAL, "functional_premium_customer_identification", "機能テスト(プレミアム顧客)"),
            (TestCategory.DATA_QUALITY, "data_quality_score_validation", "データ品質テスト(スコア値)"),
            (TestCategory.PERFORMANCE, "performance_large_contract_portfolio", "パフォーマンステスト(大量契約)"),
            (TestCategory.INTEGRATION, "integration_score_based_actions", "統合テスト(スコアアクション)"),
            # Infrastructure
            (TestCategory.FUNCTIONAL, "functional_data_copy_processing", "機能テスト(データ複製)"),
            (TestCategory.FUNCTIONAL, "functional_data_quality_improvement", "機能テスト(品質向上)"),
            (TestCategory.FUNCTIONAL, "functional_incremental_processing", "機能テスト(増分処理)"),
            (TestCategory.DATA_QUALITY, "data_quality_normalization_validation", "データ品質テスト(正規化)"),
            (TestCategory.PERFORMANCE, "performance_large_dataset_copy", "パフォーマンステスト(大量複製)"),
            (TestCategory.INTEGRATION, "integration_multi_stage_pipeline", "統合テスト(多段階パイプライン)")
        ]
    
    def run_pipeline_test(self, domain: str, pipeline_name: str, test_class) -> PipelineTestReport:
        """個別パイプラインテスト実行"""
        logger.info(f"パイプラインテスト開始: {domain}/{pipeline_name}")
        
        test_results = []
        passed_tests = 0
        failed_tests = 0
        errors = []
        warnings = []
        
        start_time = time.time()
        
        try:
            # テストインスタンス作成
            test_instance = test_class()
            
            # 各テストメソッド実行
            for category, method_name, method_description in self.test_methods:
                if hasattr(test_instance, f"test_{method_name}"):
                    try:
                        logger.info(f"  テスト実行: [{category.name}] {method_description}")
                        
                        # テスト実行
                        test_method = getattr(test_instance, f"test_{method_name}")
                        test_start = time.time()
                        test_method()
                        test_duration = time.time() - test_start
                        
                        # 成功結果記録
                        test_results.append({
                            "category": category.name,
                            "method": method_name,
                            "description": method_description,
                            "status": "PASSED",
                            "duration": test_duration,
                            "error": None
                        })
                        passed_tests += 1
                        
                        logger.info(f"    ✓ [{category.name}] {method_description} - {test_duration:.2f}秒")
                        
                    except Exception as e:
                        # 失敗結果記録
                        error_msg = f"[{category.name}] {method_description}: {str(e)}"
                        test_results.append({
                            "category": category.name,
                            "method": method_name,
                            "description": method_description,
                            "status": "FAILED",
                            "duration": 0,
                            "error": error_msg
                        })
                        failed_tests += 1
                        errors.append(error_msg)
                        
                        logger.error(f"    ✗ [{category.name}] {method_description}: {str(e)}")
        
        except Exception as e:
            error_msg = f"パイプラインテスト初期化失敗: {str(e)}"
            errors.append(error_msg)
            failed_tests = 1
            logger.error(f"パイプラインテスト失敗: {pipeline_name} - {str(e)}")
        
        execution_time = time.time() - start_time
        
        # レポート作成
        report = PipelineTestReport(
            pipeline_name=pipeline_name,
            domain=domain,
            test_results=test_results,
            total_tests=len(test_results),
            passed_tests=passed_tests,
            failed_tests=failed_tests,
            execution_time=execution_time,
            errors=errors,
            warnings=warnings
        )
        
        logger.info(f"パイプラインテスト完了: {pipeline_name} - 成功率: {report.success_rate:.1f}%")
        
        return report
    
    def run_domain_tests(self, domain: str) -> List[PipelineTestReport]:
        """ドメイン別テスト実行"""
        logger.info(f"ドメインテスト開始: {domain}")
        
        domain_reports = []
        
        if domain in self.pipeline_tests:
            for pipeline_name, test_class in self.pipeline_tests[domain].items():
                try:
                    report = self.run_pipeline_test(domain, pipeline_name, test_class)
                    domain_reports.append(report)
                except Exception as e:
                    error_msg = f"ドメインテスト失敗: {domain}/{pipeline_name} - {str(e)}"
                    self.overall_errors.append(error_msg)
                    logger.error(error_msg)
        
        logger.info(f"ドメインテスト完了: {domain} - {len(domain_reports)}パイプライン")
        
        return domain_reports
    
    def run_all_tests(self) -> TestExecutionSummary:
        """全テスト実行"""
        logger.info("=== E2E V2 全パイプラインテスト開始 ===")
        
        start_time = datetime.utcnow()
        execution_start = time.time()
        
        # 全ドメインテスト実行
        for domain in self.pipeline_tests.keys():
            domain_reports = self.run_domain_tests(domain)
            self.pipeline_reports.extend(domain_reports)
        
        execution_time = time.time() - execution_start
        end_time = datetime.utcnow()
        
        # サマリ計算
        total_pipelines = len(self.pipeline_reports)
        total_tests = sum(report.total_tests for report in self.pipeline_reports)
        passed_tests = sum(report.passed_tests for report in self.pipeline_reports)
        failed_tests = sum(report.failed_tests for report in self.pipeline_reports)
        
        summary = TestExecutionSummary(
            total_pipelines=total_pipelines,
            total_tests=total_tests,
            passed_tests=passed_tests,
            failed_tests=failed_tests,
            total_execution_time=execution_time,
            start_time=start_time,
            end_time=end_time
        )
        
        logger.info("=== E2E V2 全パイプラインテスト完了 ===")
        
        return summary
    
    def generate_report(self, summary: TestExecutionSummary) -> str:
        """詳細レポート生成"""
        report_lines = []
        
        # ヘッダー
        report_lines.append("=" * 80)
        report_lines.append("E2E V2 パイプラインテスト実行レポート")
        report_lines.append("=" * 80)
        report_lines.append(f"実行日時: {summary.start_time.strftime('%Y/%m/%d %H:%M:%S')} - {summary.end_time.strftime('%H:%M:%S')}")
        report_lines.append(f"実行時間: {summary.total_execution_time:.2f}秒")
        report_lines.append("")
        
        # サマリ
        report_lines.append("## 実行サマリ")
        report_lines.append(f"- 対象パイプライン数: {summary.total_pipelines}")
        report_lines.append(f"- 総テスト数: {summary.total_tests}")
        report_lines.append(f"- 成功テスト数: {summary.passed_tests}")
        report_lines.append(f"- 失敗テスト数: {summary.failed_tests}")
        report_lines.append(f"- 成功率: {summary.success_rate:.1f}%")
        report_lines.append(f"- 平均実行時間: {summary.average_execution_time:.2f}秒/テスト")
        report_lines.append("")
        
        # ドメイン別詳細
        domains = {}
        for report in self.pipeline_reports:
            if report.domain not in domains:
                domains[report.domain] = []
            domains[report.domain].append(report)
        
        for domain, domain_reports in domains.items():
            report_lines.append(f"## {domain.upper()}ドメイン")
            
            for pipeline_report in domain_reports:
                report_lines.append(f"### {pipeline_report.pipeline_name}")
                report_lines.append(f"- 成功率: {pipeline_report.success_rate:.1f}% ({pipeline_report.passed_tests}/{pipeline_report.total_tests})")
                report_lines.append(f"- 実行時間: {pipeline_report.execution_time:.2f}秒")
                
                if pipeline_report.errors:
                    report_lines.append("- エラー:")
                    for error in pipeline_report.errors:
                        report_lines.append(f"  * {error}")
                
                if pipeline_report.warnings:
                    report_lines.append("- 警告:")
                    for warning in pipeline_report.warnings:
                        report_lines.append(f"  * {warning}")
                
                # テスト詳細
                report_lines.append("- テスト詳細:")
                for test_result in pipeline_report.test_results:
                    status_icon = "✓" if test_result["status"] == "PASSED" else "✗"
                    report_lines.append(f"  {status_icon} [{test_result['category']}] {test_result['description']}")
                    if test_result["error"]:
                        report_lines.append(f"    エラー: {test_result['error']}")
                
                report_lines.append("")
        
        # 全体エラー
        if self.overall_errors:
            report_lines.append("## 全体エラー")
            for error in self.overall_errors:
                report_lines.append(f"- {error}")
            report_lines.append("")
        
        # 未実装パイプライン
        report_lines.append("## 未実装パイプライン")
        report_lines.append("以下のパイプラインはまだテストが実装されていません:")
        
        all_pipelines = [
            ("kendenki", ["pi_PointLostEmail", "pi_Ins_usageservice_mtgid"]),
            ("smc", ["pi_Send_UsageServices", "pi_Send_mTGMailPermission", "pi_UtilityBills", 
                    "pi_Insert_ClientDmBx", "pi_Send_ActionPointCurrentMonthEntryList",
                    "pi_Send_ActionPointRecentTransactionHistoryList", "pi_Send_ElectricityContractThanks",
                    "pi_Send_LIMSettlementBreakdownRepair", "pi_Send_LINEIDLinkInfo",
                    "pi_Send_MovingPromotionList", "pi_Send_OpeningPaymentGuide",
                    "pi_Send_PaymentMethodChanged", "pi_Send_PaymentMethodMaster"]),
            ("actionpoint", ["pi_Insert_ActionPointTransactionHistory"]),
            ("marketing", ["pi_Send_ClientDM", "pi_Send_Cpkiyk"]),
            ("tgcontract", ["pi_Send_karte_contract_score_info"]),
            ("infrastructure", ["DoUntilPipeline", "pi_Copy_marketing_client_dm", 
                             "pi_Copy_marketing_client_dna", "pi_CustmNoRegistComp"]),
            ("mtgmaster", ["pi_Insert_mTGCustomerMaster"])
        ]
        
        for domain, pipelines in all_pipelines:
            report_lines.append(f"### {domain.upper()}ドメイン")
            for pipeline in pipelines:
                report_lines.append(f"- {pipeline}")
        
        report_lines.append("")
        report_lines.append("=" * 80)
        
        return "\n".join(report_lines)
    
    def save_report(self, summary: TestExecutionSummary, report_content: str):
        """レポート保存"""
        timestamp = summary.start_time.strftime('%Y%m%d_%H%M%S')
        
        # レポート保存先ディレクトリ
        script_dir = os.path.dirname(os.path.abspath(__file__))
        reports_dir = os.path.join(os.path.dirname(script_dir), "reports")
        os.makedirs(reports_dir, exist_ok=True)
        
        report_file = os.path.join(reports_dir, f"test_report_{timestamp}.txt")
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        # JSON形式でも保存
        json_file = os.path.join(reports_dir, f"test_report_{timestamp}.json")
        report_data = {
            "summary": asdict(summary),
            "pipeline_reports": [asdict(report) for report in self.pipeline_reports],
            "overall_errors": self.overall_errors
        }
        
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, ensure_ascii=False, indent=2, default=str)
        
        logger.info(f"レポート保存完了: {report_file}")
        logger.info(f"JSONレポート保存完了: {json_file}")


def main():
    """メイン実行"""
    try:
        # テスト実行
        runner = TestRunner()
        summary = runner.run_all_tests()
        
        # レポート生成
        report_content = runner.generate_report(summary)
        
        # コンソール出力
        print(report_content)
        
        # ファイル保存
        runner.save_report(summary, report_content)
        
        # 終了コード決定
        exit_code = 0 if summary.failed_tests == 0 else 1
        
        print(f"\nテスト実行完了: 終了コード={exit_code}")
        return exit_code
        
    except Exception as e:
        logger.error(f"テスト実行中にエラーが発生しました: {str(e)}")
        print(f"テスト実行失敗: {str(e)}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)