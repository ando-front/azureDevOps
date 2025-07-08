#!/usr/bin/env python3
"""
ドメイン別E2Eテスト実行スクリプト

指定されたドメインのパイプラインE2Eテストを実行します。
"""

import sys
import argparse
import time
from datetime import datetime
import logging

# パッケージパス設定
sys.path.insert(0, '/home/ando')

from e2e_v2.scripts.run_all_tests import TestRunner

# ロギング設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    """メイン実行"""
    parser = argparse.ArgumentParser(description='ドメイン別E2Eテスト実行')
    parser.add_argument('domain', 
                       choices=['kendenki', 'smc', 'actionpoint', 'marketing', 'tgcontract', 'infrastructure', 'mtgmaster'],
                       help='実行するドメイン名')
    parser.add_argument('--verbose', '-v', action='store_true', help='詳細ログ出力')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        logger.info(f"ドメインテスト開始: {args.domain}")
        
        # テスト実行
        runner = TestRunner()
        domain_reports = runner.run_domain_tests(args.domain)
        
        if not domain_reports:
            logger.warning(f"ドメイン '{args.domain}' にはまだ実装されたテストがありません")
            return 0
        
        # 結果サマリ
        total_tests = sum(report.total_tests for report in domain_reports)
        passed_tests = sum(report.passed_tests for report in domain_reports)
        failed_tests = sum(report.failed_tests for report in domain_reports)
        total_execution_time = sum(report.execution_time for report in domain_reports)
        
        # 結果表示
        print(f"\n{'='*60}")
        print(f"ドメインテスト結果: {args.domain.upper()}")
        print(f"{'='*60}")
        print(f"対象パイプライン数: {len(domain_reports)}")
        print(f"総テスト数: {total_tests}")
        print(f"成功テスト数: {passed_tests}")
        print(f"失敗テスト数: {failed_tests}")
        print(f"成功率: {(passed_tests/total_tests*100):.1f}%" if total_tests > 0 else "N/A")
        print(f"実行時間: {total_execution_time:.2f}秒")
        print()
        
        # パイプライン別詳細
        for report in domain_reports:
            status_icon = "✓" if report.failed_tests == 0 else "✗"
            print(f"{status_icon} {report.pipeline_name}: {report.success_rate:.1f}% ({report.passed_tests}/{report.total_tests})")
            
            if report.errors and args.verbose:
                for error in report.errors:
                    print(f"   エラー: {error}")
        
        # 終了コード決定
        exit_code = 0 if failed_tests == 0 else 1
        
        print(f"\nドメインテスト完了: 終了コード={exit_code}")
        return exit_code
        
    except Exception as e:
        logger.error(f"ドメインテスト実行中にエラーが発生しました: {str(e)}")
        print(f"ドメインテスト失敗: {str(e)}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)