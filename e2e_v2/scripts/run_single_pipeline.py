#!/usr/bin/env python3
"""
単一パイプラインE2Eテスト実行スクリプト

指定された単一パイプラインのE2Eテストを実行します。
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
    parser = argparse.ArgumentParser(description='単一パイプラインE2Eテスト実行')
    parser.add_argument('domain', help='ドメイン名')
    parser.add_argument('pipeline', help='パイプライン名')
    parser.add_argument('--test', '-t', help='実行する特定のテストメソッド名（省略時は全テスト実行）')
    parser.add_argument('--verbose', '-v', action='store_true', help='詳細ログ出力')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        logger.info(f"単一パイプラインテスト開始: {args.domain}/{args.pipeline}")
        
        # テスト実行
        runner = TestRunner()
        
        # 指定されたパイプラインのテストクラス取得
        if args.domain not in runner.pipeline_tests:
            print(f"エラー: ドメイン '{args.domain}' が見つかりません")
            print(f"利用可能なドメイン: {list(runner.pipeline_tests.keys())}")
            return 1
        
        if args.pipeline not in runner.pipeline_tests[args.domain]:
            print(f"エラー: パイプライン '{args.pipeline}' がドメイン '{args.domain}' に見つかりません")
            print(f"利用可能なパイプライン: {list(runner.pipeline_tests[args.domain].keys())}")
            return 1
        
        test_class = runner.pipeline_tests[args.domain][args.pipeline]
        
        # 特定のテストのみ実行する場合
        if args.test:
            try:
                test_instance = test_class()
                
                if not hasattr(test_instance, f"test_{args.test}"):
                    print(f"エラー: テストメソッド 'test_{args.test}' が見つかりません")
                    # 利用可能なテストメソッド表示
                    available_methods = [method for method in dir(test_instance) if method.startswith('test_')]
                    print(f"利用可能なテストメソッド: {available_methods}")
                    return 1
                
                # 単一テスト実行
                logger.info(f"テスト実行: {args.test}")
                start_time = time.time()
                
                test_method = getattr(test_instance, f"test_{args.test}")
                test_method()
                
                execution_time = time.time() - start_time
                
                print(f"\n{'='*60}")
                print(f"単一テスト結果: {args.domain}/{args.pipeline}")
                print(f"{'='*60}")
                print(f"テストメソッド: {args.test}")
                print(f"実行時間: {execution_time:.2f}秒")
                print(f"結果: ✓ 成功")
                
                return 0
                
            except Exception as e:
                execution_time = time.time() - start_time if 'start_time' in locals() else 0
                
                print(f"\n{'='*60}")
                print(f"単一テスト結果: {args.domain}/{args.pipeline}")
                print(f"{'='*60}")
                print(f"テストメソッド: {args.test}")
                print(f"実行時間: {execution_time:.2f}秒")
                print(f"結果: ✗ 失敗")
                print(f"エラー: {str(e)}")
                
                return 1
        
        # 全テスト実行
        else:
            report = runner.run_pipeline_test(args.domain, args.pipeline, test_class)
            
            # 結果表示
            print(f"\n{'='*60}")
            print(f"パイプラインテスト結果: {args.domain}/{args.pipeline}")
            print(f"{'='*60}")
            print(f"総テスト数: {report.total_tests}")
            print(f"成功テスト数: {report.passed_tests}")
            print(f"失敗テスト数: {report.failed_tests}")
            print(f"成功率: {report.success_rate:.1f}%")
            print(f"実行時間: {report.execution_time:.2f}秒")
            print()
            
            # テスト詳細
            print("テスト詳細:")
            for test_result in report.test_results:
                status_icon = "✓" if test_result["status"] == "PASSED" else "✗"
                print(f"  {status_icon} {test_result['description']} ({test_result['duration']:.2f}秒)")
                
                if test_result["error"] and args.verbose:
                    print(f"     エラー: {test_result['error']}")
            
            if report.errors:
                print("\nエラーサマリ:")
                for error in report.errors:
                    print(f"  - {error}")
            
            # 終了コード決定
            exit_code = 0 if report.failed_tests == 0 else 1
            
            print(f"\nパイプラインテスト完了: 終了コード={exit_code}")
            return exit_code
        
    except Exception as e:
        logger.error(f"パイプラインテスト実行中にエラーが発生しました: {str(e)}")
        print(f"パイプラインテスト失敗: {str(e)}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)