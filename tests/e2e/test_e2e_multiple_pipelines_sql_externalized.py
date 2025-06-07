"""
複数パイプライン SQL外部化 統合E2Eテストスイート
対象パイプライン: 
- Point Grant Email (6列)
- Point Lost Email (4列) 
- Payment Method Master (7列)
- LINE ID Link Info (7列)
- Moving Promotion List (4列)
作成日: 2024-01-16
"""

import pytest
import logging
import requests
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
import sys
import os

# プロジェクトルートをsys.pathに追加
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection
from tests.e2e.helpers.sql_query_manager import E2ESQLQueryManager

logger = logging.getLogger(__name__)

class TestE2EMultiplePipelinesSQLExternalized:

    @classmethod
    def setup_class(cls):
        """Disable proxy settings for tests"""
        # Store and clear proxy environment variables
        for var in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY']:
            if var in os.environ:
                del os.environ[var]

    def _get_no_proxy_session(self):
        """Get a requests session with proxy disabled"""
        session = requests.Session()
        session.proxies = {'http': None, 'https': None}
        return session
    """複数パイプライン SQL外部化統合E2Eテスト"""
    
    # ===========================
    # パイプライン構造定義辞書
    # ===========================
    
    PIPELINE_CONFIGS = {
        'point_grant_email': {
            'sql_file': 'point_grant_email_main.sql',
            'expected_columns': ['ID_NO', 'PNT_TYPE_CD', 'MAIL_ADR', 'PICTURE_MM', 'CSV_YMD', 'OUTPUT_DATETIME'],
            'column_count': 6,
            'external_table': 'omni_ods_mytginfo_trn_point_add_ext_temp',
            'dedup_complexity': 'complex',  # ID_NO + PNT_TYPE_CD + PICTURE_MM
            'parameters': ['DYNAMIC_LOCATION', 'DYNAMIC_CSV_YMD']
        },
        'point_lost_email': {
            'sql_file': 'point_lost_email_main.sql',
            'expected_columns': ['ID_NO', 'MAIL_ADR', 'CSV_YMD', 'OUTPUT_DATETIME'],
            'column_count': 4,
            'external_table': 'omni_ods_mytginfo_trn_point_delete_ext_temp',
            'dedup_complexity': 'simple',   # ID_NO のみ
            'parameters': ['DYNAMIC_LOCATION', 'DYNAMIC_CSV_YMD']
        },
        'payment_method_master': {
            'sql_file': 'payment_method_master_main.sql',
            'expected_columns': ['Bx', 'INDEX_ID', 'SIH_KIY_NO', 'SHIK_SIH_KIY_JTKB', 'SIH_HUHU_SHBT', 'SIK_SVC_SHBT', 'OUTPUT_DATETIME'],
            'column_count': 7,
            'external_table': None,  # 外部テーブルなし
            'dedup_complexity': 'medium',   # Bx + SIH_KIY_NO
            'parameters': []
        },
        'line_id_link_info': {
            'sql_file': 'line_id_link_info_main.sql',
            'expected_columns': ['ID_NO', 'LINE_U_ID', 'IDCS_U_ID', 'LINE_RNK_DTTM', 'KJ_FLG', 'LINE_RNK_KJ_DTTM', 'OUTPUT_DATETIME'],
            'column_count': 7,
            'external_table': None,  # 外部テーブルなし
            'dedup_complexity': 'none',     # 重複削除なし
            'parameters': []
        },
        'moving_promotion_list': {
            'sql_file': 'moving_promotion_list_main.sql',
            'expected_columns': ['GASMETER_INSTALLATION_LOCATION_NUMBER_1X', 'USAGE_CONTRACT_NUMBER_4X', 'MOVING_FORECAST_WITHIN_TWO_MONTHS', 'OUTPUT_DATETIME'],
            'column_count': 4,
            'external_table': None,  # 外部テーブルなし            'dedup_complexity': 'distinct', # DISTINCT使用
            'parameters': []
        }
    }
    
    @pytest.fixture
    def synapse_helper(self):
        """Synapse E2E テストヘルパー初期化"""
        return SynapseE2EConnection()

    @pytest.fixture(params=list(PIPELINE_CONFIGS.keys()))
    def pipeline_config(self, request):
        """パラメータ化されたパイプライン設定"""
        pipeline_name = request.param
        config = self.PIPELINE_CONFIGS[pipeline_name].copy()
        config['name'] = pipeline_name
        return config

    def test_all_external_sql_files_accessibility(self):
        """全外部SQLファイルのアクセス可能性テスト"""
        logger.info("=== 全パイプライン外部SQLファイルアクセステスト開始 ===")
        
        accessible_files = []
        inaccessible_files = []
        
        for pipeline_name, config in self.PIPELINE_CONFIGS.items():
            try:
                sql_manager = E2ESQLQueryManager(config['sql_file'])
                
                # ファイルの存在確認
                assert sql_manager.file_exists(), f"SQLファイルが見つかりません: {config['sql_file']}"
                
                # SQLコンテンツの読み込み確認
                sql_content = sql_manager.get_sql_content()
                assert sql_content is not None, f"SQLコンテンツが読み込めませんでした: {config['sql_file']}"
                assert len(sql_content.strip()) > 0, f"SQLコンテンツが空です: {config['sql_file']}"
                
                accessible_files.append(pipeline_name)
                logger.info(f"✓ {pipeline_name}: {config['sql_file']} - アクセス可能")
                
            except Exception as e:
                inaccessible_files.append((pipeline_name, str(e)))
                logger.error(f"✗ {pipeline_name}: {config['sql_file']} - エラー: {e}")
        
        # 結果サマリー
        logger.info(f"アクセス可能ファイル: {len(accessible_files)}/{len(self.PIPELINE_CONFIGS)}")
        logger.info(f"アクセス不可ファイル: {len(inaccessible_files)}")
        
        # 全ファイルがアクセス可能であることを確認
        assert len(inaccessible_files) == 0, f"アクセス不可ファイル: {inaccessible_files}"
        
        logger.info("全パイプライン外部SQLファイルアクセステスト完了")

    def test_pipeline_column_structure_validation(self, pipeline_config):
        """パイプライン別列構造検証テスト"""
        logger.info(f"=== {pipeline_config['name']} 列構造検証開始 ===")
        
        sql_manager = E2ESQLQueryManager(pipeline_config['sql_file'])
        sql_content = sql_manager.get_sql_content()
        
        # 期待される列の存在確認
        for column in pipeline_config['expected_columns']:
            assert column in sql_content, f"期待される出力列が見つかりません: {column}"
        
        # 外部テーブル関連の確認
        if pipeline_config['external_table']:
            assert pipeline_config['external_table'] in sql_content, \
                f"期待される外部テーブルが見つかりません: {pipeline_config['external_table']}"
            assert "CREATE EXTERNAL TABLE" in sql_content, "外部テーブル作成処理が見つかりません"
            assert "DROP EXTERNAL TABLE" in sql_content, "外部テーブル削除処理が見つかりません"
        
        # 重複削除ロジック確認
        dedup_complexity = pipeline_config['dedup_complexity']
        if dedup_complexity == 'complex':
            assert "PARTITION BY ID_NO, PNT_TYPE_CD, PICTURE_MM" in sql_content, "複雑な重複削除ロジックが見つかりません"
        elif dedup_complexity == 'medium':
            assert "PARTITION BY Bx,SIH_KIY_NO" in sql_content, "中程度の重複削除ロジックが見つかりません"
        elif dedup_complexity == 'simple':
            assert "PARTITION BY ID_NO" in sql_content, "単純な重複削除ロジックが見つかりません"
        elif dedup_complexity == 'distinct':
            assert "SELECT distinct" in sql_content, "DISTINCT重複削除が見つかりません"
        elif dedup_complexity == 'none':
            assert "ROW_NUMBER" not in sql_content, "重複削除処理が不要なのに含まれています"
        
        logger.info(f"{pipeline_config['name']} 列構造検証完了: {pipeline_config['column_count']}列確認")

    @patch('tests.e2e.helpers.synapse_e2e_helper.pyodbc')
    def test_pipeline_execution_with_external_sql(self, mock_pyodbc, synapse_helper, pipeline_config):
        """パイプライン別外部SQL実行テスト"""
        logger.info(f"=== {pipeline_config['name']} 外部SQL実行テスト開始 ===")
        
        # モックセットアップ
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_pyodbc.connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        
        # パイプライン別模擬実行結果データ
        column_count = pipeline_config['column_count']
        mock_results = [
            tuple([f'value{i}_{j}' for j in range(column_count)])
            for i in range(10)  # 10件のテストデータ
        ]
        mock_cursor.fetchall.return_value = mock_results
        
        # SQLクエリマネージャー初期化
        sql_manager = E2ESQLQueryManager(pipeline_config['sql_file'])
        
        # パラメータ設定（必要な場合）
        query_params = {}
        if 'DYNAMIC_LOCATION' in pipeline_config['parameters']:
            current_date = datetime.now().strftime('%Y%m%d')
            query_params['DYNAMIC_LOCATION'] = f"/test/location/{current_date}/file.tsv"
        if 'DYNAMIC_CSV_YMD' in pipeline_config['parameters']:
            query_params['DYNAMIC_CSV_YMD'] = datetime.now().strftime('%Y%m%d')
        
        # 外部SQLクエリ実行
        results = synapse_helper.execute_external_query(
            sql_query_manager=sql_manager,
            query_params=query_params
        )
        
        # 実行結果検証
        assert results is not None, "クエリ実行結果が取得できませんでした"
        assert len(results) > 0, "実行結果が空です"
        
        # 各レコードの列数確認
        for record in results:
            assert len(record) == column_count, \
                f"レコードの列数が期待値と異なります。期待: {column_count}, 実際: {len(record)}"
        
        logger.info(f"{pipeline_config['name']} 外部SQL実行テスト完了: {len(results)}件処理")

    def test_cross_pipeline_column_count_analysis(self):
        """パイプライン間列数分析テスト"""
        logger.info("=== パイプライン間列数分析開始 ===")
        
        column_count_distribution = {}
        complexity_distribution = {}
        
        for pipeline_name, config in self.PIPELINE_CONFIGS.items():
            # 列数分布
            column_count = config['column_count']
            if column_count not in column_count_distribution:
                column_count_distribution[column_count] = []
            column_count_distribution[column_count].append(pipeline_name)
            
            # 複雑さ分布
            complexity = config['dedup_complexity']
            if complexity not in complexity_distribution:
                complexity_distribution[complexity] = []
            complexity_distribution[complexity].append(pipeline_name)
        
        # 分析結果ログ出力
        logger.info("=== 列数分布 ===")
        for count, pipelines in sorted(column_count_distribution.items()):
            logger.info(f"{count}列: {len(pipelines)}パイプライン - {pipelines}")
        
        logger.info("=== 重複削除複雑さ分布 ===")
        for complexity, pipelines in complexity_distribution.items():
            logger.info(f"{complexity}: {len(pipelines)}パイプライン - {pipelines}")
        
        # 検証条件
        # 1. 列数の多様性確認
        assert len(column_count_distribution) >= 3, "列数のバリエーションが少なすぎます"
        
        # 2. 複雑さの多様性確認  
        assert len(complexity_distribution) >= 3, "重複削除複雑さのバリエーションが少なすぎます"
        
        # 3. 最小・最大列数確認
        min_columns = min(column_count_distribution.keys())
        max_columns = max(column_count_distribution.keys())
        assert min_columns >= 4, f"最小列数が少なすぎます: {min_columns}"
        assert max_columns <= 10, f"最大列数が多すぎます: {max_columns}"
        
        logger.info("パイプライン間列数分析完了")

    def test_external_table_usage_patterns(self):
        """外部テーブル使用パターン分析テスト"""
        logger.info("=== 外部テーブル使用パターン分析開始 ===")
        
        external_table_pipelines = []
        non_external_table_pipelines = []
        
        for pipeline_name, config in self.PIPELINE_CONFIGS.items():
            if config['external_table']:
                external_table_pipelines.append(pipeline_name)
            else:
                non_external_table_pipelines.append(pipeline_name)
        
        logger.info(f"外部テーブル使用: {len(external_table_pipelines)}パイプライン - {external_table_pipelines}")
        logger.info(f"外部テーブル非使用: {len(non_external_table_pipelines)}パイプライン - {non_external_table_pipelines}")
        
        # Point系パイプラインは外部テーブルを使用する
        point_pipelines = [name for name in self.PIPELINE_CONFIGS.keys() if 'point' in name]
        for pipeline in point_pipelines:
            assert self.PIPELINE_CONFIGS[pipeline]['external_table'], \
                f"Pointパイプラインは外部テーブルを使用するべきです: {pipeline}"
        
        # 少なくとも1つは外部テーブルを使用する
        assert len(external_table_pipelines) > 0, "外部テーブルを使用するパイプラインがありません"
        
        # 少なくとも1つは外部テーブルを使用しない
        assert len(non_external_table_pipelines) > 0, "外部テーブルを使用しないパイプラインがありません"
        
        logger.info("外部テーブル使用パターン分析完了")

    @patch('tests.e2e.helpers.synapse_e2e_helper.pyodbc')
    def test_performance_comparison_across_pipelines(self, mock_pyodbc, synapse_helper):
        """パイプライン間パフォーマンス比較テスト"""
        logger.info("=== パイプライン間パフォーマンス比較開始 ===")
        
        # モックセットアップ
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_pyodbc.connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        
        performance_results = {}
        
        for pipeline_name, config in self.PIPELINE_CONFIGS.items():
            try:
                # 大量データ模擬
                column_count = config['column_count']
                large_mock_results = [
                    tuple([f'data{i}_{j}' for j in range(column_count)])
                    for i in range(1000)  # 1000件
                ]
                mock_cursor.fetchall.return_value = large_mock_results
                
                # SQLクエリマネージャー初期化
                sql_manager = E2ESQLQueryManager(config['sql_file'])
                
                # パフォーマンステスト実行
                start_time = datetime.now()
                
                results = synapse_helper.execute_external_query(
                    sql_query_manager=sql_manager,
                    query_params={}
                )
                
                execution_time = (datetime.now() - start_time).total_seconds()
                
                performance_results[pipeline_name] = {
                    'execution_time': execution_time,
                    'column_count': column_count,
                    'record_count': len(results),
                    'complexity': config['dedup_complexity']
                }
                
                # 個別パフォーマンス検証
                assert execution_time < 15.0, f"{pipeline_name} 実行時間が長すぎます: {execution_time}秒"
                
            except Exception as e:
                logger.error(f"{pipeline_name} パフォーマンステストエラー: {e}")
                raise
        
        # パフォーマンス結果分析
        avg_execution_time = sum(r['execution_time'] for r in performance_results.values()) / len(performance_results)
        max_execution_time = max(r['execution_time'] for r in performance_results.values())
        min_execution_time = min(r['execution_time'] for r in performance_results.values())
        
        logger.info(f"平均実行時間: {avg_execution_time:.2f}秒")
        logger.info(f"最大実行時間: {max_execution_time:.2f}秒")
        logger.info(f"最小実行時間: {min_execution_time:.2f}秒")
        
        # パフォーマンス詳細ログ
        for pipeline_name, perf in sorted(performance_results.items(), key=lambda x: x[1]['execution_time']):
            logger.info(f"{pipeline_name}: {perf['execution_time']:.2f}秒 ({perf['column_count']}列, {perf['complexity']})")
        
        # 全体パフォーマンス検証
        assert avg_execution_time < 10.0, f"平均実行時間が長すぎます: {avg_execution_time}秒"
        assert max_execution_time < 20.0, f"最大実行時間が長すぎます: {max_execution_time}秒"
        
        logger.info("パイプライン間パフォーマンス比較完了")

    def test_sql_externalization_completion_status(self):
        """SQL外部化完了状況テスト"""
        logger.info("=== SQL外部化完了状況確認開始 ===")
        
        total_pipelines = len(self.PIPELINE_CONFIGS)
        externalized_pipelines = 0
        
        externalization_status = {}
        
        for pipeline_name, config in self.PIPELINE_CONFIGS.items():
            try:
                sql_manager = E2ESQLQueryManager(config['sql_file'])
                
                # ファイル存在確認
                file_exists = sql_manager.file_exists()
                
                # コンテンツ品質確認
                if file_exists:
                    sql_content = sql_manager.get_sql_content()
                    has_content = sql_content and len(sql_content.strip()) > 0
                    has_select = 'SELECT' in sql_content.upper()
                    has_expected_columns = all(col in sql_content for col in config['expected_columns'])
                else:
                    has_content = has_select = has_expected_columns = False
                
                # 外部化品質スコア計算
                quality_score = sum([file_exists, has_content, has_select, has_expected_columns])
                
                externalization_status[pipeline_name] = {
                    'file_exists': file_exists,
                    'has_content': has_content,
                    'has_select': has_select,
                    'has_expected_columns': has_expected_columns,
                    'quality_score': quality_score,
                    'max_score': 4
                }
                
                if quality_score == 4:
                    externalized_pipelines += 1
                
                logger.info(f"{pipeline_name}: 品質スコア {quality_score}/4")
                
            except Exception as e:
                logger.error(f"{pipeline_name} 外部化状況確認エラー: {e}")
                externalization_status[pipeline_name] = {
                    'quality_score': 0,
                    'max_score': 4,
                    'error': str(e)
                }
        
        # 完了率計算
        completion_rate = (externalized_pipelines / total_pipelines) * 100
        
        logger.info(f"SQL外部化完了率: {completion_rate:.1f}% ({externalized_pipelines}/{total_pipelines})")
        
        # 完了状況検証
        assert completion_rate >= 80.0, f"SQL外部化完了率が低すぎます: {completion_rate:.1f}%"
        assert externalized_pipelines >= 4, f"外部化完了パイプライン数が少なすぎます: {externalized_pipelines}"
        
        # 詳細状況ログ
        for pipeline_name, status in externalization_status.items():
            if status['quality_score'] < 4:
                logger.warning(f"外部化未完了: {pipeline_name} - スコア: {status['quality_score']}/4")
        
        logger.info("SQL外部化完了状況確認完了")

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
