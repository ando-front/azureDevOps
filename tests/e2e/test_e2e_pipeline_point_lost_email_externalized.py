"""
Point Lost Email パイプライン E2E テスト (SQL外部化版)
対象パイプライン: pi_PointLostEmail
外部SQLファイル: point_lost_email_main.sql
出力列構造: 4列 (ID_NO, MAIL_ADR, CSV_YMD, OUTPUT_DATETIME)
重複削除: ID_NO単位でメールアドレス降順
作成日: 2024-01-16
"""

import pytest
import logging
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
import sys
import os

# プロジェクトルートをsys.pathに追加
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# from tests.e2e.helpers.synapse_e2e_helper import SynapseE2ETestHelper
from tests.e2e.helpers.sql_query_manager import E2ESQLQueryManager
from tests.e2e.helpers.missing_helpers_placeholder import MissingHelperPlaceholder

# Placeholder for missing SynapseE2ETestHelper
class SynapseE2ETestHelper(MissingHelperPlaceholder):
    pass

logger = logging.getLogger(__name__)

class TestE2EPipelinePointLostEmailExternalized:
    """Point Lost Email パイプライン SQL外部化版 E2E テスト"""
    
    # ===========================
    # Point Lost Email パイプライン構造定義
    # ===========================
    
    # 期待される出力列構造 (4列)
    EXPECTED_OUTPUT_COLUMNS = [
        'ID_NO',          # 会員ID
        'MAIL_ADR',       # メールアドレス  
        'CSV_YMD',        # CSV年月日
        'OUTPUT_DATETIME' # 出力日時
    ]
    
    # パイプライン特性
    PIPELINE_NAME = 'pi_PointLostEmail'
    EXTERNAL_TABLE_NAME = 'omni_ods_mytginfo_trn_point_delete_ext_temp'
    SQL_FILE_NAME = 'point_lost_email_main.sql'
    
    # 重複削除条件: ID_NO単位でメールアドレス降順 (Point Grant Emailより単純)
    DEDUP_PARTITION_COLUMNS = ['ID_NO']
    DEDUP_ORDER_COLUMNS = ['MAIL_ADR DESC']

    @pytest.fixture
    def synapse_helper(self):
        """Synapse E2E テストヘルパー初期化"""
        return SynapseE2ETestHelper()

    @pytest.fixture
    def sql_query_manager(self):
        """SQL外部クエリマネージャー初期化"""
        return E2ESQLQueryManager(self.SQL_FILE_NAME)

    def test_external_sql_file_accessibility(self, sql_query_manager):
        """外部SQLファイルのアクセス可能性テスト"""
        logger.info(f"=== 外部SQLファイルアクセステスト開始: {self.SQL_FILE_NAME} ===")
        
        # SQLファイルの存在確認
        assert sql_query_manager.file_exists(), f"SQLファイルが見つかりません: {self.SQL_FILE_NAME}"
        
        # SQLクエリの読み込み確認
        sql_content = sql_query_manager.get_sql_content()
        assert sql_content is not None, "SQLコンテンツが読み込めませんでした"
        assert len(sql_content.strip()) > 0, "SQLコンテンツが空です"
        
        # 必要なSQLキーワードの存在確認
        required_keywords = [
            'CREATE EXTERNAL TABLE',
            'omni_ods_mytginfo_trn_point_delete_ext_temp',
            'PARTITION BY ID_NO',
            'ORDER BY MAIL_ADR DESC',
            'ROW_NUMBER()',
            'DROP EXTERNAL TABLE'
        ]
        
        for keyword in required_keywords:
            assert keyword in sql_content, f"必要なSQLキーワードが見つかりません: {keyword}"
        
        logger.info("外部SQLファイルアクセステスト完了")

    def test_pipeline_column_structure_validation(self, sql_query_manager):
        """パイプライン出力列構造検証テスト"""
        logger.info("=== Point Lost Email パイプライン列構造検証開始 ===")
        
        sql_content = sql_query_manager.get_sql_content()
        
        # 期待される4列の出力が定義されているか確認
        for column in self.EXPECTED_OUTPUT_COLUMNS:
            assert column in sql_content, f"期待される出力列が見つかりません: {column}"
        
        # 外部テーブル定義の入力列確認 (2列のみ)
        expected_input_columns = ['ID_NO', 'MAIL_ADR']
        for column in expected_input_columns:
            assert f"{column}\tNVARCHAR" in sql_content, f"外部テーブル入力列が見つかりません: {column}"
        
        # Point Lost Email特有の重複削除ロジック確認
        assert "PARTITION BY ID_NO" in sql_content, "ID_NO単位のパーティション分割が見つかりません"
        assert "ORDER BY MAIL_ADR DESC" in sql_content, "メールアドレス降順ソートが見つかりません"
        
        # Point Grant Emailとの差分確認 (Point Lost Emailは単純)
        assert "PNT_TYPE_CD" not in sql_content, "Point Lost EmailにはPNT_TYPE_CDは不要です"
        assert "PICTURE_MM" not in sql_content, "Point Lost EmailにはPICTURE_MMは不要です"
        
        logger.info(f"Point Lost Email 列構造検証完了: {len(self.EXPECTED_OUTPUT_COLUMNS)}列出力確認")

    @patch('tests.e2e.helpers.synapse_e2e_helper.pyodbc')
    def test_pipeline_execution_with_external_sql(self, mock_pyodbc, synapse_helper, sql_query_manager):
        """外部SQL使用パイプライン実行テスト"""
        logger.info("=== Point Lost Email 外部SQL実行テスト開始 ===")
        
        # モックセットアップ
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_pyodbc.connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        
        # 模擬実行結果データ（4列）
        mock_results = [
            ('MTG001', 'test1@example.com', '20240116', '2024/01/16 10:00:00'),
            ('MTG002', 'test2@example.com', '20240116', '2024/01/16 10:01:00'),
            ('MTG003', 'test3@example.com', '20240116', '2024/01/16 10:02:00')
        ]
        mock_cursor.fetchall.return_value = mock_results
        
        # 動的パラメータ設定
        current_date = datetime.now().strftime('%Y%m%d')
        dynamic_location = f"/forRcvry/{current_date}/DCPDA017/{current_date}_DAM_PointDelete.tsv"
        
        query_params = {
            'DYNAMIC_LOCATION': dynamic_location,
            'DYNAMIC_CSV_YMD': current_date
        }
        
        # 外部SQLクエリ実行
        results = synapse_helper.execute_external_query(
            sql_query_manager=sql_query_manager,
            query_params=query_params
        )
        
        # 実行結果検証
        assert results is not None, "クエリ実行結果が取得できませんでした"
        assert len(results) > 0, "実行結果が空です"
        
        # 各レコードの列数確認（4列）
        for record in results:
            assert len(record) == len(self.EXPECTED_OUTPUT_COLUMNS), \
                f"レコードの列数が期待値と異なります。期待: {len(self.EXPECTED_OUTPUT_COLUMNS)}, 実際: {len(record)}"
        
        # パラメータ置換確認
        final_sql = sql_query_manager.get_sql_content_with_params(query_params)
        assert dynamic_location in final_sql, "DYNAMIC_LOCATIONパラメータが置換されていません"
        assert current_date in final_sql, "DYNAMIC_CSV_YMDパラメータが置換されていません"
        
        logger.info(f"Point Lost Email 外部SQL実行テスト完了: {len(results)}件処理")

    @patch('tests.e2e.helpers.synapse_e2e_helper.pyodbc')
    def test_external_table_lifecycle_management(self, mock_pyodbc, synapse_helper, sql_query_manager):
        """外部テーブルライフサイクル管理テスト"""
        logger.info("=== Point Lost Email 外部テーブルライフサイクルテスト開始 ===")
        
        # モックセットアップ
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_pyodbc.connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        
        # SQLコンテンツ取得
        sql_content = sql_query_manager.get_sql_content()
        
        # 外部テーブル作成前削除確認
        assert sql_content.count("DROP EXTERNAL TABLE") >= 1, "外部テーブル削除処理が見つかりません"
        
        # 外部テーブル作成確認
        assert "CREATE EXTERNAL TABLE" in sql_content, "外部テーブル作成処理が見つかりません"
        assert self.EXTERNAL_TABLE_NAME in sql_content, f"期待される外部テーブル名が見つかりません: {self.EXTERNAL_TABLE_NAME}"
        
        # 外部テーブル作成後削除確認
        drop_statements = sql_content.count(f"DROP EXTERNAL TABLE [omni].[{self.EXTERNAL_TABLE_NAME}]")
        assert drop_statements >= 1, "外部テーブルクリーンアップ処理が見つかりません"
        
        # TSVファイル形式確認
        assert "FILE_FORMAT=[TSV2]" in sql_content, "TSVファイル形式指定が見つかりません"
        assert "REJECT_TYPE=VALUE" in sql_content, "リジェクト設定が見つかりません"
        
        logger.info("Point Lost Email 外部テーブルライフサイクルテスト完了")

    def test_deduplication_logic_validation(self, sql_query_manager):
        """重複削除ロジック検証テスト"""
        logger.info("=== Point Lost Email 重複削除ロジック検証開始 ===")
        
        sql_content = sql_query_manager.get_sql_content()
        
        # ROW_NUMBER()関数による重複削除確認
        assert "ROW_NUMBER() OVER" in sql_content, "ROW_NUMBER関数が見つかりません"
        assert "DupRank = 1" in sql_content, "重複削除条件が見つかりません"
        
        # Point Lost Email特有の重複削除条件確認
        assert "PARTITION BY ID_NO" in sql_content, "ID_NO によるパーティション分割が見つかりません"
        assert "ORDER BY MAIL_ADR DESC" in sql_content, "メールアドレス降順ソートが見つかりません"
        
        # Point Grant Emailとの違い確認（より単純な重複削除）
        # Point Lost EmailはID_NOのみでパーティション、Point Grant EmailはID_NO+PNT_TYPE_CD+PICTURE_MM
        partition_count = sql_content.count("PARTITION BY")
        assert partition_count == 1, f"パーティション句が期待数と異なります。期待: 1, 実際: {partition_count}"
        
        logger.info("Point Lost Email 重複削除ロジック検証完了")

    @patch('tests.e2e.helpers.synapse_e2e_helper.pyodbc')
    def test_performance_and_error_handling(self, mock_pyodbc, synapse_helper, sql_query_manager):
        """パフォーマンスとエラーハンドリングテスト"""
        logger.info("=== Point Lost Email パフォーマンス・エラーハンドリングテスト開始 ===")
        
        # モックセットアップ
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_pyodbc.connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        
        # 大量データ模擬（パフォーマンステスト）
        large_mock_results = [
            (f'MTG{i:06d}', f'test{i}@example.com', '20240116', '2024/01/16 10:00:00')
            for i in range(1000)  # 1000件のテストデータ
        ]
        mock_cursor.fetchall.return_value = large_mock_results
        
        # パフォーマンステスト実行
        start_time = datetime.now()
        
        query_params = {
            'DYNAMIC_LOCATION': '/test/location/file.tsv',
            'DYNAMIC_CSV_YMD': '20240116'
        }
        
        results = synapse_helper.execute_external_query(
            sql_query_manager=sql_query_manager,
            query_params=query_params
        )
        
        execution_time = (datetime.now() - start_time).total_seconds()
        
        # パフォーマンス検証
        assert execution_time < 10.0, f"クエリ実行時間が長すぎます: {execution_time}秒"
        assert len(results) == 1000, f"期待されるレコード数と異なります。期待: 1000, 実際: {len(results)}"
        
        # エラーハンドリングテスト
        mock_cursor.execute.side_effect = Exception("Database connection error")
        
        with pytest.raises(Exception):
            synapse_helper.execute_external_query(
                sql_query_manager=sql_query_manager,
                query_params=query_params
            )
        
        logger.info(f"Point Lost Email パフォーマンス・エラーハンドリングテスト完了: {execution_time:.2f}秒")

    def test_data_quality_validation(self, sql_query_manager):
        """データ品質検証テスト"""
        logger.info("=== Point Lost Email データ品質検証開始 ===")
        
        sql_content = sql_query_manager.get_sql_content()
        
        # データ型検証
        expected_data_types = {
            'ID_NO': 'NVARCHAR(20)',
            'MAIL_ADR': 'NVARCHAR(50)'
        }
        
        for column, data_type in expected_data_types.items():
            assert f"{column}\t{data_type}" in sql_content, \
                f"期待されるデータ型が見つかりません: {column} {data_type}"
        
        # NULL許可設定確認
        assert "NULL\t -- 会員ID" in sql_content, "ID_NO のNULL設定が見つかりません"
        assert "NULL\t -- メールアドレス" in sql_content, "MAIL_ADR のNULL設定が見つかりません"
        
        # 日時フォーマット確認
        assert "FORMAT((GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Tokyo Standard Time'),'yyyy/MM/dd HH:mm:ss')" in sql_content, \
            "日時フォーマット処理が見つかりません"
        
        # タイムゾーン処理確認
        assert "Tokyo Standard Time" in sql_content, "日本標準時タイムゾーン設定が見つかりません"
        
        logger.info("Point Lost Email データ品質検証完了")

    def test_comparison_with_point_grant_email(self, sql_query_manager):
        """Point Grant Emailとの比較テスト"""
        logger.info("=== Point Lost Email vs Point Grant Email 比較テスト開始 ===")
        
        sql_content = sql_query_manager.get_sql_content()
        
        # 列数の違い確認
        # Point Lost Email: 4列, Point Grant Email: 6列
        expected_columns_count = 4
        output_columns_in_sql = sql_content.count("as CSV_YMD") + sql_content.count("as OUTPUT_DATETIME") + sql_content.count("ID_NO,") + sql_content.count("MAIL_ADR,")
        
        # Point Grant Email特有の列がないことを確認
        grant_email_specific_columns = ['PNT_TYPE_CD', 'PICTURE_MM']
        for column in grant_email_specific_columns:
            assert column not in sql_content, f"Point Lost Emailに不要な列が含まれています: {column}"
        
        # 重複削除の複雑さの違い確認
        # Point Lost Email: PARTITION BY ID_NO のみ
        # Point Grant Email: PARTITION BY ID_NO, PNT_TYPE_CD, PICTURE_MM
        partition_clause = "PARTITION BY ID_NO"
        assert partition_clause in sql_content, "Point Lost Email の単純なパーティション分割が見つかりません"
        
        # Point Grant Email特有の複雑なパーティション分割がないことを確認
        complex_partition_indicators = ["PNT_TYPE_CD", "PICTURE_MM"]
        for indicator in complex_partition_indicators:
            assert indicator not in sql_content, f"Point Lost Emailに不要な複雑なパーティション要素が含まれています: {indicator}"
        
        logger.info("Point Lost Email vs Point Grant Email 比較テスト完了: 構造差分確認")

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
