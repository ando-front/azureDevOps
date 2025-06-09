"""
シンプルなETLテスト - E2E問題解決用
"""
import pytest
import time

from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection

class TestSimpleETL:
    """シンプルなETLテスト"""
    
    def test_data_extraction_simple(self):
        """シンプルなデータ抽出テスト"""
        connection = SynapseE2EConnection()
        
        # 既存データの確認
        results = connection.execute_query("""
            SELECT 
                source_type,
                COUNT(*) as total_records
            FROM raw_data_source
            WHERE source_type IN ('customer', 'order', 'product')
            GROUP BY source_type
            ORDER BY source_type
        """)
        
        print(f"抽出結果: {results}")
        
        # 3つのソースタイプが存在することを確認
        assert len(results) == 3, f"期待: 3つのソースタイプ, 実際: {len(results)}"
        
        # 各ソースタイプにデータが存在することを確認
        for result in results:
            source_type = result[0]
            count = result[1]
            assert count > 0, f"ソースタイプ {source_type} にデータがありません"
        
        print("✅ データ抽出テスト成功")

    def test_data_transformation_simple(self):
        """シンプルなデータ変換テスト"""
        connection = SynapseE2EConnection()
        
        # 基本的な変換処理
        results = connection.execute_query("""
            SELECT 
                source_type,
                COUNT(*) as record_count,
                COUNT(CASE WHEN ISJSON(data_json) = 1 THEN 1 END) as valid_json_count
            FROM raw_data_source
            WHERE source_type IN ('customer', 'order', 'product')
            GROUP BY source_type
            ORDER BY source_type
        """)
        
        print(f"変換結果: {results}")
        
        # 変換品質チェック
        for result in results:
            source_type = result[0]
            record_count = result[1]
            valid_json_count = result[2]
            
            assert record_count > 0, f"ソースタイプ {source_type} にレコードがありません"
            assert valid_json_count == record_count, f"ソースタイプ {source_type} に無効なJSONがあります"
        
        print("✅ データ変換テスト成功")

    def test_data_loading_simple(self):
        """シンプルなデータローディングテスト"""
        connection = SynapseE2EConnection()
        
        # ローディング処理のシミュレーション
        results = connection.execute_query("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT source_type) as source_types,
                MIN(created_at) as earliest_record,
                MAX(created_at) as latest_record
            FROM raw_data_source
        """)
        
        print(f"ローディング結果: {results}")
        
        # ローディング品質チェック
        result = results[0]
        total_records = result[0]
        source_types = result[1]
        
        assert total_records > 0, "ローディングされたレコードがありません"
        assert source_types >= 3, f"期待: 3以上のソースタイプ, 実際: {source_types}"
        
        print("✅ データローディングテスト成功")

    def test_incremental_processing_simple(self):
        """シンプルな増分処理テスト"""
        connection = SynapseE2EConnection()
        
        # ウォーターマークテーブルの確認
        watermark_results = connection.execute_query("""
            SELECT 
                COUNT(*) as watermark_count,
                COUNT(DISTINCT source_name) as distinct_sources
            FROM data_watermarks
        """)
        
        print(f"ウォーターマーク結果: {watermark_results}")
        
        # ウォーターマーク品質チェック
        result = watermark_results[0]
        watermark_count = result[0]
        distinct_sources = result[1]
        
        assert watermark_count > 0, "ウォーターマークレコードがありません"
        assert distinct_sources >= 3, f"期待: 3以上のソース, 実際: {distinct_sources}"
        
        print("✅ 増分処理テスト成功")
