"""
E2Eテスト: Azure Data Factory パイプライン実行テスト

このモジュールは、ADFパイプラインの実際の実行とデータ処理フローのE2Eテストを提供します。
パイプラインの正常実行、エラーハンドリング、データ品質チェックを含みます。
"""
import pytest
import json
import os
import time
import requests
from typing import Dict, List, Any
from datetime import datetime, timedelta
from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection
from tests.e2e.helpers.sql_query_manager import E2ESQLQueryManager
# from tests.unit.helpers.azure_storage_mock import AzureBlobStorageMock


@pytest.mark.e2e
@pytest.mark.adf
class TestADFPipelineExecution:

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
    """ADFパイプライン実行のE2Eテスト"""
    
    def test_e2e_client_dm_bx_pipeline_data_flow(self, e2e_synapse_connection: SynapseE2EConnection, clean_test_data):
        """E2E: ClientDmBxパイプラインのデータフロー完全テスト"""
        
        # 1. 前提データの準備 - 利用サービステーブル
        self._prepare_usage_service_test_data(e2e_synapse_connection)
        
        # 2. 前提データの準備 - 顧客DMテーブル
        self._prepare_client_dm_test_data(e2e_synapse_connection)
          # 3. パイプライン実行前の状態確認
        initial_temp_count = e2e_synapse_connection.execute_external_query(
            'pipeline_execution', 'temp_table_record_count'
        )[0][0]
        
        # 4. パイプライン相当のSQL実行（テーブル作成とデータ処理）
        self._execute_client_dm_bx_pipeline_logic(e2e_synapse_connection)
          # 5. 結果検証 - データ変換の確認
        final_result = e2e_synapse_connection.execute_external_query(
            'pipeline_execution', 'data_flow_verification'
        )
        
        assert final_result[0][0] > initial_temp_count, "パイプライン実行後にデータが増加していない"
        assert final_result[0][1] > 0, "Bxが正しく付与されていない"
        assert final_result[0][2] == final_result[0][0], "すべてのレコードにBxが付与されていない"
        
        # 6. データ品質チェック - 重複確認
        duplicate_check = e2e_synapse_connection.execute_query(
            """
            SELECT BX, COUNT(*) as count
            FROM [omni].[omni_ods_marketing_trn_client_dm_bx_temp]
            GROUP BY BX
            HAVING COUNT(*) > 1
            """
        )
        
        assert len(duplicate_check) == 0, f"重複したBxが検出されました: {duplicate_check}"
        
        # 7. ガス契約と電気契約の分離確認
        contract_type_check = e2e_synapse_connection.execute_query(
            """
            SELECT 
                COUNT(CASE WHEN LIV0EU_4X IS NOT NULL THEN 1 END) as gas_contracts,
                COUNT(CASE WHEN LIV0EU_4X IS NULL AND EPCISCRT_3X IS NOT NULL THEN 1 END) as electric_only
            FROM [omni].[omni_ods_marketing_trn_client_dm_bx_temp]
            """
        )
        
        gas_count, electric_count = contract_type_check[0]
        assert gas_count + electric_count == final_result[0][0], "契約タイプの分類に問題があります"
    
    def test_e2e_point_grant_email_pipeline_flow(self, e2e_synapse_connection: SynapseE2EConnection):
        """E2E: ポイント付与メールパイプラインの完全フロー"""
        
        # 1. テストファイルデータの準備
        test_execution_date = datetime.now().strftime('%Y%m%d')
        test_file_content = self._create_point_grant_test_file_content()
        
        # 2. Blobストレージのモック準備
        mock_storage = AzureBlobStorageMock()
        blob_path = f"forRcvry/{test_execution_date}/DCPDA016/{test_execution_date}_DAM_PointAdd.tsv"
        mock_storage.upload_blob(blob_path, test_file_content)
        
        # 3. ファイル存在確認（パイプラインのGetMetadataアクティビティ相当）
        file_exists = mock_storage.blob_exists(blob_path)
        assert file_exists, f"テストファイルが見つかりません: {blob_path}"
        
        # 4. ファイル内容の読み取りと検証
        file_content = mock_storage.download_blob(blob_path)
        lines = file_content.strip().split('\n')
        
        assert len(lines) > 1, "ファイルにデータが含まれていません"
        
        # ヘッダー行の確認
        header = lines[0].split('\t')
        expected_columns = ['CustomerID', 'Email', 'PointAmount', 'CampaignCode', 'ProcessDate']
        assert all(col in header for col in expected_columns), f"期待される列が不足: {header}"
        
        # 5. データ行の検証
        for i, line in enumerate(lines[1:], 1):
            data_columns = line.split('\t')
            assert len(data_columns) == len(header), f"行{i}: 列数が一致しません"
            
            # ポイント数値の検証
            try:
                point_amount = float(data_columns[header.index('PointAmount')])
                assert point_amount > 0, f"行{i}: ポイント数が正の値ではありません"
            except ValueError:
                pytest.fail(f"行{i}: ポイント数が数値ではありません")
        
        # 6. SFTP送信のシミュレーション（成功確認）
        processed_file_path = f"processed/{test_execution_date}_DAM_PointAdd.gz"
        sftp_success = self._simulate_sftp_transfer(test_file_content, processed_file_path)
        assert sftp_success, "SFTP転送のシミュレーションに失敗"
        
        # 7. 処理完了後のファイル状態確認
        processed_file_exists = mock_storage.blob_exists(processed_file_path)
        assert processed_file_exists, "処理済みファイルが作成されていません"
    
    def test_e2e_pipeline_error_handling(self, e2e_synapse_connection: SynapseE2EConnection):
        """E2E: パイプラインエラーハンドリングテスト"""
        
        # 1. 不正なデータでのテスト（NULLデータ）
        test_client_code = 'E2E_ERROR_TEST'
        
        # 不正なデータを挿入
        e2e_synapse_connection.execute_query(
            """
            INSERT INTO [omni].[omni_ods_marketing_trn_client_dm] 
            (CUSTOMER_ID, LIV0EU_4X, EPCISCRT_3X)
            VALUES (?, NULL, NULL)
            """,
            (test_client_code,)
        )
        
        # 2. パイプライン実行（エラー状況での動作確認）
        try:
            self._execute_client_dm_bx_pipeline_logic(e2e_synapse_connection)
            
            # 3. 不正データが結果に含まれていないことを確認
            error_data_check = e2e_synapse_connection.execute_query(
                """
                SELECT COUNT(*)
                FROM [omni].[omni_ods_marketing_trn_client_dm_bx_temp]
                WHERE CUSTOMER_ID = ? AND BX IS NULL
                """,
                (test_client_code,)
            )
            
            # 不正データは処理されるべきではない（またはNULLのBXで処理される）
            assert error_data_check[0][0] == 0, "不正データが結果テーブルに含まれています"
            
        except Exception as e:
            # エラーが予期されたものかを確認
            assert "constraint" in str(e).lower() or "null" in str(e).lower(), \
                f"予期しないエラーが発生: {e}"
    
    def test_e2e_pipeline_performance_monitoring(self, e2e_synapse_connection: SynapseE2EConnection):
        """E2E: パイプラインパフォーマンス監視テスト"""
        
        # 1. 大量データでのパフォーマンステスト用データ作成
        batch_size = 1000
        start_time = time.time()
        
        # 大量のテストデータを準備
        for i in range(batch_size):
            e2e_synapse_connection.execute_query(
                """
                INSERT INTO [omni].[omni_ods_marketing_trn_client_dm] 
                (CUSTOMER_ID, LIV0EU_4X, EPCISCRT_3X)
                VALUES (?, ?, ?)
                """,
                (f'PERF_TEST_{i:06d}', f'4X_{i:06d}', f'3X_{i:06d}')
            )
        
        data_prep_time = time.time() - start_time
        
        # 2. パイプライン実行時間の測定
        pipeline_start_time = time.time()
        self._execute_client_dm_bx_pipeline_logic(e2e_synapse_connection)
        pipeline_execution_time = time.time() - pipeline_start_time
        
        # 3. パフォーマンス基準の確認
        assert data_prep_time < 30.0, f"データ準備時間が長すぎます: {data_prep_time:.2f}秒"
        assert pipeline_execution_time < 60.0, f"パイプライン実行時間が長すぎます: {pipeline_execution_time:.2f}秒"
        
        # 4. 処理されたレコード数の確認
        processed_count = e2e_synapse_connection.execute_query(
            """
            SELECT COUNT(*)
            FROM [omni].[omni_ods_marketing_trn_client_dm_bx_temp]
            WHERE CUSTOMER_ID LIKE 'PERF_TEST_%'
            """
        )[0][0]
        
        assert processed_count >= batch_size * 0.9, \
            f"処理されたレコード数が期待値を下回っています: {processed_count}/{batch_size}"
        
        # 5. パフォーマンスメトリクスの出力
        throughput = processed_count / pipeline_execution_time
        print(f"\nパフォーマンステスト結果:")
        print(f"- データ準備時間: {data_prep_time:.2f}秒")
        print(f"- パイプライン実行時間: {pipeline_execution_time:.2f}秒")
        print(f"- 処理レコード数: {processed_count}")
        print(f"- スループット: {throughput:.2f} レコード/秒")
    
    def _prepare_usage_service_test_data(self, connection: SynapseE2EConnection):
        """利用サービステストデータの準備"""
        # テストデータをクリア
        connection.execute_query("DELETE FROM [omni].[omni_ods_cloak_trn_usageservice] WHERE BX LIKE 'E2E_TEST_%'")
        
        # ガス契約テストデータ
        connection.execute_query(
            """
            INSERT INTO [omni].[omni_ods_cloak_trn_usageservice]
            (BX, SERVICE_KEY1, SERVICE_KEY_TYPE1, SERVICE_TYPE, TRANSFER_TYPE, 
             TRANSFER_YMD, SERVICE_KEY_START_YMD, OUTPUT_DATE)
            VALUES (?, ?, '004', '001', '02', ?, ?, ?)
            """,
            ('E2E_TEST_BX001', '4X_TEST_001', '2024-01-01', '2024-01-01', datetime.now())
        )
        
        # 電気契約テストデータ
        connection.execute_query(
            """
            INSERT INTO [omni].[omni_ods_cloak_trn_usageservice]
            (BX, USER_KEY, USER_KEY_TYPE, SERVICE_KEY1, SERVICE_KEY_TYPE1, 
             SERVICE_TYPE, TRANSFER_TYPE, TRANSFER_YMD, SERVICE_KEY_START_YMD, OUTPUT_DATE)
            VALUES (?, ?, '003', ?, '007', '006', '02', ?, ?, ?)
            """,
            ('E2E_TEST_BX002', '3X_TEST_001', 'SA_ID_001', '2024-01-01', '2024-01-01', datetime.now())
        )
    
    def _prepare_client_dm_test_data(self, connection: SynapseE2EConnection):
        """顧客DMテストデータの準備"""
        # テストデータをクリア
        connection.execute_query("DELETE FROM [omni].[omni_ods_marketing_trn_client_dm] WHERE CUSTOMER_ID LIKE 'E2E_TEST_%'")
        
        # ガス契約ありのクライアント
        connection.execute_query(
            """
            INSERT INTO [omni].[omni_ods_marketing_trn_client_dm]
            (CUSTOMER_ID, LIV0EU_4X, EPCISCRT_3X)
            VALUES (?, ?, ?)
            """,
            ('E2E_TEST_CLIENT_001', '4X_TEST_001', None)
        )
        
        # 電気契約のみのクライアント
        connection.execute_query(
            """
            INSERT INTO [omni].[omni_ods_marketing_trn_client_dm]
            (CUSTOMER_ID, LIV0EU_4X, EPCISCRT_3X, EPCISCRT_LIGHTING_SA_ID)
            VALUES (?, ?, ?, ?)
            """,
            ('E2E_TEST_CLIENT_002', None, '3X_TEST_001', 'SA_ID_001')
        )
    
    def _execute_client_dm_bx_pipeline_logic(self, connection: SynapseE2EConnection):
        """ClientDmBxパイプラインのロジック実行"""
        
        # パイプラインの主要処理を実行
        # 1. 一時テーブルのクリア
        connection.execute_query("TRUNCATE TABLE [omni].[omni_ods_cloak_trn_usageservice_bx4x_temp]")
        connection.execute_query("TRUNCATE TABLE [omni].[omni_ods_cloak_trn_usageservice_bx3xsaid_temp]")
        connection.execute_query("TRUNCATE TABLE [omni].[omni_ods_marketing_trn_client_dm_bx_temp]")
        
        # 2. Bx4xテンポラリテーブルの作成（簡略版）
        connection.execute_query(
            """
            INSERT INTO [omni].[omni_ods_cloak_trn_usageservice_bx4x_temp]
            SELECT BX, SERVICE_KEY1 as KEY_4X, 1 as INDEX_ID, 
                   TRANSFER_YMD, SERVICE_KEY_START_YMD, OUTPUT_DATE
            FROM [omni].[omni_ods_cloak_trn_usageservice]
            WHERE SERVICE_TYPE='001' AND TRANSFER_TYPE='02' 
                  AND SERVICE_KEY_TYPE1='004'
            """
        )
        
        # 3. Bx3xSAIDテンポラリテーブルの作成（簡略版）
        connection.execute_query(
            """
            INSERT INTO [omni].[omni_ods_cloak_trn_usageservice_bx3xsaid_temp]
            SELECT BX, USER_KEY as KEY_3X, SERVICE_KEY1 as KEY_SA_ID, 
                   1 as INDEX_ID, TRANSFER_YMD, SERVICE_KEY_START_YMD, OUTPUT_DATE
            FROM [omni].[omni_ods_cloak_trn_usageservice]
            WHERE SERVICE_TYPE='006' AND TRANSFER_TYPE='02' 
                  AND USER_KEY_TYPE='003' AND SERVICE_KEY_TYPE1='007'
            """
        )
        
        # 4. 最終結果テーブルの作成
        # ガス契約ありの場合
        connection.execute_query(
            """
            INSERT INTO [omni].[omni_ods_marketing_trn_client_dm_bx_temp]
            SELECT usv.BX, usv.INDEX_ID, usv.TRANSFER_YMD, 
                   usv.SERVICE_KEY_START_YMD, usv.OUTPUT_DATE, cldm.*
            FROM [omni].[omni_ods_marketing_trn_client_dm] cldm
            INNER JOIN [omni].[omni_ods_cloak_trn_usageservice_bx4x_temp] usv 
                ON cldm.LIV0EU_4X = usv.KEY_4X
            WHERE cldm.LIV0EU_4X IS NOT NULL
            """
        )
        
        # 電気契約のみの場合
        connection.execute_query(
            """
            INSERT INTO [omni].[omni_ods_marketing_trn_client_dm_bx_temp]
            SELECT usv.BX, usv.INDEX_ID, usv.TRANSFER_YMD, 
                   usv.SERVICE_KEY_START_YMD, usv.OUTPUT_DATE, cldm.*
            FROM [omni].[omni_ods_marketing_trn_client_dm] cldm
            INNER JOIN [omni].[omni_ods_cloak_trn_usageservice_bx3xsaid_temp] usv 
                ON cldm.EPCISCRT_3X = usv.KEY_3X 
                AND ISNULL(cldm.EPCISCRT_LIGHTING_SA_ID, cldm.EPCISCRT_POWER_SA_ID) = usv.KEY_SA_ID
            WHERE cldm.LIV0EU_4X IS NULL
            """
        )
    
    def _create_point_grant_test_file_content(self) -> str:
        """ポイント付与テストファイルの内容作成"""
        header = "CustomerID\tEmail\tPointAmount\tCampaignCode\tProcessDate"
        
        test_data = [
            "CUST001\ttest1@example.com\t1000\tCAMP001\t2024-01-15",
            "CUST002\ttest2@example.com\t1500\tCAMP001\t2024-01-15",
            "CUST003\ttest3@example.com\t2000\tCAMP002\t2024-01-15",
            "CUST004\ttest4@example.com\t500\tCAMP001\t2024-01-15",
            "CUST005\ttest5@example.com\t3000\tCAMP003\t2024-01-15"
        ]
        
        return header + "\n" + "\n".join(test_data)
    
    def _simulate_sftp_transfer(self, file_content: str, destination_path: str) -> bool:
        """SFTP転送のシミュレーション"""
        try:
            # 実際のSFTP転送の代わりに、ファイル作成をシミュレート
            # 本来はSFTPクライアントを使用してファイル転送を行う
            
            # 圧縮処理のシミュレーション
            import gzip
            import io
            
            # gzip圧縮
            compressed_data = io.BytesIO()
            with gzip.GzipFile(fileobj=compressed_data, mode='wb') as gz_file:
                gz_file.write(file_content.encode('utf-8'))
            
            # 圧縮サイズの確認
            compressed_size = len(compressed_data.getvalue())
            original_size = len(file_content.encode('utf-8'))

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
            
            compression_ratio = compressed_size / original_size
            print(f"圧縮率: {compression_ratio:.2%}")
            
            # SFTP転送成功のシミュレーション
            return compressed_size > 0 and compression_ratio < 1.0
            
        except Exception as e:
            print(f"SFTP転送シミュレーションエラー: {e}")
            return False


@pytest.mark.e2e
@pytest.mark.adf
@pytest.mark.integration
class TestADFPipelineIntegration:
    """ADFパイプライン統合テスト"""
    
    def test_e2e_multiple_pipeline_coordination(self, e2e_synapse_connection: SynapseE2EConnection):
        """E2E: 複数パイプラインの連携テスト"""
        
        # 1. 前提条件: データ準備パイプラインの実行
        self._execute_data_preparation_pipeline(e2e_synapse_connection)
        
        # 2. メインデータ処理パイプライン実行
        self._execute_main_data_processing_pipeline(e2e_synapse_connection)
        
        # 3. 後処理パイプラインの実行
        self._execute_post_processing_pipeline(e2e_synapse_connection)
        
        # 4. 統合結果の検証
        integration_result = e2e_synapse_connection.execute_query(
            """
            SELECT 
                COUNT(*) as total_processed,
                COUNT(DISTINCT BX) as unique_customers,
                MAX(OUTPUT_DATE) as latest_processing_date
            FROM [omni].[omni_ods_marketing_trn_client_dm_bx_temp]
            WHERE OUTPUT_DATE >= ?
            """,
            (datetime.now().date(),)
        )
        
        assert integration_result[0][0] > 0, "統合処理でデータが生成されていない"
        assert integration_result[0][1] > 0, "一意の顧客データが処理されていない"
        assert integration_result[0][2] is not None, "処理日時が設定されていない"
    
    def test_e2e_pipeline_dependency_validation(self, e2e_synapse_connection: SynapseE2EConnection):
        """E2E: パイプライン依存関係の検証テスト"""
        
        # 1. 依存するテーブルの存在確認
        required_tables = [
            "[omni].[omni_ods_cloak_trn_usageservice]",
            "[omni].[omni_ods_marketing_trn_client_dm]",
            "[omni].[omni_ods_cloak_trn_usageservice_bx4x_temp]",
            "[omni].[omni_ods_cloak_trn_usageservice_bx3xsaid_temp]",
            "[omni].[omni_ods_marketing_trn_client_dm_bx_temp]"
        ]
        
        for table in required_tables:
            table_exists = e2e_synapse_connection.execute_query(
                f"""
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_NAME = '{table.split('.')[-1].strip('[]')}'
                """
            )[0][0]
            
            assert table_exists > 0, f"必要なテーブルが存在しません: {table}"
        
        # 2. テーブル間の関連性確認
        foreign_key_check = e2e_synapse_connection.execute_query(
            """
            SELECT 
                COUNT(*) as records_with_valid_relationships
            FROM [omni].[omni_ods_marketing_trn_client_dm] cd
            LEFT JOIN [omni].[omni_ods_cloak_trn_usageservice] us 
                ON cd.LIV0EU_4X = us.SERVICE_KEY1 
                OR cd.EPCISCRT_3X = us.USER_KEY
            WHERE us.BX IS NOT NULL
            """
        )
        
        # 少なくとも一部のレコードには有効な関連性があることを確認
        assert foreign_key_check[0][0] > 0, "テーブル間の関連性が確認できません"
    
    def _execute_data_preparation_pipeline(self, connection: SynapseE2EConnection):
        """データ準備パイプラインの実行"""
        # 基本的なテストデータの準備
        test_data_insertion_queries = [
            """
            INSERT INTO [omni].[omni_ods_cloak_trn_usageservice]
            (BX, SERVICE_KEY1, SERVICE_KEY_TYPE1, USER_KEY, USER_KEY_TYPE, 
             SERVICE_TYPE, TRANSFER_TYPE, TRANSFER_YMD, SERVICE_KEY_START_YMD, OUTPUT_DATE)
            VALUES 
            ('INTEGRATION_BX001', 'PREP_4X_001', '004', NULL, NULL, '001', '02', '2024-01-01', '2024-01-01', GETDATE()),
            ('INTEGRATION_BX002', 'PREP_SA_001', '007', 'PREP_3X_001', '003', '006', '02', '2024-01-01', '2024-01-01', GETDATE())
            """,
            
            """
            INSERT INTO [omni].[omni_ods_marketing_trn_client_dm]
            (CUSTOMER_ID, LIV0EU_4X, EPCISCRT_3X, EPCISCRT_LIGHTING_SA_ID)
            VALUES 
            ('INTEGRATION_CLIENT_001', 'PREP_4X_001', NULL, NULL),
            ('INTEGRATION_CLIENT_002', NULL, 'PREP_3X_001', 'PREP_SA_001')
            """
        ]
        
        for query in test_data_insertion_queries:
            connection.execute_query(query)
    
    def _execute_main_data_processing_pipeline(self, connection: SynapseE2EConnection):
        """メインデータ処理パイプラインの実行"""
        # ClientDmBxパイプラインのロジックを実行
        self._execute_client_dm_bx_pipeline_logic(connection)
    
    def _execute_post_processing_pipeline(self, connection: SynapseE2EConnection):
        """後処理パイプラインの実行"""
        # データクリーンアップとファイナライゼーション
        connection.execute_query(
            """
            UPDATE [omni].[omni_ods_marketing_trn_client_dm_bx_temp]
            SET OUTPUT_DATE = GETDATE()
            WHERE OUTPUT_DATE IS NULL OR OUTPUT_DATE < ?
            """,
            (datetime.now().date(),)
        )
    
    def _execute_client_dm_bx_pipeline_logic(self, connection: SynapseE2EConnection):
        """ClientDmBxパイプラインのロジック実行（統合テスト用）"""
        
        # 一時テーブルのクリア
        temp_tables = [
            "[omni].[omni_ods_cloak_trn_usageservice_bx4x_temp]",
            "[omni].[omni_ods_cloak_trn_usageservice_bx3xsaid_temp]",
            "[omni].[omni_ods_marketing_trn_client_dm_bx_temp]"
        ]
        
        for table in temp_tables:
            connection.execute_query(f"TRUNCATE TABLE {table}")
        
        # 処理ロジックの実行（簡略版）
        processing_queries = [
            """
            INSERT INTO [omni].[omni_ods_cloak_trn_usageservice_bx4x_temp]
            SELECT BX, SERVICE_KEY1 as KEY_4X, 1 as INDEX_ID, 
                   TRANSFER_YMD, SERVICE_KEY_START_YMD, OUTPUT_DATE
            FROM [omni].[omni_ods_cloak_trn_usageservice]
            WHERE SERVICE_TYPE='001' AND TRANSFER_TYPE='02' AND SERVICE_KEY_TYPE1='004'
            """,
            
            """
            INSERT INTO [omni].[omni_ods_cloak_trn_usageservice_bx3xsaid_temp]
            SELECT BX, USER_KEY as KEY_3X, SERVICE_KEY1 as KEY_SA_ID, 
                   1 as INDEX_ID, TRANSFER_YMD, SERVICE_KEY_START_YMD, OUTPUT_DATE
            FROM [omni].[omni_ods_cloak_trn_usageservice]
            WHERE SERVICE_TYPE='006' AND TRANSFER_TYPE='02' 
                  AND USER_KEY_TYPE='003' AND SERVICE_KEY_TYPE1='007'
            """,
            
            """
            INSERT INTO [omni].[omni_ods_marketing_trn_client_dm_bx_temp]
            SELECT usv.BX, usv.INDEX_ID, usv.TRANSFER_YMD, 
                   usv.SERVICE_KEY_START_YMD, usv.OUTPUT_DATE, cldm.*
            FROM [omni].[omni_ods_marketing_trn_client_dm] cldm
            INNER JOIN [omni].[omni_ods_cloak_trn_usageservice_bx4x_temp] usv 
                ON cldm.LIV0EU_4X = usv.KEY_4X
            WHERE cldm.LIV0EU_4X IS NOT NULL
            """,
            
            """
            INSERT INTO [omni].[omni_ods_marketing_trn_client_dm_bx_temp]
            SELECT usv.BX, usv.INDEX_ID, usv.TRANSFER_YMD, 
                   usv.SERVICE_KEY_START_YMD, usv.OUTPUT_DATE, cldm.*
            FROM [omni].[omni_ods_marketing_trn_client_dm] cldm
            INNER JOIN [omni].[omni_ods_cloak_trn_usageservice_bx3xsaid_temp] usv 
                ON cldm.EPCISCRT_3X = usv.KEY_3X 
                AND ISNULL(cldm.EPCISCRT_LIGHTING_SA_ID, cldm.EPCISCRT_POWER_SA_ID) = usv.KEY_SA_ID
            WHERE cldm.LIV0EU_4X IS NULL
            """
        ]
        
        for query in processing_queries:
            connection.execute_query(query)
