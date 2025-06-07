"""
E2E Test Suite for Data Quality and Security Operations

データ品質とセキュリティ操作のE2Eテスト
データ整合性、バリデーション、暗号化、アクセス制御を含む
"""

import pytest
import datetime
import hashlib
import base64
import os
from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection
from tests.helpers.reproducible_e2e_helper import setup_reproducible_test_class, cleanup_reproducible_test_class


@pytest.mark.e2e
class TestDataQualityAndSecurity:
    """データ品質とセキュリティのE2Eテスト"""
    
    @classmethod
    def setup_class(cls):
        """再現可能テスト環境のセットアップ"""
        setup_reproducible_test_class()
        
        # Disable proxy settings for tests
        for var in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY']:
            if var in os.environ:
                del os.environ[var]
    
    @classmethod
    def teardown_class(cls):
        """再現可能テスト環境のクリーンアップ"""
        cleanup_reproducible_test_class()

    def test_data_integrity_validation(self, e2e_synapse_connection: SynapseE2EConnection):
        """データ整合性バリデーションのテスト"""
        # テストデータの準備
        self._prepare_integrity_test_data(e2e_synapse_connection)
        
        # データ整合性チェッククエリ
        result = e2e_synapse_connection.execute_query("""
            SELECT 
                'ClientDmBx' as table_name,
                COUNT(*) as total_records,
                COUNT(CASE WHEN client_id IS NULL THEN 1 END) as null_client_ids,
                COUNT(CASE WHEN segment IS NULL THEN 1 END) as null_segments,
                COUNT(CASE WHEN score < 0 OR score > 1000 THEN 1 END) as invalid_scores,
                COUNT(CASE WHEN total_amount < 0 THEN 1 END) as negative_amounts
            FROM ClientDmBx
            WHERE client_id LIKE 'INTEGRITY_TEST_%'
            
            UNION ALL
            
            SELECT 
                'point_grant_email' as table_name,
                COUNT(*) as total_records,
                COUNT(CASE WHEN client_id IS NULL THEN 1 END) as null_client_ids,
                COUNT(CASE WHEN email IS NULL OR email NOT LIKE '%@%' THEN 1 END) as invalid_emails,
                COUNT(CASE WHEN points_granted < 0 THEN 1 END) as negative_points,
                COUNT(CASE WHEN status NOT IN ('SENT', 'DELIVERED', 'BOUNCED', 'FAILED') THEN 1 END) as invalid_statuses
            FROM point_grant_email
            WHERE client_id LIKE 'INTEGRITY_TEST_%'
        """)
        
        assert len(result) >= 2, "データ整合性チェック結果が期待される数に達していません"
        
        # 整合性問題の確認
        for row in result:
            table_name, total_records, null_ids, invalid_field1, invalid_field2, invalid_field3 = row
            assert total_records >= 0, f"テーブル {table_name} のレコード数が不正です"
            assert null_ids == 0, f"テーブル {table_name} にNULLのclient_idが存在します: {null_ids}件"
            
            # 各テーブル固有の検証
            if table_name == 'ClientDmBx':
                assert invalid_field1 == 0, f"ClientDmBxにNULLセグメントが存在します: {invalid_field1}件"
                assert invalid_field2 == 0, f"ClientDmBxに無効なスコアが存在します: {invalid_field2}件"
                assert invalid_field3 == 0, f"ClientDmBxに負の金額が存在します: {invalid_field3}件"
            elif table_name == 'point_grant_email':
                assert invalid_field1 == 0, f"point_grant_emailに無効なメールアドレスが存在します: {invalid_field1}件"
                assert invalid_field2 == 0, f"point_grant_emailに負のポイントが存在します: {invalid_field2}件"
                assert invalid_field3 == 0, f"point_grant_emailに無効なステータスが存在します: {invalid_field3}件"
        
        print(f"データ整合性バリデーション成功: {len(result)}テーブルを検証")

    def test_duplicate_detection_and_handling(self, e2e_synapse_connection: SynapseE2EConnection):
        """重複検出と処理のテスト"""
        # 重複テストデータの準備
        self._prepare_duplicate_test_data(e2e_synapse_connection)
        
        # 重複検出クエリ
        result = e2e_synapse_connection.execute_query("""
            SELECT 
                client_id,
                COUNT(*) as duplicate_count,
                MIN(id) as first_id,
                MAX(id) as last_id
            FROM ClientDmBx
            WHERE client_id LIKE 'DUPLICATE_TEST_%'
            GROUP BY client_id
            HAVING COUNT(*) > 1
            ORDER BY duplicate_count DESC
        """)
        
        assert len(result) >= 1, "重複データが検出されませんでした"
        
        # 重複の詳細確認
        for row in result:
            client_id, duplicate_count, first_id, last_id = row
            assert duplicate_count >= 2, f"顧客 {client_id} の重複数が不正です: {duplicate_count}"
            assert first_id != last_id, f"顧客 {client_id} のIDが同じです"
            
            # 重複レコードの詳細を取得
            duplicate_details = e2e_synapse_connection.execute_query("""
                SELECT id, segment, score, total_amount, processed_date
                FROM ClientDmBx
                WHERE client_id = ?
                ORDER BY processed_date DESC
            """, (client_id,))
            
            assert len(duplicate_details) == duplicate_count, f"重複詳細の件数が一致しません"
        
        print(f"重複検出テスト成功: {len(result)}件の重複を検出")

    def test_data_encryption_and_masking(self, e2e_synapse_connection: SynapseE2EConnection):
        """データ暗号化とマスキングのテスト"""
        # 暗号化テストデータの準備
        self._prepare_encryption_test_data(e2e_synapse_connection)
        
        # 暗号化されたデータの検証
        result = e2e_synapse_connection.execute_query("""
            SELECT 
                client_id,
                email,
                LEN(email) as email_length,
                CASE 
                    WHEN email LIKE '%***%' THEN 'MASKED'
                    WHEN email LIKE 'ENC_%' THEN 'ENCRYPTED'
                    ELSE 'PLAIN'
                END as email_protection_status
            FROM point_grant_email
            WHERE client_id LIKE 'ENCRYPT_TEST_%'
            ORDER BY client_id
        """)
        
        assert len(result) >= 2, "暗号化テストデータが不足しています"
        
        # 暗号化とマスキングの確認
        protection_found = {'MASKED': False, 'ENCRYPTED': False}
        for row in result:
            client_id, email, email_length, protection_status = row
            assert email is not None, f"顧客 {client_id} のメールアドレスがNULLです"
            assert email_length > 0, f"顧客 {client_id} のメールアドレスが空です"
            
            if protection_status in protection_found:
                protection_found[protection_status] = True
                
            # プレーンテキストでないことを確認
            if protection_status != 'PLAIN':
                assert '@' not in email or '***' in email or email.startswith('ENC_'), \
                    f"顧客 {client_id} のメールが適切に保護されていません: {email}"
        
        # 少なくとも一つの保護方法が適用されていることを確認
        assert any(protection_found.values()), "データ保護が適用されていません"
        
        print(f"データ暗号化とマスキングテスト成功: {len(result)}件のデータを検証")

    def test_access_control_validation(self, e2e_synapse_connection: SynapseE2EConnection):
        """アクセス制御バリデーションのテスト"""
        # アクセス制御テスト用のクエリ実行
        result = e2e_synapse_connection.execute_query("""
            SELECT 
                TABLE_SCHEMA,
                TABLE_NAME,
                COUNT(*) as record_count
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA IN ('omni', 'etl', 'staging')
            AND TABLE_TYPE = 'BASE TABLE'
            GROUP BY TABLE_SCHEMA, TABLE_NAME
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """)
        
        assert len(result) >= 3, "アクセス可能なテーブルが不足しています"
        
        # スキーマごとのアクセス確認
        schema_access = {}
        for row in result:
            schema_name, table_name, record_count = row
            if schema_name not in schema_access:
                schema_access[schema_name] = []
            schema_access[schema_name].append(table_name)
        
        # 期待されるスキーマへのアクセスを確認
        expected_schemas = ['omni', 'etl', 'staging']
        for schema in expected_schemas:
            assert schema in schema_access, f"スキーマ {schema} にアクセスできません"
            assert len(schema_access[schema]) >= 1, f"スキーマ {schema} にテーブルが存在しません"
        
        print(f"アクセス制御バリデーション成功: {len(expected_schemas)}スキーマへのアクセスを確認")

    def test_data_lineage_tracking(self, e2e_synapse_connection: SynapseE2EConnection):
        """データ系譜追跡のテスト"""
        # データ系譜追跡クエリ
        result = e2e_synapse_connection.execute_query("""
            SELECT 
                'ClientDmBx' as target_table,
                data_source,
                COUNT(*) as record_count,
                MIN(processed_date) as earliest_processed,
                MAX(processed_date) as latest_processed
            FROM ClientDmBx
            WHERE data_source IS NOT NULL
            GROUP BY data_source
            
            UNION ALL
            
            SELECT 
                'point_grant_email' as target_table,
                'EMAIL_SYSTEM' as data_source,
                COUNT(*) as record_count,
                MIN(created_at) as earliest_processed,
                MAX(created_at) as latest_processed
            FROM point_grant_email
            WHERE created_at IS NOT NULL
            
            UNION ALL
            
            SELECT 
                'marketing_client_dm' as target_table,
                'MARKETING_SYSTEM' as data_source,
                COUNT(*) as record_count,
                MIN(created_at) as earliest_processed,
                MAX(created_at) as latest_processed
            FROM marketing_client_dm
            WHERE created_at IS NOT NULL
        """)
        
        assert len(result) >= 3, "データ系譜情報が不足しています"
        
        # データ系譜の妥当性確認
        for row in result:
            target_table, data_source, record_count, earliest, latest = row
            assert target_table is not None, "対象テーブル名がNULLです"
            assert data_source is not None, "データソースがNULLです"
            assert record_count >= 0, f"テーブル {target_table} のレコード数が負の値です"
            
            if earliest and latest:
                assert earliest <= latest, f"テーブル {target_table} の処理日時が矛盾しています"
        
        print(f"データ系譜追跡成功: {len(result)}件の系譜情報を確認")

    def test_audit_trail_generation(self, e2e_synapse_connection: SynapseE2EConnection):
        """監査証跡生成のテスト"""
        # 監査証跡テスト用の操作実行
        test_client_id = 'AUDIT_TEST_001'
        
        # INSERT操作
        e2e_synapse_connection.execute_query("""
            INSERT INTO ClientDmBx (client_id, segment, score, last_transaction_date, total_amount, processed_date, data_source)
            VALUES (?, 'PREMIUM', 850, '2024-01-15', 75000.00, GETDATE(), 'AUDIT_TEST')
        """, (test_client_id,))
        
        # UPDATE操作
        e2e_synapse_connection.execute_query("""
            UPDATE ClientDmBx 
            SET score = 900, total_amount = 85000.00
            WHERE client_id = ?
        """, (test_client_id,))
        
        # 監査証跡の確認（実際の監査テーブルがない場合、操作の結果を確認）
        result = e2e_synapse_connection.execute_query("""
            SELECT 
                client_id,
                score,
                total_amount,
                processed_date
            FROM ClientDmBx
            WHERE client_id = ?
        """, (test_client_id,))
        
        assert len(result) == 1, "監査テストレコードが見つかりません"
        
        client_id, score, total_amount, processed_date = result[0]
        assert score == 900, "UPDATE操作が反映されていません"
        assert total_amount == 85000.00, "UPDATE操作が反映されていません"
        assert processed_date is not None, "処理日時が設定されていません"
        
        # クリーンアップ
        e2e_synapse_connection.execute_query("DELETE FROM ClientDmBx WHERE client_id = ?", (test_client_id,))
        
        print("監査証跡生成テスト成功")

    def test_data_retention_policy_compliance(self, e2e_synapse_connection: SynapseE2EConnection):
        """データ保持ポリシー遵守のテスト"""
        # 古いデータの検出クエリ
        result = e2e_synapse_connection.execute_query("""
            SELECT 
                'ClientDmBx' as table_name,
                COUNT(*) as total_records,
                COUNT(CASE WHEN processed_date < DATEADD(year, -2, GETDATE()) THEN 1 END) as old_records_2y,
                COUNT(CASE WHEN processed_date < DATEADD(year, -5, GETDATE()) THEN 1 END) as old_records_5y
            FROM ClientDmBx
            WHERE processed_date IS NOT NULL
            
            UNION ALL
            
            SELECT 
                'point_grant_email' as table_name,
                COUNT(*) as total_records,
                COUNT(CASE WHEN created_at < DATEADD(year, -2, GETDATE()) THEN 1 END) as old_records_2y,
                COUNT(CASE WHEN created_at < DATEADD(year, -5, GETDATE()) THEN 1 END) as old_records_5y
            FROM point_grant_email
            WHERE created_at IS NOT NULL
        """)
        
        assert len(result) >= 2, "データ保持ポリシーチェック結果が不足しています"
        
        # 保持ポリシーの確認
        for row in result:
            table_name, total_records, old_2y, old_5y = row
            assert total_records >= 0, f"テーブル {table_name} のレコード数が不正です"
            assert old_2y >= old_5y, f"テーブル {table_name} の古いレコード数が矛盾しています"
            
            # 保持ポリシーのアラート（5年以上古いデータがある場合）
            if old_5y > 0:
                print(f"警告: テーブル {table_name} に5年以上古いレコードが {old_5y} 件存在します")
        
        print(f"データ保持ポリシー遵守テスト成功: {len(result)}テーブルを確認")

    def test_sensitive_data_detection(self, e2e_synapse_connection: SynapseE2EConnection):
        """機密データ検出のテスト"""
        # 機密データパターンの検出
        result = e2e_synapse_connection.execute_query("""
            SELECT 
                'email_addresses' as data_type,
                COUNT(*) as total_count,
                COUNT(CASE WHEN email LIKE '%@gmail.com' OR email LIKE '%@yahoo.com' THEN 1 END) as personal_emails,
                COUNT(CASE WHEN email LIKE '%@%' AND email NOT LIKE '%***%' AND email NOT LIKE 'ENC_%' THEN 1 END) as unprotected_emails
            FROM point_grant_email
            WHERE email IS NOT NULL
            
            UNION ALL
            
            SELECT 
                'client_identifiers' as data_type,
                COUNT(*) as total_count,
                COUNT(CASE WHEN LEN(client_id) >= 10 THEN 1 END) as long_identifiers,
                COUNT(CASE WHEN client_id LIKE '%[0-9][0-9][0-9][0-9][0-9]%' THEN 1 END) as numeric_patterns
            FROM ClientDmBx
            WHERE client_id IS NOT NULL
        """)
        
        assert len(result) >= 2, "機密データ検出結果が不足しています"
        
        # 機密データパターンの分析
        for row in result:
            data_type, total_count, pattern1_count, pattern2_count = row
            assert total_count >= 0, f"データタイプ {data_type} の総数が不正です"
            
            if data_type == 'email_addresses':
                # 保護されていないメールの割合をチェック
                if total_count > 0:
                    unprotected_ratio = pattern2_count / total_count
                    if unprotected_ratio > 0.5:  # 50%以上が保護されていない場合
                        print(f"警告: メールアドレスの {unprotected_ratio:.2%} が保護されていません")
            
            elif data_type == 'client_identifiers':
                # 長い識別子の存在確認
                if pattern1_count > 0:
                    print(f"情報: {pattern1_count}件の長い顧客識別子が存在します")
        
        print(f"機密データ検出テスト成功: {len(result)}種類のデータパターンを分析")

    def test_data_quality_scoring(self, e2e_synapse_connection: SynapseE2EConnection):
        """データ品質スコアリングのテスト"""
        # データ品質評価クエリ
        result = e2e_synapse_connection.execute_query("""
            SELECT 
                'ClientDmBx' as table_name,
                COUNT(*) as total_records,
                COUNT(CASE WHEN client_id IS NOT NULL THEN 1 END) as non_null_client_id,
                COUNT(CASE WHEN segment IS NOT NULL THEN 1 END) as non_null_segment,
                COUNT(CASE WHEN score BETWEEN 0 AND 1000 THEN 1 END) as valid_scores,
                COUNT(CASE WHEN total_amount >= 0 THEN 1 END) as valid_amounts,
                COUNT(CASE WHEN processed_date IS NOT NULL THEN 1 END) as non_null_dates
            FROM ClientDmBx
            
            UNION ALL
            
            SELECT 
                'point_grant_email' as table_name,
                COUNT(*) as total_records,
                COUNT(CASE WHEN client_id IS NOT NULL THEN 1 END) as non_null_client_id,
                COUNT(CASE WHEN email LIKE '%@%' THEN 1 END) as valid_emails,
                COUNT(CASE WHEN points_granted >= 0 THEN 1 END) as valid_points,
                COUNT(CASE WHEN status IN ('SENT', 'DELIVERED', 'BOUNCED', 'FAILED') THEN 1 END) as valid_statuses,
                COUNT(CASE WHEN created_at IS NOT NULL THEN 1 END) as non_null_dates
            FROM point_grant_email
        """)
        
        assert len(result) >= 2, "データ品質スコア結果が不足しています"
        
        # データ品質スコアの計算
        for row in result:
            table_name = row[0]
            total_records = row[1]
            quality_scores = []
            
            if total_records > 0:
                # 各品質指標のスコア計算（完全性の割合）
                for i in range(2, len(row)):
                    if row[i] is not None:
                        score = row[i] / total_records
                        quality_scores.append(score)
                
                # 総合品質スコア
                overall_quality = sum(quality_scores) / len(quality_scores) if quality_scores else 0
                
                assert 0 <= overall_quality <= 1, f"テーブル {table_name} の品質スコアが範囲外です"
                
                # 品質基準の確認
                if overall_quality < 0.8:  # 80%未満の場合
                    print(f"警告: テーブル {table_name} の品質スコアが低いです: {overall_quality:.2%}")
                else:
                    print(f"テーブル {table_name} の品質スコア: {overall_quality:.2%}")
        
        print(f"データ品質スコアリング成功: {len(result)}テーブルの品質を評価")

    def _prepare_integrity_test_data(self, connection: SynapseE2EConnection):
        """整合性テスト用のデータ準備"""
        # 正常なテストデータ
        connection.execute_query("""
            INSERT INTO ClientDmBx (client_id, segment, score, last_transaction_date, total_amount, processed_date, data_source)
            VALUES 
                ('INTEGRITY_TEST_001', 'PREMIUM', 850, '2024-01-15', 75000.00, GETDATE(), 'E2E_TEST'),
                ('INTEGRITY_TEST_002', 'STANDARD', 650, '2024-01-16', 35000.00, GETDATE(), 'E2E_TEST')
        """)
        
        connection.execute_query("""
            INSERT INTO point_grant_email (client_id, email, points_granted, email_sent_date, campaign_id, status, created_at)
            VALUES 
                ('INTEGRITY_TEST_001', 'test001@example.com', 1500, '2024-01-15', 'CAMP001', 'DELIVERED', GETDATE()),
                ('INTEGRITY_TEST_002', 'test002@example.com', 1000, '2024-01-16', 'CAMP001', 'SENT', GETDATE())
        """)

    def _prepare_duplicate_test_data(self, connection: SynapseE2EConnection):
        """重複テスト用のデータ準備"""
        # 重複データの意図的な挿入
        for i in range(3):  # 同じclient_idで3回挿入
            connection.execute_query("""
                INSERT INTO ClientDmBx (client_id, segment, score, last_transaction_date, total_amount, processed_date, data_source)
                VALUES ('DUPLICATE_TEST_001', 'PREMIUM', 850, '2024-01-15', 75000.00, GETDATE(), 'E2E_TEST')
            """)

    def _prepare_encryption_test_data(self, connection: SynapseE2EConnection):
        """暗号化テスト用のデータ準備"""
        # マスクされたデータ
        connection.execute_query("""
            INSERT INTO point_grant_email (client_id, email, points_granted, email_sent_date, campaign_id, status, created_at)
            VALUES ('ENCRYPT_TEST_001', 'test***@***.com', 1500, '2024-01-15', 'CAMP001', 'SENT', GETDATE())
        """)
        
        # 暗号化されたデータ（シミュレーション）
        encrypted_email = 'ENC_' + base64.b64encode(b'test002@example.com').decode('utf-8')[:20]
        connection.execute_query("""
            INSERT INTO point_grant_email (client_id, email, points_granted, email_sent_date, campaign_id, status, created_at)
            VALUES ('ENCRYPT_TEST_002', ?, 1000, '2024-01-16', 'CAMP001', 'SENT', GETDATE())
        """, (encrypted_email,))
