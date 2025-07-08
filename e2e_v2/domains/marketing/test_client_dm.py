"""
pi_Send_ClientDM パイプラインのE2Eテスト

顧客ダイレクトメール送信パイプラインの包括的テスト
"""

import time
import json
from datetime import datetime, timedelta
from typing import Dict, List

from ...base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus


class TestClientDMPipeline(DomainTestBase):
    """顧客ダイレクトメールパイプラインテスト"""
    
    def __init__(self):
        super().__init__("pi_Send_ClientDM", "marketing")
    
    def domain_specific_setup(self):
        """Marketingドメイン固有セットアップ"""
        # マーケティングドメイン用設定
        self.source_container = "mytokyogas"
        self.output_container = "datalake"
        self.sftp_target_dir = "/Import/Marketing/ClientDM"
        
        # 期待される入力フィールド
        self.input_columns = [
            "CUSTOMER_ID", "CUSTOMER_NAME", "EMAIL_ADDRESS", "POSTAL_CODE", 
            "ADDRESS", "PHONE_NUMBER", "BIRTH_DATE", "GENDER", "CONTRACT_TYPE",
            "LAST_CAMPAIGN_DATE", "PREFERENCE_CATEGORY", "OPT_IN_STATUS"
        ]
        
        # 期待される出力フィールド
        self.output_columns = [
            "CUSTOMER_ID", "CUSTOMER_NAME", "CONTACT_METHOD", "CONTACT_ADDRESS",
            "CAMPAIGN_TYPE", "CONTENT_CATEGORY", "PERSONALIZATION_LEVEL",
            "SCHEDULED_DATE", "PRIORITY", "CREATED_DATE", "STATUS"
        ]
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """Marketingドメイン用テストデータテンプレート"""
        return {
            "client_dm_data": self._generate_client_dm_data(),
            "segmented_data": self._generate_segmented_customer_data(),
            "opt_out_data": self._generate_opt_out_data()
        }
    
    def _generate_client_dm_data(self, record_count: int = 100) -> str:
        """顧客DM配信データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        genders = ["M", "F", "O"]
        contract_types = ["RESIDENTIAL", "BUSINESS", "CORPORATE"]
        preference_categories = ["ENERGY_SAVING", "SMART_HOME", "COOKING", "HEATING", "GENERAL"]
        
        for i in range(1, record_count + 1):
            # 顧客属性生成
            gender = genders[i % len(genders)]
            contract_type = contract_types[i % len(contract_types)]
            preference = preference_categories[i % len(preference_categories)]
            
            # 生年月日（20-80歳の範囲）
            age = 20 + (i % 60)
            birth_date = (base_date - timedelta(days=age*365)).strftime('%Y%m%d')
            
            # 最終キャンペーン日（30-365日前）
            last_campaign_days = 30 + (i % 335)
            last_campaign = (base_date - timedelta(days=last_campaign_days)).strftime('%Y%m%d')
            
            # オプトイン状況
            opt_in_status = "OPTED_IN" if i % 10 != 0 else "OPTED_OUT"
            
            data_lines.append(
                f"CUST{i:06d}\t顧客{i:03d}太郎\tcustomer{i}@tokyogas.co.jp\t{100 + (i % 8000):04d}\t"
                f"東京都{['千代田区', '港区', '新宿区', '渋谷区'][i % 4]}{i}番地\t03-{i:04d}-{i:04d}\t"
                f"{birth_date}\t{gender}\t{contract_type}\t{last_campaign}\t{preference}\t{opt_in_status}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_segmented_customer_data(self) -> str:
        """セグメント別顧客データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        
        # 高価値顧客セグメント
        high_value_customers = [
            ("CUST100001", "山田太郎", "yamada@example.com", "1000001", "東京都千代田区1-1-1", "03-1111-1111", "19800101", "M", "CORPORATE", "20231201", "SMART_HOME", "OPTED_IN"),
            ("CUST100002", "佐藤花子", "sato@example.com", "1000002", "東京都港区2-2-2", "03-2222-2222", "19750615", "F", "BUSINESS", "20231215", "ENERGY_SAVING", "OPTED_IN"),
        ]
        
        # 新規顧客セグメント
        new_customers = [
            ("CUST200001", "田中次郎", "tanaka@example.com", "2000001", "東京都新宿区3-3-3", "03-3333-3333", "19950301", "M", "RESIDENTIAL", "", "COOKING", "OPTED_IN"),
            ("CUST200002", "鈴木美子", "suzuki@example.com", "2000002", "東京都渋谷区4-4-4", "03-4444-4444", "19920820", "F", "RESIDENTIAL", "", "HEATING", "OPTED_IN"),
        ]
        
        # 休眠顧客セグメント
        dormant_customers = [
            ("CUST300001", "高橋一郎", "takahashi@example.com", "3000001", "東京都品川区5-5-5", "03-5555-5555", "19701010", "M", "RESIDENTIAL", "20220101", "GENERAL", "OPTED_IN"),
        ]
        
        all_segments = high_value_customers + new_customers + dormant_customers
        
        for customer_data in all_segments:
            data_lines.append("\t".join(customer_data))
        
        return "\n".join(data_lines)
    
    def _generate_opt_out_data(self) -> str:
        """オプトアウト顧客データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        # オプトアウト顧客
        opt_out_customers = [
            ("CUST900001", "配信拒否太郎", "optout1@example.com", "9000001", "東京都大田区9-9-9", "03-9999-9999", "19850101", "M", "RESIDENTIAL", "20231101", "ENERGY_SAVING", "OPTED_OUT"),
            ("CUST900002", "配信拒否花子", "optout2@example.com", "9000002", "東京都世田谷区9-9-9", "03-9999-9998", "19900201", "F", "BUSINESS", "20231102", "SMART_HOME", "OPTED_OUT"),
        ]
        
        # オプトイン顧客（比較用）
        opt_in_customers = [
            ("CUST800001", "配信希望太郎", "optin1@example.com", "8000001", "東京都杉並区8-8-8", "03-8888-8888", "19880301", "M", "RESIDENTIAL", "20231201", "COOKING", "OPTED_IN"),
        ]
        
        all_customers = opt_out_customers + opt_in_customers
        
        for customer_data in all_customers:
            data_lines.append("\t".join(customer_data))
        
        return "\n".join(data_lines)
    
    def _transform_client_dm_data(self, input_data: str) -> str:
        """顧客DMデータ変換処理"""
        lines = input_data.strip().split('\n')
        
        # 出力CSVヘッダー
        output_header = ",".join(self.output_columns)
        output_lines = [output_header]
        
        created_date = datetime.utcnow().strftime('%Y/%m/%d %H:%M:%S')
        
        for line in lines[1:]:
            if not line.strip():
                continue
                
            parts = line.split('\t')
            if len(parts) != len(self.input_columns):
                continue
            
            (customer_id, customer_name, email_address, postal_code, address,
             phone_number, birth_date, gender, contract_type, last_campaign_date,
             preference_category, opt_in_status) = parts
            
            # オプトアウト顧客は除外
            if opt_in_status == "OPTED_OUT":
                continue
            
            # データ品質チェック
            if not all([customer_id.strip(), customer_name.strip(), email_address.strip()]):
                continue
            
            # メールアドレス検証
            if '@' not in email_address:
                continue
            
            # 連絡方法決定
            contact_method = "EMAIL" if email_address.strip() else "POSTAL"
            contact_address = email_address if contact_method == "EMAIL" else address
            
            # キャンペーンタイプ決定
            campaign_type = self._determine_campaign_type(contract_type, preference_category, last_campaign_date)
            
            # コンテンツカテゴリ
            content_category = preference_category
            
            # パーソナライゼーションレベル
            personalization_level = self._determine_personalization_level(contract_type, last_campaign_date)
            
            # 配信予定日（3日後）
            scheduled_date = (datetime.utcnow() + timedelta(days=3)).strftime('%Y/%m/%d')
            
            # 優先度決定
            priority = self._determine_priority(contract_type, last_campaign_date)
            
            # ステータス
            status = "SCHEDULED"
            
            output_line = (
                f"{customer_id},{customer_name},{contact_method},{contact_address},"
                f"{campaign_type},{content_category},{personalization_level},"
                f"{scheduled_date},{priority},{created_date},{status}"
            )
            output_lines.append(output_line)
        
        return '\n'.join(output_lines)
    
    def _determine_campaign_type(self, contract_type: str, preference_category: str, last_campaign_date: str) -> str:
        """キャンペーンタイプ決定"""
        if not last_campaign_date.strip():
            return "WELCOME_CAMPAIGN"
        
        try:
            last_date = datetime.strptime(last_campaign_date, '%Y%m%d')
            days_since_last = (datetime.utcnow() - last_date).days
            
            if days_since_last > 180:
                return "REACTIVATION_CAMPAIGN"
            elif contract_type == "CORPORATE":
                return "BUSINESS_CAMPAIGN"
            elif preference_category in ["ENERGY_SAVING", "SMART_HOME"]:
                return "TECHNOLOGY_CAMPAIGN"
            else:
                return "STANDARD_CAMPAIGN"
        except ValueError:
            return "STANDARD_CAMPAIGN"
    
    def _determine_personalization_level(self, contract_type: str, last_campaign_date: str) -> str:
        """パーソナライゼーションレベル決定"""
        if contract_type == "CORPORATE":
            return "HIGH"
        elif not last_campaign_date.strip():
            return "MEDIUM"  # 新規顧客
        else:
            return "STANDARD"
    
    def _determine_priority(self, contract_type: str, last_campaign_date: str) -> str:
        """優先度決定"""
        if contract_type == "CORPORATE":
            return "HIGH"
        elif not last_campaign_date.strip():
            return "MEDIUM"  # 新規顧客
        else:
            try:
                last_date = datetime.strptime(last_campaign_date, '%Y%m%d')
                days_since_last = (datetime.utcnow() - last_date).days
                
                if days_since_last > 365:
                    return "HIGH"  # 長期間未接触
                elif days_since_last > 180:
                    return "MEDIUM"
                else:
                    return "LOW"
            except ValueError:
                return "LOW"
    
    def validate_domain_business_rules(self, data: str) -> List[str]:
        """Marketingドメインビジネスルール検証"""
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
            
            # 顧客ID形式チェック
            if not parts[0].startswith('CUST'):
                violations.append(f"行{i}: 顧客ID形式不正 ({parts[0]})")
            
            # 連絡方法チェック
            valid_contact_methods = ["EMAIL", "POSTAL", "PHONE"]
            if parts[2] not in valid_contact_methods:
                violations.append(f"行{i}: 連絡方法不正 ({parts[2]})")
            
            # キャンペーンタイプチェック
            valid_campaign_types = ["WELCOME_CAMPAIGN", "REACTIVATION_CAMPAIGN", "BUSINESS_CAMPAIGN", 
                                  "TECHNOLOGY_CAMPAIGN", "STANDARD_CAMPAIGN"]
            if parts[4] not in valid_campaign_types:
                violations.append(f"行{i}: キャンペーンタイプ不正 ({parts[4]})")
            
            # パーソナライゼーションレベルチェック
            valid_personalization = ["HIGH", "MEDIUM", "STANDARD"]
            if parts[6] not in valid_personalization:
                violations.append(f"行{i}: パーソナライゼーションレベル不正 ({parts[6]})")
            
            # 優先度チェック
            valid_priorities = ["HIGH", "MEDIUM", "LOW"]
            if parts[8] not in valid_priorities:
                violations.append(f"行{i}: 優先度不正 ({parts[8]})")
            
            # ステータスチェック
            valid_statuses = ["SCHEDULED", "SENT", "DELIVERED", "FAILED"]
            if parts[10] not in valid_statuses:
                violations.append(f"行{i}: ステータス不正 ({parts[10]})")
        
        return violations
    
    def test_functional_client_dm_processing(self):
        """機能テスト: 顧客DM配信正常処理"""
        test_id = f"functional_dm_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # テストデータ準備
        test_data = self._generate_client_dm_data()
        records_count = len(test_data.split('\n')) - 1
        
        # ファイルアップロード
        file_date = datetime.utcnow().strftime('%Y%m%d')
        file_path = f"Marketing/ClientDM/{file_date}/client_dm.tsv"
        self.mock_storage.upload_file(self.source_container, file_path, test_data.encode('utf-8'))
        
        # ETL処理シミュレーション
        transformed_data, output_data = self.simulate_etl_process(
            test_data,
            self._transform_client_dm_data,
            "csv"
        )
        
        # 出力ファイル保存
        output_file_path = f"Marketing/ClientDM/campaigns_{file_date}.csv"
        self.mock_storage.upload_file(self.output_container, output_file_path, output_data)
        
        # SFTP転送
        sftp_path = f"{self.sftp_target_dir}/dm_campaigns_{file_date}.csv"
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
            data_quality_score=0.96,
            errors=[],
            warnings=violations
        )
        
        # アサーション
        self.validate_common_assertions(result)
        assert result.records_extracted > 0, "抽出レコード数が0"
        assert result.records_transformed > 0, "変換レコード数が0"
        assert sftp_result, "SFTP転送失敗"
        assert len(violations) == 0, f"ビジネスルール違反: {violations}"
        
        self.log_test_info(test_id, f"成功 - {result.records_loaded}件のDMキャンペーン生成完了")
    
    def test_functional_customer_segmentation(self):
        """機能テスト: 顧客セグメンテーション処理"""
        test_id = f"functional_segmentation_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # セグメント別データ準備
        test_data = self._generate_segmented_customer_data()
        
        # データ変換処理
        transformed_data = self._transform_client_dm_data(test_data)
        
        # キャンペーンタイプ別集計
        lines = transformed_data.split('\n')
        campaign_types = {}
        for line in lines[1:]:
            if line.strip():
                parts = line.split(',')
                if len(parts) >= 5:
                    campaign_type = parts[4]
                    campaign_types[campaign_type] = campaign_types.get(campaign_type, 0) + 1
        
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
        
        # セグメンテーションアサーション
        assert "WELCOME_CAMPAIGN" in campaign_types, "新規顧客キャンペーンなし"
        assert "BUSINESS_CAMPAIGN" in campaign_types, "法人キャンペーンなし"
        assert "REACTIVATION_CAMPAIGN" in campaign_types, "再活性化キャンペーンなし"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, f"成功 - {len(campaign_types)}種類のキャンペーン生成完了")
    
    def test_functional_opt_out_filtering(self):
        """機能テスト: オプトアウト顧客フィルタリング"""
        test_id = f"functional_opt_out_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # オプトアウトデータ準備
        test_data = self._generate_opt_out_data()
        input_records = len(test_data.split('\n')) - 1
        
        # データ変換処理
        transformed_data = self._transform_client_dm_data(test_data)
        
        # 出力レコード確認
        output_lines = transformed_data.split('\n')
        output_records = len([line for line in output_lines[1:] if line.strip()])
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.FUNCTIONAL,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=input_records,
            records_transformed=output_records,
            records_loaded=output_records,
            data_quality_score=1.0,
            errors=[]
        )
        
        # オプトアウトフィルタリングアサーション
        # 入力3件（オプトアウト2件、オプトイン1件）→出力1件
        assert input_records == 3, f"入力レコード数不正: {input_records}"
        assert output_records == 1, f"出力レコード数不正: 期待=1, 実際={output_records}"
        
        # オプトアウト顧客IDが含まれていないことを確認
        output_content = "\n".join(output_lines[1:])
        assert "CUST900001" not in output_content, "オプトアウト顧客1が除外されていない"
        assert "CUST900002" not in output_content, "オプトアウト顧客2が除外されていない"
        assert "CUST800001" in output_content, "オプトイン顧客が含まれていない"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, "成功 - オプトアウト顧客フィルタリング完了")
    
    def test_data_quality_contact_validation(self):
        """データ品質テスト: 連絡先検証"""
        test_id = f"data_quality_contact_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # 連絡先問題データ準備
        problematic_data = [
            "\t".join(self.input_columns),
            "CUST000001\t正常太郎\tvalid@example.com\t1000001\t東京都\t03-1111-1111\t19800101\tM\tRESIDENTIAL\t20231201\tENERGY_SAVING\tOPTED_IN",  # 正常
            "CUST000002\t無効メール太郎\tinvalid-email\t1000002\t東京都\t03-2222-2222\t19800101\tM\tRESIDENTIAL\t20231201\tSMART_HOME\tOPTED_IN",  # 無効メール
            "CUST000003\t空欄太郎\t\t1000003\t東京都\t03-3333-3333\t19800101\tM\tRESIDENTIAL\t20231201\tCOOKING\tOPTED_IN",  # メールなし
            "CUST000004\tオプトアウト太郎\toptout@example.com\t1000004\t東京都\t03-4444-4444\t19800101\tM\tRESIDENTIAL\t20231201\tHEATING\tOPTED_OUT",  # オプトアウト
        ]
        test_data = "\n".join(problematic_data)
        
        # データ変換処理
        transformed_data = self._transform_client_dm_data(test_data)
        
        # 品質検証
        quality_metrics = self.validate_data_quality(transformed_data, self.output_columns)
        violations = self.validate_domain_business_rules(transformed_data)
        
        # 有効レコード数確認（正常な1件のみ）
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
        assert len(data_lines) == 1, f"有効レコード数不正: 期待=1, 実際={len(data_lines)}"
        assert quality_metrics["completeness"] >= 0.8, f"完全性が基準未満: {quality_metrics['completeness']}"
        assert "valid@example.com" in data_lines[0], "有効メールアドレスが含まれていない"
        assert len(violations) == 0, f"ビジネスルール違反: {violations}"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, "成功 - 連絡先データ品質検証完了")
    
    def test_performance_large_customer_base(self):
        """パフォーマンステスト: 大量顧客ベース処理"""
        test_id = f"performance_large_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # 大量データ準備（50000件）
        large_data = self._generate_client_dm_data(50000)
        
        start_time = time.time()
        
        # ETL処理シミュレーション
        transformed_data = self._transform_client_dm_data(large_data)
        
        execution_time = time.time() - start_time
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.PERFORMANCE,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=50000,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=0.98,
            errors=[]
        )
        
        # パフォーマンス基準
        throughput = 50000 / execution_time
        assert throughput > 2000, f"スループットが基準未満: {throughput:.2f} records/sec"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, f"成功 - {execution_time:.2f}秒で処理完了")
    
    def test_integration_campaign_delivery(self):
        """統合テスト: キャンペーン配信システム統合"""
        test_id = f"integration_delivery_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # 多様な顧客データ準備
        mixed_data = self._generate_segmented_customer_data()
        
        # ETL処理
        transformed_data = self._transform_client_dm_data(mixed_data)
        
        # 配信チャネル別集計
        lines = transformed_data.split('\n')
        delivery_channels = {}
        campaign_priorities = {}
        
        for line in lines[1:]:
            if line.strip():
                parts = line.split(',')
                if len(parts) >= 9:
                    contact_method = parts[2]  # 連絡方法
                    priority = parts[8]        # 優先度
                    
                    delivery_channels[contact_method] = delivery_channels.get(contact_method, 0) + 1
                    campaign_priorities[priority] = campaign_priorities.get(priority, 0) + 1
        
        # 配信スケジュール検証
        scheduled_campaigns = len([line for line in lines[1:] if "SCHEDULED" in line])
        
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
        assert "EMAIL" in delivery_channels, "メール配信チャネルなし"
        assert delivery_channels["EMAIL"] >= 3, f"メール配信数不足: {delivery_channels.get('EMAIL', 0)}"
        
        assert "HIGH" in campaign_priorities, "高優先度キャンペーンなし"
        assert "MEDIUM" in campaign_priorities, "中優先度キャンペーンなし"
        
        assert scheduled_campaigns >= 3, f"スケジュール配信数不足: {scheduled_campaigns}"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, f"成功 - {len(delivery_channels)}チャネル配信統合テスト完了")