"""
pi_Copy_marketing_client_dm パイプラインのE2Eテスト

マーケティング顧客データマート複製パイプラインの包括的テスト
"""

import time
import gzip
import json
from datetime import datetime, timedelta
from typing import Dict, List

from ...base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus


class TestMarketingClientDmPipeline(DomainTestBase):
    """マーケティング顧客データマート複製パイプラインテスト"""
    
    def __init__(self):
        super().__init__("pi_Copy_marketing_client_dm", "infrastructure")
    
    def domain_specific_setup(self):
        """Infrastructureドメイン固有セットアップ"""
        # インフラストラクチャドメイン用設定
        self.source_container = "mytokyogas"
        self.staging_container = "staging"
        self.output_container = "datalake"
        
        # ステージング用コンテナ作成
        self.mock_storage.create_container(self.staging_container)
        
        # 期待される入力フィールド
        self.input_columns = [
            "CUSTOMER_ID", "CUSTOMER_NAME", "EMAIL_ADDRESS", "PHONE_NUMBER",
            "POSTAL_CODE", "ADDRESS", "BIRTH_DATE", "GENDER", "REGISTRATION_DATE",
            "LAST_LOGIN_DATE", "CONTRACT_STATUS", "TOTAL_PURCHASE_AMOUNT",
            "PREFERRED_COMMUNICATION", "MARKETING_CONSENT"
        ]
        
        # 期待される出力フィールド（正規化後）
        self.output_columns = [
            "CUSTOMER_ID", "CUSTOMER_NAME", "PRIMARY_EMAIL", "PRIMARY_PHONE",
            "POSTAL_CODE", "NORMALIZED_ADDRESS", "BIRTH_DATE", "GENDER_CODE",
            "REGISTRATION_DATE", "LAST_ACTIVITY_DATE", "CONTRACT_STATUS",
            "LIFETIME_VALUE", "COMMUNICATION_PREFERENCE", "MARKETING_OPT_IN",
            "DATA_QUALITY_SCORE", "LAST_UPDATED", "SOURCE_SYSTEM"
        ]
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """Infrastructureドメイン用テストデータテンプレート"""
        return {
            "client_dm_data": self._generate_client_dm_data(),
            "data_quality_issues": self._generate_data_quality_issues(),
            "incremental_data": self._generate_incremental_data()
        }
    
    def _generate_client_dm_data(self, record_count: int = 1000) -> str:
        """顧客データマートデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        genders = ["M", "F", "O", "N"]  # Male, Female, Other, Not specified
        contract_statuses = ["ACTIVE", "INACTIVE", "SUSPENDED", "TERMINATED"]
        comm_preferences = ["EMAIL", "PHONE", "POSTAL", "SMS"]
        
        for i in range(1, record_count + 1):
            # 基本属性
            customer_name = f"顧客{i:04d}太郎" if i % 2 == 1 else f"顧客{i:04d}花子"
            email = f"customer{i:06d}@tokyogas.co.jp"
            phone = f"03-{i:04d}-{(i*7) % 10000:04d}"
            
            # 住所データ
            postal_code = f"{100 + (i % 900):03d}-{i % 10000:04d}"
            prefectures = ["東京都", "神奈川県", "千葉県", "埼玉県"]
            cities = ["千代田区", "港区", "新宿区", "渋谷区", "横浜市", "川崎市", "千葉市", "さいたま市"]
            address = f"{prefectures[i % len(prefectures)]}{cities[i % len(cities)]}{i}番地"
            
            # 日付データ
            age = 20 + (i % 60)
            birth_date = (base_date - timedelta(days=age*365)).strftime('%Y%m%d')
            registration_days = 30 + (i % 1000)
            registration_date = (base_date - timedelta(days=registration_days)).strftime('%Y%m%d')
            last_login_days = i % 90
            last_login_date = (base_date - timedelta(days=last_login_days)).strftime('%Y%m%d')
            
            # ビジネス属性
            gender = genders[i % len(genders)]
            contract_status = contract_statuses[i % len(contract_statuses)]
            total_purchase = (i % 50 + 1) * 10000  # 10,000 - 500,000円
            preferred_comm = comm_preferences[i % len(comm_preferences)]
            marketing_consent = "Y" if i % 5 != 0 else "N"  # 80%が同意
            
            data_lines.append(
                f"CUST{i:06d}\t{customer_name}\t{email}\t{phone}\t{postal_code}\t"
                f"{address}\t{birth_date}\t{gender}\t{registration_date}\t{last_login_date}\t"
                f"{contract_status}\t{total_purchase}\t{preferred_comm}\t{marketing_consent}"
            )
        
        return "\n".join(data_lines)
    
    def _generate_data_quality_issues(self) -> str:
        """データ品質問題を含むデータ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        # 品質問題データ
        quality_issues = [
            # 正常データ
            "CUST000001\t正常太郎\tvalid@tokyogas.co.jp\t03-1111-1111\t100-0001\t東京都千代田区1-1-1\t19800101\tM\t20200101\t20241201\tACTIVE\t100000\tEMAIL\tY",
            # 重複データ
            "CUST000001\t重複太郎\tduplicate@tokyogas.co.jp\t03-1111-1111\t100-0001\t東京都千代田区1-1-1\t19800101\tM\t20200101\t20241201\tACTIVE\t100000\tEMAIL\tY",
            # 無効メールアドレス
            "CUST000002\t無効メール太郎\tinvalid-email\t03-2222-2222\t100-0002\t東京都港区2-2-2\t19850101\tM\t20200201\t20241202\tACTIVE\t150000\tEMAIL\tY",
            # 無効電話番号
            "CUST000003\t無効電話太郎\tvalid3@tokyogas.co.jp\tabc-defg-hijk\t100-0003\t東京都新宿区3-3-3\t19900101\tM\t20200301\t20241203\tACTIVE\t200000\tPHONE\tY",
            # 無効生年月日
            "CUST000004\t無効生年月日太郎\tvalid4@tokyogas.co.jp\t03-4444-4444\t100-0004\t東京都渋谷区4-4-4\t99999999\tM\t20200401\t20241204\tACTIVE\t250000\tEMAIL\tY",
            # NULL値
            "CUST000005\t\t\t\t\t\t\t\t\t\t\t\t\t",
            # 無効金額
            "CUST000006\t無効金額太郎\tvalid6@tokyogas.co.jp\t03-6666-6666\t100-0006\t東京都品川区6-6-6\t19950101\tM\t20200601\t20241206\tACTIVE\t-100000\tEMAIL\tY",
        ]
        
        for issue_data in quality_issues:
            data_lines.append(issue_data)
        
        return "\n".join(data_lines)
    
    def _generate_incremental_data(self) -> str:
        """増分データ生成"""
        data_lines = ["\t".join(self.input_columns)]
        
        base_date = datetime.utcnow()
        
        # 新規登録顧客（過去7日以内）
        new_customers = []
        for i in range(10001, 10011):
            registration_days = i % 7  # 0-6日前
            registration_date = (base_date - timedelta(days=registration_days)).strftime('%Y%m%d')
            last_login_date = registration_date  # 登録日と同じ
            
            new_customers.append(
                f"CUST{i:06d}\t新規顧客{i:04d}\tnew{i}@tokyogas.co.jp\t03-{i:04d}-0000\t"
                f"100-{i % 10000:04d}\t東京都新宿区新規{i}番地\t19900101\tM\t{registration_date}\t"
                f"{last_login_date}\tACTIVE\t50000\tEMAIL\tY"
            )
        
        # 更新された既存顧客
        updated_customers = []
        for i in range(1, 11):
            last_login_date = (base_date - timedelta(days=1)).strftime('%Y%m%d')  # 昨日ログイン
            
            updated_customers.append(
                f"CUST{i:06d}\t更新顧客{i:04d}\tupdate{i}@tokyogas.co.jp\t03-{i:04d}-9999\t"
                f"100-{i:04d}\t東京都港区更新{i}番地\t19800101\tM\t20200101\t"
                f"{last_login_date}\tACTIVE\t200000\tEMAIL\tY"
            )
        
        all_incremental = new_customers + updated_customers
        for customer_data in all_incremental:
            data_lines.append(customer_data)
        
        return "\n".join(data_lines)
    
    def _transform_client_dm_data(self, input_data: str) -> str:
        """顧客データマート変換処理（正規化・品質向上）"""
        lines = input_data.strip().split('\n')
        
        # 出力CSVヘッダー
        output_header = ",".join(self.output_columns)
        output_lines = [output_header]
        
        last_updated = datetime.utcnow().strftime('%Y/%m/%d %H:%M:%S')
        processed_ids = set()  # 重複除去用
        
        for line in lines[1:]:
            if not line.strip():
                continue
                
            parts = line.split('\t')
            if len(parts) != len(self.input_columns):
                continue
            
            (customer_id, customer_name, email_address, phone_number, postal_code,
             address, birth_date, gender, registration_date, last_login_date,
             contract_status, total_purchase_amount, preferred_communication, marketing_consent) = parts
            
            # 重複チェック
            if customer_id in processed_ids:
                continue
            
            # データ品質チェック
            if not all([customer_id.strip(), customer_name.strip()]):
                continue
            
            # メールアドレス正規化
            primary_email = self._normalize_email(email_address)
            if not primary_email:
                continue  # 無効なメールは除外
            
            # 電話番号正規化
            primary_phone = self._normalize_phone(phone_number)
            
            # 住所正規化
            normalized_address = self._normalize_address(address)
            
            # 生年月日検証
            if not self._validate_birth_date(birth_date):
                continue
            
            # 性別コード正規化
            gender_code = self._normalize_gender(gender)
            
            # 最終活動日（登録日とログイン日の新しい方）
            last_activity_date = self._get_last_activity_date(registration_date, last_login_date)
            
            # ライフタイムバリュー計算
            try:
                lifetime_value = max(0, int(total_purchase_amount))
            except (ValueError, TypeError):
                lifetime_value = 0
            
            # 通信設定正規化
            communication_preference = self._normalize_communication_preference(preferred_communication)
            
            # マーケティング同意正規化
            marketing_opt_in = "1" if marketing_consent.upper() == "Y" else "0"
            
            # データ品質スコア計算
            data_quality_score = self._calculate_data_quality_score(
                primary_email, primary_phone, normalized_address, birth_date, lifetime_value
            )
            
            # 処理済みIDに追加
            processed_ids.add(customer_id)
            
            output_line = (
                f"{customer_id},{customer_name},{primary_email},{primary_phone},"
                f"{postal_code},{normalized_address},{birth_date},{gender_code},"
                f"{registration_date},{last_activity_date},{contract_status},{lifetime_value},"
                f"{communication_preference},{marketing_opt_in},{data_quality_score:.2f},"
                f"{last_updated},MARKETING_DM_SYSTEM"
            )
            output_lines.append(output_line)
        
        return '\n'.join(output_lines)
    
    def _normalize_email(self, email: str) -> str:
        """メールアドレス正規化"""
        if not email or '@' not in email:
            return ""
        
        # 小文字化とトリミング
        normalized = email.strip().lower()
        
        # 基本的な形式チェック
        if '.' not in normalized.split('@')[1]:
            return ""
        
        return normalized
    
    def _normalize_phone(self, phone: str) -> str:
        """電話番号正規化"""
        if not phone:
            return ""
        
        # 数字とハイフンのみ抽出
        digits = ''.join(c for c in phone if c.isdigit() or c == '-')
        
        # 基本的な形式チェック（日本の電話番号）
        if len(digits.replace('-', '')) < 10:
            return ""
        
        return digits
    
    def _normalize_address(self, address: str) -> str:
        """住所正規化"""
        if not address:
            return ""
        
        # 全角・半角統一、空白除去
        normalized = address.strip()
        
        # 簡易的な正規化（実際にはより複雑な処理が必要）
        replacements = {
            "　": " ",
            "丁目": "-",
            "番地": "-",
            "号": ""
        }
        
        for old, new in replacements.items():
            normalized = normalized.replace(old, new)
        
        return normalized
    
    def _validate_birth_date(self, birth_date: str) -> bool:
        """生年月日検証"""
        try:
            if len(birth_date) != 8:
                return False
            
            birth_dt = datetime.strptime(birth_date, '%Y%m%d')
            
            # 1900年以降、今日以前
            if birth_dt.year < 1900 or birth_dt > datetime.utcnow():
                return False
            
            return True
        except ValueError:
            return False
    
    def _normalize_gender(self, gender: str) -> str:
        """性別コード正規化"""
        gender_map = {
            "M": "1",  # Male
            "F": "2",  # Female  
            "O": "3",  # Other
            "N": "9"   # Not specified
        }
        return gender_map.get(gender.upper(), "9")
    
    def _get_last_activity_date(self, registration_date: str, last_login_date: str) -> str:
        """最終活動日取得"""
        try:
            reg_date = datetime.strptime(registration_date, '%Y%m%d')
            login_date = datetime.strptime(last_login_date, '%Y%m%d')
            
            return max(reg_date, login_date).strftime('%Y%m%d')
        except ValueError:
            return registration_date if registration_date else ""
    
    def _normalize_communication_preference(self, preference: str) -> str:
        """通信設定正規化"""
        preference_map = {
            "EMAIL": "E",
            "PHONE": "P", 
            "POSTAL": "M",  # Mail
            "SMS": "S"
        }
        return preference_map.get(preference.upper(), "E")
    
    def _calculate_data_quality_score(self, email: str, phone: str, address: str, 
                                    birth_date: str, lifetime_value: int) -> float:
        """データ品質スコア計算"""
        score = 0.0
        max_score = 100.0
        
        # メールアドレス（25点）
        if email and '@' in email:
            score += 25.0
        
        # 電話番号（20点）
        if phone and len(phone.replace('-', '')) >= 10:
            score += 20.0
        
        # 住所（20点）
        if address and len(address) >= 5:
            score += 20.0
        
        # 生年月日（15点）
        if self._validate_birth_date(birth_date):
            score += 15.0
        
        # ライフタイムバリュー（20点）
        if lifetime_value > 0:
            if lifetime_value >= 100000:
                score += 20.0
            elif lifetime_value >= 50000:
                score += 15.0
            elif lifetime_value >= 10000:
                score += 10.0
            else:
                score += 5.0
        
        return score
    
    def validate_domain_business_rules(self, data: str) -> List[str]:
        """Infrastructureドメインビジネスルール検証"""
        violations = []
        lines = data.strip().split('\n')
        
        if len(lines) < 2:
            return violations
        
        # ヘッダー検証
        expected_header = ",".join(self.output_columns)
        if lines[0] != expected_header:
            violations.append(f"ヘッダー不正: 期待={expected_header}, 実際={lines[0]}")
        
        customer_ids = set()
        
        # データ行検証
        for i, line in enumerate(lines[1:], 2):
            parts = line.split(',')
            
            if len(parts) != len(self.output_columns):
                violations.append(f"行{i}: フィールド数不正")
                continue
            
            # 顧客ID重複チェック
            customer_id = parts[0]
            if customer_id in customer_ids:
                violations.append(f"行{i}: 顧客ID重複 ({customer_id})")
            customer_ids.add(customer_id)
            
            # データ品質スコア範囲チェック
            try:
                quality_score = float(parts[14])
                if not (0 <= quality_score <= 100):
                    violations.append(f"行{i}: データ品質スコア範囲外 ({quality_score})")
            except ValueError:
                violations.append(f"行{i}: データ品質スコア形式不正")
            
            # ライフタイムバリュー検証
            try:
                lifetime_value = int(parts[11])
                if lifetime_value < 0:
                    violations.append(f"行{i}: ライフタイムバリューが負の値")
            except ValueError:
                violations.append(f"行{i}: ライフタイムバリュー形式不正")
        
        return violations
    
    def test_functional_data_copy_processing(self):
        """機能テスト: データ複製正常処理"""
        test_id = f"functional_copy_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # テストデータ準備
        test_data = self._generate_client_dm_data(500)
        records_count = len(test_data.split('\n')) - 1
        
        # ソースファイルアップロード
        file_date = datetime.utcnow().strftime('%Y%m%d')
        source_path = f"Marketing/ClientDM/source_{file_date}.tsv"
        self.mock_storage.upload_file(self.source_container, source_path, test_data.encode('utf-8'))
        
        # ステージング処理
        staging_path = f"staging/marketing_client_dm_{file_date}.tsv"
        self.mock_storage.upload_file(self.staging_container, staging_path, test_data.encode('utf-8'))
        
        # ETL処理シミュレーション
        transformed_data, output_data = self.simulate_etl_process(
            test_data,
            self._transform_client_dm_data,
            "csv"
        )
        
        # 最終出力
        output_path = f"DataMart/Marketing/client_dm_{file_date}.csv"
        self.mock_storage.upload_file(self.output_container, output_path, output_data)
        
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
            data_quality_score=0.95,
            errors=[],
            warnings=violations
        )
        
        # アサーション
        self.validate_common_assertions(result)
        assert result.records_extracted > 0, "抽出レコード数が0"
        assert result.records_transformed > 0, "変換レコード数が0"
        assert len(violations) == 0, f"ビジネスルール違反: {violations}"
        
        # ファイル存在確認
        assert self.mock_storage.file_exists(self.staging_container, staging_path), "ステージングファイル不存在"
        assert self.mock_storage.file_exists(self.output_container, output_path), "出力ファイル不存在"
        
        self.log_test_info(test_id, f"成功 - {result.records_loaded}件のデータ複製完了")
    
    def test_functional_data_quality_improvement(self):
        """機能テスト: データ品質向上処理"""
        test_id = f"functional_quality_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # 品質問題データ準備
        test_data = self._generate_data_quality_issues()
        input_records = len(test_data.split('\n')) - 1
        
        # データ変換処理
        transformed_data = self._transform_client_dm_data(test_data)
        
        # 品質向上結果確認
        output_lines = transformed_data.split('\n')
        valid_records = len([line for line in output_lines[1:] if line.strip()])
        
        # 品質スコア分析
        quality_scores = []
        for line in output_lines[1:]:
            if line.strip():
                parts = line.split(',')
                if len(parts) >= 15:
                    quality_scores.append(float(parts[14]))
        
        avg_quality_score = sum(quality_scores) / len(quality_scores) if quality_scores else 0
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.FUNCTIONAL,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=input_records,
            records_transformed=valid_records,
            records_loaded=valid_records,
            data_quality_score=avg_quality_score / 100,  # 0-1スケール
            errors=[]
        )
        
        # 品質向上アサーション
        assert valid_records < input_records, "無効レコードが除外されていない"
        assert valid_records >= 1, "有効レコードがない"
        assert avg_quality_score >= 50, f"平均品質スコアが低すぎる: {avg_quality_score:.2f}"
        
        # 重複除去確認
        customer_ids = []
        for line in output_lines[1:]:
            if line.strip():
                customer_ids.append(line.split(',')[0])
        
        assert len(customer_ids) == len(set(customer_ids)), "重複レコードが残存"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, f"成功 - 品質向上処理完了 (平均スコア: {avg_quality_score:.2f})")
    
    def test_functional_incremental_processing(self):
        """機能テスト: 増分処理"""
        test_id = f"functional_incremental_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # 増分データ準備
        incremental_data = self._generate_incremental_data()
        
        # データ変換処理
        transformed_data = self._transform_client_dm_data(incremental_data)
        
        # 新規・更新レコード分析
        lines = transformed_data.split('\n')
        new_customers = []
        updated_customers = []
        
        for line in lines[1:]:
            if line.strip():
                parts = line.split(',')
                if len(parts) >= 2:
                    customer_name = parts[1]
                    if "新規顧客" in customer_name:
                        new_customers.append(parts)
                    elif "更新顧客" in customer_name:
                        updated_customers.append(parts)
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.FUNCTIONAL,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=len(incremental_data.split('\n')) - 1,
            records_transformed=len(lines) - 1,
            records_loaded=len(lines) - 1,
            data_quality_score=0.98,
            errors=[]
        )
        
        # 増分処理アサーション
        assert len(new_customers) >= 5, f"新規顧客数不足: {len(new_customers)}"
        assert len(updated_customers) >= 5, f"更新顧客数不足: {len(updated_customers)}"
        
        # 新規顧客の登録日確認（過去7日以内）
        recent_registrations = 0
        current_date = datetime.utcnow()
        for customer in new_customers:
            if len(customer) >= 9:
                reg_date = datetime.strptime(customer[8], '%Y%m%d')
                days_diff = (current_date - reg_date).days
                if days_diff <= 7:
                    recent_registrations += 1
        
        assert recent_registrations >= 5, f"最近の登録数不足: {recent_registrations}"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, f"成功 - 増分処理完了 (新規:{len(new_customers)}, 更新:{len(updated_customers)})")
    
    def test_data_quality_normalization_validation(self):
        """データ品質テスト: 正規化検証"""
        test_id = f"data_quality_normalization_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # 正規化テスト用データ
        normalization_data = [
            "\t".join(self.input_columns),
            "CUST000001\t正規化テスト太郎\tTEST@TOKYOGAS.CO.JP\t03-1111-1111\t100-0001\t東京都千代田区１丁目１番地１号\t19800101\tm\t20200101\t20241201\tACTIVE\t100000\temail\ty",
        ]
        test_data = "\n".join(normalization_data)
        
        # データ変換処理
        transformed_data = self._transform_client_dm_data(test_data)
        
        # 正規化結果確認
        lines = transformed_data.split('\n')
        if len(lines) >= 2:
            parts = lines[1].split(',')
            
            # メールアドレス正規化確認
            normalized_email = parts[2]
            assert normalized_email == "test@tokyogas.co.jp", f"メール正規化失敗: {normalized_email}"
            
            # 性別コード正規化確認
            gender_code = parts[7]
            assert gender_code == "1", f"性別コード正規化失敗: {gender_code}"
            
            # 通信設定正規化確認
            comm_pref = parts[12]
            assert comm_pref == "E", f"通信設定正規化失敗: {comm_pref}"
            
            # マーケティング同意正規化確認
            marketing_opt = parts[13]
            assert marketing_opt == "1", f"マーケティング同意正規化失敗: {marketing_opt}"
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.DATA_QUALITY,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=1,
            records_transformed=1,
            records_loaded=1,
            data_quality_score=1.0,
            errors=[]
        )
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, "成功 - 正規化検証完了")
    
    def test_performance_large_dataset_copy(self):
        """パフォーマンステスト: 大量データセット複製"""
        test_id = f"performance_copy_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # 大量データ準備（100000件）
        large_data = self._generate_client_dm_data(100000)
        
        start_time = time.time()
        
        # ETL処理シミュレーション
        transformed_data = self._transform_client_dm_data(large_data)
        
        execution_time = time.time() - start_time
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.PERFORMANCE,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=100000,
            records_transformed=len(transformed_data.split('\n')) - 1,
            records_loaded=len(transformed_data.split('\n')) - 1,
            data_quality_score=0.96,
            errors=[]
        )
        
        # パフォーマンス基準
        throughput = 100000 / execution_time
        assert throughput > 3000, f"スループットが基準未満: {throughput:.2f} records/sec"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, f"成功 - {execution_time:.2f}秒で処理完了")
    
    def test_integration_multi_stage_pipeline(self):
        """統合テスト: 多段階パイプライン連携"""
        test_id = f"integration_multistage_{int(time.time())}"
        self.log_test_info(test_id, "開始")
        
        # 段階的処理テスト
        test_data = self._generate_client_dm_data(100)
        
        # Stage 1: ソースからステージングへ
        file_date = datetime.utcnow().strftime('%Y%m%d')
        source_path = f"Marketing/ClientDM/source_{file_date}.tsv"
        staging_path = f"staging/marketing_client_dm_{file_date}.tsv"
        
        self.mock_storage.upload_file(self.source_container, source_path, test_data.encode('utf-8'))
        
        # ステージング複製
        source_data = self.mock_storage.download_file(self.source_container, source_path)
        self.mock_storage.upload_file(self.staging_container, staging_path, source_data)
        
        # Stage 2: ステージングでデータ変換
        staging_data = self.mock_storage.download_file(self.staging_container, staging_path).decode('utf-8')
        transformed_data = self._transform_client_dm_data(staging_data)
        
        # Stage 3: データマートへ最終出力
        output_path = f"DataMart/Marketing/client_dm_{file_date}.csv"
        compressed_output = gzip.compress(transformed_data.encode('utf-8'))
        self.mock_storage.upload_file(self.output_container, output_path, compressed_output)
        
        # Stage 4: 品質レポート生成
        lines = transformed_data.split('\n')
        quality_report = {
            "total_records": len(lines) - 1,
            "avg_quality_score": 0.0,
            "high_quality_count": 0,
            "processing_date": file_date
        }
        
        quality_scores = []
        for line in lines[1:]:
            if line.strip():
                parts = line.split(',')
                if len(parts) >= 15:
                    score = float(parts[14])
                    quality_scores.append(score)
                    if score >= 80:
                        quality_report["high_quality_count"] += 1
        
        if quality_scores:
            quality_report["avg_quality_score"] = sum(quality_scores) / len(quality_scores)
        
        # レポート保存
        report_path = f"Reports/Marketing/quality_report_{file_date}.json"
        report_json = json.dumps(quality_report, ensure_ascii=False, indent=2)
        self.mock_storage.upload_file(self.output_container, report_path, report_json.encode('utf-8'))
        
        # テスト結果作成
        result = self.create_test_result(
            test_id=test_id,
            category=TestCategory.INTEGRATION,
            status=PipelineStatus.SUCCEEDED,
            records_extracted=100,
            records_transformed=len(lines) - 1,
            records_loaded=len(lines) - 1,
            data_quality_score=quality_report["avg_quality_score"] / 100,
            errors=[]
        )
        
        # 統合テストアサーション
        assert self.mock_storage.file_exists(self.source_container, source_path), "ソースファイル不存在"
        assert self.mock_storage.file_exists(self.staging_container, staging_path), "ステージングファイル不存在"
        assert self.mock_storage.file_exists(self.output_container, output_path), "出力ファイル不存在"
        assert self.mock_storage.file_exists(self.output_container, report_path), "レポートファイル不存在"
        
        assert quality_report["total_records"] > 0, "処理レコード数が0"
        assert quality_report["avg_quality_score"] >= 70, f"平均品質スコア不足: {quality_report['avg_quality_score']:.2f}"
        assert quality_report["high_quality_count"] >= 50, f"高品質レコード数不足: {quality_report['high_quality_count']}"
        
        self.validate_common_assertions(result)
        
        self.log_test_info(test_id, f"成功 - 多段階パイプライン統合テスト完了")