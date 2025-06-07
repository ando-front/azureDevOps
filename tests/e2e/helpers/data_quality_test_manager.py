"""
データ品質テスト管理ヘルパー

E2Eテストで使用される重複した処理を共通化し、テスト設定を統一管理します。
"""
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection


class DataQualityTableType(Enum):
    """データ品質テーブルタイプ"""
    QUALITY_RULES = "data_quality_rules"
    QUALITY_RESULTS = "data_quality_results"
    LINEAGE_TRACKING = "data_lineage_tracking"
    PROFILING_RESULTS = "data_profiling_results"
    SECURITY_AUDIT = "security_audit_logs"
    ACCESS_CONTROL = "access_control_policies"


@dataclass
class ProfilingTarget:
    """プロファイリング対象の設定"""
    table_name: str
    column_name: str
    data_type: str
    business_rules: List[str]


@dataclass
class QualityRule:
    """データ品質ルール"""
    rule_name: str
    table_name: str
    rule_type: str
    column_name: str
    rule_condition: str
    threshold_percent: float
    severity: str


@dataclass
class LineageStep:
    """データ系譜ステップ"""
    step_order: int
    source_table: str
    target_table: str
    transformation_type: str
    transformation_logic: str


class DataQualityTestManager:
    """データ品質テスト管理クラス"""
    
    def __init__(self, connection: SynapseE2EConnection):
        self.connection = connection
        self.table_schemas = self._get_table_schemas()
        self.test_data_templates = self._get_test_data_templates()
    
    def initialize_all_tables(self):
        """すべてのデータ品質テーブルの初期化"""
        for table_type in DataQualityTableType:
            self._initialize_table(table_type)
    
    def initialize_table(self, table_type: DataQualityTableType):
        """指定されたテーブルの初期化"""
        self._initialize_table(table_type)
    
    def _initialize_table(self, table_type: DataQualityTableType):
        """テーブル初期化の実行"""
        schema = self.table_schemas.get(table_type)
        if schema:
            try:
                self.connection.execute_query(schema)
            except Exception as e:                # テーブルが既に存在する場合はスキップ
                if "already exists" not in str(e).lower():
                    print(f"テーブル {table_type.value} の初期化で警告: {e}")
    
    def prepare_test_data(self, data_type: str = "comprehensive"):
        """テストデータの準備"""
        # まず、必要なモックテーブルを作成
        self._create_mock_tables()
        
        templates = self.test_data_templates.get(data_type, {})
        
        for table_name, inserts in templates.items():
            for insert_sql in inserts:
                try:
                    self.connection.execute_query(insert_sql)
                except Exception as e:
                    print(f"テストデータ挿入で警告 {table_name}: {e}")
    
    def _create_mock_tables(self):
        """テストに必要なモックテーブルの作成"""
        mock_tables = {
            # マーケティング顧客DM テーブル
            "[omni].[omni_ods_marketing_trn_client_dm]": """
                IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'omni')
                BEGIN
                    EXEC('CREATE SCHEMA omni')
                END
                
                IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[omni].[omni_ods_marketing_trn_client_dm]') AND type in (N'U'))
                BEGIN
                    CREATE TABLE [omni].[omni_ods_marketing_trn_client_dm] (
                        CUSTOMER_ID NVARCHAR(50) PRIMARY KEY,
                        CLIENT_KEY_AX NVARCHAR(50),
                        LIV0EU_4X NVARCHAR(50),
                        EMAIL NVARCHAR(255),
                        REC_REG_YMD DATETIME2 DEFAULT GETDATE(),
                        REC_UPD_YMD DATETIME2 DEFAULT GETDATE(),
                        STATUS NVARCHAR(20) DEFAULT 'ACTIVE'
                    )
                END
            """,
            
            # 使用サービス テーブル
            "[omni].[omni_ods_cloak_trn_usageservice]": """
                IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[omni].[omni_ods_cloak_trn_usageservice]') AND type in (N'U'))
                BEGIN
                    CREATE TABLE [omni].[omni_ods_cloak_trn_usageservice] (
                        BX NVARCHAR(50),
                        SERVICE_KEY1 NVARCHAR(50),
                        SERVICE_TYPE NVARCHAR(10),
                        USER_KEY NVARCHAR(50),
                        TRANSFER_YMD DATETIME2,
                        SERVICE_KEY_START_YMD DATETIME2,
                        OUTPUT_DATE DATETIME2 DEFAULT GETDATE(),
                        INDEX (idx_bx) (BX),
                        INDEX (idx_service_type) (SERVICE_TYPE)
                    )
                END
            """
        }
        
        for table_name, create_sql in mock_tables.items():
            try:
                self.connection.execute_query(create_sql)
                print(f"モックテーブル作成/確認完了: {table_name}")
            except Exception as e:
                print(f"モックテーブル作成で警告 {table_name}: {e}")
        
        # テストデータの挿入
        self._insert_mock_data()
    
    def execute_quality_rule(self, rule: QualityRule) -> Dict[str, Any]:
        """データ品質ルールの実行"""
        try:
            # 総レコード数の取得
            total_query = f"SELECT COUNT(*) FROM {rule.table_name}"
            total_records = self.connection.execute_query(total_query)[0][0]
            
            # 条件に合致するレコード数の取得
            if rule.column_name:
                valid_query = f"""
                SELECT COUNT(*) FROM {rule.table_name} 
                WHERE {rule.column_name} {rule.rule_condition}
                """
            else:
                valid_query = f"""
                SELECT COUNT(*) FROM {rule.table_name} 
                WHERE {rule.rule_condition}
                """
            
            valid_records = self.connection.execute_query(valid_query)[0][0]
            invalid_records = total_records - valid_records
            
            # 品質スコアの計算
            quality_score = (valid_records / total_records * 100) if total_records > 0 else 0
            threshold_met = quality_score >= rule.threshold_percent
            
            # 結果の返却
            return {
                'rule_name': rule.rule_name,
                'total_records': total_records,
                'valid_records': valid_records,
                'invalid_records': invalid_records,
                'quality_score': quality_score,
                'threshold_met': threshold_met,
                'execution_time': datetime.now()
            }
            
        except Exception as e:
            return {
                'rule_name': rule.rule_name,
                'total_records': 0,
                'valid_records': 0,
                'invalid_records': 0,
                'quality_score': 0.0,
                'threshold_met': False,
                'execution_time': datetime.now(),
                'error': str(e)
            }
    
    def execute_column_profiling(self, target: ProfilingTarget) -> Dict[str, Any]:
        """カラムプロファイリングの実行"""
        try:
            # 基本統計の取得
            stats_query = f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT({target.column_name}) as non_null_count,
                COUNT(DISTINCT {target.column_name}) as distinct_count
            FROM {target.table_name}
            """
            
            basic_stats = self.connection.execute_query(stats_query)[0]
            total_records, non_null_count, distinct_count = basic_stats
            null_count = total_records - non_null_count
            
            # データ型別の詳細分析
            if target.data_type in ['varchar', 'nvarchar', 'char', 'text']:
                return self._analyze_string_column(target, total_records, null_count, 
                                                 non_null_count, distinct_count)
            elif target.data_type in ['datetime', 'datetime2', 'date']:
                return self._analyze_datetime_column(target, total_records, null_count, 
                                                   non_null_count, distinct_count)
            else:
                return self._analyze_numeric_column(target, total_records, null_count, 
                                                  non_null_count, distinct_count)
                
        except Exception as e:
            return {
                'total_records': 0,
                'null_count': 0,
                'distinct_count': 0,
                'min_value': "エラー",
                'max_value': "エラー",
                'avg_length': 0.0,
                'pattern_compliance': 0.0,
                'anomaly_count': 0,
                'error': str(e)
            }
    
    def execute_lineage_tracking(self, transformation_id: str, steps: List[LineageStep]) -> Dict[str, Any]:
        """データ系譜追跡の実行"""
        results = []
        
        for step in steps:
            # 変換前のレコード数取得
            before_count = self.connection.execute_query(
                f"SELECT COUNT(*) FROM {step.source_table}"
            )[0][0]
            
            # 変換処理の実行（シミュレート）
            self._execute_transformation_step(step)
            
            # 変換後のレコード数取得
            after_count = self.connection.execute_query(
                f"SELECT COUNT(*) FROM {step.target_table}"
            )[0][0]
            
            # 系譜情報の記録
            self.connection.execute_query(
                """
                INSERT INTO data_lineage_tracking 
                (transformation_id, step_order, source_table, target_table, 
                 transformation_type, transformation_logic, record_count_before, 
                 record_count_after, execution_time, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (transformation_id, step.step_order, step.source_table,
                 step.target_table, step.transformation_type, 
                 step.transformation_logic, before_count, after_count,
                 datetime.now(), datetime.now())
            )
            
            results.append({
                'step_order': step.step_order,
                'source_table': step.source_table,
                'target_table': step.target_table,
                'before_count': before_count,
                'after_count': after_count,
                'retention_rate': (after_count / before_count * 100) if before_count > 0 else 0
            })
        
        return {
            'transformation_id': transformation_id,
            'steps': results,
            'total_steps': len(steps)
        }
    
    def get_profiling_targets_preset(self, preset_name: str = "standard") -> List[ProfilingTarget]:
        """事前定義されたプロファイリング対象の取得"""
        presets = {
            "standard": [
                ProfilingTarget(
                    table_name='[omni].[omni_ods_marketing_trn_client_dm]',
                    column_name='CUSTOMER_ID',
                    data_type='varchar',
                    business_rules=['unique', 'not_null', 'format_check']
                ),
                ProfilingTarget(
                    table_name='[omni].[omni_ods_cloak_trn_usageservice]',
                    column_name='BX',
                    data_type='varchar',
                    business_rules=['not_null', 'length_check', 'pattern_check']
                ),
                ProfilingTarget(
                    table_name='[omni].[omni_ods_cloak_trn_usageservice]',
                    column_name='OUTPUT_DATE',
                    data_type='datetime',
                    business_rules=['not_null', 'range_check', 'freshness_check']
                )
            ],
            "comprehensive": [
                # 追加のカラムプロファイリング設定
            ]
        }
        
        return presets.get(preset_name, presets["standard"])
    
    def get_quality_rules_preset(self, preset_name: str = "standard") -> List[QualityRule]:
        """事前定義された品質ルールの取得"""
        presets = {
            "standard": [
                QualityRule(
                    rule_name='client_dm_completeness',
                    table_name='[omni].[omni_ods_marketing_trn_client_dm]',
                    rule_type='completeness',
                    column_name='CUSTOMER_ID',
                    rule_condition='IS NOT NULL',
                    threshold_percent=99.5,
                    severity='Critical'
                ),
                QualityRule(
                    rule_name='usage_service_consistency',
                    table_name='[omni].[omni_ods_cloak_trn_usageservice]',
                    rule_type='consistency',
                    column_name='BX',
                    rule_condition='LEN(BX) = 10',
                    threshold_percent=95.0,
                    severity='High'
                ),
                QualityRule(
                    rule_name='email_format_validity',
                    table_name='[omni].[omni_ods_marketing_trn_client_dm]',
                    rule_type='validity',
                    column_name='EMAIL_ADDRESS',
                    rule_condition="LIKE '%@%.%'",
                    threshold_percent=98.0,
                    severity='Medium'
                )
            ]
        }
        
        return presets.get(preset_name, presets["standard"])
    
    def get_lineage_steps_preset(self, preset_name: str = "client_dm_processing") -> List[LineageStep]:
        """事前定義された系譜ステップの取得"""
        presets = {
            "client_dm_processing": [
                LineageStep(
                    step_order=1,
                    source_table='[omni].[omni_ods_cloak_trn_usageservice]',
                    target_table='[omni].[omni_ods_cloak_trn_usageservice_bx4x_temp]',
                    transformation_type='filter_and_deduplicate',
                    transformation_logic='SELECT BX, SERVICE_KEY1 WHERE SERVICE_TYPE=001 AND ROW_NUMBER()=1'
                ),
                LineageStep(
                    step_order=2,
                    source_table='[omni].[omni_ods_marketing_trn_client_dm]',
                    target_table='[omni].[omni_ods_marketing_trn_client_dm_bx_temp]',
                    transformation_type='join_and_enrich',
                    transformation_logic='LEFT JOIN with usage service data'
                )
            ]
        }
        
        return presets.get(preset_name, presets["client_dm_processing"])
    
    def _analyze_string_column(self, target: ProfilingTarget, total_records: int, 
                             null_count: int, non_null_count: int, distinct_count: int) -> Dict[str, Any]:
        """文字列カラムの分析"""
        # 最小/最大値の取得
        min_max_query = f"""
        SELECT 
            MIN({target.column_name}) as min_value,
            MAX({target.column_name}) as max_value,
            AVG(CAST(LEN({target.column_name}) AS FLOAT)) as avg_length
        FROM {target.table_name}
        WHERE {target.column_name} IS NOT NULL
        """
        
        min_max_result = self.connection.execute_query(min_max_query)[0]
        min_value, max_value, avg_length = min_max_result
        
        # パターン適合性チェック（例：メールアドレス）
        if 'EMAIL' in target.column_name.upper():
            pattern_check = self.connection.execute_query(
                f"""
                SELECT COUNT(*) 
                FROM {target.table_name}
                WHERE {target.column_name} LIKE '%@%.%'
                AND {target.column_name} IS NOT NULL
                """
            )[0][0]
            pattern_compliance = (pattern_check / non_null_count * 100) if non_null_count > 0 else 0
        else:
            pattern_compliance = 100.0
        
        # 異常値検出
        anomaly_count = self._detect_string_anomalies(target, total_records)
        
        return {
            'total_records': total_records,
            'null_count': null_count,
            'distinct_count': distinct_count,
            'min_value': str(min_value) if min_value else "N/A",
            'max_value': str(max_value) if max_value else "N/A",
            'avg_length': float(avg_length) if avg_length else 0.0,
            'pattern_compliance': pattern_compliance,
            'anomaly_count': anomaly_count
        }
    
    def _analyze_datetime_column(self, target: ProfilingTarget, total_records: int,
                               null_count: int, non_null_count: int, distinct_count: int) -> Dict[str, Any]:
        """日時カラムの分析"""
        # 最小/最大値の取得
        min_max_query = f"""
        SELECT 
            MIN({target.column_name}) as min_value,
            MAX({target.column_name}) as max_value
        FROM {target.table_name}
        WHERE {target.column_name} IS NOT NULL
        """
        
        min_max_result = self.connection.execute_query(min_max_query)[0]
        min_value, max_value = min_max_result
        
        # 日付の妥当性チェック
        future_dates = self.connection.execute_query(
            f"""
            SELECT COUNT(*) 
            FROM {target.table_name}
            WHERE {target.column_name} > GETDATE()
            AND {target.column_name} IS NOT NULL
            """
        )[0][0]
        
        old_dates = self.connection.execute_query(
            f"""
            SELECT COUNT(*) 
            FROM {target.table_name}
            WHERE {target.column_name} < DATEADD(year, -10, GETDATE())
            AND {target.column_name} IS NOT NULL
            """
        )[0][0]
        
        valid_dates = non_null_count - future_dates - old_dates
        pattern_compliance = (valid_dates / non_null_count * 100) if non_null_count > 0 else 0
        
        return {
            'total_records': total_records,
            'null_count': null_count,
            'distinct_count': distinct_count,
            'min_value': str(min_value) if min_value else "N/A",
            'max_value': str(max_value) if max_value else "N/A",
            'avg_length': 0.0,
            'pattern_compliance': pattern_compliance,
            'anomaly_count': future_dates + old_dates
        }
    
    def _analyze_numeric_column(self, target: ProfilingTarget, total_records: int,
                              null_count: int, non_null_count: int, distinct_count: int) -> Dict[str, Any]:
        """数値カラムの分析"""
        return {
            'total_records': total_records,
            'null_count': null_count,
            'distinct_count': distinct_count,
            'min_value': "N/A",
            'max_value': "N/A",
            'avg_length': 0.0,
            'pattern_compliance': 100.0,
            'anomaly_count': 0
        }
    
    def _detect_string_anomalies(self, target: ProfilingTarget, total_records: int) -> int:
        """文字列の異常値検出"""
        try:
            anomaly_check = self.connection.execute_query(
                f"""
                SELECT COUNT(*) as anomaly_count
                FROM (
                    SELECT {target.column_name}, COUNT(*) as freq
                    FROM {target.table_name}
                    WHERE {target.column_name} IS NOT NULL
                    GROUP BY {target.column_name}
                    HAVING COUNT(*) > ({total_records} * 0.5)
                ) anomalies
                """
            )
            
            return anomaly_check[0][0] if anomaly_check else 0
        except Exception:
            return 0
    
    def _execute_transformation_step(self, step: LineageStep):
        """変換ステップの実行（シミュレート）"""
        try:
            if step.step_order == 1 and "usageservice" in step.target_table:
                # 最初のステップ: フィルタリングと重複排除
                self.connection.execute_query(
                    """
                    INSERT INTO [omni].[omni_ods_cloak_trn_usageservice_bx4x_temp]
                    SELECT BX, SERVICE_KEY1 as KEY_4X, 1 as INDEX_ID, 
                           TRANSFER_YMD, SERVICE_KEY_START_YMD, OUTPUT_DATE
                    FROM [omni].[omni_ods_cloak_trn_usageservice]
                    WHERE SERVICE_TYPE='001' AND BX IS NOT NULL
                    """
                )
            elif step.step_order == 2 and "client_dm" in step.target_table:
                # 2番目のステップ: 結合と集約
                self.connection.execute_query(
                    """
                    INSERT INTO [omni].[omni_ods_marketing_trn_client_dm_bx_temp]
                    SELECT usv.BX, usv.INDEX_ID, usv.TRANSFER_YMD, 
                           usv.SERVICE_KEY_START_YMD, usv.OUTPUT_DATE, cldm.*
                    FROM [omni].[omni_ods_marketing_trn_client_dm] cldm
                    INNER JOIN [omni].[omni_ods_cloak_trn_usageservice_bx4x_temp] usv 
                        ON cldm.LIV0EU_4X = usv.KEY_4X
                    """
                )
        except Exception as e:
            print(f"変換ステップ {step.step_order} でエラー: {e}")
    
    def _get_table_schemas(self) -> Dict[DataQualityTableType, str]:
        """テーブルスキーマの定義"""
        return {
            DataQualityTableType.QUALITY_RULES: """
                CREATE TABLE data_quality_rules (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    rule_name NVARCHAR(200) NOT NULL,
                    table_name NVARCHAR(200) NOT NULL,
                    rule_type NVARCHAR(50) NOT NULL,
                    column_name NVARCHAR(100),
                    rule_condition NVARCHAR(MAX) NOT NULL,
                    threshold_percent DECIMAL(5,2) NOT NULL,
                    severity NVARCHAR(20) NOT NULL,
                    is_active BIT DEFAULT 1,
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """,
            DataQualityTableType.QUALITY_RESULTS: """
                CREATE TABLE data_quality_results (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    rule_name NVARCHAR(200) NOT NULL,
                    execution_time DATETIME2 NOT NULL,
                    total_records INT,
                    valid_records INT,
                    invalid_records INT,
                    quality_score DECIMAL(5,2),
                    threshold_met BIT,
                    issues_found NVARCHAR(MAX),
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """,
            DataQualityTableType.LINEAGE_TRACKING: """
                CREATE TABLE data_lineage_tracking (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    transformation_id NVARCHAR(100) NOT NULL,
                    step_order INT NOT NULL,
                    source_table NVARCHAR(200) NOT NULL,
                    target_table NVARCHAR(200) NOT NULL,
                    transformation_type NVARCHAR(100) NOT NULL,
                    transformation_logic NVARCHAR(MAX),
                    record_count_before INT,
                    record_count_after INT,
                    execution_time DATETIME2,
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """,
            DataQualityTableType.PROFILING_RESULTS: """
                CREATE TABLE data_profiling_results (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    session_id NVARCHAR(100) NOT NULL,
                    table_name NVARCHAR(200) NOT NULL,
                    column_name NVARCHAR(100) NOT NULL,
                    data_type NVARCHAR(50),
                    total_records INT,
                    null_count INT,
                    distinct_count INT,
                    min_value NVARCHAR(500),
                    max_value NVARCHAR(500),
                    avg_length DECIMAL(10,2),
                    pattern_compliance DECIMAL(5,2),
                    anomaly_count INT,
                    profiling_time DATETIME2,
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """,
            DataQualityTableType.SECURITY_AUDIT: """
                CREATE TABLE security_audit_logs (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    event_id NVARCHAR(100) NOT NULL,
                    event_type NVARCHAR(50) NOT NULL,
                    user_identity NVARCHAR(200),
                    resource_name NVARCHAR(200),
                    action_taken NVARCHAR(100),
                    event_timestamp DATETIME2,
                    source_ip NVARCHAR(50),
                    user_agent NVARCHAR(500),
                    status_code INT,
                    details NVARCHAR(MAX),
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """,
            DataQualityTableType.ACCESS_CONTROL: """
                CREATE TABLE access_control_policies (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    policy_name NVARCHAR(200) NOT NULL,
                    resource_type NVARCHAR(100) NOT NULL,
                    principal_type NVARCHAR(50) NOT NULL,
                    principal_id NVARCHAR(200) NOT NULL,
                    permissions NVARCHAR(500) NOT NULL,
                    conditions NVARCHAR(MAX),
                    is_active BIT DEFAULT 1,
                    effective_from DATETIME2,
                    effective_to DATETIME2,
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """
        }
    
    def _get_test_data_templates(self) -> Dict[str, Dict[str, List[str]]]:
        """テストデータテンプレートの定義"""
        return {
            "comprehensive": {
                "[omni].[omni_ods_marketing_trn_client_dm]": [
                    """
                    INSERT INTO [omni].[omni_ods_marketing_trn_client_dm] 
                    (CUSTOMER_ID, EMAIL_ADDRESS, LIV0EU_4X)
                    VALUES 
                    ('DQ_TEST_001', 'test1@example.com', 'VALID_4X_01'),
                    ('DQ_TEST_002', 'test2@example.com', 'VALID_4X_02'),
                    ('DQ_TEST_003', 'test3@example.com', 'VALID_4X_03')
                    """,
                    """
                    INSERT INTO [omni].[omni_ods_marketing_trn_client_dm] 
                    (CUSTOMER_ID, EMAIL_ADDRESS, LIV0EU_4X)
                    VALUES 
                    ('DQ_TEST_004', 'invalid-email', 'VALID_4X_04'),
                    (NULL, 'test5@example.com', 'VALID_4X_05')
                    """
                ],
                "[omni].[omni_ods_cloak_trn_usageservice]": [
                    """
                    INSERT INTO [omni].[omni_ods_cloak_trn_usageservice] 
                    (BX, SERVICE_KEY1, SERVICE_TYPE, OUTPUT_DATE)
                    VALUES 
                    ('1234567890', 'VALID_4X_01', '001', GETDATE()),
                    ('ABCD123456', 'VALID_4X_02', '001', GETDATE()),
                    ('SHORT_BX', 'VALID_4X_03', '001', DATEADD(year, -10, GETDATE()))
                    """
                ]
            }
        }
    
    def setup_mock_tables(self) -> None:
        """テスト用のモックテーブルを作成"""
        try:
            # モックテーブルのスキーマ定義
            mock_schemas = {
                "[omni].[omni_ods_marketing_trn_client_dm]": """
                    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'omni_ods_marketing_trn_client_dm')
                    CREATE TABLE [omni].[omni_ods_marketing_trn_client_dm] (
                        CUSTOMER_ID NVARCHAR(50) PRIMARY KEY,
                        EMAIL_ADDRESS NVARCHAR(255),
                        LIV0EU_4X NVARCHAR(50),
                        CLIENT_KEY_AX NVARCHAR(50),
                        REGISTRATION_DATE DATE DEFAULT GETDATE(),
                        STATUS NVARCHAR(50) DEFAULT 'ACTIVE',
                        CREATED_AT DATETIME2 DEFAULT GETDATE(),
                        UPDATED_AT DATETIME2 DEFAULT GETDATE()
                    )
                """,
                "[omni].[omni_ods_cloak_trn_usageservice]": """
                    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'omni_ods_cloak_trn_usageservice')
                    CREATE TABLE [omni].[omni_ods_cloak_trn_usageservice] (
                        BX NVARCHAR(50),
                        SERVICE_KEY1 NVARCHAR(50),
                        SERVICE_TYPE NVARCHAR(10),
                        OUTPUT_DATE DATETIME2 DEFAULT GETDATE(),
                        TRANSFER_YMD DATE,
                        SERVICE_KEY_START_YMD DATE,
                        CREATED_AT DATETIME2 DEFAULT GETDATE()
                    )
                """
            }
            
            # モックテーブルの作成
            for table_name, schema_sql in mock_schemas.items():
                try:
                    self.connection.execute_query(schema_sql)
                    print(f"モックテーブル {table_name} を作成しました")
                except Exception as e:
                    print(f"モックテーブル {table_name} の作成で警告: {e}")
            
            # テストデータの挿入
            self._insert_mock_data()
            
        except Exception as e:
            print(f"モックテーブル設定でエラー: {e}")
    
    def _insert_mock_data(self) -> None:
        """モックテーブルにテストデータを挿入"""
        mock_data_inserts = [
            # マーケティング顧客DMのテストデータ
            """
            INSERT INTO [omni].[omni_ods_marketing_trn_client_dm] 
            (CUSTOMER_ID, CLIENT_KEY_AX, LIV0EU_4X, EMAIL, REC_REG_YMD, REC_UPD_YMD, STATUS)
            VALUES 
            ('CUST001', 'AX001', '4X001', 'test1@example.com', GETDATE(), GETDATE(), 'ACTIVE'),
            ('CUST002', 'AX002', '4X002', 'test2@example.com', GETDATE(), GETDATE(), 'ACTIVE'),
            ('CUST003', 'AX003', '4X003', 'test3@example.com', GETDATE(), GETDATE(), 'ACTIVE'),
            ('CUST004', 'AX004', '4X004', 'test4@example.com', GETDATE(), GETDATE(), 'ACTIVE'),
            ('CUST005', 'AX005', '4X005', 'test5@example.com', GETDATE(), GETDATE(), 'ACTIVE')
            """,
            
            # 使用サービステーブルのテストデータ
            """
            INSERT INTO [omni].[omni_ods_cloak_trn_usageservice] 
            (BX, SERVICE_KEY1, SERVICE_TYPE, USER_KEY, TRANSFER_YMD, SERVICE_KEY_START_YMD, OUTPUT_DATE)
            VALUES 
            ('BX001', 'SK001', '001', 'USER001', GETDATE(), GETDATE(), GETDATE()),
            ('BX002', 'SK002', '001', 'USER002', GETDATE(), GETDATE(), GETDATE()),
            ('BX003', 'SK003', '001', 'USER003', GETDATE(), GETDATE(), GETDATE()),
            ('BX004', 'SK004', '002', 'USER004', GETDATE(), GETDATE(), GETDATE()),
            ('BX005', 'SK005', '001', 'USER005', GETDATE(), GETDATE(), GETDATE())
            """
        ]
        
        for insert_sql in mock_data_inserts:
            try:
                self.connection.execute_query(insert_sql)
            except Exception as e:
                # データが既に存在する場合はスキップ
                if "duplicate" not in str(e).lower() and "violation" not in str(e).lower():
                    print(f"モックデータ挿入で警告: {e}")

    # ...existing code...
