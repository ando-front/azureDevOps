#!/usr/bin/env python3
"""
包括的テーブル構造検証フレームワーク
Enhanced Table Structure Validator (Comprehensive)

このフレームワークは、主要なデータベーステーブルの構造検証を統一的に提供します。
marketing_client_dmだけでなく、以下のテーブルにも対応：
- client_dm
- ClientDmBx  
- point_grant_email
- marketing_client_dm（460列構造）
- data_quality_results
- data_lineage_tracking

Author: E2E Test Framework Team
Created: 2025-06-12
"""

import logging
import pyodbc
import os
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from enum import Enum

# ロギング設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TableType(Enum):
    """検証対象テーブルタイプ"""
    MARKETING_CLIENT_DM = "marketing_client_dm"
    CLIENT_DM = "client_dm"  
    CLIENT_DM_BX = "ClientDmBx"
    POINT_GRANT_EMAIL = "point_grant_email"
    DATA_QUALITY_RESULTS = "data_quality_results"
    DATA_LINEAGE_TRACKING = "data_lineage_tracking"
    E2E_TEST_EXECUTION_LOG = "e2e_test_execution_log"

@dataclass
class TableStructureSpec:
    """テーブル構造仕様"""
    table_name: str
    schema_name: str
    expected_min_columns: int
    expected_max_columns: Optional[int]
    critical_columns: List[str]
    column_patterns: List[str]  # 特定パターンのカラム（LIV0EU_*, TESHSMC_*等）
    description: str

@dataclass 
class ValidationResult:
    """検証結果"""
    table_type: TableType
    is_valid: bool
    actual_column_count: int
    expected_range: str
    missing_critical_columns: List[str]
    missing_pattern_groups: List[str]
    validation_details: Dict[str, Any]
    error_message: Optional[str] = None

class ComprehensiveTableStructureValidator:
    """包括的テーブル構造検証クラス"""
    
    def __init__(self, connection_string: str = None):
        """
        初期化
        
        Args:
            connection_string: SQL Server接続文字列（省略時は環境変数から取得）
        """
        self.connection_string = connection_string or self._get_connection_string()
        self.table_specs = self._initialize_table_specs()
        
    def _get_connection_string(self) -> str:
        """環境変数から接続文字列を構築"""
        server = os.getenv('E2E_SQL_SERVER', 'localhost,1433')
        database = os.getenv('E2E_SQL_DATABASE', 'TGMATestDB')
        username = os.getenv('E2E_SQL_USERNAME', 'sa')
        password = os.getenv('E2E_SQL_PASSWORD', 'YourStrong!Passw0rd123')
        driver = os.getenv('E2E_SQL_DRIVER', 'ODBC Driver 18 for SQL Server')
        
        return (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"TrustServerCertificate=yes;"
            f"Encrypt=no;"
            f"Timeout=30;"
        )
    
    def _initialize_table_specs(self) -> Dict[TableType, TableStructureSpec]:
        """テーブル仕様の初期化"""
        return {
            TableType.MARKETING_CLIENT_DM: TableStructureSpec(
                table_name="omni_ods_marketing_trn_client_dm",
                schema_name="omni",
                expected_min_columns=460,
                expected_max_columns=470,
                critical_columns=["CLIENT_KEY_AX", "REC_REG_YMD", "REC_UPD_YMD"],
                column_patterns=["LIV0EU_*", "TESHSMC_*", "TESHSEQ_*", "EPCISCRT_*", "WEBHIS_*"],
                description="Marketing Client DM 460列構造テーブル"
            ),
            TableType.CLIENT_DM: TableStructureSpec(
                table_name="client_dm",
                schema_name="dbo",
                expected_min_columns=5,
                expected_max_columns=15,
                critical_columns=["client_id", "client_name", "status"],
                column_patterns=[],
                description="基本顧客情報テーブル"
            ),
            TableType.CLIENT_DM_BX: TableStructureSpec(
                table_name="ClientDmBx",
                schema_name="dbo", 
                expected_min_columns=10,
                expected_max_columns=20,
                critical_columns=["client_id", "segment", "score"],
                column_patterns=[],
                description="顧客データマート（BXフラグ付き）"
            ),
            TableType.POINT_GRANT_EMAIL: TableStructureSpec(
                table_name="point_grant_email",
                schema_name="dbo",
                expected_min_columns=8,
                expected_max_columns=15,
                critical_columns=["client_id", "email", "points_granted"],
                column_patterns=[],
                description="ポイント付与メール管理テーブル"
            ),
            TableType.DATA_QUALITY_RESULTS: TableStructureSpec(
                table_name="data_quality_results",
                schema_name="dbo",
                expected_min_columns=10,
                expected_max_columns=15,
                critical_columns=["rule_name", "execution_time", "quality_score"],
                column_patterns=[],
                description="データ品質結果管理テーブル"
            ),
            TableType.DATA_LINEAGE_TRACKING: TableStructureSpec(
                table_name="data_lineage_tracking", 
                schema_name="dbo",
                expected_min_columns=8,
                expected_max_columns=15,
                critical_columns=["pipeline_name", "source_table", "target_table"],
                column_patterns=[],
                description="データ系譜追跡テーブル"
            ),
            TableType.E2E_TEST_EXECUTION_LOG: TableStructureSpec(
                table_name="e2e_test_execution_log",
                schema_name="etl",
                expected_min_columns=6,
                expected_max_columns=10,
                critical_columns=["TestName", "ExecutionTime", "Status"],
                column_patterns=[],
                description="E2Eテスト実行ログテーブル"
            )
        }
    
    def validate_table_structure(self, table_type: TableType) -> ValidationResult:
        """指定されたテーブルタイプの構造を検証"""
        spec = self.table_specs[table_type]
        logger.info(f"テーブル構造検証開始: {spec.description}")
        
        try:
            # テーブル存在確認
            if not self._check_table_exists(spec.schema_name, spec.table_name):
                return ValidationResult(
                    table_type=table_type,
                    is_valid=False,
                    actual_column_count=0,
                    expected_range=f"{spec.expected_min_columns}-{spec.expected_max_columns or 'unlimited'}",
                    missing_critical_columns=spec.critical_columns,
                    missing_pattern_groups=spec.column_patterns,
                    validation_details={},
                    error_message=f"テーブル {spec.schema_name}.{spec.table_name} が存在しません"
                )
            
            # カラム情報取得
            columns = self._get_table_columns(spec.schema_name, spec.table_name)
            actual_count = len(columns)
            
            # 基本的なカラム数チェック
            column_count_valid = actual_count >= spec.expected_min_columns
            if spec.expected_max_columns:
                column_count_valid = column_count_valid and actual_count <= spec.expected_max_columns
            
            # 重要なカラムの存在チェック
            column_names = [col[0] for col in columns]
            missing_critical = [col for col in spec.critical_columns if col not in column_names]
            
            # パターンマッチングカラムの存在チェック
            missing_patterns = []
            for pattern in spec.column_patterns:
                pattern_base = pattern.replace("*", "")
                pattern_columns = [col for col in column_names if col.startswith(pattern_base)]
                if not pattern_columns:
                    missing_patterns.append(pattern)
            
            # 検証結果の決定
            is_valid = (
                column_count_valid and
                len(missing_critical) == 0 and
                len(missing_patterns) == 0
            )
            
            validation_details = {
                "column_names": column_names,
                "column_data_types": {col[0]: col[1] for col in columns},
                "pattern_matches": {
                    pattern: [col for col in column_names 
                             if col.startswith(pattern.replace("*", ""))]
                    for pattern in spec.column_patterns
                }
            }
            
            return ValidationResult(
                table_type=table_type,
                is_valid=is_valid,
                actual_column_count=actual_count,
                expected_range=f"{spec.expected_min_columns}-{spec.expected_max_columns or 'unlimited'}",
                missing_critical_columns=missing_critical,
                missing_pattern_groups=missing_patterns,
                validation_details=validation_details
            )
            
        except Exception as e:
            logger.error(f"テーブル構造検証エラー: {e}")
            return ValidationResult(
                table_type=table_type,
                is_valid=False,
                actual_column_count=0,
                expected_range="unknown",
                missing_critical_columns=[],
                missing_pattern_groups=[],
                validation_details={},
                error_message=str(e)
            )
    
    def validate_all_tables(self) -> Dict[TableType, ValidationResult]:
        """全ての登録されたテーブルを検証"""
        results = {}
        logger.info("全テーブル構造検証開始")
        
        for table_type in TableType:
            results[table_type] = self.validate_table_structure(table_type)
            
        return results
    
    def _check_table_exists(self, schema_name: str, table_name: str) -> bool:
        """テーブル存在確認"""
        try:
            with pyodbc.connect(self.connection_string) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                """, (schema_name, table_name))
                return cursor.fetchone()[0] > 0
        except Exception as e:
            logger.error(f"テーブル存在確認エラー: {e}")
            return False
    
    def _get_table_columns(self, schema_name: str, table_name: str) -> List[Tuple[str, str]]:
        """テーブルのカラム情報を取得"""
        try:
            with pyodbc.connect(self.connection_string) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT COLUMN_NAME, DATA_TYPE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                    ORDER BY ORDINAL_POSITION
                """, (schema_name, table_name))
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"カラム情報取得エラー: {e}")
            return []
    
    def generate_validation_report(self, results: Dict[TableType, ValidationResult]) -> str:
        """検証結果レポートを生成"""
        report_lines = [
            "=" * 80,
            "包括的テーブル構造検証レポート",
            "=" * 80,
            f"検証実行日時: {self._get_current_timestamp()}",
            ""
        ]
        
        # サマリー
        total_tables = len(results)
        valid_tables = sum(1 for result in results.values() if result.is_valid)
        
        report_lines.extend([
            "【検証サマリー】",
            f"総テーブル数: {total_tables}",
            f"検証成功: {valid_tables}",
            f"検証失敗: {total_tables - valid_tables}",
            f"成功率: {(valid_tables / total_tables * 100):.1f}%",
            ""
        ])
        
        # 詳細結果
        for table_type, result in results.items():
            spec = self.table_specs[table_type]
            status = "✅ 成功" if result.is_valid else "❌ 失敗"
            
            report_lines.extend([
                f"【{spec.description}】",
                f"テーブル名: {spec.schema_name}.{spec.table_name}",
                f"検証結果: {status}",
                f"カラム数: {result.actual_column_count} (期待範囲: {result.expected_range})"
            ])
            
            if result.missing_critical_columns:
                report_lines.append(f"不足カラム: {', '.join(result.missing_critical_columns)}")
                
            if result.missing_pattern_groups:
                report_lines.append(f"不足パターン: {', '.join(result.missing_pattern_groups)}")
                
            if result.error_message:
                report_lines.append(f"エラー: {result.error_message}")
                
            report_lines.append("")
        
        return "\n".join(report_lines)
    
    def _get_current_timestamp(self) -> str:
        """現在のタイムスタンプを取得"""
        from datetime import datetime
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def main():
    """メイン実行関数"""
    try:
        # バリデーターの初期化
        validator = ComprehensiveTableStructureValidator()
        
        # 全テーブルの検証実行
        results = validator.validate_all_tables()
        
        # レポート生成と出力
        report = validator.generate_validation_report(results)
        print(report)
        
        # レポートファイルの保存
        report_file = "table_structure_validation_report.txt"
        with open(report_file, "w", encoding="utf-8") as f:
            f.write(report)
        
        print(f"\n詳細レポートが '{report_file}' に保存されました。")
        
        # 検証失敗がある場合はエラー終了
        failed_tables = [table_type for table_type, result in results.items() if not result.is_valid]
        if failed_tables:
            print(f"\n警告: {len(failed_tables)}個のテーブルで検証が失敗しました")
            return 1
        
        print("\n全てのテーブル構造検証が成功しました！")
        return 0
        
    except Exception as e:
        logger.error(f"検証実行エラー: {e}")
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())
