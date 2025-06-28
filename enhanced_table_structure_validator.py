"""
E2Eテスト用 汎用構造検証ヘルパーメソッド（改善提案）

これは、marketing_client_dmで発見された問題を他のテーブルでも防ぐための
汎用的な検証フレームワークの実装案です。
"""

from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


class TableStructureValidator:
    """テーブル構造検証の汎用クラス"""
    
    def __init__(self, db_connection):
        self.db_connection = db_connection
    
    def validate_table_structure_comprehensive(
        self, 
        table_name: str, 
        schema_name: str = "dbo",
        expected_column_count: Optional[int] = None,
        required_columns: Optional[List[str]] = None,
        forbidden_columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        テーブル構造の包括的検証
        
        Args:
            table_name: テーブル名
            schema_name: スキーマ名
            expected_column_count: 期待されるカラム数（None時はチェックしない）
            required_columns: 必須カラムのリスト
            forbidden_columns: 存在してはいけないカラムのリスト
            
        Returns:
            検証結果の詳細辞書
        """
        validation_results = {
            "table_name": table_name,
            "schema_name": schema_name,
            "validation_passed": True,
            "errors": [],
            "warnings": [],
            "details": {}
        }
        
        try:
            # 1. テーブル存在確認
            table_exists = self._check_table_exists(table_name, schema_name)
            validation_results["details"]["table_exists"] = table_exists
            
            if not table_exists:
                validation_results["validation_passed"] = False
                validation_results["errors"].append(f"Table {schema_name}.{table_name} does not exist")
                return validation_results
            
            # 2. カラム情報取得
            column_info = self._get_column_info(table_name, schema_name)
            actual_column_count = len(column_info)
            actual_column_names = [col["column_name"] for col in column_info]
            
            validation_results["details"]["actual_column_count"] = actual_column_count
            validation_results["details"]["actual_columns"] = actual_column_names
            
            # 3. カラム数検証
            if expected_column_count is not None:
                validation_results["details"]["expected_column_count"] = expected_column_count
                validation_results["details"]["column_count_match"] = actual_column_count == expected_column_count
                
                if actual_column_count != expected_column_count:
                    validation_results["validation_passed"] = False
                    validation_results["errors"].append(
                        f"Column count mismatch: expected {expected_column_count}, actual {actual_column_count}"
                    )
            
            # 4. 必須カラム検証
            if required_columns:
                missing_columns = []
                for required_col in required_columns:
                    if required_col not in actual_column_names:
                        missing_columns.append(required_col)
                
                validation_results["details"]["missing_required_columns"] = missing_columns
                if missing_columns:
                    validation_results["validation_passed"] = False
                    validation_results["errors"].append(f"Missing required columns: {missing_columns}")
            
            # 5. 禁止カラム検証
            if forbidden_columns:
                forbidden_found = []
                for forbidden_col in forbidden_columns:
                    if forbidden_col in actual_column_names:
                        forbidden_found.append(forbidden_col)
                
                validation_results["details"]["forbidden_columns_found"] = forbidden_found
                if forbidden_found:
                    validation_results["validation_passed"] = False
                    validation_results["errors"].append(f"Forbidden columns found: {forbidden_found}")
            
            # 6. データ型整合性チェック
            data_type_issues = self._validate_data_types(column_info)
            validation_results["details"]["data_type_issues"] = data_type_issues
            if data_type_issues:
                validation_results["warnings"].extend(data_type_issues)
            
            logger.info(f"Table structure validation completed for {schema_name}.{table_name}")
            return validation_results
            
        except Exception as e:
            validation_results["validation_passed"] = False
            validation_results["errors"].append(f"Validation failed with exception: {str(e)}")
            logger.error(f"Table structure validation failed for {schema_name}.{table_name}: {e}")
            return validation_results
    
    def _check_table_exists(self, table_name: str, schema_name: str) -> bool:
        """テーブル存在確認"""
        query = """
        SELECT COUNT(*) as table_count
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ?
        """
        result = self.db_connection.execute_query(query, (table_name, schema_name))
        return result[0]["table_count"] > 0 if result else False
    
    def _get_column_info(self, table_name: str, schema_name: str) -> List[Dict[str, Any]]:
        """カラム情報取得"""
        query = """
        SELECT 
            COLUMN_NAME as column_name,
            DATA_TYPE as data_type,
            IS_NULLABLE as is_nullable,
            CHARACTER_MAXIMUM_LENGTH as max_length,
            NUMERIC_PRECISION as precision,
            NUMERIC_SCALE as scale,
            COLUMN_DEFAULT as default_value
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ?
        ORDER BY ORDINAL_POSITION
        """
        result = self.db_connection.execute_query(query, (table_name, schema_name))
        return [dict(row) for row in result] if result else []
    
    def _validate_data_types(self, column_info: List[Dict[str, Any]]) -> List[str]:
        """データ型整合性チェック"""
        issues = []
        
        for col in column_info:
            col_name = col["column_name"]
            data_type = col["data_type"]
            
            # 一般的な問題パターンをチェック
            if data_type == "text" and col["max_length"] is None:
                issues.append(f"Column {col_name}: TEXT type without length specification")
            
            if col["is_nullable"] == "YES" and col_name.endswith("_KEY"):
                issues.append(f"Column {col_name}: Key column should not be nullable")
        
        return issues
    
    def validate_multiple_tables(self, table_configs: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """複数テーブルの一括検証"""
        results = {}
        
        for config in table_configs:
            table_name = config["table_name"]
            validation_result = self.validate_table_structure_comprehensive(**config)
            results[table_name] = validation_result
        
        return results


# 使用例
def example_usage():
    """汎用構造検証の使用例"""
    
    # Marketing Client DMの検証設定
    marketing_client_dm_config = {
        "table_name": "omni_ods_marketing_trn_client_dm",
        "schema_name": "omni",
        "expected_column_count": 460,  # 修正後の正しい期待値
        "required_columns": ["CLIENT_KEY_AX", "LIV0EU_1X", "LIV0EU_8X"],
        "forbidden_columns": ["KNUMBER_AX", "ADDRESS_KEY_AX"]  # 存在しないカラム
    }
    
    # Client DMの検証設定
    client_dm_config = {
        "table_name": "client_dm",
        "schema_name": "dbo", 
        "expected_column_count": 9,
        "required_columns": ["client_id", "client_name", "email", "status"],
        "forbidden_columns": ["obsolete_field"]
    }
    
    # ClientDmBxの検証設定
    clientdm_bx_config = {
        "table_name": "ClientDmBx",
        "schema_name": "dbo",
        "expected_column_count": 11,
        "required_columns": ["client_id", "segment", "bx_flag"],
        "forbidden_columns": []
    }
    
    # 一括検証実行
    validator = TableStructureValidator(db_connection)
    results = validator.validate_multiple_tables([
        marketing_client_dm_config,
        client_dm_config,
        clientdm_bx_config
    ])
    
    # 結果レポート
    for table_name, result in results.items():
        print(f"\n=== {table_name} ===")
        print(f"Validation Passed: {result['validation_passed']}")
        if result['errors']:
            print(f"Errors: {result['errors']}")
        if result['warnings']:
            print(f"Warnings: {result['warnings']}")
        print(f"Column Count: {result['details']['actual_column_count']}")


# E2Eテストへの統合例
class EnhancedE2ETestMixin:
    """E2Eテストクラス用のミックスイン"""
    
    def setup_structure_validator(self, db_connection):
        """構造検証の初期化"""
        self.structure_validator = TableStructureValidator(db_connection)
    
    def assert_table_structure(self, **kwargs):
        """テーブル構造のアサーション"""
        result = self.structure_validator.validate_table_structure_comprehensive(**kwargs)
        
        assert result["validation_passed"], \
            f"Table structure validation failed: {result['errors']}"
        
        return result
    
    def test_all_table_structures(self):
        """全テーブル構造の検証テスト例"""
        table_configs = self.get_table_validation_configs()
        results = self.structure_validator.validate_multiple_tables(table_configs)
        
        failed_tables = []
        for table_name, result in results.items():
            if not result["validation_passed"]:
                failed_tables.append(f"{table_name}: {result['errors']}")
        
        assert not failed_tables, f"Table structure validation failed for: {failed_tables}"
