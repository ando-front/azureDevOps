"""
ADFパイプライン: pi_Insert_ClientDmBx のユニットテスト。
パイプライン構造・必須プロパティ・Synapse接続の検証を行う。

テストケースID: UT-PI-003
テスト戦略準拠: パイプライン個別検証 (Unit Test Layer)
自動化レベル: 完全自動化
"""

import pytest
from tests.conftest import pipeline_insert_clientdm_bx
from tests.unit.helpers.sql_column_extractor import extract_normalized_columns
from tests.unit.helpers.synapse_test_helper import SynapseTestConnection, verify_synapse_connection


@pytest.mark.unit
@pytest.mark.test_case_id("UT-PI-003-001")
def test_pipeline_name(pipeline_insert_clientdm_bx):
    """パイプライン名検証 - UT-PI-003-001"""
    assert "pi_Insert_ClientDmBx" in pipeline_insert_clientdm_bx["name"]


@pytest.mark.test_case_id("UT-PI-003-002")
def test_activities_exist(pipeline_insert_clientdm_bx):
    """アクティビティ存在検証 - UT-PI-003-002"""
    acts = pipeline_insert_clientdm_bx["properties"]["activities"]
    assert len(acts) == 1  # SQLスクリプトアクティビティが1つ


@pytest.mark.test_case_id("UT-PI-003-003")
def test_input_output_columns_match(pipeline_insert_clientdm_bx):
    """SQLスクリプトアクティビティのSQL文構造検証 - UT-PI-003-003"""
    activity = pipeline_insert_clientdm_bx["properties"]["activities"][0]
    sql_script = activity["typeProperties"]["scripts"][0]["text"]
    
    # 主要なテーブル操作が含まれているかチェック
    assert "TRUNCATE TABLE" in sql_script, "TRUNCATE文が見つかりません"
    assert "INSERT INTO" in sql_script, "INSERT文が見つかりません"
    assert "SELECT" in sql_script, "SELECT文が見つかりません"
    
    # 期待されるテーブル名が含まれているかチェック
    expected_tables = [
        "omni_ods_cloak_trn_usageservice_bx4x_temp",
        "omni_ods_cloak_trn_usageservice_bx3xsaid_temp", 
        "omni_ods_marketing_trn_client_dm_bx_temp"
    ]
    
    for table in expected_tables:
        assert table in sql_script, f"期待されるテーブル {table} が見つかりません"


def test_missing_required_property(pipeline_insert_clientdm_bx):
    import copy
    broken = copy.deepcopy(pipeline_insert_clientdm_bx)
    # scriptsプロパティを削除
    del broken["properties"]["activities"][0]["typeProperties"]["scripts"]
    with pytest.raises(KeyError):
        _ = broken["properties"]["activities"][0]["typeProperties"]["scripts"]


def test_column_count_mismatch(pipeline_insert_clientdm_bx):
    """SQLスクリプトアクティビティのカラム一致性検証"""
    activity = pipeline_insert_clientdm_bx["properties"]["activities"][0]
    sql_script = activity["typeProperties"]["scripts"][0]["text"]
    
    # SELECT文とINSERT文の対応関係をチェック
    import re
    
    # INSERT INTO文のパターンを検索
    insert_patterns = re.findall(r'INSERT INTO\s+\[[\w\]\.]+\]', sql_script, re.IGNORECASE)
    assert len(insert_patterns) >= 3, f"期待される INSERT 文の数が不足: {len(insert_patterns)}"
    
    # 主要なSELECT句の存在確認
    select_bx_pattern = re.search(r'SELECT[\s\S]*?BX[\s\S]*?FROM', sql_script, re.IGNORECASE)
    assert select_bx_pattern is not None, "BXカラムを含むSELECT文が見つかりません"
    
    # JOINの正しい使用確認
    join_patterns = re.findall(r'LEFT JOIN|INNER JOIN|JOIN', sql_script, re.IGNORECASE)
    assert len(join_patterns) >= 2, f"期待されるJOIN文の数が不足: {len(join_patterns)}"


def test_synapse_connection():
    """Synapse Analytics接続テスト"""
    assert verify_synapse_connection(), "Synapse Analytics (SQL Server)への接続に失敗しました"


def test_client_dm_table_exists(synapse_connection):
    """ClientDmテーブルの存在確認"""
    query = """
    SELECT COUNT(*) as table_count 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_NAME = 'ClientDm' AND TABLE_SCHEMA = 'dbo'
    """
    result = synapse_connection.execute_query(query)
    assert result[0][0] == 1, "ClientDmテーブルが存在しません"


def test_client_dm_insert_and_select(synapse_connection, clean_test_data):
    """ClientDmテーブルへの挿入と選択テスト"""
    # テストデータ挿入
    insert_query = """
    INSERT INTO dbo.ClientDm (ClientName, ClientCode, Status) 
    VALUES (?, ?, ?)
    """
    params = ('テストクライアント_自動', 'AUTO001', 'Active')
    rows_affected = synapse_connection.execute_query(insert_query, params)
    assert rows_affected == 1, "データの挿入に失敗しました"
    
    # 挿入されたデータの確認
    select_query = """
    SELECT ClientName, ClientCode, Status 
    FROM dbo.ClientDm 
    WHERE ClientCode = ?
    """
    result = synapse_connection.execute_query(select_query, ('AUTO001',))
    assert len(result) == 1, "挿入されたデータが見つかりません"
    assert result[0][0] == 'テストクライアント_自動', "クライアント名が一致しません"
    assert result[0][1] == 'AUTO001', "クライアントコードが一致しません"
    assert result[0][2] == 'Active', "ステータスが一致しません"


def test_client_dm_update(synapse_connection, clean_test_data):
    """ClientDmテーブルの更新テスト"""
    # まずテストデータを挿入
    insert_query = """
    INSERT INTO dbo.ClientDm (ClientName, ClientCode, Status) 
    VALUES (?, ?, ?)
    """
    synapse_connection.execute_query(insert_query, ('更新テスト', 'UPD001', 'Active'))
    
    # データを更新
    update_query = """
    UPDATE dbo.ClientDm 
    SET Status = ? 
    WHERE ClientCode = ?
    """
    rows_affected = synapse_connection.execute_query(update_query, ('Inactive', 'UPD001'))
    assert rows_affected == 1, "データの更新に失敗しました"
    
    # 更新結果を確認
    select_query = """
    SELECT Status 
    FROM dbo.ClientDm 
    WHERE ClientCode = ?
    """
    result = synapse_connection.execute_query(select_query, ('UPD001',))
    assert result[0][0] == 'Inactive', "ステータスの更新が反映されていません"
