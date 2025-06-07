"""
ADFパイプライン: pi_Copy_marketing_client_dm のユニットテスト。
パイプライン名・アクティビティ・カラム整合性の検証を行う。
"""

import pytest
import copy
from tests.unit.helpers.english_column_extractor import compare_english_columns
from tests.unit.normalize_column import normalize_column_name


def test_pipeline_name(pipeline_copy_marketing_client_dm):
    assert "pi_Copy_marketing_client_dm" in pipeline_copy_marketing_client_dm["name"]


def test_activities_exist(pipeline_copy_marketing_client_dm):
    acts = pipeline_copy_marketing_client_dm["properties"]["activities"]
    assert len(acts) == 2


def test_input_output_columns_match(pipeline_copy_marketing_client_dm):
    activities = pipeline_copy_marketing_client_dm["properties"]["activities"]
    select_sql = activities[0]["typeProperties"]["source"]["sqlReaderQuery"]
    insert_sql = activities[1]["typeProperties"]["scripts"][0]["text"]
    
    is_match, select_cols, insert_cols = compare_english_columns(select_sql, insert_sql)
    
    if not is_match:
        # 差分を詳細に表示
        select_only = select_cols - insert_cols
        insert_only = insert_cols - select_cols
        print(f"\nSELECT句のみにある英語カラム: {sorted(select_only)}")
        print(f"INSERT句のみにある英語カラム: {sorted(insert_only)}")
        print(f"SELECT句英語カラム数: {len(select_cols)}")
        print(f"INSERT句英語カラム数: {len(insert_cols)}")
    
    assert is_match, f"SELECTとINSERTの英語カラムリストが一致しません"


def test_missing_required_property(pipeline_copy_marketing_client_dm):
    broken = copy.deepcopy(pipeline_copy_marketing_client_dm)
    del broken["properties"]["activities"][0]["typeProperties"]["source"]
    with pytest.raises(KeyError):
        _ = broken["properties"]["activities"][0]["typeProperties"]["source"]["sqlReaderQuery"]


def test_column_count_mismatch(pipeline_copy_marketing_client_dm):
    broken = copy.deepcopy(pipeline_copy_marketing_client_dm)
    broken["properties"]["activities"][0]["typeProperties"]["source"]["sqlReaderQuery"] = '''
        SELECT
            COL1 AS COL1,
            COL2 AS COL2
        FROM DUAL
    '''
    broken["properties"]["activities"][1]["typeProperties"]["scripts"][0]["text"] = '''
        INSERT INTO DUMMY_TABLE (COL1)
        SELECT
            COL1
        FROM DUAL
    '''
    select_sql = broken["properties"]["activities"][0]["typeProperties"]["source"]["sqlReaderQuery"]
    insert_sql = broken["properties"]["activities"][1]["typeProperties"]["scripts"][0]["text"]
    
    is_match, select_cols, insert_cols = compare_english_columns(select_sql, insert_sql)
    assert not is_match


def test_mock_column_names(pipeline_copy_marketing_client_dm):
    sql = pipeline_copy_marketing_client_dm["properties"]["activities"][0]["typeProperties"]["source"]["sqlReaderQuery"]
    from tests.unit.helpers.english_column_extractor import extract_english_column_names
    cols = extract_english_column_names(sql)
    expected = ["CLIENT_KEY_AX", "LIV0EU_1X", "LIV0EU_8X"]
    for c in expected:
        assert c in cols
