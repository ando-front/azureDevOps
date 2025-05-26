import pytest
import copy
from tests.unit.helpers.sql_column_extractor import extract_normalized_columns
from tests.unit.normalize_column import normalize_column_name


def test_pipeline_name(pipeline_copy_marketing_client_dm):
    assert "pi_Copy_marketing_client_dm" in pipeline_copy_marketing_client_dm["name"]


def test_activities_exist(pipeline_copy_marketing_client_dm):
    acts = pipeline_copy_marketing_client_dm["properties"]["activities"]
    assert len(acts) == 2


def test_input_output_columns_match(pipeline_copy_marketing_client_dm):
    activities = pipeline_copy_marketing_client_dm["properties"]["activities"]
    cols_in  = extract_normalized_columns(activities[0]["typeProperties"]["source"]["sqlReaderQuery"])
    cols_out = extract_normalized_columns(activities[1]["typeProperties"]["scripts"][0]["text"])
    assert cols_in == cols_out, f"SELECTとINSERTのカラムリストが一致しません: {cols_in} != {cols_out}"


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
        INSERT INTO DUMMY_TABLE
        SELECT
            COL1
        FROM DUAL
    '''
    cols_in  = extract_normalized_columns(broken["properties"]["activities"][0]["typeProperties"]["source"]["sqlReaderQuery"])
    cols_out = extract_normalized_columns(broken["properties"]["activities"][1]["typeProperties"]["scripts"][0]["text"])
    assert cols_in != cols_out


def test_mock_column_names(pipeline_copy_marketing_client_dm):
    sql = pipeline_copy_marketing_client_dm["properties"]["activities"][0]["typeProperties"]["source"]["sqlReaderQuery"]
    cols = extract_normalized_columns(sql)
    expected = ["CLIENT_KEY_AX", "LIV0EU_1X", "LIV0EU_8X"]
    for c in expected:
        assert normalize_column_name(c) in cols
