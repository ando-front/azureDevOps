import pytest
from tests.conftest import pipeline_insert_clientdm_bx
from tests.unit.helpers.sql_column_extractor import extract_normalized_columns


def test_pipeline_name(pipeline_insert_clientdm_bx):
    assert "pi_Insert_ClientDmBx" in pipeline_insert_clientdm_bx["name"]


def test_activities_exist(pipeline_insert_clientdm_bx):
    acts = pipeline_insert_clientdm_bx["properties"]["activities"]
    assert len(acts) == 2


def test_input_output_columns_match(pipeline_insert_clientdm_bx):
    acts = pipeline_insert_clientdm_bx["properties"]["activities"]
    cols_in  = extract_normalized_columns(acts[0]["typeProperties"]["source"]["sqlReaderQuery"])
    cols_out = extract_normalized_columns(acts[1]["typeProperties"]["scripts"][0]["text"])
    assert cols_in == cols_out, f"SELECTとINSERTのカラムリストが一致しません: {cols_in} != {cols_out}"


def test_missing_required_property(pipeline_insert_clientdm_bx):
    import copy
    broken = copy.deepcopy(pipeline_insert_clientdm_bx)
    del broken["properties"]["activities"][0]["typeProperties"]["source"]
    with pytest.raises(KeyError):
        _ = broken["properties"]["activities"][0]["typeProperties"]["source"]["sqlReaderQuery"]


def test_column_count_mismatch(pipeline_insert_clientdm_bx):
    import copy
    broken = copy.deepcopy(pipeline_insert_clientdm_bx)
    broken["properties"]["activities"][0]["typeProperties"]["source"]["sqlReaderQuery"] = '''
        SELECT
            COL1 AS COL1,
            COL2 AS COL2
        FROM DUAL
    '''
    broken["properties"]["activities"][1]["typeProperties"]["scripts"][0]["text"] = '''
        INSERT INTO DUMMY
        SELECT COL1
        FROM DUAL
    '''
    cols_in  = extract_normalized_columns(broken["properties"]["activities"][0]["typeProperties"]["source"]["sqlReaderQuery"])
    cols_out = extract_normalized_columns(broken["properties"]["activities"][1]["typeProperties"]["scripts"][0]["text"])
    assert cols_in != cols_out
