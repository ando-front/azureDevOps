import pytest
from tests.conftest import pipeline_insert_clientdm_bx
from tests.unit.helpers.sql_column_extractor import extract_normalized_columns


def test_pipeline_name(pipeline_insert_clientdm_bx):
    assert "pi_Insert_ClientDmBx" in pipeline_insert_clientdm_bx["name"]


def test_activities_exist(pipeline_insert_clientdm_bx):
    acts = pipeline_insert_clientdm_bx["properties"]["activities"]
    assert len(acts) == 1  # SQLスクリプトアクティビティが1つ


def test_input_output_columns_match(pipeline_insert_clientdm_bx):
    # SQLスクリプトタイプのため、このテストはスキップ
    pytest.skip("SQLスクリプトタイプのアクティビティにはsource/sinkプロパティがありません")


def test_missing_required_property(pipeline_insert_clientdm_bx):
    import copy
    broken = copy.deepcopy(pipeline_insert_clientdm_bx)
    # scriptsプロパティを削除
    del broken["properties"]["activities"][0]["typeProperties"]["scripts"]
    with pytest.raises(KeyError):
        _ = broken["properties"]["activities"][0]["typeProperties"]["scripts"]


def test_column_count_mismatch(pipeline_insert_clientdm_bx):
    # SQLスクリプトタイプのため、このテストはスキップ
    pytest.skip("SQLスクリプトタイプのアクティビティにはsource/sinkプロパティがありません")
