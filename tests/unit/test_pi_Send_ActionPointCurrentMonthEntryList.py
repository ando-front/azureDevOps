from .normalize_column import normalize_column_name
from .helpers.sql_column_extractor import extract_normalized_columns
import unittest
import json
import os
import re
import copy

class TestPiSendActionPointCurrentMonthEntryList(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.json_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "src", "dev", "pipeline", "pi_Send_ActionPointCurrentMonthEntryList.json"
        )
        with open(cls.json_path, encoding="utf-8") as f:
            cls.pipeline = json.load(f)

    def test_pipeline_name(self):
        print("[INFO] パイプライン名テスト")
        name = self.pipeline["name"]
        self.assertIn("pi_Send_ActionPointCurrentMonthEntryList", name)

    def test_activities_exist(self):
        print("[INFO] activities数テスト")
        activities = self.pipeline["properties"]["activities"]
        self.assertGreaterEqual(len(activities), 2)

    def test_first_activity_copy(self):
        print("[INFO] 1つ目のCopyアクティビティ内容テスト")
        activities = self.pipeline["properties"]["activities"]
        first = activities[0]
        self.assertEqual(first["type"], "Copy")
        self.assertIn("source", first["typeProperties"])
        self.assertIn("sink", first["typeProperties"])
        self.assertIn("sqlReaderQuery", first["typeProperties"]["source"])

    def test_second_activity_copy(self):
        print("[INFO] 2つ目のCopyアクティビティ内容テスト")
        activities = self.pipeline["properties"]["activities"]
        second = activities[1]
        self.assertEqual(second["type"], "Copy")
        self.assertIn("source", second["typeProperties"])
        self.assertIn("sink", second["typeProperties"])

    def test_missing_required_property(self):
        print("[INFO] 必須プロパティ欠損時の異常系テスト")
        broken = copy.deepcopy(self.pipeline)
        del broken["properties"]["activities"][0]["typeProperties"]["source"]
        with self.assertRaises(KeyError):
            _ = broken["properties"]["activities"][0]["typeProperties"]["source"]

    def test_mock_select_columns(self):
        print("[INFO] モックデータでSELECTカラム検証")
        # モックデータのSELECT句から期待カラム(ASエイリアス)が抽出されること
        sql = self.pipeline["properties"]["activities"][0]["typeProperties"]["source"]["sqlReaderQuery"]
        cols = extract_normalized_columns(sql)
        expected = ['MTGID', 'ACTIONPOINT_TYPE', 'ENTRY_DATE', 'OUTPUT_DATETIME']
        for exp in expected:
            self.assertIn(exp, cols, f"期待カラム {exp} が存在しません: {cols}")
        print("[INFO] モックデータによるSELECTカラムテスト成功")

if __name__ == "__main__":
    unittest.main(verbosity=2)
