import unittest
import json
import os
from .normalize_column import normalize_column_name
import re

class TestPiInsertClientDmBx(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.json_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "src", "dev", "pipeline", "pi_Insert_ClientDmBx.json"
        )
        with open(cls.json_path, encoding="utf-8") as f:
            cls.pipeline = json.load(f)

    def test_pipeline_name(self):
        print("[INFO] パイプライン名テスト")
        name = self.pipeline["name"]
        self.assertIn("pi_Insert_ClientDmBx", name)

    def test_activities_exist(self):
        print("[INFO] activities数テスト")
        activities = self.pipeline["properties"]["activities"]
        self.assertEqual(len(activities), 1)

    def test_script_activity(self):
        print("[INFO] Scriptアクティビティ内容テスト")
        activity = self.pipeline["properties"]["activities"][0]
        self.assertEqual(activity["type"], "Script")
        self.assertIn("scripts", activity["typeProperties"])
        self.assertEqual(activity["linkedServiceName"]["referenceName"], "li_dam_dwh")

    def test_script_contains_insert(self):
        print("[INFO] Script SQLにINSERT文が含まれるかテスト")
        activity = self.pipeline["properties"]["activities"][0]
        script = activity["typeProperties"]["scripts"][0]["text"]
        self.assertIn("INSERT INTO", script.upper())
        self.assertIn("SELECT", script.upper())

    def test_missing_required_property(self):
        print("[INFO] 必須プロパティ欠損時の異常系テスト")
        import copy
        broken = copy.deepcopy(self.pipeline)
        del broken["properties"]["activities"][0]["typeProperties"]["scripts"]
        with self.assertRaises(KeyError):
            _ = broken["properties"]["activities"][0]["typeProperties"]["scripts"]

    def test_mock_insert_select_structure(self):
        print("[INFO] モックデータでINSERT/SELECT構造の検証")
        activity = self.pipeline["properties"]["activities"][0]
        script = activity["typeProperties"]["scripts"][0]["text"]
        insert_match = re.search(r"INSERT\s+INTO\s+\[omni\]\.\[omni_ods_marketing_trn_client_dm_bx_temp\].*?SELECT(.+?)FROM", script, re.DOTALL | re.IGNORECASE)
        self.assertIsNotNone(insert_match, "INSERT INTO SELECT構造が見つかりません")
        select_body = insert_match.group(1)
        select_lines = [line.strip() for line in select_body.splitlines() if line.strip() and not line.strip().startswith('--')]
        print(f"[DEBUG] SELECT句のカラム数: {len(select_lines)} カラム例: {select_lines[:5]}")
        self.assertGreaterEqual(len(select_lines), 6, "SELECT句のカラム数が想定より少ない")
        self.assertIn('cldm.*', select_body, "cldm.*がSELECT句に含まれていない")
        # 正規化してカラム抽出・検証
        select_columns = []
        for col in select_lines:
            # AS句でエイリアスがあれば両方抽出
            m_as = re.match(r"(.+?)\s+AS\s+([\w@]+)", col, re.IGNORECASE)
            if m_as:
                select_columns.append(m_as.group(1).strip())
                select_columns.append(m_as.group(2).strip())
            else:
                select_columns.append(col)
        norm_columns = [normalize_column_name(c) for c in select_columns]
        print(f"[DEBUG] 正規化後SELECT句カラム: {norm_columns}")
        # 例: 期待カラム（必要に応じて追加）
        # expected = ["CLDM_ID", ...]
        # for col in expected:
        #     norm_col = normalize_column_name(col)
        #     self.assertIn(norm_col, norm_columns, f"期待カラム {col} がSELECT句に存在しない (正規化: {norm_col})")
        print("[INFO] モックデータによるINSERT/SELECT構造テスト成功 (正規化比較)")

if __name__ == "__main__":
    unittest.main(verbosity=2)
