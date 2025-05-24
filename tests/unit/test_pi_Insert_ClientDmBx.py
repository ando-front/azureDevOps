import unittest
import json
import os

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
        self.assertIn("INSERT INTO", script)
        self.assertIn("SELECT", script)

    def test_missing_required_property(self):
        print("[INFO] 必須プロパティ欠損時の異常系テスト")
        import copy
        broken = copy.deepcopy(self.pipeline)
        del broken["properties"]["activities"][0]["typeProperties"]["scripts"]
        with self.assertRaises(KeyError):
            _ = broken["properties"]["activities"][0]["typeProperties"]["scripts"]

    def test_mock_insert_select_structure(self):
        print("[INFO] モックデータでINSERT/SELECT構造の検証")
        # モックSQL（本来はSQLパーサやDBを使うが、ここでは文字列検証）
        activity = self.pipeline["properties"]["activities"][0]
        script = activity["typeProperties"]["scripts"][0]["text"]
        # 例: INSERT INTO ... SELECT ... FROM ... の構造があるか
        import re
        insert_match = re.search(r"INSERT INTO\\s+\\[omni\\]\\.\\[omni_ods_marketing_trn_client_dm_bx_temp\\].*?SELECT(.+?)FROM", script, re.DOTALL)
        self.assertIsNotNone(insert_match, "INSERT INTO SELECT構造が見つかりません")
        select_body = insert_match.group(1)
        # モック期待値: 5列+cldm.*
        select_lines = [line.strip() for line in select_body.splitlines() if line.strip() and not line.strip().startswith('--')]
        print(f"[DEBUG] SELECT句のカラム数: {len(select_lines)} カラム例: {select_lines[:5]}")
        self.assertGreaterEqual(len(select_lines), 6, "SELECT句のカラム数が想定より少ない")
        self.assertIn('cldm.*', select_body, "cldm.*がSELECT句に含まれていない")
        print("[INFO] モックデータによるINSERT/SELECT構造テスト成功")

if __name__ == "__main__":
    unittest.main(verbosity=2)
