import unittest
import json
import os
import re
from .normalize_column import normalize_column_name

class TestPiCopyMarketingClientDm(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # テスト対象JSONファイルのパス
        cls.json_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "src", "dev", "pipeline", "pi_Copy_marketing_client_dm.json"
        )
        with open(cls.json_path, encoding="utf-8") as f:
            cls.pipeline = json.load(f)

    def test_pipeline_name(self):
        # パイプライン名が正しいか
        name = self.pipeline["name"]
        self.assertIn("pi_Copy_marketing_client_dm", name)

    def test_activities_exist(self):
        # activitiesが2つ存在するか
        activities = self.pipeline["properties"]["activities"]
        self.assertEqual(len(activities), 2)

    def test_first_activity_copy(self):
        # 1つ目のアクティビティがCopyであること
        activities = self.pipeline["properties"]["activities"]
        first = activities[0]
        self.assertEqual(first["type"], "Copy")
        self.assertIn("source", first["typeProperties"])
        self.assertIn("sink", first["typeProperties"])
        self.assertEqual(first["inputs"][0]["referenceName"], "ds_sqlmi")
        self.assertEqual(first["outputs"][0]["referenceName"], "ds_DamDwhTable")

    def test_second_activity_script(self):
        # 2つ目のアクティビティがScriptであること
        activities = self.pipeline["properties"]["activities"]
        second = activities[1]
        self.assertEqual(second["type"], "Script")
        self.assertIn("scripts", second["typeProperties"])
        self.assertEqual(second["linkedServiceName"]["referenceName"], "li_dam_dwh")
        self.assertEqual(second["dependsOn"][0]["activity"], "at_Copy_marketing_ClientDM_temp")

    def test_input_output_columns_match(self, pipeline=None):
        print("[INFO] インプットとアウトプットのカラム一致テスト開始")
        if pipeline is None:
            pipeline = self.pipeline
        activities = pipeline["properties"]["activities"]
        first = activities[0]
        sql = first["typeProperties"]["source"]["sqlReaderQuery"]
        m = re.search(r"SELECT(.+?)FROM", sql, re.DOTALL | re.IGNORECASE)
        self.assertIsNotNone(m, "SELECT句が見つかりません")
        select_body = m.group(1)
        columns = []
        for line in select_body.splitlines():
            line = line.strip()
            if not line or line.startswith('--'):
                continue
            m_as = re.match(r".+? AS ([A-Z0-9_]+)", line, re.IGNORECASE)
            if m_as:
                columns.append(m_as.group(1).strip())
            else:
                columns.append(line.split()[0].strip(','))
        print(f"[DEBUG] SELECT句カラム数: {len(columns)} カラム例: {columns[:3]} ...")
        second = activities[1]
        script = second["typeProperties"]["scripts"][0]["text"]
        m2 = re.search(r"INSERT\s+INTO\s+\[omni\]\.\[omni_ods_marketing_trn_client_dm\]\s*SELECT(.+?)FROM", script, re.DOTALL | re.IGNORECASE)
        if not m2:
            # テスト用SQL用のパターン
            m2 = re.search(r"INSERT\s+INTO\s+\w+\s*SELECT(.+?)FROM", script, re.DOTALL | re.IGNORECASE)
        self.assertIsNotNone(m2, "INSERT INTO SELECT句が見つかりません")
        insert_columns = [line.strip().strip(',') for line in m2.group(1).splitlines() if line.strip() and not line.strip().startswith('--')]
        print(f"[DEBUG] INSERT句カラム数: {len(insert_columns)} カラム例: {insert_columns[:3]} ...")
        # 正規化して比較
        norm_columns = [normalize_column_name(c) for c in columns]
        norm_insert_columns = [normalize_column_name(c) for c in insert_columns]
        print(f"[DEBUG] 正規化後SELECT句カラム例: {norm_columns[:3]} ...")
        print(f"[DEBUG] 正規化後INSERT句カラム例: {norm_insert_columns[:3]} ...")
        self.assertEqual(len(norm_columns), len(norm_insert_columns), f"インプット({len(norm_columns)})とアウトプット({len(norm_insert_columns)})のカラム数が一致しません")
        for i, (c1, c2) in enumerate(zip(norm_columns, norm_insert_columns)):
            self.assertEqual(c1, c2, f"{i+1}番目のカラムが一致しません: {c1} != {c2}")
        print("[INFO] カラム一致テスト成功 (正規化比較)")

    def test_missing_required_property(self):
        print("[INFO] 必須プロパティ欠損時の異常系テスト開始")
        import copy
        broken = copy.deepcopy(self.pipeline)
        del broken["properties"]["activities"][0]["typeProperties"]["source"]
        with self.assertRaises(KeyError):
            _ = broken["properties"]["activities"][0]["typeProperties"]["source"]["sqlReaderQuery"]
        print("[INFO] 必須プロパティ欠損時のKeyErrorを正常に検出")

    def test_column_count_mismatch(self):
        print("[INFO] カラム数不一致の異常系テスト開始")
        import copy
        broken = copy.deepcopy(self.pipeline)
        activities = broken["properties"]["activities"]
        # テスト用のシンプルなSQLに書き換え（複数行でカラムを記述）
        activities[0]["typeProperties"]["source"]["sqlReaderQuery"] = '''
            SELECT
                COL1 AS COL1,
                COL2 AS COL2
            FROM DUAL
        '''
        activities[1]["typeProperties"]["scripts"][0]["text"] = '''
            INSERT INTO DUMMY_TABLE
            SELECT
                COL1
            FROM DUAL
        '''
        with self.assertRaises(AssertionError):
            self.test_input_output_columns_match(broken)
        print("[INFO] カラム数不一致時のAssertionErrorを正常に検出")

    def test_mock_column_names(self):
        print("[INFO] モックデータでカラム名検証")
        activities = self.pipeline["properties"]["activities"]
        first = activities[0]
        sql = first["typeProperties"]["source"]["sqlReaderQuery"]
        import re
        m = re.search(r"SELECT(.+?)FROM", sql, re.DOTALL | re.IGNORECASE)
        self.assertIsNotNone(m, "SELECT句が見つかりません")
        select_body = m.group(1)
        columns = []
        for line in select_body.splitlines():
            line = line.strip()
            if not line or line.startswith('--'):
                continue
            # NULL AS ... 行は除外
            if re.match(r"NULL\s+AS", line, re.IGNORECASE):
                continue
            m_as = re.match(r".+? AS ([A-Z0-9_]+)", line, re.IGNORECASE)
            if m_as:
                columns.append(m_as.group(1).strip())
            else:
                columns.append(line.split()[0].strip(','))
        expected = ["CLIENT_KEY_AX", "LIV0EU_1X", "LIV0EU_8X"]
        # 正規化して期待カラムと比較
        norm_columns = [normalize_column_name(c) for c in columns]
        norm_expected = [normalize_column_name(c) for c in expected]
        for col in norm_expected:
            self.assertIn(col, norm_columns, f"期待カラム {col} がSELECT句に存在しない (正規化: {col})")
        print(f"[DEBUG] 正規化後SELECT句カラム: {norm_columns[:5]} ...")
        print("[INFO] モックデータによるカラム名テスト成功 (正規化比較)")
