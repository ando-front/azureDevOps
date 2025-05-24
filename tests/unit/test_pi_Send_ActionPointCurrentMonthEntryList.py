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
        activities = self.pipeline["properties"]["activities"]
        first = activities[0]
        sql = first["typeProperties"]["source"]["sqlReaderQuery"]
        m = re.search(r"SELECT(.+?)FROM", sql, re.DOTALL | re.IGNORECASE)
        self.assertIsNotNone(m, "SELECT句が見つかりません")
        select_body = m.group(1)
        # カンマ区切りで1行にまとめてから分割し、関数やAS句も考慮
        select_lines = []
        buf = ''
        for line in select_body.splitlines():
            line = line.strip()
            if not line or line.startswith('--'):
                continue
            buf += ' ' + line
            if ',' in line or line.lower().endswith(' as'):
                select_lines.append(buf.strip())
                buf = ''
        if buf:
            select_lines.append(buf.strip())
        columns = []
        aliases = []
        for col in ','.join(select_lines).split(','):
            col = col.strip()
            if not col:
                continue
            # AS句でエイリアスがあれば両方抽出
            m_as = re.match(r"(.+?)\s+AS\s+([\w@]+)", col, re.IGNORECASE)
            if m_as:
                columns.append(m_as.group(1).strip())
                aliases.append(m_as.group(2).strip())
            else:
                columns.append(col)
        print(f"[DEBUG] SELECT句カラム: {columns}")
        print(f"[DEBUG] SELECT句エイリアス: {aliases}")
        # 期待カラム例
        expected = ['MTGID', 'ACTIONPOINT_TYPE', 'FORMAT(ENTRY_DATE,', '@outputDT']
        for col in expected:
            found = any(col in c for c in columns) or any(col in a for a in aliases)
            self.assertTrue(found, f"期待カラム {col} がSELECT句に存在しない")
        print("[INFO] モックデータによるSELECTカラムテスト成功")

if __name__ == "__main__":
    unittest.main(verbosity=2)
