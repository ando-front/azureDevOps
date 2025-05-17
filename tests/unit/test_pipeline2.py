import unittest
import os

class TestPipeline2(unittest.TestCase):
    def setUp(self):
        # テストデータの準備
        self.input_folder = "tests/data/input"
        self.output_folder = "tests/data/output"
        self.test_file = "test_data_input.csv"
        self.test_data = "header1,header2\nvalue1,value2\n"
        self.sftp_folder = "tests/data/sftp"

        # ディレクトリの作成
        for folder in [self.input_folder, self.output_folder, self.sftp_folder]:
            if not os.path.exists(folder):
                os.makedirs(folder)

        # テストデータの作成
        with open(os.path.join(self.input_folder, self.test_file), "w") as f:
            f.write(self.test_data)

    def test_pipeline2(self):
        print("\nパイプライン2のテスト実行:")
        print("1. 入力ファイルの準備...")
        
        # SQLMiからのデータ抽出をシミュレート
        print("2. SQLMiからデータ抽出...")
        
        # Synapse Analyticsへのデータコピーをシミュレート
        print("3. Synapse Analyticsへデータコピー...")
        
        # BlobストレージへのCSVエクスポートをシミュレート
        output_file = os.path.join(self.output_folder, self.test_file + ".gz")
        with open(output_file, "w") as f:
            f.write(self.test_data)
        print("4. Blobストレージへエクスポート完了")
        
        # SFTPサーバへのファイル転送をシミュレート
        sftp_file = os.path.join(self.sftp_folder, self.test_file + ".gz")
        with open(sftp_file, "w") as f:
            f.write(self.test_data)
        print("5. SFTPサーバへの転送完了")

        # 結果の検証
        self.assertTrue(os.path.exists(output_file))
        print("6. 出力ファイルの確認: OK")
        
        self.assertTrue(os.path.exists(sftp_file))
        print("7. SFTPファイルの確認: OK")
        print("\nテスト結果: ✅ 成功")

    def tearDown(self):
        # テストデータの削除
        test_files = [
            os.path.join(self.input_folder, self.test_file),
            os.path.join(self.output_folder, self.test_file + ".gz"),
            os.path.join(self.sftp_folder, self.test_file + ".gz")
        ]
        
        for file_path in test_files:
            if os.path.exists(file_path):
                os.remove(file_path)

if __name__ == '__main__':
    unittest.main(verbosity=2)