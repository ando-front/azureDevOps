import unittest
import os

class TestPipeline2(unittest.TestCase):
    def setUp(self):
        # テストデータの準備
        self.input_folder = "input"
        self.output_folder = "output"
        self.test_file = "test.csv"
        self.test_data = "header1,header2\nvalue1,value2\n"
        self.sftp_folder = "sftp"

        # inputフォルダの作成
        if not os.path.exists(self.input_folder):
            os.makedirs(self.input_folder)

        # outputフォルダの作成
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)

        # sftpフォルダの作成
        if not os.path.exists(self.sftp_folder):
            os.makedirs(self.sftp_folder)

        # テストデータの作成
        with open(os.path.join(self.input_folder, self.test_file), "w") as f:
            f.write(self.test_data)

    def test_pipeline2(self):
        # pipeline2.jsonに対するテスト
        # モックデータを使用してパイプラインを実行し、結果を検証

        # パイプラインの実行 (ここではモック)
        # 実際には、ADFのエミュレータまたはモック環境を使用してパイプラインを実行する必要があります
        # 例: adf_emulator.run_pipeline("pipeline2.json")

        # 出力ファイルの確認
        output_file = os.path.join(self.output_folder, self.test_file + ".gz")
        self.assertTrue(os.path.exists(output_file))

        # SFTPサーバにファイルが転送されたことを確認
        sftp_file = os.path.join(self.sftp_folder, self.test_file + ".gz")
        self.assertTrue(os.path.exists(sftp_file))

    def tearDown(self):
        # テストデータの削除
        input_file = os.path.join(self.input_folder, self.test_file)
        if os.path.exists(input_file):
            os.remove(input_file)

        output_file = os.path.join(self.output_folder, self.test_file + ".gz")
        if os.path.exists(output_file):
            os.remove(output_file)

        sftp_file = os.path.join(self.sftp_folder, self.test_file + ".gz")
        if os.path.exists(sftp_file):
            os.remove(sftp_file)
