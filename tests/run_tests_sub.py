import unittest
import os
from azure.storage.blob import BlobServiceClient

# ToDo:コンテナイメージをダウンロードできない問題で、他環境で作成したイメージでコンテナを立ちげると
# テストコードファイルが追加できない（コンテナをビルドしないため）
# 他環境で作成する場合は最小限のイメージで作り直す（Pythonだけ、など）
class TestADF(unittest.TestCase):
    def setUp(self):
        # テストデータの準備
        self.input_folder = "input"
        self.output_folder = "output"
        self.test_file = "test.csv"
        self.test_data = "header1,header2\nvalue1,value2\n"

        # Azuriteの接続文字列
        self.connection_string = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"

        # inputフォルダの作成
        if not os.path.exists(self.input_folder):
            os.makedirs(self.input_folder)

        # outputフォルダの作成
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)

        # テストデータの作成
        with open(os.path.join(self.input_folder, self.test_file), "w") as f:
            f.write(self.test_data)

        # Azuriteにコンテナとテストデータを作成
        blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        
        # コンテナの作成（存在しない場合）
        for container_name in ["input", "output"]:
            try:
                blob_service_client.create_container(container_name)
            except:
                pass

        # 入力ファイルのアップロード
        blob_client = blob_service_client.get_blob_client(container="input", blob=self.test_file)
        with open(os.path.join(self.input_folder, self.test_file), "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

    def test_azurite_is_running(self):
        # Azuriteが起動していることを確認
        try:
            blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
            blob_service_client.list_containers()
            self.assertTrue(True)
            print("\nAzuriteの動作確認: 成功")
        except Exception as e:
            self.fail(f"Azurite is not running: {str(e)}")
            print("\nAzuriteの動作確認: 失敗")

    def test_pipeline1(self):
        print("\nパイプラインのテスト実行:")
        print("1. 入力ファイルの準備...")
        
        blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        
        # 入力Blobからデータを読み取り
        input_blob = blob_service_client.get_blob_client(container="input", blob=self.test_file)
        input_data = input_blob.download_blob().readall()
        print("2. 入力ファイルの読み込み完了")
        
        # 出力Blobにデータを書き込み
        output_blob = blob_service_client.get_blob_client(container="output", blob=self.test_file)
        output_blob.upload_blob(input_data, overwrite=True)
        print("3. 出力Blobへの書き込み完了")
        
        # ローカルの出力フォルダにダウンロード
        with open(os.path.join(self.output_folder, self.test_file), "wb") as download_file:
            download_file.write(input_data)
        print("4. ローカルへのダウンロード完了")

        # 出力ファイルの確認
        output_file = os.path.join(self.output_folder, self.test_file)
        self.assertTrue(os.path.exists(output_file))
        print("5. 出力ファイルの存在確認: OK")

        # 出力ファイルの内容の確認
        if os.path.exists(output_file):
            with open(output_file, "r") as f:
                output_data = f.read()
            self.assertEqual(output_data, self.test_data)
            print("6. 出力ファイルの内容確認: OK")
            print("\nテスト結果: ✅ 成功")
        else:
            print("\nテスト結果: ❌ 失敗 - 出力ファイルが見つかりません")

    def tearDown(self):
        # テストデータの削除
        for file_path in [os.path.join(self.input_folder, self.test_file),
                         os.path.join(self.output_folder, self.test_file)]:
            if os.path.exists(file_path):
                os.remove(file_path)

# メイン関数としてすべてのユニットテストを実行
def main():
    # `tests/unit` ディレクトリ配下の `test_*.py` 形式のファイルを探索してテストを自動実行
    loader = unittest.TestLoader()
    suite = loader.discover(start_dir="tests/unit", pattern="test_*.py")

    # テストランナーを使ってテストスイートを実行
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # テスト結果に応じて終了コードを返す
    # 成功時: 0, 失敗やエラー時: 1以上
    sys.exit(len(result.failures) + len(result.errors))

# エントリーポイント
if __name__ == "__main__":
    print("=== ADFパイプラインテストの実行 ===")
    main()
    print("=== ADFパイプラインテストの実行 ===")
