import unittest
import os
from azure.storage.blob import BlobServiceClient

class TestPipeline1(unittest.TestCase):
    def setUp(self):
        # テストデータの準備
        self.input_folder = "tests/data/input"
        self.output_folder = "tests/data/output"
        self.test_file = "test_data_input.csv"
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

        # BlobServiceClientの作成
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)

    def tearDown(self):
        # テストデータの削除
        if os.path.exists(os.path.join(self.input_folder, self.test_file)):
            os.remove(os.path.join(self.input_folder, self.test_file))

        # inputフォルダの削除
        if os.path.exists(self.input_folder):
            os.rmdir(self.input_folder)

        # outputフォルダの削除
        if os.path.exists(self.output_folder):
            os.rmdir(self.output_folder)

    def test_blob_upload(self):
        # テスト用のコンテナとBlobの作成
        container_name = "test-container"
        blob_name = "test_blob.csv"
        container_client = self.blob_service_client.get_container_client(container_name)
        container_client.create_container()

        # Blobのアップロード
        blob_client = container_client.get_blob_client(blob_name)
        with open(os.path.join(self.input_folder, self.test_file), "rb") as data:
            blob_client.upload_blob(data)

        # アップロードされたBlobのダウンロード
        downloaded_blob = blob_client.download_blob().readall()
        self.assertEqual(downloaded_blob.decode("utf-8"), self.test_data)

        # テスト用のコンテナの削除
        container_client.delete_container()

if __name__ == "__main__":
    unittest.main()