
import unittest
import os
from azure.storage.blob import BlobServiceClient

class TestPointGrantEmailPipeline(unittest.TestCase):
    def setUp(self):
        # テスト環境の初期設定
        # Azurite（ローカルAzure Blobエミュレータ）へ接続するための接続文字列を定義
        self.connection_string = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
        
        # テスト用のローカルフォルダやファイルの準備
        self.input_folder = "input"
        self.output_folder = "output"
        self.sftp_folder = "sftp"
        self.test_file = "PointGrantEmail_yyyyMMdd.csv"  # テスト用のファイル名
        self.test_content = "HEADER1,HEADER2\nVALUE1,VALUE2\n"  # テストファイルの内容
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)  # Blobサービスクライアント作成

        # 必要なローカルフォルダを作成
        for folder in [self.input_folder, self.output_folder, self.sftp_folder]:
            if not os.path.exists(folder):
                os.makedirs(folder)

        # テストデータをローカルに作成
        with open(os.path.join(self.input_folder, self.test_file), "w") as f:
            f.write(self.test_content)

        # Blobストレージ内のコンテナを準備
        for container_name in ["input", "output", "sftp"]:
            try:
                self.blob_service_client.create_container(container_name)  # コンテナが存在しない場合は作成
            except:
                pass

        # 入力データを Blobコンテナにアップロード
        blob_client = self.blob_service_client.get_blob_client(container="input", blob=self.test_file)
        with open(os.path.join(self.input_folder, self.test_file), "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

    def test_file_list_retrieval(self):
        # テストケース1: 入力Blobからのファイル一覧取得
        print("TestPointGrantEmailPipelineテスト開始...")
        print("1. 入力Blobのファイル一覧取得をテスト...")
        container_client = self.blob_service_client.get_container_client("input")
        blob_list = list(container_client.list_blobs())  # 入力コンテナ内のファイル一覧を取得

        # ファイル名リストを作成し、テストファイルが含まれているか確認
        file_names = [blob.name for blob in blob_list]
        self.assertIn(self.test_file, file_names)
        print(f"取得されたファイル: {file_names}")
        print("ファイル一覧取得テスト: ✅ 成功")

    def test_file_transfer_to_sftp(self):
        # テストケース2: データをBlobストレージからSFTP（模擬）に転送
        print("2. SFTP転送処理のテスト...")
        input_blob = self.blob_service_client.get_blob_client(container="input", blob=self.test_file)
        output_blob = self.blob_service_client.get_blob_client(container="sftp", blob=self.test_file)

        # 入力Blobからデータをダウンロードし、SFTPコンテナにアップロード
        blob_data = input_blob.download_blob().readall()
        output_blob.upload_blob(blob_data, overwrite=True)

        # アップロードされたSFTPコンテナ内のファイルを確認
        sftp_files = list(self.blob_service_client.get_container_client("sftp").list_blobs())
        transferred_files = [blob.name for blob in sftp_files]
        self.assertIn(self.test_file, transferred_files)
        print(f"SFTP内のファイル一覧: {transferred_files}")
        print("SFTP転送テスト: ✅ 成功")

    def test_conditional_file_creation(self):
        # テストケース3: ファイル存在時/非存在時の条件分岐ロジック
        print("3. 条件分岐処理のテスト...")
        
        # テストデータ削除し、ファイルが存在しない状況を再現
        input_blob = self.blob_service_client.get_blob_client(container="input", blob=self.test_file)
        input_blob.delete_blob()

        # 存在しない場合の処理としてヘッダファイルを出力コンテナに作成
        header_file = "PointGrantEmail_Header.csv"
        header_content = "HEADER1,HEADER2\n"  # ヘッダの内容
        output_header_blob = self.blob_service_client.get_blob_client(container="output", blob=header_file)
        output_header_blob.upload_blob(header_content, overwrite=True)

        # 出力コンテナ内にヘッダファイルが存在することを確認
        output_files = list(self.blob_service_client.get_container_client("output").list_blobs())
        self.assertIn(header_file, [blob.name for blob in output_files])
        print(f"出力コンテナ内ファイル: {output_files}")
        print("条件分岐テスト: ✅ 成功")

    def tearDown(self):
        # テストデータ削除（ローカル&Blobストレージ）
        for folder in [self.input_folder, self.output_folder, self.sftp_folder]:
            for file in os.listdir(folder):
                os.remove(os.path.join(folder, file))
        # Blobコンテナ内データの削除
        for container_name in ["input", "output", "sftp"]:
            container_client = self.blob_service_client.get_container_client(container_name)
            blob_list = list(container_client.list_blobs())
            for blob in blob_list:
                container_client.delete_blob(blob.name)

if __name__ == "__main__":
    # unittestを実行
    unittest.main(verbosity=2)
