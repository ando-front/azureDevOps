"""
Blob Storageへのアップロード・ダウンロードのユニットテスト。
"""

import pytest
from azure.storage.blob import BlobServiceClient


def test_blob_upload(blob_service_client, tmp_path):
    # テストファイルを tmp_path 上に作成
    content = "header1,header2\nvalue1,value2\n"
    test_file = tmp_path / "test_data_input.csv"
    test_file.write_text(content)

    # Blob コンテナと Blob クライアントの作成
    container_name = "test-container"
    blob_name = "test_blob.csv"
    container_client = blob_service_client.get_container_client(container_name)
    container_client.create_container()

    # Blob のアップロード
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(test_file.read_bytes(), overwrite=True)    # アップロードされた Blob のダウンロードと検証
    downloaded = blob_client.download_blob().readall().decode("utf-8")
    # 改行文字を正規化して比較
    assert downloaded.replace('\r\n', '\n') == content

    # コンテナのクリーンアップ
    container_client.delete_container()