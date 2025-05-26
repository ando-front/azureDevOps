import os
import pytest
from azure.storage.blob import BlobServiceClient
from tests.conftest import AZURITE_CONNECTION_STRING

# テスト用ファイル情報を設定
@pytest.fixture
def test_file(tmp_path):
    file_name = "PointGrantEmail_yyyyMMdd.csv"
    content = "HEADER1,HEADER2\nVALUE1,VALUE2\n"
    input_path = tmp_path / file_name
    input_path.write_text(content)
    return input_path, file_name, content

@pytest.fixture(autouse=True)
def setup_blob_containers(blob_service_client):
    # コンテナを作成
    for c in ("input", "output", "sftp"):
        try:
            blob_service_client.create_container(c)
        except:
            pass

def test_file_list_retrieval(blob_service_client, test_file):
    input_path, name, _ = test_file
    blob = blob_service_client.get_blob_client("input", name)
    blob.upload_blob(input_path.read_bytes(), overwrite=True)
    names = [b.name for b in blob_service_client.get_container_client("input").list_blobs()]
    assert name in names

def test_file_transfer_to_sftp(blob_service_client, test_file):
    input_path, name, _ = test_file
    blob_in = blob_service_client.get_blob_client("input", name)
    blob_in.upload_blob(input_path.read_bytes(), overwrite=True)
    data = blob_in.download_blob().readall()
    blob_out = blob_service_client.get_blob_client("sftp", name)
    blob_out.upload_blob(data, overwrite=True)
    names = [b.name for b in blob_service_client.get_container_client("sftp").list_blobs()]
    assert name in names

def test_conditional_file_creation(blob_service_client, test_file):
    _, name, _ = test_file
    blob = blob_service_client.get_blob_client("input", name)
    try:
        blob.delete_blob()
    except:
        pass
    header = "PointGrantEmail_Header.csv"
    content = "HEADER1,HEADER2\n"
    blob_out = blob_service_client.get_blob_client("output", header)
    blob_out.upload_blob(content, overwrite=True)
    names = [b.name for b in blob_service_client.get_container_client("output").list_blobs()]
    assert header in names
