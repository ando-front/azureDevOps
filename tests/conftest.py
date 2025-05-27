"""
テストの共通設定を管理するモジュール
"""
import os
import json
import pytest
from azure.storage.blob import BlobServiceClient

# テストデータのパス設定
DATA_ROOT = "tests/data"
INPUT_DIR = os.path.join(DATA_ROOT, "input")
OUTPUT_DIR = os.path.join(DATA_ROOT, "output")
SFTP_DIR = os.path.join(DATA_ROOT, "sftp")

# Azurite設定 - docker-compose環境とスタンドアロン環境の両方に対応
AZURITE_HOST = os.environ.get('AZURITE_HOST', '127.0.0.1')
AZURITE_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    f"BlobEndpoint=http://{AZURITE_HOST}:10000/devstoreaccount1;"
)

@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """テスト環境のセットアップ"""
    # 必要なディレクトリの作成
    for dir_path in [INPUT_DIR, OUTPUT_DIR, SFTP_DIR]:
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

    yield

    # テスト終了後のクリーンアップ
    for dir_path in [INPUT_DIR, OUTPUT_DIR, SFTP_DIR]:
        for root, _, files in os.walk(dir_path):
            for file in files:
                os.remove(os.path.join(root, file))

# パイプライン読み込み用フィクスチャ
@pytest.fixture(scope="module")
def pipeline_copy_marketing_client_dm():
    path = os.path.join("/tests", "src", "dev", "pipeline", "pi_Copy_marketing_client_dm.json")
    with open(path, encoding="utf-8") as f:
        return json.load(f)

@pytest.fixture(scope="module")
def pipeline_insert_clientdm_bx():
    path = os.path.join("/tests", "src", "dev", "pipeline", "pi_Insert_ClientDmBx.json")
    with open(path, encoding="utf-8") as f:
        return json.load(f)

@pytest.fixture(scope="module")
def pipeline_send_actionpoint():
    path = os.path.join("/tests", "src", "dev", "pipeline", "pi_Send_ActionPointCurrentMonthEntryList.json")
    with open(path, encoding="utf-8") as f:
        return json.load(f)

@pytest.fixture(scope="module")
def pipeline_point_grant_email():
    path = os.path.join("/tests", "src", "dev", "pipeline", "pi_PointGrantEmail.json")
    with open(path, encoding="utf-8") as f:
        return json.load(f)

# BlobServiceClient フィクスチャ
@pytest.fixture(scope="session")
def blob_service_client():
    return BlobServiceClient.from_connection_string(AZURITE_CONNECTION_STRING)
