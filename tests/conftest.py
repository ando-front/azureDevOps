"""
テストの共通設定を管理するモジュール
"""
import os
import pytest

# テストデータのパス設定
DATA_ROOT = "tests/data"
INPUT_DIR = os.path.join(DATA_ROOT, "input")
OUTPUT_DIR = os.path.join(DATA_ROOT, "output")
SFTP_DIR = os.path.join(DATA_ROOT, "sftp")

# Azurite設定
AZURITE_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
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
