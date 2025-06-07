"""
テストの共通設定を管理するモジュール
"""
import os
import json
import pytest
from azure.storage.blob import BlobServiceClient
from tests.unit.helpers.synapse_test_helper import SynapseTestConnection
from tests.unit.helpers.azure_storage_mock import create_mock_blob_service_client

# テストデータのパス設定
DATA_ROOT = "tests/data"
INPUT_DIR = os.path.join(DATA_ROOT, "input")
OUTPUT_DIR = os.path.join(DATA_ROOT, "output")
SFTP_DIR = os.path.join(DATA_ROOT, "sftp")

# Azurite設定 - E2Eテスト用
def get_azurite_host():
    """環境に応じたAzuriteホストを取得（E2Eテスト用）"""
    if os.environ.get('GITHUB_ACTIONS'):
        return 'azurite-test'
    elif os.environ.get('AZURITE_HOST'):
        # Docker Compose環境では環境変数を使用
        return os.environ.get('AZURITE_HOST')
    else:
        # スタンドアロン環境ではlocalhostを使用
        return '127.0.0.1'

AZURITE_HOST = get_azurite_host()
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


@pytest.fixture
def synapse_connection():
    """Synapse Analytics (SQL Server) 接続のフィクスチャ"""
    connection_helper = SynapseTestConnection()
    yield connection_helper


@pytest.fixture
def clean_test_data(synapse_connection):
    """テストデータをクリーンアップするフィクスチャ"""
    yield
    
    # テスト後にデータをクリーンアップ
    cleanup_queries = [
        "DELETE FROM dbo.ActionPointEntry WHERE EntryId > 3",
        "DELETE FROM dbo.PointGrantEmail WHERE EmailId > 3", 
        "DELETE FROM dbo.ClientDm WHERE ClientId > 3",
        "DELETE FROM dbo.TestTable WHERE ID > 3"
    ]
    
    for query in cleanup_queries:
        try:
            synapse_connection.execute_query(query)
        except Exception as e:
            print(f"クリーンアップエラー: {e}")


@pytest.fixture(scope="session")
def blob_service_client(pytestconfig):
    """環境に応じたBlob Service Clientフィクスチャ"""
    # E2Eテストの場合は実際のAzurite接続
    test_markers = getattr(pytestconfig.option, 'markexpr', '') or ''
    
    if 'e2e' in test_markers:
        print("Using real Azurite connection for E2E tests")
        client = BlobServiceClient.from_connection_string(AZURITE_CONNECTION_STRING)
        yield client
    else:
        # 単体テストの場合は常にモック
        print("Using mock BlobServiceClient for unit tests")
        mock_client = create_mock_blob_service_client()
        yield mock_client

# パイプライン読み込み用フィクスチャ
@pytest.fixture(scope="module")
def pipeline_copy_marketing_client_dm():
    path = os.path.join("src", "dev", "pipeline", "pi_Copy_marketing_client_dm.json")
    with open(path, encoding="utf-8") as f:
        return json.load(f)

@pytest.fixture(scope="module")
def pipeline_insert_clientdm_bx():
    path = os.path.join("src", "dev", "pipeline", "pi_Insert_ClientDmBx.json")
    with open(path, encoding="utf-8") as f:
        return json.load(f)

@pytest.fixture(scope="module")
def pipeline_send_actionpoint():
    path = os.path.join("src", "dev", "pipeline", "pi_Send_ActionPointCurrentMonthEntryList.json")
    with open(path, encoding="utf-8") as f:
        return json.load(f)

@pytest.fixture(scope="module")
def pipeline_point_grant_email():
    path = os.path.join("src", "dev", "pipeline", "pi_PointGrantEmail.json")
    with open(path, encoding="utf-8") as f:
        return json.load(f)

# E2Eテスト用のfixture
@pytest.fixture(scope="module")
def e2e_synapse_connection():
    """E2Eテスト用のSynapse接続フィクスチャ"""
    from tests.e2e.helpers.synapse_e2e_helper import SynapseE2EConnection
    
    connection = SynapseE2EConnection()
    try:
        # 接続テスト - 簡単なクエリを実行
        result = connection.execute_query("SELECT 1 as test")
        assert len(result) > 0 and result[0][0] == 1, "接続テストに失敗しました"
        yield connection
    except Exception as e:
        pytest.skip(f"E2E Synapse接続が利用できません: {e}")
    # 接続は自動的に閉じられるため、明示的なクリーンアップは不要
