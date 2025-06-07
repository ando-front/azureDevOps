"""
Azure Storage Blob操作のためのモック実装。
"""
"""
Azure Storage用のモッククラス
"""
from typing import Dict, Optional, List
import io


class MockBlobProperties:
    """BlobPropertiesのモック"""
    
    def __init__(self, name: str):
        self.name = name
        self.size = 1024  # デフォルトサイズ


class MockDownloadedBlob:
    """ダウンロードされたBlobのモック"""
    
    def __init__(self, data: bytes):
        self._data = data
        
    def readall(self) -> bytes:
        """全データを読み取り"""
        return self._data
        
    def read(self, size: int = -1) -> bytes:
        """指定サイズでデータを読み取り"""
        if size == -1:
            return self._data
        return self._data[:size]


class MockBlobClient:
    """BlobClientのモック"""
    
    def __init__(self, account_url: str, container_name: str, blob_name: str):
        self.account_url = account_url
        self.container_name = container_name
        self.blob_name = blob_name
        self._data: Optional[bytes] = None
        
    def upload_blob(self, data: bytes, overwrite: bool = False):
        """Blobアップロードのモック"""
        print(f"Mock uploading blob: {self.container_name}/{self.blob_name}")
        if isinstance(data, str):
            self._data = data.encode('utf-8')
        elif isinstance(data, bytes):
            self._data = data
        else:
            # パスやファイルオブジェクトの場合
            try:
                self._data = bytes(data)
            except:
                self._data = str(data).encode('utf-8')
        
    def download_blob(self):
        """Blobダウンロードのモック"""
        return MockDownloadedBlob(self._data or b"mock data")
        
    def exists(self) -> bool:
        """Blob存在確認のモック"""
        return self._data is not None
        
    def get_blob_properties(self):
        """Blobプロパティ取得のモック"""
        return MockBlobProperties(self.blob_name)


class MockContainerClient:
    """ContainerClientのモック"""
    
    def __init__(self, account_url: str, container_name: str):
        self.account_url = account_url
        self.container_name = container_name
        self._blobs: Dict[str, MockBlobClient] = {}
        
    def list_blobs(self, name_starts_with: Optional[str] = None):
        """Blob一覧取得のモック"""
        print(f"Mock listing blobs in container: {self.container_name}")
        print(f"Currently tracked blobs: {list(self._blobs.keys())}")
        
        # 実際にアップロードされたBlobを返す
        blob_names = list(self._blobs.keys())
        
        # デフォルトのモックデータも含める（実際のファイルが優先）
        default_blobs = [
            "PointGrantEmail_20250529.csv",
            "PointGrantEmail_20250528.csv", 
            "test_file.csv"
        ]
        
        # 実際のBlobとデフォルトBlobをマージ（重複を除去）
        all_blob_names = blob_names + [name for name in default_blobs if name not in blob_names]
        mock_blobs = [MockBlobProperties(name) for name in all_blob_names]
        
        if name_starts_with:
            mock_blobs = [b for b in mock_blobs if b.name.startswith(name_starts_with)]
            
        print(f"Returning blobs: {[b.name for b in mock_blobs]}")
        return mock_blobs
        
    def upload_blob(self, name: str, data: bytes, overwrite: bool = False):
        """コンテナレベルでのBlobアップロードモック"""
        blob_client = MockBlobClient(self.account_url, self.container_name, name)
        blob_client.upload_blob(data, overwrite)
        self._blobs[name] = blob_client
        return blob_client
        
    def create_container(self):
        """コンテナの作成（既に作成済みなので何もしない）"""
        print(f"Mock container {self.container_name} already exists")
        pass
    
    def get_blob_client(self, blob_name: str):
        """BlobClientのモック取得"""
        if blob_name not in self._blobs:
            self._blobs[blob_name] = MockBlobClient(self.account_url, self.container_name, blob_name)
        return self._blobs[blob_name]
        
    def delete_container(self):
        """コンテナの削除（モック）"""
        print(f"Mock deleting container: {self.container_name}")
        self._blobs.clear()


class MockBlobServiceClient:
    """BlobServiceClientのモック"""
    
    def __init__(self, account_url: str, credential=None):
        self.account_url = account_url
        self.credential = credential
        self._containers: Dict[str, MockContainerClient] = {}
        
    def get_blob_client(self, container: str, blob: str):
        """BlobClientのモック取得"""
        # コンテナが存在しない場合は作成
        if container not in self._containers:
            self._containers[container] = MockContainerClient(self.account_url, container)
        
        # コンテナからBlobを取得、存在しない場合は新規作成
        container_client = self._containers[container]
        if blob not in container_client._blobs:
            container_client._blobs[blob] = MockBlobClient(self.account_url, container, blob)
        
        return container_client._blobs[blob]
        
    def get_container_client(self, container: str):
        """ContainerClientのモック取得"""
        if container not in self._containers:
            self._containers[container] = MockContainerClient(self.account_url, container)
        return self._containers[container]
        
    def create_container(self, name: str):
        """コンテナ作成のモック"""
        print(f"Mock creating container: {name}")
        self._containers[name] = MockContainerClient(self.account_url, name)
        return self._containers[name]
        
    @classmethod
    def from_connection_string(cls, conn_str: str):
        """接続文字列からの作成（モック）"""
        print(f"Mock creating BlobServiceClient from connection string")
        return cls("https://mock.blob.core.windows.net")


# テスト用のフィクスチャ
def create_mock_blob_service_client():
    """モックBlobServiceClientの作成"""
    return MockBlobServiceClient("https://mock.blob.core.windows.net")
