"""
Azure サービスモッククラス

Azure Blob Storage、SFTP、Database などのモックサービスを提供
"""

import gzip
from datetime import datetime
from typing import Dict, List, Any, Optional


class MockBlobStorage:
    """Azure Blob Storage モック"""
    
    def __init__(self):
        self.containers = {}
        self.files = {}
        self.metadata = {}
    
    def create_container(self, container_name: str, metadata: Dict[str, str] = None):
        """コンテナ作成"""
        self.containers[container_name] = {
            "created_at": datetime.utcnow(),
            "metadata": metadata or {}
        }
        return True
    
    def upload_file(
        self,
        container_name: str,
        file_path: str,
        content: bytes,
        metadata: Dict[str, str] = None
    ):
        """ファイルアップロード"""
        if container_name not in self.containers:
            raise ValueError(f"Container {container_name} does not exist")
        
        key = f"{container_name}/{file_path}"
        self.files[key] = {
            "content": content,
            "uploaded_at": datetime.utcnow(),
            "size": len(content),
            "metadata": metadata or {}
        }
        return True
    
    def download_file(self, container_name: str, file_path: str) -> bytes:
        """ファイルダウンロード"""
        key = f"{container_name}/{file_path}"
        if key not in self.files:
            raise FileNotFoundError(f"File {file_path} not found in {container_name}")
        return self.files[key]["content"]
    
    def file_exists(self, container_name: str, file_path: str) -> bool:
        """ファイル存在確認"""
        key = f"{container_name}/{file_path}"
        return key in self.files
    
    def list_files(self, container_name: str, prefix: str = "") -> List[str]:
        """ファイル一覧取得"""
        if container_name not in self.containers:
            return []
        
        files = []
        container_prefix = f"{container_name}/"
        for key in self.files.keys():
            if key.startswith(container_prefix):
                file_path = key[len(container_prefix):]
                if file_path.startswith(prefix):
                    files.append(file_path)
        return sorted(files)
    
    def delete_file(self, container_name: str, file_path: str) -> bool:
        """ファイル削除"""
        key = f"{container_name}/{file_path}"
        if key in self.files:
            del self.files[key]
            return True
        return False
    
    def get_file_metadata(self, container_name: str, file_path: str) -> Dict[str, Any]:
        """ファイルメタデータ取得"""
        key = f"{container_name}/{file_path}"
        if key not in self.files:
            raise FileNotFoundError(f"File {file_path} not found in {container_name}")
        return self.files[key].copy()


class MockSFTPServer:
    """SFTP サーバーモック"""
    
    def __init__(self):
        self.directories = {
            "/": {},
            "/Import": {},
            "/Import/DAM": {},
            "/Import/DAM/3A01b_PointAddition": {},
            "/Export": {},
            "/Staging": {}
        }
        self.files = {}
        self.transfer_log = []
    
    def create_directory(self, remote_path: str) -> bool:
        """ディレクトリ作成"""
        self.directories[remote_path] = {}
        return True
    
    def upload(
        self,
        local_path: str,
        remote_path: str,
        content: bytes,
        metadata: Dict[str, str] = None
    ) -> bool:
        """ファイルアップロード"""
        
        # ディレクトリが存在しない場合は作成
        remote_dir = "/".join(remote_path.split("/")[:-1])
        if remote_dir not in self.directories:
            self.create_directory(remote_dir)
        
        # ファイル保存
        self.files[remote_path] = {
            "content": content,
            "uploaded_at": datetime.utcnow(),
            "size": len(content),
            "local_path": local_path,
            "metadata": metadata or {}
        }
        
        # 転送ログ記録
        self.transfer_log.append({
            "action": "upload",
            "local_path": local_path,
            "remote_path": remote_path,
            "size": len(content),
            "timestamp": datetime.utcnow(),
            "success": True
        })
        
        return True
    
    def download(self, remote_path: str, local_path: str) -> bytes:
        """ファイルダウンロード"""
        if remote_path not in self.files:
            raise FileNotFoundError(f"File {remote_path} not found")
        
        content = self.files[remote_path]["content"]
        
        # 転送ログ記録
        self.transfer_log.append({
            "action": "download",
            "local_path": local_path,
            "remote_path": remote_path,
            "size": len(content),
            "timestamp": datetime.utcnow(),
            "success": True
        })
        
        return content
    
    def file_exists(self, remote_path: str) -> bool:
        """ファイル存在確認"""
        return remote_path in self.files
    
    def list_files(self, remote_directory: str) -> List[str]:
        """ファイル一覧取得"""
        files = []
        for path in self.files.keys():
            if path.startswith(remote_directory) and path != remote_directory:
                files.append(path)
        return sorted(files)
    
    def delete_file(self, remote_path: str) -> bool:
        """ファイル削除"""
        if remote_path in self.files:
            del self.files[remote_path]
            
            # 転送ログ記録
            self.transfer_log.append({
                "action": "delete",
                "local_path": "",
                "remote_path": remote_path,
                "size": 0,
                "timestamp": datetime.utcnow(),
                "success": True
            })
            return True
        return False
    
    def get_transfer_count(self, action: str = None) -> int:
        """転送回数取得"""
        if action:
            return len([log for log in self.transfer_log if log["action"] == action])
        return len(self.transfer_log)
    
    def get_transfer_history(self) -> List[Dict[str, Any]]:
        """転送履歴取得"""
        return self.transfer_log.copy()


class MockDatabase:
    """データベースモック"""
    
    def __init__(self):
        self.tables = {}
        self.connections = {}
        self.query_log = []
    
    def create_table(self, table_name: str, columns: List[str]):
        """テーブル作成"""
        self.tables[table_name] = {
            "columns": columns,
            "data": [],
            "created_at": datetime.utcnow()
        }
    
    def insert_data(self, table_name: str, data: List[Dict[str, Any]]) -> int:
        """データ挿入"""
        if table_name not in self.tables:
            # テーブルが存在しない場合は自動作成
            if data:
                columns = list(data[0].keys())
                self.create_table(table_name, columns)
            else:
                raise ValueError(f"Table {table_name} does not exist and no data provided for auto-creation")
        
        before_count = len(self.tables[table_name]["data"])
        self.tables[table_name]["data"].extend(data)
        after_count = len(self.tables[table_name]["data"])
        
        # クエリログ記録
        self.query_log.append({
            "action": "INSERT",
            "table": table_name,
            "records": len(data),
            "timestamp": datetime.utcnow()
        })
        
        return after_count - before_count
    
    def select_data(
        self,
        table_name: str,
        where_condition: Dict[str, Any] = None,
        limit: int = None
    ) -> List[Dict[str, Any]]:
        """データ選択"""
        if table_name not in self.tables:
            # テーブルが存在しない場合は空のテーブルを作成
            self.create_table(table_name, ["id", "name", "value"])
        
        data = self.tables[table_name]["data"]
        
        # 条件フィルタ
        if where_condition:
            filtered_data = []
            for row in data:
                match = True
                for key, value in where_condition.items():
                    if key not in row or row[key] != value:
                        match = False
                        break
                if match:
                    filtered_data.append(row)
            data = filtered_data
        
        # 制限
        if limit:
            data = data[:limit]
        
        # クエリログ記録
        self.query_log.append({
            "action": "SELECT",
            "table": table_name,
            "records": len(data),
            "timestamp": datetime.utcnow()
        })
        
        return data
    
    def update_data(
        self,
        table_name: str,
        set_values: Dict[str, Any],
        where_condition: Dict[str, Any]
    ) -> int:
        """データ更新"""
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")
        
        updated_count = 0
        for row in self.tables[table_name]["data"]:
            match = True
            for key, value in where_condition.items():
                if key not in row or row[key] != value:
                    match = False
                    break
            if match:
                row.update(set_values)
                updated_count += 1
        
        # クエリログ記録
        self.query_log.append({
            "action": "UPDATE",
            "table": table_name,
            "records": updated_count,
            "timestamp": datetime.utcnow()
        })
        
        return updated_count
    
    def delete_data(self, table_name: str, where_condition: Dict[str, Any]) -> int:
        """データ削除"""
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist")
        
        original_data = self.tables[table_name]["data"]
        filtered_data = []
        deleted_count = 0
        
        for row in original_data:
            match = True
            for key, value in where_condition.items():
                if key not in row or row[key] != value:
                    match = False
                    break
            if not match:
                filtered_data.append(row)
            else:
                deleted_count += 1
        
        self.tables[table_name]["data"] = filtered_data
        
        # クエリログ記録
        self.query_log.append({
            "action": "DELETE",
            "table": table_name,
            "records": deleted_count,
            "timestamp": datetime.utcnow()
        })
        
        return deleted_count
    
    def get_table_count(self, table_name: str) -> int:
        """テーブル行数取得"""
        if table_name not in self.tables:
            return 0
        return len(self.tables[table_name]["data"])
    
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """SQLクエリ実行（簡易版）"""
        # 実際の実装では SQL パーサーが必要
        # ここでは簡易的な処理
        
        self.query_log.append({
            "action": "EXECUTE",
            "query": query[:100],  # 最初の100文字のみ記録
            "timestamp": datetime.utcnow()
        })
        
        return []
    
    def get_query_history(self) -> List[Dict[str, Any]]:
        """クエリ履歴取得"""
        return self.query_log.copy()
    
    # エイリアスメソッド（後方互換性のため）
    def insert_records(self, table_name: str, records: List[Dict[str, Any]]) -> int:
        """insert_dataのエイリアス"""
        return self.insert_data(table_name, records)
    
    def query_records(self, table_name: str, conditions = None) -> List[Dict[str, Any]]:
        """select_dataのエイリアス（SQL文字列にも対応）"""
        if isinstance(conditions, str):
            # SQL文字列の場合は簡易的にパース
            return self.execute_sql_query(table_name, conditions)
        else:
            # 辞書型の条件の場合は従来通り
            return self.select_data(table_name, conditions)
    
    def execute_sql_query(self, table_name: str, sql_query: str) -> List[Dict[str, Any]]:
        """簡易SQL実行（モック用）"""
        # SQLクエリの簡易パース（実際のプロダクションでは適切なSQLパーサーを使用）
        if table_name not in self.tables:
            # テーブルが存在しない場合は空のテーブルを作成
            self.create_table(table_name, ["id", "name", "value"])
        
        # 簡易的にSELECTクエリに対して空の結果を返す（モック目的）
        if "SELECT" in sql_query.upper():
            # ログ記録
            self.query_log.append({
                "action": "SELECT_SQL",
                "table": table_name,
                "query": sql_query[:100],
                "records": 0,
                "timestamp": datetime.utcnow()
            })
            return []
        
        return []