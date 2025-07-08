# E2E V2 テストフレームワーク API リファレンス

## 目次
1. [基底クラス API](#基底クラス-api)
2. [モックサービス API](#モックサービス-api)
3. [データ構造](#データ構造)
4. [ユーティリティ関数](#ユーティリティ関数)
5. [拡張用インターフェース](#拡張用インターフェース)

## 基底クラス API

### PipelineTestBase

#### コンストラクタ
```python
def __init__(self, pipeline_name: str, domain: str)
```

**パラメータ**:
- `pipeline_name` (str): パイプライン名 (例: "pi_PointGrantEmail")
- `domain` (str): ドメイン名 (例: "kendenki")

**説明**: 基底テストクラスのインスタンスを初期化し、モックサービスをセットアップします。

---

#### setup_mocks
```python
def setup_mocks(self) -> None
```

**説明**: MockBlobStorage、MockSFTPServer、MockDatabaseを初期化し、必要なコンテナを作成します。

**作成されるコンテナ**:
- mytokyogas
- datalake
- source
- staging
- output

---

#### create_test_result
```python
def create_test_result(
    self,
    test_id: str,
    category: TestCategory,
    status: PipelineStatus,
    records_extracted: int = 0,
    records_transformed: int = 0,
    records_loaded: int = 0,
    data_quality_score: float = 1.0,
    errors: List[str] = None,
    warnings: List[str] = None
) -> PipelineTestResult
```

**パラメータ**:
- `test_id` (str): 一意のテストID
- `category` (TestCategory): テストカテゴリ
- `status` (PipelineStatus): 実行ステータス
- `records_extracted` (int): 抽出レコード数
- `records_transformed` (int): 変換レコード数
- `records_loaded` (int): ロードレコード数
- `data_quality_score` (float): データ品質スコア (0.0-1.0)
- `errors` (List[str]): エラーメッセージリスト
- `warnings` (List[str]): 警告メッセージリスト

**戻り値**: PipelineTestResult オブジェクト

---

#### validate_common_assertions
```python
def validate_common_assertions(self, result: PipelineTestResult) -> None
```

**パラメータ**:
- `result` (PipelineTestResult): 検証対象のテスト結果

**例外**:
- `AssertionError`: 共通アサーションが失敗した場合

**検証項目**:
- テスト成功判定 (`result.is_successful`)
- 実行時間チェック (SKIPPED以外で > 0)
- パフォーマンステスト時間制限 (< 60秒)
- 通常テスト時間制限 (< 30秒)

---

#### simulate_etl_process
```python
def simulate_etl_process(
    self,
    input_data: str,
    transform_logic: callable = None,
    output_format: str = "csv"
) -> tuple[str, str]
```

**パラメータ**:
- `input_data` (str): 入力データ (TSV形式)
- `transform_logic` (callable): カスタム変換ロジック関数
- `output_format` (str): 出力形式 ("csv" または "csv.gz")

**戻り値**: (変換済みデータ, バイナリデータ) のタプル

**説明**: Extract-Transform-Load処理をシミュレートします。

---

#### validate_data_quality
```python
def validate_data_quality(
    self, 
    data: str, 
    expected_columns: List[str] = None
) -> Dict[str, float]
```

**パラメータ**:
- `data` (str): 検証対象データ (CSV形式)
- `expected_columns` (List[str]): 期待される列名リスト

**戻り値**: 品質メトリクス辞書
```python
{
    "completeness": float,  # 完全性 (0.0-1.0)
    "validity": float,      # 有効性 (0.0-1.0)
    "consistency": float,   # 一貫性 (0.0-1.0)
    "accuracy": float       # 精度 (0.0-1.0)
}
```

---

#### generate_test_data
```python
def generate_test_data(
    self, 
    record_count: int = 100, 
    columns: List[str] = None
) -> str
```

**パラメータ**:
- `record_count` (int): 生成するレコード数
- `columns` (List[str]): 列名リスト

**戻り値**: TSV形式のテストデータ

**デフォルト列**:
- ID: "ID{番号:06d}"形式
- NAME: "TestName{番号}"形式
- VALUE: "Value{番号}"形式
- DATE: 現在日付 (YYYYMMDD形式)
- EMAIL: "test{番号}@example.com"形式

---

#### convert_csv_to_dict_list
```python
def convert_csv_to_dict_list(self, csv_data: str) -> List[Dict[str, Any]]
```

**パラメータ**:
- `csv_data` (str): CSV形式の文字列データ

**戻り値**: 辞書のリスト (各行がDict)

**説明**: CSV文字列を辞書リストに変換します。ヘッダー行をキーとして使用。

---

#### ログメソッド
```python
def log_test_info(self, test_id: str, message: str) -> None
def log_test_error(self, test_id: str, error: str) -> None
```

**パラメータ**:
- `test_id` (str): テストID
- `message/error` (str): ログメッセージ

### DomainTestBase

#### 抽象メソッド（必須実装）

##### domain_specific_setup
```python
def domain_specific_setup(self) -> None
```

**説明**: ドメイン固有の初期化処理を実装します。

**実装例**:
```python
def domain_specific_setup(self):
    self.source_container = "mytokyogas"
    self.output_container = "datalake"
    self.input_columns = ["ID", "NAME", "EMAIL"]
    self.output_columns = ["ID", "NAME", "STATUS"]
```

---

##### get_domain_test_data_template
```python
def get_domain_test_data_template(self) -> Dict[str, Any]
```

**戻り値**: テストデータテンプレート辞書

**実装例**:
```python
def get_domain_test_data_template(self):
    return {
        "normal_data": self._generate_normal_data(),
        "error_data": self._generate_error_data(),
        "large_data": self._generate_large_data(10000)
    }
```

---

##### validate_domain_business_rules
```python
def validate_domain_business_rules(self, data: str) -> List[str]
```

**パラメータ**:
- `data` (str): 検証対象データ

**戻り値**: ビジネスルール違反メッセージのリスト

**実装例**:
```python
def validate_domain_business_rules(self, data: str) -> List[str]:
    violations = []
    lines = data.strip().split('\n')
    
    for i, line in enumerate(lines[1:], 2):
        parts = line.split(',')
        if not parts[0].startswith('CUST'):
            violations.append(f"行{i}: 顧客ID形式不正")
    
    return violations
```

## モックサービス API

### MockBlobStorage

#### create_container
```python
def create_container(
    self, 
    container_name: str, 
    metadata: Dict[str, str] = None
) -> bool
```

#### upload_file
```python
def upload_file(
    self,
    container_name: str,
    file_path: str,
    content: bytes,
    metadata: Dict[str, str] = None
) -> bool
```

#### download_file
```python
def download_file(self, container_name: str, file_path: str) -> bytes
```

#### file_exists
```python
def file_exists(self, container_name: str, file_path: str) -> bool
```

#### list_files
```python
def list_files(self, container_name: str, prefix: str = "") -> List[str]
```

#### delete_file
```python
def delete_file(self, container_name: str, file_path: str) -> bool
```

#### get_file_metadata
```python
def get_file_metadata(self, container_name: str, file_path: str) -> Dict[str, Any]
```

### MockSFTPServer

#### create_directory
```python
def create_directory(self, remote_path: str) -> bool
```

#### upload
```python
def upload(
    self,
    local_path: str,
    remote_path: str,
    content: bytes,
    metadata: Dict[str, str] = None
) -> bool
```

#### download
```python
def download(self, remote_path: str, local_path: str) -> bytes
```

#### file_exists
```python
def file_exists(self, remote_path: str) -> bool
```

#### list_files
```python
def list_files(self, remote_directory: str) -> List[str]
```

#### get_transfer_count
```python
def get_transfer_count(self, action: str = None) -> int
```

**パラメータ**:
- `action` (str): "upload", "download", "delete" または None (全件)

#### get_transfer_history
```python
def get_transfer_history(self) -> List[Dict[str, Any]]
```

**戻り値構造**:
```python
{
    "action": str,           # "upload", "download", "delete"
    "local_path": str,       # ローカルパス
    "remote_path": str,      # リモートパス
    "size": int,            # ファイルサイズ
    "timestamp": datetime,   # 実行時刻
    "success": bool         # 成功フラグ
}
```

### MockDatabase

#### create_table
```python
def create_table(self, table_name: str, columns: List[str]) -> None
```

#### insert_data / insert_records
```python
def insert_data(self, table_name: str, data: List[Dict[str, Any]]) -> int
def insert_records(self, table_name: str, records: List[Dict[str, Any]]) -> int
```

**戻り値**: 挿入されたレコード数

#### select_data / query_records
```python
def select_data(
    self,
    table_name: str,
    where_condition: Dict[str, Any] = None,
    limit: int = None
) -> List[Dict[str, Any]]

def query_records(self, table_name: str, conditions = None) -> List[Dict[str, Any]]
```

**パラメータ**:
- `conditions`: Dict形式の条件またはSQL文字列

#### update_data
```python
def update_data(
    self,
    table_name: str,
    set_values: Dict[str, Any],
    where_condition: Dict[str, Any]
) -> int
```

**戻り値**: 更新されたレコード数

#### delete_data
```python
def delete_data(self, table_name: str, where_condition: Dict[str, Any]) -> int
```

**戻り値**: 削除されたレコード数

#### get_table_count
```python
def get_table_count(self, table_name: str) -> int
```

#### execute_query
```python
def execute_query(self, query: str) -> List[Dict[str, Any]]
```

#### get_query_history
```python
def get_query_history(self) -> List[Dict[str, Any]]
```

## データ構造

### Enum定義

#### PipelineStatus
```python
class PipelineStatus(Enum):
    PENDING = "Pending"
    IN_PROGRESS = "InProgress"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    CANCELLED = "Cancelled"
    SKIPPED = "Skipped"
```

#### TestCategory
```python
class TestCategory(Enum):
    FUNCTIONAL = "functional"
    DATA_QUALITY = "data_quality"
    PERFORMANCE = "performance"
    INTEGRATION = "integration"
```

### データクラス

#### PipelineTestResult
```python
@dataclass
class PipelineTestResult:
    test_id: str                    # テストID
    pipeline_name: str              # パイプライン名
    domain: str                     # ドメイン名
    category: TestCategory          # テストカテゴリ
    status: PipelineStatus          # 実行ステータス
    start_time: datetime            # 開始時刻
    end_time: datetime              # 終了時刻
    records_extracted: int          # 抽出レコード数
    records_transformed: int        # 変換レコード数
    records_loaded: int             # ロードレコード数
    data_quality_score: float       # データ品質スコア
    errors: List[str]               # エラーメッセージ
    warnings: List[str]             # 警告メッセージ
    
    @property
    def execution_time_seconds(self) -> float:
        """実行時間（秒）"""
        return (self.end_time - self.start_time).total_seconds()
    
    @property
    def is_successful(self) -> bool:
        """成功判定"""
        return (
            (self.status == PipelineStatus.SUCCEEDED or self.status == PipelineStatus.SKIPPED) and
            len(self.errors) == 0 and
            self.data_quality_score >= 0.5
        )
    
    @property
    def has_warnings(self) -> bool:
        """警告有無"""
        return len(self.warnings) > 0
```

## ユーティリティ関数

### テストID生成
```python
import time

def generate_test_id(prefix: str) -> str:
    """一意のテストID生成"""
    return f"{prefix}_{int(time.time())}"

# 使用例
test_id = generate_test_id("functional_test")
# => "functional_test_1751978359"
```

### 日付生成ヘルパー
```python
from datetime import datetime, timedelta

def get_test_date(days_offset: int = 0) -> str:
    """テスト用日付文字列生成 (YYYYMMDD形式)"""
    date = datetime.utcnow() + timedelta(days=days_offset)
    return date.strftime('%Y%m%d')

# 使用例
today = get_test_date()           # 今日
yesterday = get_test_date(-1)     # 昨日
next_week = get_test_date(7)      # 来週
```

### データサイズ計算
```python
def calculate_data_size(data: str) -> dict:
    """データサイズ情報計算"""
    lines = data.strip().split('\n')
    return {
        "total_lines": len(lines),
        "header_lines": 1,
        "data_lines": len(lines) - 1,
        "total_bytes": len(data.encode('utf-8')),
        "average_line_length": len(data) / len(lines) if lines else 0
    }
```

### パフォーマンス測定
```python
import time
from contextlib import contextmanager

@contextmanager
def measure_execution_time():
    """実行時間測定コンテキストマネージャー"""
    start_time = time.time()
    try:
        yield
    finally:
        execution_time = time.time() - start_time
        print(f"実行時間: {execution_time:.2f}秒")

# 使用例
with measure_execution_time():
    # 測定対象の処理
    test.test_performance_large_dataset()
```

## 拡張用インターフェース

### 新規パイプライン追加

#### 1. テストクラス作成
```python
from e2e_v2.base.pipeline_test_base import DomainTestBase, TestCategory, PipelineStatus

class TestNewPipelinePipeline(DomainTestBase):
    """新規パイプラインテストクラス"""
    
    def __init__(self):
        super().__init__("pi_NewPipeline", "new_domain")
    
    def domain_specific_setup(self):
        """ドメイン固有セットアップ"""
        self.source_container = "source_container"
        self.output_container = "output_container"
        self.input_columns = ["COL1", "COL2", "COL3"]
        self.output_columns = ["COL1", "COL2", "COL3", "STATUS"]
    
    def get_domain_test_data_template(self) -> Dict[str, str]:
        """テストデータテンプレート"""
        return {
            "normal_data": self._generate_normal_data(),
            "error_data": self._generate_error_data()
        }
    
    def validate_domain_business_rules(self, data: str) -> List[str]:
        """ビジネスルール検証"""
        violations = []
        # ビジネスルール検証ロジック実装
        return violations
    
    def _generate_normal_data(self, record_count: int = 100) -> str:
        """正常データ生成"""
        # データ生成ロジック実装
        pass
    
    def _transform_data(self, input_data: str) -> str:
        """データ変換処理"""
        # 変換ロジック実装
        pass
    
    # テストメソッド実装
    def test_functional_normal_processing(self):
        """機能テスト: 正常処理"""
        test_id = generate_test_id("functional_normal")
        # テストロジック実装
        pass
    
    def test_data_quality_validation(self):
        """データ品質テスト"""
        test_id = generate_test_id("data_quality")
        # データ品質テストロジック実装
        pass
    
    def test_performance_large_dataset(self):
        """パフォーマンステスト"""
        test_id = generate_test_id("performance_large")
        # パフォーマンステストロジック実装
        pass
    
    def test_integration_external_systems(self):
        """統合テスト"""
        test_id = generate_test_id("integration_external")
        # 統合テストロジック実装
        pass
```

#### 2. テストランナー登録
```python
# e2e_v2/scripts/run_all_tests.py に追加
from e2e_v2.domains.new_domain.test_new_pipeline import TestNewPipelinePipeline

class E2ETestRunner:
    def __init__(self):
        self.pipeline_tests = {
            # 既存ドメイン...
            "new_domain": {
                "pi_NewPipeline": TestNewPipelinePipeline
            }
        }
```

### カスタムモックサービス追加

```python
class MockCustomService:
    """カスタムサービスモック"""
    
    def __init__(self):
        self.connections = {}
        self.operations_log = []
    
    def connect(self, config: Dict[str, Any]) -> bool:
        """接続処理"""
        connection_id = f"conn_{len(self.connections)}"
        self.connections[connection_id] = {
            "config": config,
            "created_at": datetime.utcnow(),
            "active": True
        }
        return True
    
    def execute_operation(self, operation: str, params: Dict[str, Any] = None) -> Any:
        """操作実行"""
        operation_log = {
            "operation": operation,
            "params": params or {},
            "timestamp": datetime.utcnow(),
            "result": "success"
        }
        self.operations_log.append(operation_log)
        
        # 操作別処理
        if operation == "custom_operation":
            return self._handle_custom_operation(params)
        
        return {"status": "success"}
    
    def _handle_custom_operation(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """カスタム操作処理"""
        # カスタムロジック実装
        return {"custom_result": "processed"}
    
    def get_operation_history(self) -> List[Dict[str, Any]]:
        """操作履歴取得"""
        return self.operations_log.copy()
```

### 基底クラス拡張での新機能追加

```python
# e2e_v2/base/pipeline_test_base.py に機能追加例

class PipelineTestBase:
    # 既存メソッド...
    
    def validate_security_requirements(self, data: str) -> List[str]:
        """セキュリティ要件検証"""
        violations = []
        
        # PII検出
        if self._contains_pii(data):
            violations.append("個人情報が検出されました")
        
        # 機密情報検出
        if self._contains_sensitive_data(data):
            violations.append("機密情報が検出されました")
        
        return violations
    
    def _contains_pii(self, data: str) -> bool:
        """個人情報検出"""
        import re
        # 電話番号パターン
        phone_pattern = r'\d{2,4}-\d{2,4}-\d{4}'
        # メールパターン
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        
        return bool(re.search(phone_pattern, data) or re.search(email_pattern, data))
    
    def _contains_sensitive_data(self, data: str) -> bool:
        """機密情報検出"""
        sensitive_keywords = ["password", "secret", "token", "key"]
        data_lower = data.lower()
        return any(keyword in data_lower for keyword in sensitive_keywords)
    
    def benchmark_performance(self, function: callable, *args, **kwargs) -> Dict[str, Any]:
        """パフォーマンスベンチマーク"""
        import psutil
        import time
        
        # 開始時のリソース状況
        process = psutil.Process()
        start_memory = process.memory_info().rss
        start_cpu = process.cpu_percent()
        start_time = time.time()
        
        # 関数実行
        try:
            result = function(*args, **kwargs)
            success = True
            error = None
        except Exception as e:
            result = None
            success = False
            error = str(e)
        
        # 終了時のリソース状況
        end_time = time.time()
        end_memory = process.memory_info().rss
        end_cpu = process.cpu_percent()
        
        return {
            "success": success,
            "result": result,
            "error": error,
            "execution_time": end_time - start_time,
            "memory_usage": {
                "start_mb": start_memory / 1024 / 1024,
                "end_mb": end_memory / 1024 / 1024,
                "delta_mb": (end_memory - start_memory) / 1024 / 1024
            },
            "cpu_usage": {
                "start_percent": start_cpu,
                "end_percent": end_cpu
            }
        }
```

---

**Document Version**: 1.0  
**Last Updated**: 2025-07-08  
**Author**: Claude Code Assistant  
**Status**: Production Ready