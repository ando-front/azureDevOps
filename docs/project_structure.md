# プロジェクト構造とファイル命名規則

## フォルダ構造

```
azureDevOps/
├── docker/                      # Docker関連ファイル
│   ├── Dockerfile
│   └── docker-compose.yml
│
├── docs/                        # ドキュメント
│   ├── architecture/           # アーキテクチャ関連文書
│   │   ├── adf_pipeline_flow.md
│   │   └── job_flow.md
│   ├── specs/                  # 仕様書
│   │   ├── pipeline_specs.md   # パイプライン仕様
│   │   └── test_specs.md      # テスト仕様
│   └── project_structure.md    # 本ドキュメント
│
├── src/                         # ソースコード
│   └── factory/                # ADFファクトリー定義
│       ├── pipelines/         # パイプライン定義
│       │   ├── pipeline1.json
│       │   └── pipeline2.json
│       ├── datasets/          # データセット定義
│       │   ├── dataset1.json
│       │   └── dataset2.json
│       ├── linked_services/   # リンクされたサービス
│       │   └── linkedService1.json
│       └── triggers/          # トリガー定義
│           └── trigger1.json
│
└── tests/                      # テストコード
    ├── unit/                   # ユニットテスト
    │   ├── test_pipeline1.py
    │   └── test_pipeline2.py
    ├── integration/           # 結合テスト
    ├── data/                  # テストデータ
    │   ├── input/            # 入力データ
    │   │   └── test.csv
    │   └── output/           # 出力データ
    └── conftest.py           # テスト共通設定

```

## ファイル命名規則

### 1. 一般規則
- すべてのファイル名は小文字を使用
- 単語の区切りはアンダースコア（`_`）を使用
- 拡張子は適切なものを使用（`.py`, `.json`, `.md`等）

### 2. パイプライン関連ファイル
- パイプライン定義: `pipeline_<用途>.json`
  例: `pipeline_data_copy.json`, `pipeline_data_transform.json`
- データセット: `dataset_<データ種類>.json`
  例: `dataset_sql.json`, `dataset_blob.json`
- リンクサービス: `linked_service_<サービス名>.json`
  例: `linked_service_sql.json`, `linked_service_blob.json`

### 3. テストファイル
- ユニットテスト: `test_<テスト対象>.py`
  例: `test_pipeline1.py`, `test_pipeline2.py`
- テストデータ: `test_data_<用途>.csv`
  例: `test_data_input.csv`, `test_data_expected.csv`

### 4. ドキュメント
- 仕様書: `<コンポーネント>_spec.md`
  例: `pipeline_spec.md`, `test_spec.md`
- フロー図: `<プロセス>_flow.md`
  例: `data_flow.md`, `pipeline_flow.md`

## コーディング規則

### Python
- クラス名: UpperCamelCase
  例: `TestPipeline`, `DataProcessor`
- メソッド名: lowercase_with_underscores
  例: `test_pipeline_execution`, `process_data`
- 変数名: lowercase_with_underscores
  例: `input_file`, `test_data`

### JSON
- プロパティ名: camelCase
  例: `linkedService`, `typeProperties`
- 設定値: 適切な型を使用（文字列、数値、ブール値）

## 環境変数
- 大文字とアンダースコア
  例: `AZURE_STORAGE_CONNECTION`, `SFTP_PASSWORD`
