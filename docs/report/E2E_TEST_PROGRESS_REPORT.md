# E2Eテスト進捗レポート - SQL Server接続問題

## 概要

本レポートは、E2Eテスト環境におけるSQL Server接続問題の解決に向けたこれまでの取り組みと、現在の状況をまとめたものです。

## 調査と対応の経緯

### 1. `results.xml` の分析
- `C:\Users\angie\git\azureDevOps\test_results\results.xml` を確認。
- エラー: `pyodbc.ProgrammingError: ('42S02', "[42S02] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]Invalid object name 'ClientDmBx'. (208) (SQLExecDirectW)")`
- 問題は直接的な接続エラーではなく、`ClientDmBx` オブジェクトが見つからないことによるクエリ実行エラーと判明。

### 2. 関連レポートファイルの確認
- `docs/report/UNRESOLVED_E2E_ISSUES.md`、`docs/report/E2E_TEST_STABILITY_IMPROVEMENT_REPORT.md`、`docs/report/IR_ERROR_TROUBLESHOOTING.md`、`pytest_results.txt`、`UNRESOLVED_E2E_ISSUES.md` を確認。
- `UNRESOLVED_E2E_ISSUES.md` にて、SQL Server接続におけるSSL証明書エラーと`ClientDmBx` オブジェクトが見つからないエラーが未解決課題として明記されていることを確認。
- `pytest_results.txt` にて、`Cannot open database "SynapseTestDB" requested by the login. The login failed. (4060) (SQLDriverConnect)` エラーが頻繁に発生していることを確認。

### 3. SQL Serverコンテナのログ確認
- `docker-compose.e2e.yml` を確認し、SQL Serverサービス名が `sqlserver-test` であることを特定。
- `docker-compose -f docker-compose.e2e.yml logs sqlserver-test` を実行。
- ログから `Login failed for user 'sa'. Reason: Failed to open the explicitly specified database 'TGMATestDB'.` エラーを確認。これは、`e2e-test-runner` が接続を試みる際に `TGMATestDB` がまだ利用可能でない可能性を示唆。

### 4. データベース初期化スクリプトの確認
- `e2e_db_auto_initializer.py` はプレースホルダーであり、実際のDB初期化はDocker Composeの `command` セクションのSQLスクリプトに依存していることを確認。
- `docker/sql/init/00_create_synapse_db_fixed.sql` と `docker/sql/init/01_init_database_fixed.sql` を確認。`SynapseTestDB` と `TGMATestDB` が作成されていることを確認。
- `docker/sql/init/02_create_test_tables.sql` に `ClientDmBx` テーブルの作成ステートメントが含まれていることを確認。
- `docker/sql/init/03_insert_test_data.sql` に `ClientDmBx` テーブルへのデータ挿入ステートメントが含まれていることを確認。

### 5. `e2e-test-runner` Dockerfileと接続スクリプトの確認
- `docker/test-runner/Dockerfile` にて `msodbcsql18` がインストールされ、`TrustServerCertificate=Yes` が設定されていることを確認。
- `docker/test-runner/run_e2e_tests_in_container.sh` が `check_db_connection.py` を呼び出していることを確認。
- `docker/test-runner/check_db_connection.py` が `SQL_SERVER_DATABASE` 環境変数で指定されたDB（`TGMATestDB`）への接続を試みていることを確認。

## 現在の状況と課題

- **SQL Server接続のタイミング問題:** `check_db_connection.py` を修正し、`TGMATestDB` への接続と `ClientDmBx` テーブルの存在を確認するロジックを追加しました。これにより、データベースの準備が整うまでテストの実行を待機するようになりました。
- **`pytest-html` オプション認識問題:** `pytest` が `--html` オプションを認識しない問題が継続しています。
    - `pytest.ini` に `plugins = html` を追加する試み -> 失敗
    - `run_e2e_tests_in_container.sh` から `--self-contained-html` を削除する試み -> 失敗
    - `pytest` のバージョンを `8.2.2` に更新する試み -> 失敗
    - `run_e2e_tests_in_container.sh` で `--html` オプションを `pytest` の前に移動する試み -> 失敗
    - `run_e2e_tests_in_container.sh` で `--plugin=pytest_html` を明示的に指定する試み -> 失敗

## 結論

SQL Serverへの接続自体は `check_db_connection.py` の改善により安定しましたが、`pytest` が `pytest-html` プラグインのオプションを認識しないという新たな（または継続的な）問題に直面しています。この問題は、テストレポートの生成を妨げています。
