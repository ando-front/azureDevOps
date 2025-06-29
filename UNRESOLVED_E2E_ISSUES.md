## E2Eテスト実行問題レポート - 未解決の課題

**日付:** 2025年6月29日

**問題の概要:**
E2Eテスト環境のセットアップと実行において、Dockerコンテナ内のテストスクリプトをホストのシェルスクリプト (`run-e2e-fast.sh` または `run-e2e-flexible.sh`) から正しく実行できない問題が継続していました。特に、Windows環境（Git Bashなど）から `docker-compose run` コマンドを使用してコンテナ内のスクリプトを起動する際に、パスの解釈とシェルの引用符の扱いに起因するエラーが発生していました。

**発生していた症状/エラーメッセージ:**
*   `exec: "C:/Program Files/Git/usr/local/bin/run_e2e_tests_in_container.sh": stat C:/Program Files/Git/usr/local/bin/run_e2e_tests_in_container.sh: no such file or directory: unknown`
*   `bash: line 1: C:/Program: No such file or directory`
*   `bash: - : invalid option`

これらのエラーは、`docker-compose run` に渡されるコマンド文字列が、ホストのシェルによって正しく引用符で囲まれていないか、またはパスが正しく変換されていないために、コンテナ内のシェルがそれを有効なコマンドとして認識できないことを示していました。

**特定された根本原因:**
`run-e2e-fast.sh` や `run-e2e-flexible.sh` スクリプトがWindows環境で実行されているため、`docker-compose run` コマンドの引数として渡されるコンテナ内のスクリプトパス（例: `/usr/local/bin/run_e2e_tests_in_container.sh`）が、Windowsのパス形式（例: `C:/Program Files/Git/...`）に誤って変換されたり、スペースを含むパスが適切に引用符で囲まれずに渡されたりしていました。これにより、Dockerコンテナ内の `bash` がコマンドを正しく解釈できず、実行に失敗していました。

**試行された解決策（およびその結果）:**

1.  **DB接続タイムアウトの延長とリトライロジックの強化:**
    *   `synapse_e2e_helper.py` に `LoginTimeout` を追加し、`wait_for_connection` のリトライ回数を増やしました。
    *   `SynapseE2EConnection` の `__init__` で `wait_for_connection` を呼び出すようにしました。
    *   **結果:** ログインタイムアウトエラーは減少しましたが、根本的なパスの問題は解決しませんでした。
2.  **`SQL_SERVER_DATABASE` 環境変数の修正:**
    *   `docker-compose.e2e.yml` で `e2e-test-runner` サービスの `SQL_SERVER_DATABASE` を `TGMATestDB` に変更しました。
    *   **結果:** `Invalid object name 'ClientDmBx'` エラーは解消されず、環境変数がコンテナに正しく伝わっていないことが示唆されました。
3.  **`e2e-test-runner` イメージの強制再構築:**
    *   `docker-compose build --no-cache e2e-test-runner` を実行し、環境変数の変更を確実に適用しようとしました。
    *   **結果:** 環境変数の問題は解決しましたが、`run-e2e-fast.sh` からのコマンド実行の問題が顕在化しました。
4.  **`run-e2e-fast.sh` の `docker-compose exec` への変更:**
    *   `docker-compose run` から `docker-compose exec` に切り替え、実行中のコンテナ内でコマンドを実行しようとしました。
    *   **結果:** `e2e-test-runner` コンテナが起動後すぐにテストを実行してしまうため、`exec` がタイムアウトする問題が発生しました。
5.  **`run_e2e_tests_in_container.sh` から `sleep 240` の削除:**
    *   コンテナの起動を遅延させていた `sleep` コマンドを削除しました。
    *   **結果:** コンテナの起動タイムアウトは改善しましたが、パス解釈の問題が残りました。
6.  **`docker-compose run` コマンドの引数と引用符の調整:**
    *   `cygpath -u` を使用してパスを変換したり、`bash -c "..."` を使用したり、引数を配列として渡したりするなど、様々な引用符とパスの渡し方を試しました。
    *   **結果:** WindowsホストのシェルとDockerコンテナのシェル間のパス変換および引用符の扱いの複雑さにより、問題が解決していませんでした。
7.  **`adf-deploy.yml` からの冗長なリトライロジックの削除:**
    *   CI/CDパイプラインの効率化のため、`adf-deploy.yml`から不要なSQL Server接続リトライロジックを削除しました。
    *   **結果:** パイプラインの実行効率が向上しました。
8.  **`run-e2e-flexible.sh` の簡素化と `docker-compose.e2e.no-proxy.yml` の活用、およびヘルスチェックの導入:**
    *   `run-e2e-flexible.sh` 内で生成される `docker-compose.e2e.no-proxy.yml` に、`sql-server`、`azurite`、`e2e-test-runner` サービスに対する `healthcheck` を追加しました。
    *   `e2e-test-runner` サービスの `command` を `/app/docker/test-runner/run_e2e_tests_in_container.sh` に直接指定し、コンテナ起動時にテストが自動実行されるようにしました。
    *   `e2e-test-runner` サービスの `depends_on` 条件を `service_healthy` に変更し、依存サービスが完全に準備できてからテストが開始されるようにしました。
    *   `run-e2e-flexible.sh` に、E2Eテストランナーコンテナの終了を待機し、その終了コードを確認するロジックを追加しました。
    *   **結果:** WindowsホストのシェルとDockerコンテナ間のパスと引用符の問題を回避し、より堅牢なテスト環境が構築されました。
9.  **`docker/test-runner/run_e2e_tests_in_container.sh` での `pytest` 実行の直接化:**
    *   `run_e2e_tests_in_container.sh` スクリプト内で `pytest tests/e2e $PYTEST_ARGS` を直接実行するように変更しました。
    *   **結果:** コンテナ内でのE2Eテストの実行が明確化され、テスト結果の取得が容易になりました。

**現在の状況と次のステップ:**
上記8, 9の変更により、Windows環境でのE2Eテスト実行における主要な問題は解決されたと考えられます。現在は、これらの変更が正しく機能し、すべてのE2Eテストがパスするかどうかを検証する段階です。

**次のステップ:**
1.  E2Eテストを実際に実行し、すべてのテストがパスすることを確認します。
2.  もしテストが失敗した場合、そのエラーメッセージを分析し、対応する修正を行います。
3.  テストがすべてパスした場合、このレポートを「解決済み」として更新します。