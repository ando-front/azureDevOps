## E2Eテスト実行問題レポート - 未解決の課題

**日付:** 2025年6月28日

**問題の概要:**
E2Eテスト環境のセットアップと実行において、Dockerコンテナ内のテストスクリプトをホストのシェルスクリプト (`run-e2e-fast.sh`) から正しく実行できない問題が継続しています。特に、Windows環境（Git Bashなど）から `docker-compose run` コマンドを使用してコンテナ内のスクリプトを起動する際に、パスの解釈とシェルの引用符の扱いに起因するエラーが発生しています。

**発生している症状/エラーメッセージ:**
*   `exec: "C:/Program Files/Git/usr/local/bin/run_e2e_tests_in_container.sh": stat C:/Program Files/Git/usr/local/bin/run_e2e_tests_in_container.sh: no such file or directory: unknown`
*   `bash: line 1: C:/Program: No such file or directory`
*   `bash: - : invalid option`

これらのエラーは、`docker-compose run` に渡されるコマンド文字列が、ホストのシェルによって正しく引用符で囲まれていないか、またはパスが正しく変換されていないために、コンテナ内のシェルがそれを有効なコマンドとして認識できないことを示しています。

**特定された根本原因:**
`run-e2e-fast.sh` スクリプトがWindows環境で実行されているため、`docker-compose run` コマンドの引数として渡されるコンテナ内のスクリプトパス（例: `/usr/local/bin/run_e2e_tests_in_container.sh`）が、Windowsのパス形式（例: `C:/Program Files/Git/...`）に誤って変換されたり、スペースを含むパスが適切に引用符で囲まれずに渡されたりしています。これにより、Dockerコンテナ内の `bash` がコマンドを正しく解釈できず、実行に失敗しています。

**試行された解決策（および失敗の理由）:**
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
    *   **結果:** WindowsホストのシェルとDockerコンテナのシェル間のパス変換および引用符の扱いの複雑さにより、問題が解決していません。

**次のステップ/提案される解決策:**
現在の問題は、WindowsホストのシェルスクリプトからDockerコンテナ内のLinux環境へコマンドを渡す際のパス変換と引用符の扱いの複雑さに集約されています。

1.  **`run-e2e-fast.sh` の簡素化と `docker-compose.e2e.yml` の活用:**
    *   `run-e2e-fast.sh` は、Docker Composeの起動と停止、およびテスト結果の収集に特化させます。
    *   テストの実行自体は、`docker-compose.e2e.yml` の `e2e-test-runner` サービスの `command` に直接記述するか、または `docker-compose.e2e.yml` の `command` を `run_e2e_tests_in_container.sh` に戻し、`docker-compose run e2e-test-runner` のみで実行するようにします。
    *   `docker-compose.e2e.yml` の `e2e-test-runner` サービスに `healthcheck` を追加し、テストが開始される前にコンテナが完全に準備できたことを確認するようにします。
2.  **Windows Subsystem for Linux (WSL) の利用:**
    *   もし可能であれば、Windows Subsystem for Linux (WSL) 環境で `run-e2e-fast.sh` を実行することを検討します。WSL環境ではLinuxのシェルが使用されるため、パス変換の問題が大幅に軽減される可能性があります。
3.  **`docker-compose.e2e.yml` の `entrypoint` の検討:**
    *   `command` の代わりに `entrypoint` を使用して、コンテナ起動時に常に特定のスクリプトを実行するように設定することも検討できます。

これらのアプローチを検討し、最も堅牢でシンプルな解決策を見つける必要があります。
