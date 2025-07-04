# トラブルシューティングガイド

よくある問題とその解決方法をまとめたガイドです。

## Docker関連の問題

### ❌ apt-getネットワーク接続エラー

#### 問題: Dockerビルド時にパッケージダウンロードが失敗する

```bash
# エラー例
Err:1 http://deb.debian.org/debian bookworm InRelease
  Connection failed [IP: 151.101.110.132 80]
E: Unable to locate package curl
```

**原因**: Debianパッケージリポジトリへのネットワーク接続問題（プロキシ、DNS、ファイアウォール）

**⚡ 即座の解決策**:

1. **修正版Dockerfileを使用**:
   ```bash
   # 修正版Dockerfileでビルド
   docker build -f Dockerfile.fixed -t your-app-fixed .
   ```

2. **DNS設定の確認**:
   ```bash
   # Dockerデーモンの再起動
   docker system prune -f
   docker builder prune -f
   ```

3. **プロキシ環境の場合**:
   ```bash
   # プロキシ設定でビルド
   docker build --build-arg HTTP_PROXY=http://your-proxy:port \
               --build-arg HTTPS_PROXY=http://your-proxy:port \
               -f Dockerfile.fixed -t your-app .
   ```

4. **ミラーサーバー使用版**:
   - `Dockerfile.fixed`に日本のミラーサーバー設定済み
   - タイムアウト設定とリトライ機能付き
   - Microsoft ODBCドライバーはオプション（失敗許容）

**📋 詳細ガイド**: `docs/DOCKER_NETWORK_EMERGENCY_FIX.md`を参照

### コンテナが起動しない

#### 問題: docker-compose が失敗する

```bash
# エラー例
ERROR: Service 'adf-unit-test' failed to build
```

**解決方法:**

1. Dockerサービスが起動しているか確認

   ```bash
   docker version
   ```

2. Docker Composeファイルの構文確認

   ```bash
   docker-compose config
   ```

3. イメージの再ビルド

   ```bash
   docker-compose build --no-cache
   ```

#### 問題: E2Eテスト用コンテナが見つからない

```bash
# エラー例
ERROR: No such service: adf-e2e-test
```

**解決方法:**

`adf-e2e-test` コンテナは `Dockerfile.complete` が見つからないため現在使用できません。
代わりに `ir-simulator-e2e` コンテナを使用してください。

```bash
# 正しい実行方法
docker exec -it ir-simulator-e2e pytest tests/e2e/ -v
```

### ネットワーク接続エラー

#### 問題: プロキシ環境でのDocker接続エラー

**解決方法:**

1. Docker設定でプロキシを設定

   ```json
   {
     "proxies": {
       "default": {
         "httpProxy": "http://proxy.company.com:8080",
         "httpsProxy": "http://proxy.company.com:8080"
       }
     }
   }
   ```

2. 環境変数の設定

   ```bash
   export HTTP_PROXY=http://proxy.company.com:8080
   export HTTPS_PROXY=http://proxy.company.com:8080
   export NO_PROXY=localhost,127.0.0.1
   ```

### Dockerビルド時のネットワーク接続エラー

#### 問題: apt-get update が失敗する

```bash
# エラー例
Err:1 http://deb.debian.org/debian bookworm InRelease
  Connection failed [IP: 151.101.110.132 80]
E: Unable to locate package curl
E: Package 'gnupg' has no installation candidate
```

**原因:**
- ネットワーク接続の問題
- プロキシ設定の問題
- DNSリゾルバの問題
- Dockerのネットワーク設定の問題

**解決方法:**

1. **ネットワーク接続の確認**

   ```bash
   # ホストマシンからの接続確認
   ping 8.8.8.8
   curl -I http://deb.debian.org/debian/
   ```

2. **Dockerのネットワーク設定確認**

   ```bash
   # Docker Daemonの再起動
   sudo systemctl restart docker
   
   # Dockerのネットワーク一覧確認
   docker network ls
   
   # デフォルトブリッジの確認
   docker network inspect bridge
   ```

3. **DNSの設定確認**

   ```bash
   # Docker Daemon設定ファイルの確認・編集
   sudo nano /etc/docker/daemon.json
   ```

   ```json
   {
     "dns": ["8.8.8.8", "8.8.4.4"]
   }
   ```

4. **プロキシ設定（企業環境の場合）**

   ```bash
   # Docker Daemon用プロキシ設定
   sudo mkdir -p /etc/systemd/system/docker.service.d/
   sudo nano /etc/systemd/system/docker.service.d/http-proxy.conf
   ```

   ```ini
   [Service]
   Environment="HTTP_PROXY=http://proxy.company.com:port"
   Environment="HTTPS_PROXY=http://proxy.company.com:port"
   Environment="NO_PROXY=localhost,127.0.0.1"
   ```

5. **代替パッケージリポジトリの使用**

   Dockerfileを修正して異なるミラーを使用:

   ```dockerfile
   # 日本のミラーを使用
   RUN sed -i 's/deb.debian.org/ftp.jp.debian.org/g' /etc/apt/sources.list
   RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y...
   ```

6. **段階的なパッケージインストール**

   ```dockerfile
   # パッケージを分割してインストール
   RUN apt-get update
   RUN apt-get install -y curl gnupg lsb-release
   RUN apt-get install -y unixodbc unixodbc-dev
   RUN apt-get install -y freetds-dev tdsodbc gcc g++ freetds-bin
   RUN rm -rf /var/lib/apt/lists/*
   ```

7. **一時的な回避策: キャッシュを使用**

   ```bash
   # 既存のイメージからコンテナを起動
   docker run -it --name temp-container debian:bookworm /bin/bash
   
   # 手動でパッケージをインストール
   apt-get update
   apt-get install -y curl gnupg lsb-release unixodbc unixodbc-dev freetds-dev tdsodbc gcc g++ freetds-bin
   
   # コンテナをコミット
   docker commit temp-container my-custom-image:latest
   
   # Dockerfileのベースイメージを変更
   FROM my-custom-image:latest
   ```

8. **Windows環境での追加対応**

   ```powershell
   # Windows DockerでのDNS設定
   # Docker Desktop設定でDNSサーバを指定
   # Settings → Resources → Network → DNS Server: 8.8.8.8
   
   # WSL2使用時のネットワーク設定確認
   wsl --list --verbose
   wsl --shutdown
   wsl
   ```

## テスト実行の問題

### pytest実行エラー

#### 問題: pytest がインストールされていない

```bash
# エラー例
bash: pytest: command not found
```

**解決方法:**

```bash
# Dockerコンテナ内でpytestをインストール
docker exec -it ir-simulator-e2e pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org pytest
```

#### 問題: 構文エラーでテストが失敗する

```bash
# エラー例
SyntaxError: invalid syntax (test_basic_connections.py, line 11)
```

**解決方法:**

1. 該当ファイルの構文確認

   ```bash
   python -m py_compile tests/e2e/test_basic_connections.py
   ```

2. Python バージョンの確認

   ```bash
   python --version
   ```

3. 必要に応じてファイルの修正

### モジュール不足エラー

#### 問題: 依存パッケージが見つからない

```bash
# エラー例
ModuleNotFoundError: No module named 'pyodbc'
```

**解決方法:**

1. 単体テスト用

   ```bash
   pip install -r requirements.txt
   ```

2. E2Eテスト用

   ```bash
   pip install -r requirements.e2e.txt
   ```

3. 個別インストール

   ```bash
   pip install pyodbc pytest requests
   ```

## データベース接続の問題

### SQL Server接続エラー

#### 問題: データベース接続タイムアウト

```bash
# エラー例
ConnectionError: Unable to connect to SQL Server
```

**解決方法:**

1. SQL Serverコンテナの状態確認

   ```bash
   docker-compose -f docker-compose.e2e.yml ps
   ```

2. ログの確認

   ```bash
   docker-compose -f docker-compose.e2e.yml logs sqlserver-e2e-test
   ```

3. 接続パラメータの確認

   ```python
   # 接続文字列の例
   connection_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost,1433;DATABASE=TestDB;UID=sa;PWD=YourStrong!Passw0rd"
   ```

### ODBC ドライバーの問題

#### 問題: ODBC ドライバーが見つからない

```bash
# エラー例
Data source name not found and no default driver specified
```

**解決方法:**

1. Dockerベースのテスト使用（推奨）

   ```bash
   # ODBCドライバー不要
   docker-compose up --build adf-unit-test
   ```

2. ローカル環境でのドライバーインストール（Windows）

   ```bash
   # Microsoft ODBC Driver for SQL Server をダウンロード・インストール
   ```

3. Ubuntu/WSLでのドライバーインストール

   ```bash
   curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
   curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
   apt-get update
   ACCEPT_EULA=Y apt-get install -y msodbcsql17
   ```

## Azure Data Factory 連携の問題

### Git 統合エラー

#### 問題: ADF ポータルでGit設定が失敗する

**解決方法:**

1. リポジトリURLの確認

   ```text
   正しい形式: https://github.com/organization/repository.git
   ```

2. 権限の確認
   - Data Factory Contributor 権限が必要
   - リポジトリへの読み取り/書き込み権限が必要

3. ブランチの存在確認

   ```bash
   git branch -a
   ```

#### 問題: パブリッシュが失敗する

**解決方法:**

1. 変更の確認

   ```bash
   git status
   git diff
   ```

2. 競合の解決

   ```bash
   git pull origin develop
   # 競合ファイルを手動マージ
   git add .
   git commit -m "Resolve conflicts"
   ```

3. ADF ポータルでの再同期

### パイプライン実行エラー

#### 問題: パイプライン実行時のパラメータエラー

**解決方法:**

1. パラメータの型確認

   ```json
   {
     "campaign_id": "string",
     "points_to_grant": 100
   }
   ```

2. バリデーションロジックの確認

   ```python
   if not campaign_id or campaign_id.strip() == "":
       raise ValueError("campaign_id is required")
   ```

## CI/CD パイプラインの問題

### GitHub Actions エラー

#### 問題: ワークフロー実行が失敗する

**解決方法:**

1. シークレットの設定確認

   ```text
   AZURE_CREDENTIALS
   AZURE_SUBSCRIPTION_ID
   AZURE_RG
   ```

2. 権限の確認
   - GitHub Actions での Azure へのアクセス権限
   - Service Principal の設定

3. ログの確認

   ```bash
   # GitHub Actions の詳細ログを確認
   ```

### デプロイエラー

#### 問題: Azure リソースのデプロイが失敗する

**解決方法:**

1. ARM テンプレートの検証

   ```bash
   az deployment group validate \
     --resource-group myResourceGroup \
     --template-file template.json
   ```

2. リソース名の重複確認

   ```bash
   az resource list --resource-group myResourceGroup
   ```

3. リソース制限の確認

   ```bash
   az vm list-usage --location eastus
   ```

## パフォーマンスの問題

### テスト実行速度

#### 問題: テスト実行が遅い

**解決方法:**

1. 並列実行の使用

   ```bash
   pytest -n auto tests/unit/
   ```

2. モックの活用

   ```python
   # 外部依存をモック化
   @patch('azure.storage.blob.BlobServiceClient')
   def test_with_mock(mock_blob):
       pass
   ```

3. テストデータの最適化

   ```python
   # 必要最小限のテストデータを使用
   ```

### メモリ使用量

#### 問題: メモリ不足でテストが失敗する

**解決方法:**

1. Dockerメモリ制限の調整

   ```yaml
   services:
     test:
       mem_limit: 2g
   ```

2. 大量データテストの分割

   ```python
   # バッチ処理でデータを分割
   for batch in chunks(large_dataset, 100):
       process_batch(batch)
   ```

## ヘルプとサポート

### ログの確認方法

```bash
# Dockerログ
docker-compose logs service-name

# pytestの詳細ログ
pytest -v --tb=long tests/

# Azure CLIログ
az --debug command
```

### 開発チームへの問い合わせ

問題が解決しない場合は、以下の情報を含めて開発チームに問い合わせてください：

1. **環境情報**
   - OS (Windows/Linux/Mac)
   - Docker バージョン
   - Python バージョン

2. **エラー情報**
   - 完全なエラーメッセージ
   - 実行したコマンド
   - 期待する結果

3. **再現手順**
   - 問題が発生するまでの手順
   - 使用したテストデータ

4. **ログファイル**
   - 関連するログファイルの添付
