## ジョブフロー仕様書

### 1. Checkout code

*   リポジトリのコードをチェックアウトします。
*   `actions/checkout@v3`を使用します。

### 2. Set up Docker

*   Dockerをセットアップします。
*   `docker/setup-docker@v2`を使用します。

### 3. Build and run Docker container

*   Dockerコンテナをビルドし、実行します。
*   `docker build -t adf-test .`を使用します。
*   `docker run -d -p 8080:80 adf-test`を使用します。
*   `azurite`が起動していることを確認します。
*   `docker exec -it adf-test azurite -v`を使用します。

### 4. Run tests

*   テストスクリプトを実行します。
*   `docker exec -it adf-test python3 run_tests.py`を使用します。

### 5. Azure Login

*   Azureにログインします。
*   `azure/login@v1`を使用します。
*   `secrets.AZURE_CREDENTIALS`を使用します。

### 6. Azure Deploy

*   Azureにデプロイします。
*   `az deployment group create`を使用します。
*   `adf-rg01`リソースグループにデプロイします。
*   `factory/adf-rg01.json`をテンプレートファイルとして使用します。
*   `factory/publish_config.json`をパラメータファイルとして使用します。
