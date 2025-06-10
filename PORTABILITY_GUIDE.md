# E2Eテスト移植性ガイド

## 🎯 概要
このE2Eテストスイートは、Docker環境を使用して高い移植性を実現しています。

## ✅ 動作要件

### 必須ソフトウェア
- Docker Desktop または Docker Engine
- Docker Compose v2.0+
- Python 3.11+
- Git

### 推奨システム要件
- RAM: 4GB以上の空きメモリ
- ディスク: 2GB以上の空き容量
- OS: Windows 10/11, macOS 12+, Ubuntu 20.04+

## 🚀 セットアップ手順

### 1. リポジトリクローン
```bash
git clone <repository-url>
cd azureDevOps
```

### 2. 環境構築
```bash
# Docker環境の起動
make e2e-up

# または直接実行
docker-compose -f docker-compose.e2e.yml up -d
```

### 3. Python依存関係インストール
```bash
pip install -r requirements.e2e.txt
```

### 4. テスト実行
```bash
# 全E2Eテスト実行
make test-e2e

# または直接実行
python -m pytest tests/e2e/ -v
```

## 🔧 プラットフォーム別注意事項

### Windows
- Docker Desktop for Windowsが必要
- WSL2推奨
- PowerShellまたはCmd使用時:
  ```powershell
  # Makefileの代わりにコマンド直接実行
  docker-compose -f docker-compose.e2e.yml up -d
  python -m pytest tests/e2e/ -v
  ```

### macOS
- Docker Desktop for Macが必要
- Intel/Apple Siliconどちらも対応
- Homebrewでの依存関係インストール推奨:
  ```bash
  brew install --cask docker
  ```

### Linux
- Docker EngineとDocker Composeをインストール
- ユーザーをdockerグループに追加:
  ```bash
  sudo usermod -aG docker $USER
  # ログアウト・ログインまたは再起動
  ```

## 🐛 トラブルシューティング

### よくある問題と解決方法

#### 1. ポート1433が使用中
```bash
# 使用中のプロセス確認
netstat -tulpn | grep 1433
# または
lsof -i :1433

# 解決: ポートを変更する場合
# docker-compose.e2e.yml の ports を "1434:1433" に変更
# テストのconnection stringも変更
```

#### 2. SQL Serverの初期化エラー
```bash
# ログ確認
docker logs sqlserver-e2e-test

# 解決: コンテナ再作成
docker-compose -f docker-compose.e2e.yml down -v
docker-compose -f docker-compose.e2e.yml up -d
```

#### 3. メモリ不足エラー
```bash
# Dockerのメモリ設定を4GB以上に増加
# Docker Desktop > Settings > Resources > Memory
```

#### 4. ODBCドライバーエラー
**Windows:**
```powershell
# Microsoft ODBC Driver 18 for SQL Serverをダウンロード・インストール
```

**macOS:**
```bash
# HomebrewでODBCドライバーインストール
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew install msodbcsql18 mssql-tools18
```

**Linux (Ubuntu/Debian):**
```bash
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list | sudo tee /etc/apt/sources.list.d/msprod.list
sudo apt-get update
sudo apt-get install msodbcsql18 mssql-tools18
```

#### 5. プロキシ環境での問題
```bash
# プロキシ設定をクリア
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY

# またはDocker composeでプロキシ設定
# docker-compose.e2e.no-proxy.yml を使用
docker-compose -f docker-compose.e2e.no-proxy.yml up -d
```

## 📊 テスト実行結果の確認

### 成功例
```
================== 402 passed, 3 failed, 4 skipped in 19.57s ==================
```

### エラーの場合
```bash
# 詳細なログ出力
python -m pytest tests/e2e/ -v -s --tb=long

# 特定のテストのみ実行
python -m pytest tests/e2e/test_final_integration.py -v
```

## 🔄 継続的インテグレーション対応

### GitHub Actions
```yaml
# .github/workflows/e2e-tests.yml
name: E2E Tests
on: [push, pull_request]
jobs:
  e2e-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - name: Install dependencies
        run: pip install -r requirements.e2e.txt
      - name: Start test environment
        run: docker-compose -f docker-compose.e2e.yml up -d
      - name: Wait for services
        run: sleep 60
      - name: Run E2E tests
        run: python -m pytest tests/e2e/ -v --tb=short
```

## 📝 環境変数設定

以下の環境変数で設定を変更可能:

```bash
# データベース設定
export SQL_SERVER_HOST=localhost
export SQL_SERVER_PORT=1433
export SQL_SERVER_USER=sa
export SQL_SERVER_PASSWORD="YourStrong!Passw0rd123"
export SQL_SERVER_DATABASE=TGMATestDB

# テスト設定
export E2E_TIMEOUT=120
export E2E_RETRIES=3
```

## ✅ 移植性チェックリスト

新しい環境でテストを実行する前に確認:

- [ ] Docker Desktop/Engineが起動している
- [ ] ポート1433が利用可能
- [ ] Python 3.11+がインストール済み
- [ ] 必要な依存関係がインストール済み
- [ ] ODBCドライバーがインストール済み
- [ ] 十分なメモリ/ディスク容量がある
- [ ] プロキシ設定が正しい

## 🎉 成功確認

全て正常に動作すれば、以下のような結果が期待されます：

1. **Docker環境**: SQLServerコンテナが正常に起動
2. **データベース**: テーブルとテストデータが自動作成
3. **テスト実行**: 402個以上のテストがパス
4. **クリーンアップ**: テスト終了後の自動クリーンアップ

## 📞 サポート

問題が発生した場合は、以下の情報を含めて報告してください：

- OS/Dockerバージョン
- エラーメッセージの全文
- `docker logs sqlserver-e2e-test` の出力
- 実行したコマンドの履歴
