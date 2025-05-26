# azureDevOps プロジェクト オンボーディングガイド

本リポジトリは Azure Data Factory (ADF) パイプライン開発・CI/CD・単体テスト自動化のためのサンプル/実践プロジェクトです。

---

## 目次
1. [前提知識・全体像](#前提知識・全体像)
2. [開発環境セットアップ (Windows/WSL/プロキシ)](#開発環境セットアップ-windowswslプロキシ)
3. [Python/依存パッケージのインストール](#python依存パッケージのインストール)
4. [ローカルBlobエミュレータ(Azurite)の起動](#ローカルblobエミュレータazuriteの起動)
5. [プロキシ・ネットワーク設定の注意点](#プロキシ・ネットワーク設定の注意点)
6. [テストデータ・ディレクトリ構成](#テストデータ・ディレクトリ構成)
7. [テスト実行・カバレッジ計測](#テスト実行・カバレッジ計測)
8. [CI/CD・GitHub Actions連携](#cicdgithub-actions連携)
9. [トラブルシュート・FAQ](#トラブルシュートfaq)
10. [よく使うコマンド集](#よく使うコマンド集)

---

## 前提知識・全体像
- 本リポジトリは **Windows/WSL/Linux/Mac** いずれでも開発可能です。
- ADFパイプラインのJSON定義・Pythonによる単体テスト・CI/CD自動化（GitHub Actions）をカバーします。
- 社内プロキシ/WSL/Docker環境でも動作するよう工夫されています。

---

## 開発環境セットアップ (Windows/WSL/プロキシ)

### 1. WSL Ubuntuのインストール（Windowsユーザーのみ）
- Windows11でLinuxコマンドを使う場合はWSL上にUbuntuを導入
```powershell
wsl --install -d Ubuntu
```

### 2. プロキシ環境でのapt-get利用
- bashrcに以下を追記し、プロキシ経由でapt/curl等が使えるように
```bash
export http_proxy="http://your.proxy.server:8080"
export https_proxy="http://your.proxy.server:8080"
export HTTP_PROXY="http://your.proxy.server:8080"
export HTTPS_PROXY="http://your.proxy.server:8080"
```
- 設定反映: `source ~/.bashrc`
- 以降 `apt-get update` などが利用可能

---

## Python/依存パッケージのインストール
- Python 3.8以降（3.13動作確認済み）
- 依存インストール:
```powershell
pip install -r requirements.txt
```
- 主要依存: pytest, pytest-cov, azure-storage-blob など

---

## ローカルBlobエミュレータ(Azurite)の起動
- ADF/Blob連携テスト用にAzruiteをローカルで起動
- Docker推奨:
```powershell
docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite
```
- Windows/Mac/WSL いずれも利用可
- ポート10000が競合していないか確認

---

## プロキシ・ネットワーク設定の注意点
- **社内プロキシ環境では `NO_PROXY` に `127.0.0.1,localhost` を必ず追加**
  - PowerShell: `$env:NO_PROXY="127.0.0.1,localhost"`
  - bash/WSL: `export NO_PROXY=127.0.0.1,localhost`
- Docker/CI/CDでも `NO_PROXY` を明示的に設定
- Application Insights等の外部送信が不要な場合は `APPLICATIONINSIGHTS_DISABLED=true` を推奨

---

## テストデータ・ディレクトリ構成
- テスト用データは `tests/data/input`, `tests/data/output`, `tests/data/sftp` など
- `pytest` 実行時に自動生成されるため手動作成不要
- 追加のテストデータが必要な場合は `tests/data/input` 配下に配置

---

## テスト実行・カバレッジ計測
- テスト実行:
```powershell
pytest
```
- カバレッジ付き（`pytest.ini`で自動的に `--cov` オプション付与）
- テストファイル命名規則: `test_*.py` または `*_test.py`
- カバレッジHTMLレポート:
```powershell
pytest --cov=tests/unit --cov-report=html
# htmlcov/index.html をブラウザで参照
```

---

## CI/CD・GitHub Actions連携
- `.github/workflows/adf-deploy.yml` で全ブランチ自動テスト
- Docker/Proxy/NO_PROXY 設定もCIで自動化
- テスト失敗時はログを確認し、NO_PROXYやAzurite起動状況を再確認

---

## トラブルシュート・FAQ
- **ERR_ACCESS_DENIED/HTTP接続時にエラー**: NO_PROXY未設定やAzurite未起動が主因
- **Application Insightsの送信エラー**: `APPLICATIONINSIGHTS_DISABLED=true` で抑止
- **pytestでテストが検出されない**: ファイル名/関数名が `test_` で始まっているか確認
- **Blobストレージ関連のテスト失敗**: Azurite起動・NO_PROXY・ポート競合を再確認
- **apt-get fetch エラー (Connection reset by peer)**:
  - Dockerビルド時にネットワークホストモードを使う:  
    ```powershell
    docker build --network host .
    ```
  - `Dockerfile` には `Acquire::Retries=3`, `--fix-missing`, `DEBIAN_FRONTEND=noninteractive` を追加済みです

---

## よく使うコマンド集
- 依存インストール: `pip install -r requirements.txt`
- Azurite起動: `docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite`
- テスト実行: `pytest`
- カバレッジHTML: `pytest --cov=tests/unit --cov-report=html`

---