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

### Docker環境でのテスト実行（推奨）
最新のテスト環境では、Dockerを使用した統合テスト実行が推奨されます：

```powershell
# Docker環境でのテスト実行
.\test.ps1 test
```

**Docker環境の特徴:**
- Azurite（Azure Blob Storage エミュレータ）が自動起動
- プロキシ設定が自動適用（`NO_PROXY=127.0.0.1,localhost,devstoreaccount1,azurite`）
- Python仮想環境とすべての依存関係が事前設定済み
- ネットワーク分離によりローカル環境への影響なし

**Docker環境の起動確認:**
```powershell
# サービス状況確認
docker-compose ps

# ログ確認
docker-compose logs pytest-test
docker-compose logs azurite
```

### ローカル環境でのテスト実行
従来通りのローカル環境でもテスト実行可能です：

```powershell
# 依存パッケージインストール
pip install -r requirements.txt

# Azurite起動（別ターミナル）
docker run -p 10000:10000 -p 10001:10001 -p 10002:10002 mcr.microsoft.com/azure-storage/azurite:latest azurite --location /data --debug /data/debug.log --loose --skipApiVersionCheck --blobHost 0.0.0.0 --queueHost 0.0.0.0 --tableHost 0.0.0.0

# プロキシ設定（企業環境の場合）
$env:NO_PROXY="127.0.0.1,localhost,devstoreaccount1"

# テスト実行
pytest tests/unit/ -v
```

### テスト対象とアーキテクチャ

**主要テストファイル:**
- `test_pi_Copy_marketing_client_dm.py` - SQLカラム名マッチングテスト（英語名抽出アプローチ）
- `test_pi_Insert_ClientDmBx.py` - INSERT文解析テスト
- `test_pi_PointGrantEmail.py` - メール送信パイプラインテスト
- `test_pi_Send_ActionPointCurrentMonthEntryList.py` - アクションポイントリストテスト
- `test_pipeline1.py` - 基本パイプライン構造テスト

**英語カラム名抽出アプローチ:**
日本語・英語・全角・半角混在のカラム名問題を解決するため、新しいアプローチを採用：
- SQL AS句（エイリアス）の英語名部分のみを抽出
- `helpers/english_column_extractor.py`で実装
- より安定したカラム名マッチングを実現

### カバレッジ計測
```powershell
# カバレッジ付きテスト実行
pytest tests/unit/ --cov=tests/unit --cov-report=html --cov-report=term

# HTMLレポート確認
start htmlcov/index.html
```

### テスト結果の例
```
=================== test session starts ===================
collected 23 items

test_pi_Copy_marketing_client_dm.py::test_pipeline_name PASSED     [ 4%]
test_pi_Copy_marketing_client_dm.py::test_activities_exist PASSED  [ 8%]
test_pi_Copy_marketing_client_dm.py::test_input_output_columns_match PASSED [12%]
test_pi_Copy_marketing_client_dm.py::test_missing_required_property PASSED [16%]
test_pi_Copy_marketing_client_dm.py::test_column_count_mismatch PASSED     [20%]
test_pi_Copy_marketing_client_dm.py::test_mock_column_names PASSED         [24%]
...

=================== 20 passed, 1 failed, 2 skipped in 19.34s ===================
```

---

## CI/CD・GitHub Actions連携
- `.github/workflows/adf-deploy.yml` で全ブランチ自動テスト
- Docker/Proxy/NO_PROXY 設定もCIで自動化
- テスト失敗時はログを確認し、NO_PROXYやAzurite起動状況を再確認

---

## トラブルシュート・FAQ

### Docker環境のトラブルシューティング
**Dockerサービスが起動しない場合:**
```powershell
# 全サービス停止・再起動
docker-compose down
docker-compose up -d

# 構成ファイルの検証
docker-compose config

# 個別サービスの状況確認
docker-compose ps
docker-compose logs [service-name]
```

**Azurite接続エラーの場合:**
```powershell
# Azuriteのヘルスチェック
curl http://localhost:10000/devstoreaccount1

# プロキシ設定の確認
docker-compose exec pytest-test env | grep PROXY
```

### テスト実行のトラブルシューティング
**ERR_ACCESS_DENIED/HTTP接続時にエラー**: 
- NO_PROXY未設定やAzurite未起動が主因
- Docker環境では自動設定されるため、`.\test.ps1 test`の使用を推奨

**Application Insightsの送信エラー**: 
- `APPLICATIONINSIGHTS_DISABLED=true` で抑止
- Docker環境では事前設定済み

**pytestでテストが検出されない**: 
- ファイル名/関数名が `test_` で始まっているか確認
- `tests/unit/` ディレクトリ内にテストファイルがあるか確認

**SQLカラム名マッチングテストの失敗**:
- 英語カラム名抽出アプローチを使用（`english_column_extractor.py`）
- 日本語カラム名の正規化問題を回避
- AS句（エイリアス）の英語名部分のみで比較

**Blobストレージ関連のテスト失敗**: 
- Azurite起動・NO_PROXY・ポート競合を再確認
- Docker環境では `azurite` サービスとの連携が自動設定済み

**Docker build時のネットワークエラー**:
- プロキシ環境では以下のコマンドを使用:
  ```powershell
  docker build --network host .
  ```
- `Dockerfile` には `Acquire::Retries=3`, `--fix-missing`, `DEBIAN_FRONTEND=noninteractive` を追加済み

### パフォーマンス最適化
**テスト実行時間の短縮:**
- 特定のテストファイルのみ実行: `pytest tests/unit/test_pi_Copy_marketing_client_dm.py -v`
- 並列実行: `pytest -n auto` (pytest-xdist必要)
- カバレッジ無効化: `pytest --no-cov`

---

## よく使うコマンド集

### Docker環境（推奨）
```powershell
# 統合テスト実行
.\test.ps1 test

# Docker環境の起動・停止
docker-compose up -d          # バックグラウンド起動
docker-compose down           # 全サービス停止
docker-compose ps             # サービス状況確認
docker-compose logs pytest-test  # pytestログ確認
docker-compose logs azurite     # Azuriteログ確認

# Docker環境内で直接コマンド実行
docker-compose exec pytest-test python -m pytest /tests/unit/ -v
docker-compose exec pytest-test python -c "import azure.storage.blob; print('Azure SDK OK')"
```

### ローカル環境
```powershell
# 依存パッケージインストール
pip install -r requirements.txt

# Azurite起動（スタンドアロン）
docker run -p 10000:10000 -p 10001:10001 -p 10002:10002 mcr.microsoft.com/azure-storage/azurite:latest azurite --location /data --debug /data/debug.log --loose --skipApiVersionCheck --blobHost 0.0.0.0 --queueHost 0.0.0.0 --tableHost 0.0.0.0

# プロキシ設定
$env:NO_PROXY="127.0.0.1,localhost,devstoreaccount1"

# テスト実行
pytest tests/unit/ -v                    # 詳細出力
pytest tests/unit/ --cov=tests/unit --cov-report=html  # カバレッジ付き
pytest tests/unit/test_pi_Copy_marketing_client_dm.py   # 特定ファイルのみ
```

### デバッグ・検証
```powershell
# 構成ファイル検証
docker-compose config

# ネットワーク接続確認
curl http://localhost:10000/devstoreaccount1    # Azurite接続確認
docker-compose exec pytest-test curl http://azurite:10000/devstoreaccount1  # コンテナ間通信確認

# Python環境確認
python -c "import pytest; print(f'pytest version: {pytest.__version__}')"
python -c "import azure.storage.blob; print('Azure Storage SDK imported successfully')"

# ファイル構造確認
tree tests/unit/                        # テストファイル構造
docker-compose exec pytest-test ls -la /tests/unit/  # コンテナ内ファイル確認
```

### クリーンアップ
```powershell
# Docker環境のリセット
docker-compose down -v           # ボリュームも削除
docker system prune -f           # 未使用リソース削除

# Python環境のクリーンアップ
pip uninstall -y -r requirements.txt
Remove-Item -Recurse -Force htmlcov/  # カバレッジレポート削除
Remove-Item -Recurse -Force tests/__pycache__/  # キャッシュ削除
```

---

## プロジェクト構成・アーキテクチャ

### テスト環境のアーキテクチャ
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   pytest-test   │    │     azurite     │    │   adf-test      │
│                 │    │                 │    │                 │
│ - Python 3.13   │◄──►│ - Blob Storage  │    │ - Legacy Tests  │
│ - pytest        │    │ - Queue Storage │    │ - Backup        │
│ - Azure SDK     │    │ - Table Storage │    │                 │
│                 │    │                 │    │                 │
│ Port: 8080,2222 │    │ Port: 10000-02  │    │ Port: 8081,2223 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │  test-network   │
                    │ (Docker Bridge) │
                    └─────────────────┘
```

### 重要なファイルとその役割

**Docker環境:**
- `docker-compose.yml` - サービス統合・ネットワーク・プロキシ設定
- `Dockerfile` - Python/pytest環境の構築
- `test.ps1` - テスト実行用PowerShellスクリプト

**テストコード:**
- `tests/unit/test_pi_*.py` - 各パイプラインの単体テスト
- `tests/unit/helpers/english_column_extractor.py` - 英語カラム名抽出ロジック
- `tests/unit/helpers/sql_column_extractor.py` - SQL解析・カラム抽出
- `tests/conftest.py` - pytest設定・共通フィクスチャ

**パイプライン定義:**
- `src/dev/pipeline/pi_*.json` - ADF パイプライン定義ファイル
- `pipeline/*.json` - レガシーパイプライン定義

### 新機能・改善点

**英語カラム名抽出アプローチ:**
- 従来の日本語カラム名正規化から英語名マッチングに変更
- より安定したテスト結果を実現
- AS句（エイリアス）の英語名部分のみを比較対象とする

**プロキシ対応の強化:**
- Docker環境での自動プロキシ設定
- `NO_PROXY`環境変数の適切な設定
- 企業ネットワーク環境での安定動作

**テスト実行環境の改善:**
- Docker Composeによる統合環境
- Azuriteの自動起動・ヘルスチェック
- ネットワーク分離によるローカル環境への影響回避

### テスト実行フロー
1. **環境準備**: `docker-compose up -d` でサービス群を起動
2. **サービス確認**: Azurite、pytest-testの正常起動を確認
3. **テスト実行**: `.\test.ps1 test` で統合テストを実行
4. **結果確認**: テスト結果とカバレッジレポートを確認
5. **クリーンアップ**: `docker-compose down` で環境をリセット

---

## 更新履歴

### 2025年5月27日 - 大幅リファクタリング完了
- **Docker環境での統合テスト実行を実装** - `.\test.ps1 test`で安定実行可能
- **英語カラム名抽出アプローチを導入** - 日本語カラム名の正規化問題を解決
- **プロキシ設定の自動化とネットワーク問題の解決** - NO_PROXY設定により接続安定化
- **テスト実行の完全成功を達成** - **19/21テスト成功、2テストスキップ**
- **問題ファイルの削除と環境最適化** - `test_pipeline2.py`削除、Azurite重複起動問題解決
- **startup.shスクリプトの改善** - 外部Azuriteサービス連携、重複起動防止

### 以前のバージョン
- 基本的なpytestテスト環境の構築
- ADF パイプライン定義とテストケースの作成
- CI/CD環境の基盤整備