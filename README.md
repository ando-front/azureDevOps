# azureDevOps プロジェクト オンボーディングガイド

---

## はじめに

このリポジトリは、Azure Data Factory (ADF) パイプライン開発・CI/CD・単体テスト自動化のための実践サンプルです。新規参画エンジニアが迷わず開発に着手できるよう、プロジェクトの全体像・アーキテクチャ・ディレクトリ構成・ローカル開発/テスト手順を体系的にまとめています。

- **対象読者:** ADF/Python/Docker/GitHub Actionsを用いたデータ基盤開発に参画するITエンジニア
- **ゴール:** クローン直後からローカルでテストを実行し、CI/CDパイプラインの仕組みを理解・拡張できる

---

## プロジェクト概要

- **クロスプラットフォーム対応:** Windows/WSL/Linux/Mac いずれでも開発可能
- **CI/CD自動化:** GitHub Actionsによる自動テスト・デプロイ
- **Docker統合:** Azurite（Blob Storageエミュレータ）・pytest環境をDockerで一発構築
- **プロキシ/企業ネットワーク対応:** NO_PROXY自動設定・ネットワーク分離
- **テスト自動化:** pytestによるパイプライン単体テスト・カバレッジ計測

---

## ディレクトリ構成（抜粋）

```
azureDevOps/
├── docker-compose.yml         # サービス統合・ネットワーク設定
├── Dockerfile                # pytest/Azurite環境構築
├── startup.sh                # サービス起動・環境自動判定
├── test.ps1                  # テスト実行スクリプト
├── src/                      # ADFパイプライン定義(JSON)
├── tests/
│   ├── unit/                 # 各パイプラインのpytestテスト
│   ├── conftest.py           # pytest共通設定
│   └── data/                 # テスト用データ
└── .github/workflows/        # CI/CDワークフロー
```

---

## 開発・テスト環境の構築手順

### 1. リポジトリのクローン
```powershell
git clone <本リポジトリURL>
cd azureDevOps
```

### 2. Docker環境での統合テスト実行（推奨）
※Docker環境は「Rancher Desktop(OSSのDocker代替ツール)」を使用します。導入方法は「Rancher Desktopによるローカルコンテナ環境」を参照ください。
```powershell
# サービス起動・テスト実行
./test.ps1 test
```
- Azurite・pytest環境が自動構築され、全テストが実行されます
- テスト結果・カバレッジはコンソールに出力

### 3. ローカル環境でのテスト実行（参考）
```powershell
pip install -r requirements.txt
# Azurite起動（別ターミナル）
docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite
pytest tests/unit/ -v
```

---

## テスト・CI/CDアーキテクチャ概要

```
┌──────────────┐    ┌──────────────┐
│  pytest-test │◄─►│   azurite    │
│  (Python/CI) │    │ (Blob Emu)  │
└──────────────┘    └──────────────┘
        │                │
        └─────test-network(Docker)───┘
```
- **pytest-test:** Python/pytest/依存パッケージ入りのテスト用コンテナ
- **azurite:** Azure Blob Storage互換のエミュレータ
- **test-network:** サービス間通信用Dockerネットワーク

---

## ユニットテスト仕様

本プロジェクトのユニットテストは、ADFパイプラインのJSON定義・SQLカラム名抽出・Blobストレージ連携・SFTP転送など、データ基盤の各種処理をpytestベースで自動検証します。

- **テストフレームワーク:** pytest（`tests/unit/` 配下に `test_*.py` 形式で配置）
- **テストデータ:** `tests/data/input` などに自動生成。手動作成不要
- **主なテスト観点:**
  - パイプラインJSONの構造・必須プロパティ検証
  - SQLカラム名の正規化・英語名抽出（`helpers/english_column_extractor.py`）
  - Blobストレージへのアップロード/ダウンロード
  - SFTPサーバへのファイル転送
  - エラー・例外発生時のハンドリング
  - カバレッジ計測（`pytest-cov`）
- **CI/CD連携:** GitHub Actions上でも全テストが自動実行され、失敗時は詳細ログが出力されます
- **スキップ/未実装テスト:** `pytest.skip`＋`TODO`コメントで明示。実装漏れ防止
- **テスト追加方法:** `tests/unit/` に `test_新規機能名.py` を追加し、`test_` で始まる関数を実装

---

## Rancher Desktopによるローカルコンテナ環境

本プロジェクトのDockerコンテナ実行環境には[Rancher Desktop](https://rancherdesktop.io/)を推奨しています。

### 1. Rancher Desktopの導入方法
- [公式サイト](https://rancherdesktop.io/)からインストーラをダウンロードし、OSに応じてインストール
- Windowsの場合はインストーラを実行し、指示に従ってセットアップ
- インストール後、Rancher Desktopアプリを起動

### 2. 初期設定
- 初回起動時に「コンテナランタイム」として `dockerd (moby)` を選択（Docker互換）
- WSL統合（Windowsの場合）を有効化すると、WSL2からも `docker` コマンドが利用可能
- 設定画面で「Kubernetes」機能は不要な場合はOFFにしてOK
- 必要に応じてリソース（CPU/メモリ）割り当てを調整

### 3. ステータス確認方法
- Rancher DesktopのGUIで「Containers」「Images」「Settings」などから状態を確認
- タスクトレイアイコンから「Dashboard」を開くと、現在のコンテナ・イメージ・ネットワーク状況が一覧表示
- コマンドラインからも `docker ps` `docker images` `docker network ls` などDocker互換コマンドが利用可能

### 4. よく使う機能・操作方法
- **コンテナの起動/停止/削除:**
  - GUIの「Containers」タブで個別にStart/Stop/Removeが可能
  - CLIから `docker-compose up -d` `docker-compose down` も利用可
- **イメージの管理:**
  - 「Images」タブでローカルイメージの確認・削除
  - CLIから `docker images` `docker rmi <image>`
- **ログの確認:**
  - GUIで各コンテナの「Logs」ボタンからリアルタイムログ閲覧
  - CLIから `docker logs <container>`
- **ネットワークの確認:**
  - 「Networks」タブでブリッジネットワークや接続状況を可視化
  - CLIから `docker network ls` `docker network inspect <network>`
- **リソース割り当て変更:**
  - 「Settings」>「Resources」からCPU/メモリ/ディスク割り当てを調整
- **Kubernetes機能:**
  - 必要な場合のみ「Kubernetes」タブで有効化

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