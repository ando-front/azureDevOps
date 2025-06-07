# Azure Data Factory テスト環境ガイド

> Azure Data Factory (ADF) パイプライン開発・テスト・デプロイの統合環境

## 🎯 プロジェクト概要

### 対象と目的

- **対象**: ADFとPython/Docker/GitHub Actionsを使用するデータエンジニア
- **目標**: クローン後すぐにテスト実行可能、CI/CDパイプライン理解と拡張
- **特徴**: ODBC不要、Windows対応、高速テスト、Docker統合

### 技術的成果

- ✅ ODBC依存性の完全排除
- ✅ 4層テスト戦略の実装
- ✅ Docker統合環境の構築
- ✅ 24.3%のARMテンプレート軽量化
- ✅ 100%成功のDocker E2Eテスト環境

## 📊 テスト戦略とアーキテクチャ

### 4層テスト戦略

```text
統合テスト → 単体テスト → E2Eテスト → 本番テスト
(SQL外部化)  (Mock使用)   (実DB接続)   (実Azure)
```

### テスト結果サマリー

| テスト層 | 成功率 | 環境 | 用途 |
|---------|-------|------|------|
| 統合テスト | 100% (4/4) | ローカル | SQL外部化・テンプレート分割検証 |
| 単体テスト | 86% (24/28) | Docker/ローカル | 高速開発、ODBC不要 |
| E2Eテスト | 100% (4/4) | Docker+SQL Server | 実データベース統合検証 |
| 本番テスト | 準備中 | Azure | CI/CD統合検証 |

## 🚀 クイックスタート

### 1. 環境セットアップ

```bash
git clone <repository-url>
cd tgma-MA-POC
```

### 2. 単体テスト実行（推奨）

```bash
# Docker環境での高速テスト
docker-compose up --build adf-unit-test
```

**実行時間**: 約30秒  
**結果**: 24/28テスト成功  
**特徴**: ODBC不要、モック使用

### 3. E2Eテスト実行

```bash
# E2E環境起動
docker-compose -f docker-compose.e2e.yml up -d

# テスト実行
docker exec -it ir-simulator-e2e pytest tests/e2e/test_docker_e2e_point_grant_email_fixed.py -v

# 環境停止
docker-compose -f docker-compose.e2e.yml down
```

## 📁 ファイル構造

```text
tgma-MA-POC/
├── docs/                           # 📚 詳細ドキュメント
│   ├── ADF_GIT_INTEGRATION.md     # Azure Data Factory Git統合ガイド
│   ├── E2E_TESTING.md             # E2Eテスト環境詳細ガイド
│   ├── CI_CD_GUIDE.md             # CI/CDパイプラインガイド
│   └── TROUBLESHOOTING.md         # トラブルシューティング
├── src/dev/                        # 🏭 Azure Data Factory定義
│   ├── pipeline/                  # 27個の本番パイプライン
│   ├── dataset/                   # データセット定義
│   └── linkedService/             # リンクサービス定義
├── tests/                          # 🧪 テストコード
│   ├── unit/                      # 単体テスト
│   ├── e2e/                       # E2Eテスト
│   └── integration/               # 統合テスト
├── docker-compose.yml             # 🐳 単体テスト環境
├── docker-compose.e2e.yml        # 🐳 E2Eテスト環境
└── .github/workflows/             # 🚀 CI/CDパイプライン
```

## 📚 詳細ドキュメント

### 専門ガイド

| ドキュメント | 内容 | 対象者 |
|-------------|------|--------|
| [🔗 ADF Git統合ガイド](./docs/ADF_GIT_INTEGRATION.md) | Azure Data Factory Git設定、27個の本番パイプライン一覧 | ADF開発者 |
| [🧪 E2Eテストガイド](./docs/E2E_TESTING.md) | Docker環境でのE2Eテスト詳細手順 | テストエンジニア |
| [🚀 CI/CDガイド](./docs/CI_CD_GUIDE.md) | GitHub Actions、ブランチ戦略、デプロイ | DevOpsエンジニア |
| [🔧 トラブルシューティング](./docs/TROUBLESHOOTING.md) | よくある問題と解決方法 | 全開発者 |

### 技術仕様

| 項目 | 詳細 |
|------|------|
| **Python版** | 3.9+ |
| **Docker環境** | Docker Compose v2対応 |
| **データベース** | SQL Server 2022 (E2E)、Mock (単体) |
| **Azure統合** | Data Factory、Storage Account |
| **CI/CD** | GitHub Actions、自動テスト |

## 🤝 貢献ガイド

### 開発フロー

1. **Issue作成** - バグ報告・機能要求
2. **Feature Branch** - `feature/機能名` でブランチ作成
3. **開発・テスト** - 単体テスト → E2Eテスト → 統合テスト
4. **Pull Request** - コードレビュー後マージ
5. **自動デプロイ** - CI/CDパイプラインで自動デプロイ

### 品質基準

- ✅ 単体テスト カバレッジ 80%以上
- ✅ E2Eテスト 全て成功
- ✅ Linting・フォーマット チェック合格
- ✅ セキュリティ脆弱性 スキャン合格

## 📞 サポート

### 問題報告

- **バグ報告**: GitHub Issues
- **機能要求**: GitHub Discussions
- **セキュリティ**: 直接連絡

### 関連リンク

- [Azure Data Factory ドキュメント](https://docs.microsoft.com/azure/data-factory/)
- [Docker Compose ガイド](https://docs.docker.com/compose/)
- [GitHub Actions ドキュメント](https://docs.github.com/actions)

---

**更新日**: 2024年1月  
**メンテナー**: Data Engineering Team  
**ライセンス**: MIT License
