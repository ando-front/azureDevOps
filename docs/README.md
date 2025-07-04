# Azure Data Factory ETL プロジェクト ドキュメント体系

このディレクトリは、Azure Data Factory ETLプロジェクトの包括的なドキュメント体系を提供します。

## 📋 ドキュメント構造

### 1. 戦略・設計基盤 (Strategy & Design Foundation)

#### 1.1 プロジェクト戦略
- [📋 要件定義書](./REQUIREMENTS_SPECIFICATION.md) - プロジェクト要件の詳細定義
- [🎯 プロジェクト構成](./project_layout.md) - プロジェクト全体の構成・配置
- [📊 テスト戦略](./TEST_STRATEGY_DOCUMENT.md) - 包括的テスト戦略・方針

#### 1.2 設計仕様
- [🏗️ ARM テンプレート設計仕様](./ARM_TEMPLATE_DESIGN_SPECIFICATION.md) - インフラ設計の詳細
- [📋 ARM テンプレート要件定義](./ARM_TEMPLATE_REQUIREMENTS_DEFINITION.md) - インフラ要件
- [⚙️ 本番コード設計仕様](./PRODUCTION_CODE_DESIGN_SPECIFICATION.md) - 本番環境設計

### 2. テスト仕様・実装 (Test Specifications & Implementation)

#### 2.1 テスト設計
- [🧪 テスト設計仕様](./TEST_DESIGN_SPECIFICATION.md) - テスト設計の基本方針
- [🔍 包括的テスト仕様](./COMPREHENSIVE_TEST_SPECIFICATION.md) - 全体テスト仕様
- [🎯 E2E テスト仕様](./E2E_TEST_SPECIFICATION.md) - エンドツーエンドテスト詳細

#### 2.2 テスト分類・実装
- [📊 パイプラインテスト分類仕様](./PIPELINE_TEST_CLASSIFICATION_SPECIFICATION.md) - 38パイプライン業務分類
- [🔧 単体テスト仕様](./UNIT_TEST_SPECIFICATION.md) - 単体テストの詳細
- [🔄 E2E テスト実装](./E2E_TESTING.md) - E2Eテスト実装ガイド

#### 2.3 テスト分析・改善
- [📈 テスト仕様・実装整合性分析](./test_specification_alignment_analysis.md) - 整合性分析結果
- [✅ テスト改善完了報告](./TEST_IMPROVEMENT_COMPLETION_REPORT.md) - 改善成果・ROI評価
- [🔄 継続的改善プロセス](./CONTINUOUS_IMPROVEMENT_PROCESS.md) - PDCAサイクル運用

### 3. CI/CD・運用 (CI/CD & Operations)

#### 3.1 CI/CD設計
- [🚀 CI/CD パイプラインガイド](./CI_CD_GUIDE.md) - GitHub Actions設定・運用
- [🔧 テスト環境セットアップ](./test_environment_setup.md) - Docker・Azure環境構築

#### 3.2 運用・保守
- [🔧 トラブルシューティング](./TROUBLESHOOTING.md) - 問題解決ガイド
- [📦 ポータビリティガイド](./PORTABILITY_GUIDE.md) - 環境移行・展開

### 4. パイプライン・統合 (Pipeline & Integration)

#### 4.1 パイプライン仕様
- [🔄 データファクトリーパイプライン](./data_factory_pipeline.md) - パイプライン基本設計
- [📋 ADFパイプラインテスト仕様](./adf_pipeline_test_specification.md) - パイプライン固有テスト
- [⚙️ ジョブフロー仕様](./job_flow_specification.md) - ジョブ実行フロー
- [🔧 パイプライン2仕様](./pipeline2_specification.md) - 拡張パイプライン仕様

#### 4.2 統合・連携
- [📝 ADF Git統合](./ADF_GIT_INTEGRATION.md) - GitOpsワークフロー
- [🔗 Integration Runtime設定](./IR_PORTAL_SETUP_GUIDE.md) - IR構成・設定
- [🤝 IR共有設定](./IR_SHARING_SETUP.md) - IR共有・管理

### 5. 成果・改善報告 (Results & Improvement Reports)

#### 5.1 主要成果報告
- [📊 改善サマリー](./IMPROVEMENT_SUMMARY.md) - 全体改善成果
- [✅ テスト改善完了報告](./TEST_IMPROVEMENT_COMPLETION_REPORT.md) - 詳細改善成果

#### 5.2 問題解決・最適化
- [🔧 E2E最終解決報告](./E2E_FINAL_RESOLUTION_REPORT.md) - E2E課題解決
- [🔍 E2E SQL Server接続解決報告](./E2E_SQL_SERVER_CONNECTION_RESOLUTION_REPORT.md) - 接続問題解決
- [✅ E2E SQL Server接続完了報告](./E2E_SQL_SERVER_CONNECTION_FINAL_COMPLETION_REPORT.md) - 接続完了

### 6. 詳細レポート (Detailed Reports)

#### 6.1 技術レポート
- [📁 詳細レポート](./report/) - 技術詳細・分析レポート集
- [🔍 未解決E2E課題](./UNRESOLVED_E2E_ISSUES.md) - 継続課題・対応予定

#### 6.2 特定プロジェクト
- [📁 CLOAK プロジェクト](./cloak/) - 特定プロジェクト仕様・TODO

## 🗂️ カテゴリ別ドキュメント索引

### 📋 要件・仕様書
- [REQUIREMENTS_SPECIFICATION.md](./REQUIREMENTS_SPECIFICATION.md)
- [ARM_TEMPLATE_DESIGN_SPECIFICATION.md](./ARM_TEMPLATE_DESIGN_SPECIFICATION.md)
- [ARM_TEMPLATE_REQUIREMENTS_DEFINITION.md](./ARM_TEMPLATE_REQUIREMENTS_DEFINITION.md)
- [PRODUCTION_CODE_DESIGN_SPECIFICATION.md](./PRODUCTION_CODE_DESIGN_SPECIFICATION.md)

### 🧪 テスト関連
- [TEST_STRATEGY_DOCUMENT.md](./TEST_STRATEGY_DOCUMENT.md)
- [TEST_DESIGN_SPECIFICATION.md](./TEST_DESIGN_SPECIFICATION.md)
- [COMPREHENSIVE_TEST_SPECIFICATION.md](./COMPREHENSIVE_TEST_SPECIFICATION.md)
- [E2E_TEST_SPECIFICATION.md](./E2E_TEST_SPECIFICATION.md)
- [UNIT_TEST_SPECIFICATION.md](./UNIT_TEST_SPECIFICATION.md)
- [PIPELINE_TEST_CLASSIFICATION_SPECIFICATION.md](./PIPELINE_TEST_CLASSIFICATION_SPECIFICATION.md)

### 🚀 CI/CD・運用
- [CI_CD_GUIDE.md](./CI_CD_GUIDE.md)
- [test_environment_setup.md](./test_environment_setup.md)
- [TROUBLESHOOTING.md](./TROUBLESHOOTING.md)
- [PORTABILITY_GUIDE.md](./PORTABILITY_GUIDE.md)

### 🔄 パイプライン
- [data_factory_pipeline.md](./data_factory_pipeline.md)
- [adf_pipeline_test_specification.md](./adf_pipeline_test_specification.md)
- [job_flow_specification.md](./job_flow_specification.md)
- [pipeline2_specification.md](./pipeline2_specification.md)

### 📊 分析・改善
- [test_specification_alignment_analysis.md](./test_specification_alignment_analysis.md)
- [TEST_IMPROVEMENT_COMPLETION_REPORT.md](./TEST_IMPROVEMENT_COMPLETION_REPORT.md)
- [CONTINUOUS_IMPROVEMENT_PROCESS.md](./CONTINUOUS_IMPROVEMENT_PROCESS.md)
- [IMPROVEMENT_SUMMARY.md](./IMPROVEMENT_SUMMARY.md)

### 🔗 統合・連携
- [ADF_GIT_INTEGRATION.md](./ADF_GIT_INTEGRATION.md)
- [IR_PORTAL_SETUP_GUIDE.md](./IR_PORTAL_SETUP_GUIDE.md)
- [IR_SHARING_SETUP.md](./IR_SHARING_SETUP.md)

## 📖 利用ガイド

### 新規参加者向け
1. [プロジェクト構成](./project_layout.md) でプロジェクト全体を把握
2. [テスト戦略](./TEST_STRATEGY_DOCUMENT.md) でテスト方針を理解
3. [CI/CD ガイド](./CI_CD_GUIDE.md) で開発・デプロイフローを確認

### 開発者向け
1. [テスト設計仕様](./TEST_DESIGN_SPECIFICATION.md) でテスト設計を理解
2. [パイプラインテスト分類仕様](./PIPELINE_TEST_CLASSIFICATION_SPECIFICATION.md) で業務分類を確認
3. [E2E テスト仕様](./E2E_TEST_SPECIFICATION.md) で実装要件を確認

### 運用者向け
1. [テスト環境セットアップ](./test_environment_setup.md) で環境構築
2. [トラブルシューティング](./TROUBLESHOOTING.md) で問題解決
3. [継続的改善プロセス](./CONTINUOUS_IMPROVEMENT_PROCESS.md) で改善運用

### 管理者向け
1. [テスト改善完了報告](./TEST_IMPROVEMENT_COMPLETION_REPORT.md) で成果確認
2. [改善サマリー](./IMPROVEMENT_SUMMARY.md) で全体進捗確認
3. [詳細レポート](./report/) で技術詳細確認

## 🎯 品質保証

### 文書品質基準
- ✅ テスト戦略準拠
- ✅ トレーサビリティ100%
- ✅ 統一命名規則適用
- ✅ 継続的更新プロセス

### 更新管理
- 📅 月次レビュー
- 🔄 四半期アップデート
- 📋 変更履歴管理
- 🎯 品質メトリクス追跡

---

**最終更新**: 2025年7月4日  
**管理者**: ETLテスト設計担当  
**版数**: v2.0 (体系化完了版)
