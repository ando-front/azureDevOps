# Azure Data Factory ETL プロジェクト ディレクトリマップ

```
docs/
│
├── 📋 README.md                                      # 📖 ドキュメント体系ガイド
│
├── 🎯 戦略・設計基盤/
│   ├── REQUIREMENTS_SPECIFICATION.md                # 📋 要件定義書
│   ├── project_layout.md                            # 🗂️ プロジェクト構成
│   ├── TEST_STRATEGY_DOCUMENT.md                    # 📊 テスト戦略
│   ├── ARM_TEMPLATE_DESIGN_SPECIFICATION.md         # 🏗️ ARM設計仕様
│   ├── ARM_TEMPLATE_REQUIREMENTS_DEFINITION.md      # 📋 ARM要件定義
│   └── PRODUCTION_CODE_DESIGN_SPECIFICATION.md      # ⚙️ 本番コード設計
│
├── 🧪 テスト仕様・実装/
│   ├── TEST_DESIGN_SPECIFICATION.md                 # 🧪 テスト設計仕様
│   ├── COMPREHENSIVE_TEST_SPECIFICATION.md          # 🔍 包括的テスト仕様
│   ├── E2E_TEST_SPECIFICATION.md                    # 🎯 E2Eテスト仕様
│   ├── UNIT_TEST_SPECIFICATION.md                   # 🔧 単体テスト仕様
│   ├── PIPELINE_TEST_CLASSIFICATION_SPECIFICATION.md# 📊 パイプラインテスト分類
│   ├── E2E_TESTING.md                               # 🔄 E2Eテスト実装
│   ├── test_specification_alignment_analysis.md     # 📈 整合性分析
│   ├── TEST_IMPROVEMENT_COMPLETION_REPORT.md        # ✅ 改善完了報告
│   └── CONTINUOUS_IMPROVEMENT_PROCESS.md            # 🔄 継続改善プロセス
│
├── 🚀 CI/CD・運用/
│   ├── CI_CD_GUIDE.md                               # 🚀 CI/CDガイド
│   ├── test_environment_setup.md                    # 🔧 環境セットアップ
│   ├── TROUBLESHOOTING.md                           # 🔧 トラブルシューティング
│   ├── DOCKER_APT_GET_SOLUTION.md                  # 🐳 Dockerエラー解決
│   └── PORTABILITY_GUIDE.md                        # 📦 ポータビリティガイド
│
├── 🔄 パイプライン・統合/
│   ├── data_factory_pipeline.md                     # 🔄 DFパイプライン
│   ├── adf_pipeline_test_specification.md           # 📋 ADFパイプラインテスト
│   ├── job_flow_specification.md                    # ⚙️ ジョブフロー
│   ├── pipeline2_specification.md                   # 🔧 パイプライン2
│   ├── ADF_GIT_INTEGRATION.md                       # 📝 ADF Git統合
│   ├── IR_PORTAL_SETUP_GUIDE.md                     # 🔗 IR設定ガイド
│   └── IR_SHARING_SETUP.md                          # 🤝 IR共有設定
│
├── 📊 成果・改善報告/
│   ├── IMPROVEMENT_SUMMARY.md                       # 📊 改善サマリー
│   ├── E2E_FINAL_RESOLUTION_REPORT.md               # 🔧 E2E最終解決
│   ├── E2E_SQL_SERVER_CONNECTION_RESOLUTION_REPORT.md # 🔍 SQL接続解決
│   ├── E2E_SQL_SERVER_CONNECTION_FINAL_COMPLETION_REPORT.md # ✅ SQL接続完了
│   └── UNRESOLVED_E2E_ISSUES.md                     # 🔍 未解決課題
│
├── 📁 report/                                       # 🗂️ 詳細レポート集
│   ├── README.md                                    # 📋 レポート目次
│   ├── 📊 E2E最適化完了報告/
│   │   ├── E2E_FINAL_OPTIMIZATION_COMPLETION_REPORT.md
│   │   ├── E2E_FINAL_OPTIMIZATION_COMPLETION_REPORT_UPDATED.md
│   │   ├── E2E_OPTIMIZATION_COMPLETION_REPORT.md
│   │   └── E2E_TEST_COMPLETION_REPORT.md
│   ├── 🔍 品質・検証報告/
│   │   ├── E2E_COMPREHENSIVE_VALIDATION_AUDIT_REPORT.md
│   │   ├── E2E_TEST_FINAL_QUALITY_ASSESSMENT_REPORT.md
│   │   └── E2E_TEST_STABILITY_IMPROVEMENT_REPORT.md
│   ├── 📋 戦略・分析報告/
│   │   ├── E2E_TEST_STRATEGY_ANALYSIS_AND_RESOLUTION_PLAN.md
│   │   ├── E2E_TEST_STRATEGY_REVIEW.md
│   │   └── TEST_PASSING_ANALYSIS_REPORT.md
│   ├── 🔧 ファイル管理報告/
│   │   ├── E2E_TEST_FILE_OPTIMIZATION_PROJECT_FINAL_REPORT.md
│   │   ├── E2E_TEST_FILE_INTEGRATION_EXECUTION_REPORT.md
│   │   ├── E2E_TEST_FILE_CLEANUP_EXECUTION_REPORT.md
│   │   └── E2E_TEST_FILE_DUPLICATION_ANALYSIS_REPORT.md
│   ├── 📊 データベース報告/
│   │   ├── E2E_TEST_460_COLUMN_FINAL_COMPLETION_REPORT.md
│   │   ├── E2E_TEST_460_COLUMN_MIGRATION_REPORT.md
│   │   └── table_structure_validation_report.txt
│   ├── 🚀 CI/CD報告/
│   │   ├── GITHUB_ACTIONS_CI_SYNTAX_FIX_REPORT.md
│   │   ├── GITHUB_ACTIONS_ORG_RESTRICTIONS_FIX_REPORT.md
│   │   ├── PIPELINE_PATH_RESOLUTION_FIX_REPORT.md
│   │   └── PIPELINE_IMPLEMENTATION_ALIGNMENT_COMPLETION_REPORT.md
│   └── 🔧 トラブルシューティング/
│       ├── IR_ERROR_TROUBLESHOOTING.md
│       ├── E2E_TEST_PROGRESS_REPORT.md
│       ├── PROJECT_COMPLETION_REPORT.md
│       └── UNRESOLVED_E2E_ISSUES.md
│
└── 📁 cloak/                                        # 🎯 特定プロジェクト
    ├── README.md                                    # 📋 CLOAKプロジェクト目次
    ├── tgcrm_to_synapse_spec.md                     # 📊 TG-CRM統合仕様
    ├── tgcrm_to_synapse_todo_list.md               # 📋 TODO管理
    └── 要件定義・仕様書（案）の目次.md                # 📋 要件定義構成
```

## 📊 カテゴリ別ファイル統計

| カテゴリ | ファイル数 | 主要用途 |
|---------|-----------|---------|
| **戦略・設計** | 6 | 要件定義・アーキテクチャ設計 |
| **テスト仕様** | 9 | テスト設計・実装・改善 |
| **CI/CD・運用** | 5 | 開発・デプロイ・保守 |
| **パイプライン** | 7 | データ処理・統合設計 |
| **成果報告** | 5 | 改善結果・課題管理 |
| **詳細レポート** | 26 | 技術詳細・履歴管理 |
| **特定プロジェクト** | 4 | CLOAK固有実装 |
| **合計** | **62** | **包括的ドキュメント体系** |

## 🎯 利用パターン別アクセス

### 新規参加者
```
README.md → project_layout.md → TEST_STRATEGY_DOCUMENT.md
```

### 開発者
```
TEST_DESIGN_SPECIFICATION.md → E2E_TEST_SPECIFICATION.md → CI_CD_GUIDE.md
```

### 運用者
```
test_environment_setup.md → TROUBLESHOOTING.md → CONTINUOUS_IMPROVEMENT_PROCESS.md
```

### 管理者
```
TEST_IMPROVEMENT_COMPLETION_REPORT.md → IMPROVEMENT_SUMMARY.md → report/
```

### 技術調査
```
report/README.md → 目的別レポート選択 → 詳細技術レポート
```

## 🔄 更新・メンテナンス方針

### 定期更新
- **月次**: 進捗・成果報告類
- **四半期**: 戦略・設計仕様類  
- **随時**: トラブルシューティング・レポート類

### 品質保証
- ✅ トレーサビリティ確保
- ✅ 統一命名規則適用
- ✅ リンク整合性確認
- ✅ 継続的改善反映

---

**体系化完了日**: 2025年7月4日  
**管理者**: ETLテスト設計担当  
**バージョン**: v2.0 (構造最適化版)
