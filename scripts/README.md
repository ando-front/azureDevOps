# スクリプトツール総合ガイド

Azure Data Factory開発・保守で使用するPowerShellスクリプトツールセットです。

## フォルダ構成

```
scripts/
├── sql-externalization/          # SQL外部化ツール（メイン）
│   ├── run-optimized-sql-externalization.ps1    # 統合実行スクリプト
│   ├── optimized-sql-externalization.ps1        # 最適化処理エンジン
│   └── README.md
├── arm-template-tools/           # ARMテンプレート処理ツール
│   ├── optimized-arm-replacement.ps1            # 最適化置換ツール
│   ├── arm-template-sql-replacement.ps1         # 基本置換ツール
│   └── README.md
├── diagnostics/                  # 診断・トラブルシューティング
│   ├── troubleshoot-sql-externalization.ps1     # 診断ツール
│   └── README.md
└── deprecated/                   # 非推奨・旧バージョン
    ├── enhanced-sql-externalization.ps1
    ├── simple-sql-externalization.ps1
    ├── sql-externalization.ps1
    ├── test-sql-externalization.ps1
    └── README.md
```

## クイックスタート

### 1. 基本的なSQL外部化
```powershell
# 推奨：統合実行スクリプト使用
.\scripts\sql-externalization\run-optimized-sql-externalization.ps1 -Operation both
```

### 2. 問題発生時の診断
```powershell
# システム診断実行
.\scripts\diagnostics\troubleshoot-sql-externalization.ps1 -DiagnosticLevel full -FixIssues
```

### 3. 個別ARMテンプレート処理
```powershell
# 特定ファイルの処理
.\scripts\arm-template-tools\optimized-arm-replacement.ps1 `
  -ArmTemplateFile "src\dev\arm_template\ARMTemplateForFactory.json" `
  -OutputFile "build\arm_template\ARMTemplateForFactory_optimized.json"
```

## 推奨使用パターン

### 開発時（日常使用）
```powershell
# 1. 事前診断
.\scripts\diagnostics\troubleshoot-sql-externalization.ps1 -DiagnosticLevel quick

# 2. ドライラン実行
.\scripts\sql-externalization\run-optimized-sql-externalization.ps1 -Operation both -DryRun

# 3. 実際の処理
.\scripts\sql-externalization\run-optimized-sql-externalization.ps1 -Operation both
```

### 大容量処理時
```powershell
# メモリ・タイムアウト調整
.\scripts\sql-externalization\run-optimized-sql-externalization.ps1 `
  -Operation both `
  -TimeoutSeconds 180 `
  -BatchSize 3
```

### CI/CD統合時
```powershell
# 自動化パイプライン例
$diagnostics = .\scripts\diagnostics\troubleshoot-sql-externalization.ps1 -DiagnosticLevel validation
if ($diagnostics.Issues.Count -eq 0) {
    .\scripts\sql-externalization\run-optimized-sql-externalization.ps1 -Operation both
} else {
    Write-Error "Pre-deployment validation failed"
    exit 1
}
```

## 主要機能

### SQL外部化ツール
- **長いSQLクエリの外部ファイル分離**
- **タイムアウト制御・バッチ処理**
- **包括的なエラーハンドリング**
- **詳細な実行レポート生成**

### ARMテンプレート処理ツール
- **JSON検証付きSQL置換**
- **自動バックアップ機能**
- **大容量ファイル対応**
- **バッチ処理サポート**

### 診断・トラブルシューティング
- **システムリソース分析**
- **パフォーマンス測定**
- **ファイル整合性検証**
- **自動問題修復**

## パフォーマンス指標

### 推奨設定値

| 環境・用途 | TimeoutSeconds | BatchSize | MaxQueryLength |
|-----------|---------------|-----------|----------------|
| 小規模（<10MB） | 30 | 10 | 50000 |
| 中規模（10-50MB） | 60 | 5 | 100000 |
| 大規模（>50MB） | 180 | 2 | 200000 |
| メモリ制約環境 | 120 | 1 | 30000 |

### 処理時間目安

| ARMテンプレートサイズ | SQLクエリ数 | 処理時間（目安） |
|-------------------|-----------|----------------|
| 1-5MB | 50-100個 | 30秒-1分 |
| 5-20MB | 100-500個 | 1-3分 |
| 20-50MB | 500-1000個 | 3-10分 |
| 50MB以上 | 1000個以上 | 10分以上 |

## メンテナンス・運用

### 定期メンテナンス
```powershell
# 週次ヘルスチェック
.\scripts\diagnostics\troubleshoot-sql-externalization.ps1 -DiagnosticLevel full -GenerateReport

# 月次最適化
.\scripts\sql-externalization\run-optimized-sql-externalization.ps1 -Operation analyze
```

### ログ・レポート管理
- **診断レポート**: `reports\diagnostic_*.json`
- **処理レポート**: `reports\*_report_*.json`
- **最適化済みファイル**: `build\arm_template\*.json`
- **外部化SQLファイル**: `sql\e2e_queries\query_*.sql`

### バックアップ戦略
- **自動バックアップ**: 処理前に元ファイルを自動バックアップ
- **バージョン管理**: タイムスタンプ付きバックアップファイル
- **復旧手順**: バックアップからの迅速な復旧サポート

## トラブルシューティング

### よくある問題

1. **メモリ不足エラー**
   ```powershell
   # BatchSizeを減らして実行
   .\scripts\sql-externalization\run-optimized-sql-externalization.ps1 -BatchSize 2
   ```

2. **タイムアウトエラー**
   ```powershell
   # タイムアウト時間を延長
   .\scripts\sql-externalization\run-optimized-sql-externalization.ps1 -TimeoutSeconds 300
   ```

3. **JSON検証エラー**
   ```powershell
   # 詳細診断実行
   .\scripts\diagnostics\troubleshoot-sql-externalization.ps1 -DiagnosticLevel validation
   ```

### サポート・問い合わせ

問題が解決しない場合：
1. 診断レポートを生成
2. エラーメッセージをコピー
3. システム環境情報を収集
4. 開発チームに問い合わせ

## 更新履歴

- **2025年6月2日**: スクリプト整理・フォルダ分離完了
- **2025年6月1日**: 最適化版スクリプト実装完了
- **2025年5月31日**: 診断ツール追加
- **2025年5月30日**: 基本SQL外部化ツール作成
