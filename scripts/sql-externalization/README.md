# SQL外部化ツール

Azure Data Factory ARMテンプレート内の長いSQLクエリを外部ファイルに分離するためのツールセットです。

## ファイル構成

### メインツール
- **`run-optimized-sql-externalization.ps1`** - 統合実行スクリプト（推奨）
- **`optimized-sql-externalization.ps1`** - 最適化されたSQL外部化処理エンジン

## 使用方法

### 基本的な実行
```powershell
# 統合実行（分析 + 置換）
.\scripts\sql-externalization\run-optimized-sql-externalization.ps1 -Operation both

# 分析のみ実行
.\scripts\sql-externalization\run-optimized-sql-externalization.ps1 -Operation analyze

# 置換のみ実行
.\scripts\sql-externalization\run-optimized-sql-externalization.ps1 -Operation replace

# ドライラン（実際の変更なし）
.\scripts\sql-externalization\run-optimized-sql-externalization.ps1 -Operation both -DryRun
```

### パフォーマンス調整
```powershell
# タイムアウト時間を調整（大量データ処理時）
.\scripts\sql-externalization\run-optimized-sql-externalization.ps1 -TimeoutSeconds 120 -BatchSize 3

# 高速処理設定（小規模データ時）
.\scripts\sql-externalization\run-optimized-sql-externalization.ps1 -TimeoutSeconds 30 -BatchSize 10
```

## 活用ケース

### 1. CI/CDパイプライン統合
```yaml
# GitHub Actions例
- name: SQL Externalization
  run: |
    .\scripts\sql-externalization\run-optimized-sql-externalization.ps1 -Operation both -DryRun
    if ($LASTEXITCODE -eq 0) {
      .\scripts\sql-externalization\run-optimized-sql-externalization.ps1 -Operation both
    }
```

### 2. 大規模ARMテンプレート処理
- **10MB以上のARMテンプレート**: `TimeoutSeconds 180`、`BatchSize 2`
- **複雑なSQLクエリ**: `MaxQueryLength 100000`
- **メモリ制約環境**: `BatchSize 1`、段階的実行

### 3. 定期メンテナンス
```powershell
# 月次実行例
$timestamp = Get-Date -Format "yyyyMM"
.\scripts\sql-externalization\run-optimized-sql-externalization.ps1 -Operation analyze > "reports\sql-analysis-$timestamp.log"
```

## パラメータ詳細

| パラメータ | デフォルト | 説明 |
|-----------|-----------|------|
| `Operation` | `both` | 実行操作（analyze/replace/both/test） |
| `TimeoutSeconds` | `60` | 処理タイムアウト時間 |
| `BatchSize` | `5` | 一度に処理するクエリ数 |
| `DryRun` | `false` | 実際の変更を行わない |
| `Verbose` | `false` | 詳細ログ出力 |

## 出力ファイル

- **分析レポート**: `reports\analysis_report_yyyyMMdd_HHmmss.json`
- **置換レポート**: `reports\replacement_report_yyyyMMdd_HHmmss.json`
- **最適化済みARMテンプレート**: `build\arm_template\*.json`
- **外部化SQLファイル**: `sql\e2e_queries\query_*.sql`

## トラブルシューティング

処理でエラーが発生した場合は、診断ツールを使用してください：
```powershell
.\scripts\diagnostics\troubleshoot-sql-externalization.ps1 -DiagnosticLevel full
```
