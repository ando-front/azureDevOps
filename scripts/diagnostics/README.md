# 診断・トラブルシューティングツール

SQL外部化プロセスやARMテンプレート処理で発生する問題を診断・解決するためのツールです。

## ファイル構成

- **`troubleshoot-sql-externalization.ps1`** - 包括的な診断・問題解決ツール

## 使用方法

### 基本的な診断
```powershell
# 全体診断（推奨）
.\scripts\diagnostics\troubleshoot-sql-externalization.ps1 -DiagnosticLevel full

# クイック診断
.\scripts\diagnostics\troubleshoot-sql-externalization.ps1 -DiagnosticLevel quick

# 問題の自動修復
.\scripts\diagnostics\troubleshoot-sql-externalization.ps1 -DiagnosticLevel full -FixIssues
```

### 特定領域の診断
```powershell
# システムリソース診断
.\scripts\diagnostics\troubleshoot-sql-externalization.ps1 -DiagnosticLevel memory

# パフォーマンス診断
.\scripts\diagnostics\troubleshoot-sql-externalization.ps1 -DiagnosticLevel performance

# ファイル検証診断
.\scripts\diagnostics\troubleshoot-sql-externalization.ps1 -DiagnosticLevel validation
```

## 診断レベル詳細

### `full` - 包括的診断（推奨）
- システムリソース分析（CPU、メモリ、ディスク）
- ファイルシステム整合性チェック
- ARMテンプレートJSON検証
- パフォーマンステスト実行
- 推奨解決策の生成

### `quick` - 高速診断
- ファイル・ディレクトリ存在確認
- 基本的な権限チェック
- 最低限の整合性検証

### `memory` - メモリ関連診断
- メモリ使用量分析
- メモリリーク検出
- 推奨バッチサイズ計算

### `performance` - パフォーマンス診断
- JSON解析速度測定
- 正規表現処理速度測定
- ボトルネック特定

### `validation` - ファイル検証診断
- ARMテンプレートJSON形式検証
- SQLファイル構文チェック
- ファイルサイズ・エンコーディング確認

## 活用ケース

### 1. 定期ヘルスチェック
```powershell
# 日次ヘルスチェック
$timestamp = Get-Date -Format "yyyyMMdd"
.\scripts\diagnostics\troubleshoot-sql-externalization.ps1 `
  -DiagnosticLevel full `
  -GenerateReport > "logs\health-check-$timestamp.log"
```

### 2. CI/CD前の事前検証
```yaml
# GitHub Actions例
- name: Pre-deployment Health Check
  run: |
    $result = .\scripts\diagnostics\troubleshoot-sql-externalization.ps1 -DiagnosticLevel validation
    if ($result.Issues.Count -gt 0) {
      Write-Error "Health check failed. Fix issues before deployment."
      exit 1
    }
```

### 3. パフォーマンス問題解決
```powershell
# 処理が遅い場合の診断
.\scripts\diagnostics\troubleshoot-sql-externalization.ps1 -DiagnosticLevel performance

# メモリ不足の場合の診断
.\scripts\diagnostics\troubleshoot-sql-externalization.ps1 -DiagnosticLevel memory -FixIssues
```

### 4. 新環境セットアップ検証
```powershell
# 新しい開発環境での初期検証
.\scripts\diagnostics\troubleshoot-sql-externalization.ps1 `
  -DiagnosticLevel full `
  -FixIssues `
  -GenerateReport
```

### 5. 大規模処理前の事前確認
```powershell
# 大量のARMテンプレート処理前
$diagnosticResult = .\scripts\diagnostics\troubleshoot-sql-externalization.ps1 -DiagnosticLevel memory

if ($diagnosticResult.SystemInfo.MemoryUsagePercent -gt 80) {
    Write-Warning "High memory usage detected. Consider reducing batch size."
    $recommendedBatchSize = 2
} else {
    $recommendedBatchSize = 10
}

# 推奨設定で実行
.\scripts\sql-externalization\run-optimized-sql-externalization.ps1 -BatchSize $recommendedBatchSize
```

## パラメータ詳細

| パラメータ | デフォルト | 説明 |
|-----------|-----------|------|
| `DiagnosticLevel` | `full` | 診断レベル（full/quick/memory/performance/validation） |
| `FixIssues` | `false` | 検出された問題の自動修復 |
| `GenerateReport` | `true` | 詳細レポートファイル生成 |

## 出力ファイル

### レポートファイル
- **詳細レポート**: `reports\diagnostic_report_yyyyMMdd_HHmmss.json`
- **概要レポート**: `reports\diagnostic_summary_yyyyMMdd_HHmmss.txt`

### レポート内容
- システム情報（CPU、メモリ、ディスク使用量）
- 検出された問題一覧
- 推奨解決策
- パフォーマンスメトリクス
- ファイル検証結果

## 自動修復機能

`-FixIssues` スイッチを使用すると、以下の問題が自動修復されます：

1. **不足ディレクトリの作成**
   - `sql\e2e_queries`
   - `build\arm_template`
   - `reports`

2. **権限問題の解決**
   - ファイル読み取り権限の確認
   - 書き込み権限の確認

3. **設定最適化**
   - 推奨タイムアウト値の提案
   - 推奨バッチサイズの計算

## よくある問題と解決策

### メモリ不足エラー
```
問題: OutOfMemoryException発生
解決: BatchSizeを減らす（推奨値：3-5）
診断: -DiagnosticLevel memory
```

### タイムアウトエラー
```
問題: 処理がタイムアウトする
解決: TimeoutSecondsを増やす（推奨値：120-300）
診断: -DiagnosticLevel performance
```

### JSON検証エラー
```
問題: ARMテンプレートが無効
解決: 元ファイルの構文確認
診断: -DiagnosticLevel validation
```

### ファイルアクセスエラー
```
問題: ファイル読み込み/書き込み失敗
解決: 権限確認、ファイルロック解除
診断: -DiagnosticLevel quick -FixIssues
```

## スケジュール実行例

```powershell
# 週次メンテナンススクリプト
# weekly-maintenance.ps1

Write-Host "Weekly maintenance started..." -ForegroundColor Green

# 1. システム診断
$diagnostics = .\scripts\diagnostics\troubleshoot-sql-externalization.ps1 `
  -DiagnosticLevel full `
  -FixIssues

# 2. 問題がない場合のみ最適化実行
if ($diagnostics.Issues.Count -eq 0) {
    Write-Host "System healthy. Running optimization..." -ForegroundColor Green
    .\scripts\sql-externalization\run-optimized-sql-externalization.ps1 -Operation analyze
} else {
    Write-Warning "Issues detected. Check diagnostic report."
}

Write-Host "Weekly maintenance completed." -ForegroundColor Green
```
