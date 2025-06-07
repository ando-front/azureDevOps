# ARMテンプレート処理ツール

Azure Resource Manager（ARM）テンプレートの分析・変更・最適化を行うツールセットです。

## ファイル構成

- **`optimized-arm-replacement.ps1`** - 最適化されたARMテンプレート内SQL置換ツール
- **`arm-template-sql-replacement.ps1`** - 基本的なARMテンプレートSQL置換ツール

## 使用方法

### 最適化版（推奨）
```powershell
# 基本的な置換実行
.\scripts\arm-template-tools\optimized-arm-replacement.ps1 `
  -ArmTemplateFile "src\dev\arm_template\ARMTemplateForFactory.json" `
  -SqlDirectory "sql\e2e_queries" `
  -OutputFile "build\arm_template\ARMTemplateForFactory_optimized.json"

# ドライラン（変更確認のみ）
.\scripts\arm-template-tools\optimized-arm-replacement.ps1 -DryRun

# バックアップ無効化（高速処理）
.\scripts\arm-template-tools\optimized-arm-replacement.ps1 -Backup:$false
```

### パフォーマンス調整
```powershell
# 大容量ファイル処理
.\scripts\arm-template-tools\optimized-arm-replacement.ps1 `
  -TimeoutSeconds 180 `
  -MaxReplacementLength 200000

# 高速処理（小規模ファイル）
.\scripts\arm-template-tools\optimized-arm-replacement.ps1 `
  -TimeoutSeconds 30 `
  -MaxReplacementLength 50000
```

## 活用ケース

### 1. DevOps自動化
```powershell
# デプロイ前の自動最適化
foreach ($template in Get-ChildItem "src\dev\arm_template\*.json") {
    .\scripts\arm-template-tools\optimized-arm-replacement.ps1 `
      -ArmTemplateFile $template.FullName `
      -OutputFile "build\arm_template\$($template.Name)" `
      -Backup
}
```

### 2. 大規模テンプレート処理
- **50MB以上のテンプレート**: `TimeoutSeconds 300`
- **1000個以上のSQLクエリ**: `MaxReplacementLength 500000`
- **メモリ制約環境**: 段階的処理、バックアップ無効化

### 3. バッチ処理
```powershell
# 複数環境一括処理
$environments = @("dev", "staging", "prod")
foreach ($env in $environments) {
    $inputPath = "src\$env\arm_template\ARMTemplateForFactory.json"
    $outputPath = "build\$env\ARMTemplateForFactory_optimized.json"
    
    if (Test-Path $inputPath) {
        .\scripts\arm-template-tools\optimized-arm-replacement.ps1 `
          -ArmTemplateFile $inputPath `
          -OutputFile $outputPath `
          -SqlDirectory "sql\$env"
    }
}
```

### 4. 品質管理
```powershell
# JSON検証付き処理
.\scripts\arm-template-tools\optimized-arm-replacement.ps1 -DryRun
if ($LASTEXITCODE -eq 0) {
    Write-Host "Validation passed. Proceeding with actual replacement..."
    .\scripts\arm-template-tools\optimized-arm-replacement.ps1
} else {
    Write-Error "Validation failed. Check ARM template syntax."
}
```

## パラメータ詳細

| パラメータ | デフォルト | 説明 |
|-----------|-----------|------|
| `ArmTemplateFile` | 必須 | 入力ARMテンプレートファイルパス |
| `SqlDirectory` | `sql\e2e_queries` | SQLファイル格納ディレクトリ |
| `OutputFile` | 必須 | 出力ファイルパス |
| `TimeoutSeconds` | `60` | 処理タイムアウト時間 |
| `MaxReplacementLength` | `100000` | 置換対象クエリの最大文字数 |
| `DryRun` | `false` | 実際の変更を行わない |
| `Backup` | `true` | 元ファイルのバックアップ作成 |

## 出力ファイル

- **最適化済みARMテンプレート**: 指定したOutputFileパス
- **バックアップファイル**: `元ファイル名.backup_yyyyMMdd_HHmmss`
- **外部化SQLファイル**: `SqlDirectory\query_*.sql`
- **処理レポート**: 統計情報をコンソール出力

## 注意事項

1. **JSON形式の検証**: 出力ファイルは自動的にJSON形式検証が行われます
2. **バックアップ**: デフォルトで元ファイルのバックアップが作成されます
3. **タイムアウト**: 大容量ファイルでは適切なタイムアウト値を設定してください
4. **メモリ使用量**: MaxReplacementLengthで処理対象クエリサイズを制限できます

## エラー対応

処理でエラーが発生した場合：
```powershell
# 診断実行
.\scripts\diagnostics\troubleshoot-sql-externalization.ps1 -DiagnosticLevel validation

# 手動検証
Test-Json (Get-Content "path\to\arm_template.json" -Raw)
```
