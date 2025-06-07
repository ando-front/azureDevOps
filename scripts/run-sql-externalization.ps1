# SQL外部化統合実行スクリプト
param(
    [string]$RootPath = (Get-Location).Path,
    [string]$SourceFile = "src\dev\arm_template\ARMTemplateForFactory.json",
    [string]$DestinationFile = "ARMTemplateForFactory_External.json",
    [int]$BatchSize = 5,
    [int]$TimeoutSeconds = 30
)

Write-Host "=== SQL外部化統合実行スクリプト ===" -ForegroundColor Cyan
Write-Host "作業ディレクトリ: $RootPath" -ForegroundColor Yellow

# パス設定
$sourceFilePath = Join-Path $RootPath $SourceFile
$destinationFilePath = Join-Path $RootPath $DestinationFile
$sqlDirectory = Join-Path $RootPath "external_sql"
$optimizedScript = Join-Path $RootPath "scripts\sql-externalization\optimized-sql-externalization.ps1"

# 前提条件チェック
Write-Host "前提条件チェック..." -ForegroundColor Yellow

if (-not (Test-Path $sourceFilePath)) {
    Write-Host "ERROR: ソースファイルが見つかりません: $sourceFilePath" -ForegroundColor Red
    exit 1
}

if (-not (Test-Path $optimizedScript)) {
    Write-Host "ERROR: 最適化スクリプトが見つかりません: $optimizedScript" -ForegroundColor Red
    exit 1
}

# SQLディレクトリ作成
if (-not (Test-Path $sqlDirectory)) {
    New-Item -Path $sqlDirectory -ItemType Directory -Force
    Write-Host "SQLディレクトリを作成しました: $sqlDirectory" -ForegroundColor Green
}

# 最適化されたSQL外部化スクリプト実行
Write-Host "最適化されたSQL外部化処理を開始..." -ForegroundColor Yellow

try {
    & $optimizedScript -SourceFile $sourceFilePath -OutputFile $destinationFilePath -SqlDirectory $sqlDirectory -BatchSize $BatchSize -TimeoutSeconds $TimeoutSeconds
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "SQL外部化処理が正常に完了しました" -ForegroundColor Green
    } else {
        Write-Host "SQL外部化処理でエラーが発生しました (終了コード: $LASTEXITCODE)" -ForegroundColor Red
        exit 1
    }
} catch {
    Write-Host "スクリプト実行中にエラーが発生しました: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

# 結果確認
if (Test-Path $destinationFilePath) {
    $originalSize = (Get-Item $sourceFilePath).Length
    $newSize = (Get-Item $destinationFilePath).Length
    
    Write-Host "=== 処理結果 ===" -ForegroundColor Cyan
    Write-Host "元ファイルサイズ: $([math]::Round($originalSize / 1MB, 2)) MB" -ForegroundColor Yellow
    Write-Host "新ファイルサイズ: $([math]::Round($newSize / 1MB, 2)) MB" -ForegroundColor Yellow
    Write-Host "サイズ削減: $([math]::Round(($originalSize - $newSize) / 1MB, 2)) MB" -ForegroundColor Green
    
    # SQLファイル確認
    if (Test-Path $sqlDirectory) {
        $sqlFiles = Get-ChildItem -Path $sqlDirectory -Filter "*.sql"
        Write-Host "作成されたSQLファイル数: $($sqlFiles.Count)" -ForegroundColor Green
        
        if ($sqlFiles.Count -gt 0) {
            Write-Host "作成されたSQLファイル:" -ForegroundColor Yellow
            $sqlFiles | ForEach-Object { Write-Host "  - $($_.Name)" -ForegroundColor White }
        }
    }
} else {
    Write-Host "ERROR: 出力ファイルが作成されませんでした" -ForegroundColor Red
    exit 1
}

Write-Host "SQL外部化処理が完了しました" -ForegroundColor Green
