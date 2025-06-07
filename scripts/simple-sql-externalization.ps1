
param(
    [string]$SourceFile = "src\dev\arm_template\ARMTemplateForFactory.json",
    [string]$SqlDirectory = "external_sql"
)

Write-Host "=== シンプルSQL外部化処理 ===" -ForegroundColor Cyan

# パス設定
$sourcePath = Join-Path (Get-Location).Path $SourceFile
$sqlDir = Join-Path (Get-Location).Path $SqlDirectory

# ファイル存在確認
if (-not (Test-Path $sourcePath)) {
    Write-Host "エラー: ソースファイルが見つかりません: $sourcePath" -ForegroundColor Red
    exit 1
}

# SQLディレクトリ作成
if (-not (Test-Path $sqlDir)) {
    New-Item -Path $sqlDir -ItemType Directory -Force | Out-Null
}

Write-Host "ソースファイル: $sourcePath" -ForegroundColor Yellow
Write-Host "SQLディレクトリ: $sqlDir" -ForegroundColor Yellow

# ARMテンプレート読み込み
try {
    Write-Host "ARMテンプレートを読み込んでいます..." -ForegroundColor Yellow
    $content = Get-Content -Path $sourcePath -Raw -Encoding UTF8
    Write-Host "ファイルサイズ: $([math]::Round($content.Length / 1MB, 2)) MB" -ForegroundColor Yellow
}
catch {
    Write-Host "エラー: ファイル読み込みに失敗しました: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

# JSON解析
try {
    Write-Host "JSONを解析しています..." -ForegroundColor Yellow
    $jsonObject = $content | ConvertFrom-Json
}
catch {
    Write-Host "エラー: JSON解析に失敗しました: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

# 長いSQLクエリを検索
Write-Host "長いSQLクエリを検索しています..." -ForegroundColor Yellow

$longQueries = @()
$queryCount = 0
$minLength = 1000

# sqlReaderQueryを検索
$sqlReaderMatches = [regex]::Matches($content, '"sqlReaderQuery":\s*"([^"]*(?:\\.[^"]*)*)"', [System.Text.RegularExpressions.RegexOptions]::Singleline)

foreach ($match in $sqlReaderMatches) {
    $queryContent = $match.Groups[1].Value
    $unescapedQuery = $queryContent -replace '\\n', "`n" -replace '\\t', "`t" -replace '\\"', '"' -replace '\\\\', '\'
    
    if ($unescapedQuery.Length -gt $minLength) {
        $queryCount++
        $fileName = "query_$queryCount.sql"
        $longQueries += @{
            FileName      = $fileName
            Content       = $unescapedQuery
            OriginalMatch = $match.Groups[1].Value
            Length        = $unescapedQuery.Length
        }
    }
}

Write-Host "発見された長いクエリ数: $($longQueries.Count)" -ForegroundColor Green

if ($longQueries.Count -eq 0) {
    Write-Host "外部化対象の長いSQLクエリが見つかりませんでした" -ForegroundColor Yellow
    exit 0
}

# SQLファイル作成
Write-Host "SQLファイルを作成しています..." -ForegroundColor Yellow

foreach ($query in $longQueries) {
    $sqlFilePath = Join-Path $sqlDir $query.FileName
    
    try {
        $query.Content | Out-File -FilePath $sqlFilePath -Encoding UTF8
        Write-Host "作成: $($query.FileName) ($($query.Length) 文字)" -ForegroundColor Green
    }
    catch {
        Write-Host "エラー: SQLファイル作成に失敗しました: $($_.Exception.Message)" -ForegroundColor Red
    }
}

# 置換されたARMテンプレート作成
Write-Host "ARMテンプレートを更新しています..." -ForegroundColor Yellow

$updatedContent = $content
$replacementCount = 0

foreach ($query in $longQueries) {
    $originalPattern = [regex]::Escape($query.OriginalMatch)
    $replacement = "{{EXTERNAL_SQL:$($query.FileName)}}"
    
    if ($updatedContent -match $originalPattern) {
        $updatedContent = $updatedContent -replace $originalPattern, $replacement
        $replacementCount++
        Write-Host "置換: $($query.FileName)" -ForegroundColor Green
    }
}

# 更新されたファイル保存
$outputPath = Join-Path (Get-Location).Path "ARMTemplateForFactory_External.json"
try {
    $updatedContent | Out-File -FilePath $outputPath -Encoding UTF8
    Write-Host "更新されたARMテンプレートを保存しました: $outputPath" -ForegroundColor Green
}
catch {
    Write-Host "エラー: 更新ファイル保存に失敗しました: $($_.Exception.Message)" -ForegroundColor Red
}

# 結果サマリー
Write-Host "=== 処理結果 ===" -ForegroundColor Cyan
Write-Host "発見されたクエリ数: $($longQueries.Count)" -ForegroundColor Yellow
Write-Host "置換された箇所数: $replacementCount" -ForegroundColor Yellow
Write-Host "作成されたSQLファイル:" -ForegroundColor Yellow

Get-ChildItem -Path $sqlDir -Filter "*.sql" | ForEach-Object {
    $size = [math]::Round($_.Length / 1KB, 1)
    Write-Host "  - $($_.Name) ($size KB)" -ForegroundColor White
}

$originalSize = (Get-Item $sourcePath).Length
$newSize = (Get-Item $outputPath).Length
$reduction = $originalSize - $newSize

Write-Host "ファイルサイズ変化:" -ForegroundColor Yellow
Write-Host "  元のサイズ: $([math]::Round($originalSize / 1MB, 2)) MB" -ForegroundColor White
Write-Host "  新しいサイズ: $([math]::Round($newSize / 1MB, 2)) MB" -ForegroundColor White
Write-Host "  削減量: $([math]::Round($reduction / 1MB, 2)) MB" -ForegroundColor Green

Write-Host "SQL外部化処理が完了しました" -ForegroundColor Green
