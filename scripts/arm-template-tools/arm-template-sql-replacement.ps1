# ARM Template SQL Query Replacement Implementation
# ARM テンプレート内のインラインSQLクエリをOPENROWSET BULK参照に置き換える

param(
    [switch]$DryRun = $false,
    [switch]$Backup = $true
)

Write-Host "=== ARM TEMPLATE SQL REPLACEMENT ===" -ForegroundColor Magenta
Write-Host ""

$armTemplatePath = "src\dev\arm_template\ARMTemplateForFactory.json"
$externalizedDir = "build\externalized"
$backupPath = "src\dev\arm_template\ARMTemplateForFactory.json.backup"

# クエリ置換マッピング
$replacements = @(
    @{
        Name          = "Usage Services Query"
        SearchText    = '"sqlReaderQuery": "-- 「利用サービス」出力 at_CreateCSV_UsageServices\n\n-- 結果出力 => 利用サービス\n\nDECLARE @today_jst varchar(20);\nSET @today_jst=format(CONVERT(DATETIME, GETDATE() AT TIME ZONE ''UTC'' AT TIME ZONE ''Tokyo Standard Time''), ''yyyy/MM/dd HH:mm:ss'');\n\nSELECT \n     *\n    ,@today_jst as OUTPUT_DATETIME    -- 出力日付\nFROM [omni].[omni_ods_cloak_trn_usageservice]\n;\n"'
        ExternalFile  = "usage_services_main_query.sql"
        BulkReference = '"sqlReaderQuery": "SELECT query_text FROM OPENROWSET(BULK ''external_sql/usage_services_main_query.sql'', DATA_SOURCE = ''sql_queries_storage'', SINGLE_CLOB) AS [query_content]"'
    }
)

try {
    Write-Host "Processing ARM template: $armTemplatePath"
    
    # ARM テンプレートの読み込み
    if (-not (Test-Path $armTemplatePath)) {
        Write-Error "ARM template not found: $armTemplatePath"
        return
    }
    
    $armContent = Get-Content -Path $armTemplatePath -Raw -Encoding UTF8
    Write-Host "Original ARM template size: $([math]::Round($armContent.Length/1KB, 2)) KB"
    
    # バックアップの作成
    if ($Backup -and -not $DryRun) {
        Copy-Item $armTemplatePath $backupPath -Force
        Write-Host "✓ Backup created: $backupPath" -ForegroundColor Green
    }
    
    $modifiedContent = $armContent
    $replacementsMade = 0
    
    foreach ($replacement in $replacements) {
        Write-Host ""
        Write-Host "Processing: $($replacement.Name)" -ForegroundColor Cyan
        
        # 外部SQLファイルの確認
        $externalSqlPath = Join-Path $externalizedDir $replacement.ExternalFile
        if (-not (Test-Path $externalSqlPath)) {
            Write-Warning "External SQL file not found: $externalSqlPath"
            continue
        }
        
        Write-Host "  External SQL file: $($replacement.ExternalFile) ($(Get-Item $externalSqlPath | ForEach-Object { [math]::Round($_.Length/1KB, 2) }) KB)"
        
        # 検索テキストの確認と置換
        if ($modifiedContent.Contains($replacement.SearchText)) {
            Write-Host "  ✓ Found target query in ARM template" -ForegroundColor Green
            
            if (-not $DryRun) {
                $modifiedContent = $modifiedContent.Replace($replacement.SearchText, $replacement.BulkReference)
                $replacementsMade++
                Write-Host "  ✓ Replaced with OPENROWSET BULK reference" -ForegroundColor Green
            }
            else {
                Write-Host "  → Would replace with: $($replacement.BulkReference.Substring(0, 80))..." -ForegroundColor Yellow
            }
        }
        else {
            Write-Host "  ⚠ Target query not found in ARM template" -ForegroundColor Yellow
        }
    }
    
    # 修正された ARM テンプレートの保存
    if (-not $DryRun -and $replacementsMade -gt 0) {
        Set-Content -Path $armTemplatePath -Value $modifiedContent -Encoding UTF8
        Write-Host ""
        Write-Host "✓ Modified ARM template saved" -ForegroundColor Green
        Write-Host "Modified ARM template size: $([math]::Round($modifiedContent.Length/1KB, 2)) KB"
        
        $sizeDiff = $armContent.Length - $modifiedContent.Length
        Write-Host "Size reduction: $([math]::Round($sizeDiff/1KB, 2)) KB" -ForegroundColor Cyan
    }
    
    Write-Host ""
    Write-Host "=== REPLACEMENT SUMMARY ===" -ForegroundColor Magenta
    Write-Host "Total replacement targets: $($replacements.Count)"
    Write-Host "Successful replacements: $replacementsMade" -ForegroundColor Green
    
    if ($DryRun) {
        Write-Host ""
        Write-Warning "DRY RUN - No files were modified"
        Write-Host "Run without -DryRun to execute the replacement"
    }
    
}
catch {
    Write-Error "Replacement process failed: $($_.Exception.Message)"
}

Write-Host ""
Write-Host "ARM template SQL replacement completed!" -ForegroundColor Green
