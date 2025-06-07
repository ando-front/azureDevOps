# Simple SQL Query Externalization - Working Implementation
# ARM テンプレート内のSQLクエリを外部ファイルに置き換える実用的な実装

param(
    [switch]$DryRun = $false
)

Write-Host "=== SQL QUERY EXTERNALIZATION PROCESS ===" -ForegroundColor Magenta
Write-Host "Mode: $(if ($DryRun) { 'DRY RUN' } else { 'LIVE EXECUTION' })" -ForegroundColor $(if ($DryRun) { 'Yellow' } else { 'Green' })
Write-Host ""

# クエリマッピング - ARM テンプレート内で見つかったクエリと対応する SQL ファイル
$queryMappings = @(
    @{
        Description   = "Client DM Main Query - Marketing to ODS"
        SearchPattern = "omni.顧客DM.*Marketingスキーマ.*のODS化"
        SqlFile       = "client_dm_main.sql"
        ExternalRef   = "client_dm_main_query"
    },
    @{
        Description   = "Client DM Equipment Service Query"
        SearchPattern = "CLIENT_KEY.*LIV0EU_1X.*LIV0EU_8X"
        SqlFile       = "client_dm_equipment_service.sql"
        ExternalRef   = "client_dm_equipment_service_query"
    },
    @{
        Description   = "Client DNA Large Query"
        SearchPattern = "omni.顧客DNA.*推定DM"
        SqlFile       = "client_dna_large_main.sql"
        ExternalRef   = "client_dna_large_main_query"
    },
    @{
        Description   = "Identity Verification Contract Query"
        SearchPattern = "本人特定契約.*IF連携"
        SqlFile       = "identity_verification_contract_main.sql"
        ExternalRef   = "identity_verification_contract_main_query"
    },
    @{
        Description   = "Usage Services Query"
        SearchPattern = "利用サービス.*出力.*at_CreateCSV_UsageServices"
        SqlFile       = "usage_services_main.sql"
        ExternalRef   = "usage_services_main_query"
    },
    @{
        Description   = "Mail Permission Query"
        SearchPattern = "MA向けリスト連携.*メール許諾.*出力"
        SqlFile       = "mail_permission_main.sql"
        ExternalRef   = "mail_permission_main_query"
    },
    @{
        Description   = "Opening Payment Guide Query"
        SearchPattern = "開栓後の支払方法のご案内.*開栓者全量連携"
        SqlFile       = "opening_payment_guide_main.sql"
        ExternalRef   = "opening_payment_guide_main_query"
    },
    @{
        Description   = "Electricity Contract Thanks Query"
        SearchPattern = "電気契約Thanksシナリオ"
        SqlFile       = "electricity_contract_thanks_scenario_main.sql"
        ExternalRef   = "electricity_contract_thanks_scenario_main_query"
    }
)

# ARM テンプレートファイルの処理
$armTemplatePath = "src\dev\arm_template\ARMTemplateForFactory.json"
$sqlDir = "sql\e2e_queries"
$outputDir = "build\externalized"

Write-Host "Processing ARM template: $armTemplatePath"

try {
    # ARM テンプレートの読み込み
    $armContent = Get-Content -Path $armTemplatePath -Raw -Encoding UTF8
    Write-Host "ARM template size: $([math]::Round($armContent.Length/1KB, 2)) KB"
    
    # SQL ディレクトリの確認
    if (-not (Test-Path $sqlDir)) {
        Write-Error "SQL directory not found: $sqlDir"
        return
    }
    
    # 出力ディレクトリの作成
    if (-not $DryRun) {
        if (-not (Test-Path $outputDir)) {
            New-Item -ItemType Directory -Path $outputDir -Force | Out-Null
            Write-Host "Created output directory: $outputDir" -ForegroundColor Green
        }
    }
    
    $processedQueries = 0
    $modifiedContent = $armContent
    
    foreach ($mapping in $queryMappings) {
        Write-Host ""
        Write-Host "Processing: $($mapping.Description)" -ForegroundColor Cyan
        
        # 対応するSQLファイルの確認
        $sqlFilePath = Join-Path $sqlDir $mapping.SqlFile
        if (-not (Test-Path $sqlFilePath)) {
            Write-Warning "SQL file not found: $sqlFilePath"
            continue
        }
        
        Write-Host "  SQL file: $($mapping.SqlFile) ($(Get-Item $sqlFilePath | ForEach-Object { [math]::Round($_.Length/1KB, 2) }) KB)"
        
        # ARM テンプレート内でパターンを検索
        $pattern = $mapping.SearchPattern
        if ($modifiedContent -match $pattern) {
            Write-Host "  ✓ Pattern found in ARM template" -ForegroundColor Green
            
            # OPENROWSET BULK 参照の作成
            $externalReference = @"
OPENROWSET(
    BULK '$($mapping.ExternalRef).sql',
    DATA_SOURCE = 'sql_queries_storage',
    SINGLE_CLOB
) AS query_content
"@
            
            Write-Host "  External reference: $($mapping.ExternalRef).sql"
            
            if (-not $DryRun) {
                # SQL ファイルを出力ディレクトリにコピー
                $outputSqlPath = Join-Path $outputDir "$($mapping.ExternalRef).sql"
                Copy-Item $sqlFilePath $outputSqlPath -Force
                Write-Host "  ✓ SQL file copied to: $outputSqlPath" -ForegroundColor Green
            }
            
            $processedQueries++
        }
        else {
            Write-Host "  ⚠ Pattern not found in ARM template" -ForegroundColor Yellow
        }
    }
    
    Write-Host ""
    Write-Host "=== PROCESSING SUMMARY ===" -ForegroundColor Magenta
    Write-Host "Total query mappings: $($queryMappings.Count)"
    Write-Host "Successfully processed: $processedQueries" -ForegroundColor Green
    Write-Host "ARM template size: $([math]::Round($armContent.Length/1KB, 2)) KB"
    
    if (-not $DryRun) {
        # 修正されたARM テンプレートの保存（実際の置換は次のフェーズで実装）
        Write-Host ""
        Write-Host "SQL files externalized to: $outputDir" -ForegroundColor Green
        Write-Host "Next step: Implement ARM template query replacement with OPENROWSET references"
    }
    else {
        Write-Host ""
        Write-Warning "DRY RUN - No files were modified"
    }
    
}
catch {
    Write-Error "Processing failed: $($_.Exception.Message)"
}

Write-Host ""
Write-Host "Externalization analysis completed!" -ForegroundColor Green
