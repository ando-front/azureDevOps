#!/usr/bin/env powershell
# ArmTemplate_4.json専用SQL外部化スクリプト

param(
    [Parameter(Mandatory = $true)]
    [string]$TemplatePath,
    
    [Parameter(Mandatory = $false)]
    [string]$OutputPath = ".\output_armtemplate4",
    
    [Parameter(Mandatory = $false)]
    [string]$SqlOutputPath = ".\sql_externalized_armtemplate4"
)

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "ArmTemplate_4.json SQL Externalization" -ForegroundColor Yellow
Write-Host "=========================================" -ForegroundColor Cyan

# Create output directories
if (-not (Test-Path $OutputPath)) {
    New-Item -ItemType Directory -Path $OutputPath -Force | Out-Null
    Write-Host "[INFO] Created output directory: $OutputPath" -ForegroundColor Green
}

if (-not (Test-Path $SqlOutputPath)) {
    New-Item -ItemType Directory -Path $SqlOutputPath -Force | Out-Null
    Write-Host "[INFO] Created SQL output directory: $SqlOutputPath" -ForegroundColor Green
}

# Initialize counters
$script:ExternalizedQueries = 0
$script:TotalSavedBytes = 0

function Process-ArmTemplate4SqlQueries {
    param(
        [string]$FilePath,
        [string]$Content
    )
    
    $fileName = Split-Path $FilePath -Leaf
    $baseName = [System.IO.Path]::GetFileNameWithoutExtension($fileName)
    $modifiedContent = $Content
    $queriesProcessed = 0
    
    try {
        # Find SQL queries with the specific pattern: "sqlReaderQuery": { "value": "...", "type": "Expression" }
        $pattern = '"sqlReaderQuery":\s*\{\s*"value":\s*"((?:[^"\\]|\\.)*)"\s*,\s*"type":\s*"Expression"\s*\}'
        $matches = [regex]::Matches($Content, $pattern, [System.Text.RegularExpressions.RegexOptions]::Singleline)
        
        if ($matches.Count -gt 0) {
            Write-Host "[PROCESSING] $fileName - Found $($matches.Count) SQL queries" -ForegroundColor Yellow
            
            for ($i = 0; $i -lt $matches.Count; $i++) {
                $match = $matches[$i]
                $sqlQuery = $match.Groups[1].Value
                
                # Process only long queries (>500 chars to capture more queries)
                if ($sqlQuery.Length -gt 500) {
                    $sqlFileName = "${baseName}_query_${i}.sql"
                    $sqlFilePath = Join-Path $SqlOutputPath $sqlFileName
                    
                    # Clean and format SQL
                    $cleanedSql = $sqlQuery -replace '\\n', "`n" -replace '\\r', "`r" -replace '\\"', '"' -replace '\\\\', '\'
                    
                    # Save SQL to external file
                    Set-Content -Path $sqlFilePath -Value $cleanedSql -Encoding UTF8
                    
                    # Create file reference for ARM template
                    $replacement = '"sqlReaderQuery": { "value": "@{variables(''sqlQueries'').query_' + $i + '}", "type": "Expression" }'
                    
                    # Replace in content
                    $modifiedContent = $modifiedContent.Replace($match.Value, $replacement)
                    
                    $queriesProcessed++
                    $script:TotalSavedBytes += $sqlQuery.Length
                    
                    Write-Host "  - Externalized query $i to: $sqlFileName ($($sqlQuery.Length) chars)" -ForegroundColor Gray
                }
            }
        }
        
        return @{
            Content          = $modifiedContent
            QueriesProcessed = $queriesProcessed
        }
    }
    catch {
        Write-Host "[ERROR] Error processing $fileName : $($_.Exception.Message)" -ForegroundColor Red
        return @{
            Content          = $Content
            QueriesProcessed = 0
        }
    }
}

# Main processing
Write-Host "[INFO] Processing: $TemplatePath" -ForegroundColor Cyan

try {
    # Read the ARM template file
    $content = Get-Content -Path $TemplatePath -Raw -Encoding UTF8
    Write-Host "[INFO] File size: $([math]::Round((Get-Item $TemplatePath).Length / 1KB, 0)) KB" -ForegroundColor White
    
    # Process SQL queries
    $result = Process-ArmTemplate4SqlQueries -FilePath $TemplatePath -Content $content
    
    if ($result.QueriesProcessed -gt 0) {
        # Save modified ARM template
        $outputFileName = Split-Path $TemplatePath -Leaf
        $outputFilePath = Join-Path $OutputPath $outputFileName
        Set-Content -Path $outputFilePath -Value $result.Content -Encoding UTF8
        
        $script:ExternalizedQueries = $result.QueriesProcessed
        Write-Host "[SUCCESS] Externalized $($result.QueriesProcessed) queries from $outputFileName" -ForegroundColor Green
        
        # Generate variables section for the externalized SQL files
        $variablesSection = @"

    // Externalized SQL queries variables
    "sqlQueries": {
"@
        
        for ($i = 0; $i -lt $script:ExternalizedQueries; $i++) {
            $sqlFileName = "ArmTemplate_4_query_${i}.sql"
            $sqlFilePath = Join-Path $SqlOutputPath $sqlFileName
            $sqlContent = Get-Content -Path $sqlFilePath -Raw
            $escapedSql = $sqlContent -replace '"', '\"' -replace "`n", '\n' -replace "`r", '\r'
            
            $variablesSection += @"

      "query_$i": "$escapedSql"
"@
            if ($i -lt ($script:ExternalizedQueries - 1)) {
                $variablesSection += ","
            }
        }
        
        $variablesSection += @"

    }
"@
        
        # Save variables section to a separate file for reference
        $variablesFilePath = Join-Path $OutputPath "sql_variables.json"
        Set-Content -Path $variablesFilePath -Value $variablesSection -Encoding UTF8
        Write-Host "[INFO] SQL variables section saved to: $variablesFilePath" -ForegroundColor Green
        
    }
    else {
        Write-Host "[SKIP] No long queries found in the template" -ForegroundColor Gray
    }
}
catch {
    Write-Host "[ERROR] Failed to process template: $($_.Exception.Message)" -ForegroundColor Red
}

# Generate summary report
Write-Host "" -ForegroundColor White
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "EXTERNALIZATION SUMMARY" -ForegroundColor Yellow
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "Queries externalized: $script:ExternalizedQueries" -ForegroundColor White
Write-Host "Total size reduced: $([math]::Round($script:TotalSavedBytes / 1KB, 0)) KB" -ForegroundColor White
Write-Host "Output directory: $OutputPath" -ForegroundColor White
Write-Host "SQL files directory: $SqlOutputPath" -ForegroundColor White

if ($script:ExternalizedQueries -gt 0) {
    Write-Host "" -ForegroundColor White
    Write-Host "[SUCCESS] SQL externalization completed successfully!" -ForegroundColor Green
    Write-Host "Next steps:" -ForegroundColor Yellow
    Write-Host "1. Review the modified ARM template in: $OutputPath" -ForegroundColor Gray
    Write-Host "2. Review the externalized SQL files in: $SqlOutputPath" -ForegroundColor Gray
    Write-Host "3. Review the variables section in: $OutputPath\sql_variables.json" -ForegroundColor Gray
    Write-Host "4. Integrate the variables section into your ARM template" -ForegroundColor Gray
    Write-Host "5. Test the modified template before deployment" -ForegroundColor Gray
}
else {
    Write-Host "[INFO] No long SQL queries found. Template is already optimized." -ForegroundColor Green
}

Write-Host "=========================================" -ForegroundColor Cyan
