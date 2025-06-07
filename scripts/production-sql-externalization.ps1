#!/usr/bin/env powershell
# Production-ready SQL externalization script with timeout optimization

param(
    [Parameter(Mandatory=$true)]
    [string]$TemplatePath,
    
    [Parameter(Mandatory=$false)]
    [string]$OutputPath = ".\output_optimized",
    
    [Parameter(Mandatory=$false)]
    [string]$SqlOutputPath = ".\sql_externalized",
    
    [Parameter(Mandatory=$false)]
    [int]$BatchSize = 5,
    
    [Parameter(Mandatory=$false)]
    [int]$TimeoutSeconds = 30
)

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "SQL Externalization Production Process" -ForegroundColor Yellow
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
$script:ProcessedFiles = 0
$script:ExternalizedQueries = 0
$script:TotalSavedBytes = 0

function Invoke-WithTimeout {
    param(
        [scriptblock]$ScriptBlock,
        [int]$TimeoutSeconds = 30
    )
    
    $job = Start-Job -ScriptBlock $ScriptBlock
    $completed = Wait-Job -Job $job -Timeout $TimeoutSeconds
    
    if ($completed) {
        $result = Receive-Job -Job $job
        Remove-Job -Job $job
        return $result
    } else {
        Stop-Job -Job $job
        Remove-Job -Job $job
        throw "Operation timed out after $TimeoutSeconds seconds"
    }
}

function Process-LongSqlQueries {
    param(
        [string]$FilePath,
        [string]$Content
    )
    
    $fileName = Split-Path $FilePath -Leaf
    $baseName = [System.IO.Path]::GetFileNameWithoutExtension($fileName)
    $modifiedContent = $Content
    $queriesProcessed = 0
    
    try {
        # Find SQL queries longer than 1000 characters with timeout protection
        $findResult = Invoke-WithTimeout -TimeoutSeconds $TimeoutSeconds -ScriptBlock {
            param($content)
            [regex]::Matches($content, '"sqlReaderQuery":\s*"((?:[^"\\]|\\.)*)"\s*,', [System.Text.RegularExpressions.RegexOptions]::Singleline)
        } -ArgumentList $Content
        
        if ($findResult -and $findResult.Count -gt 0) {
            Write-Host "[PROCESSING] $fileName - Found $($findResult.Count) SQL queries" -ForegroundColor Yellow
            
            for ($i = 0; $i -lt $findResult.Count; $i++) {
                $match = $findResult[$i]
                $sqlQuery = $match.Groups[1].Value
                
                # Process only long queries (>1000 chars)
                if ($sqlQuery.Length -gt 1000) {
                    $sqlFileName = "${baseName}_query_${i}.sql"
                    $sqlFilePath = Join-Path $SqlOutputPath $sqlFileName
                    
                    # Clean and format SQL
                    $cleanedSql = $sqlQuery -replace '\\n', "`n" -replace '\\r', "`r" -replace '\\"', '"' -replace '\\\\', '\'
                    
                    # Save SQL to external file
                    Set-Content -Path $sqlFilePath -Value $cleanedSql -Encoding UTF8
                    
                    # Create file reference for ARM template
                    $relativePath = "sql_externalized/$sqlFileName"
                    $replacement = '"sqlReaderQuery": { "value": "@{string(pipeline().parameters.sqlQueries.query_${i})}", "type": "Expression" },'
                    
                    # Replace in content
                    $modifiedContent = $modifiedContent.Replace($match.Value, $replacement)
                    
                    $queriesProcessed++
                    $script:TotalSavedBytes += $sqlQuery.Length
                    
                    Write-Host "  - Externalized query $i to: $sqlFileName ($($sqlQuery.Length) chars)" -ForegroundColor Gray
                }
            }
        }
        
        return @{
            Content = $modifiedContent
            QueriesProcessed = $queriesProcessed
        }
    }
    catch {
        Write-Host "[WARNING] Timeout or error processing $fileName : $($_.Exception.Message)" -ForegroundColor Yellow
        return @{
            Content = $Content
            QueriesProcessed = 0
        }
    }
}

# Main processing
Write-Host "[INFO] Starting batch processing (batch size: $BatchSize)" -ForegroundColor Cyan

$armFiles = Get-ChildItem -Path $TemplatePath -Filter "*.json" -Recurse | Where-Object { $_.Length -gt 10KB }
$totalFiles = $armFiles.Count
Write-Host "[INFO] Found $totalFiles large JSON files to process" -ForegroundColor Green

# Process files in batches
for ($batch = 0; $batch -lt $totalFiles; $batch += $BatchSize) {
    $endIndex = [Math]::Min($batch + $BatchSize - 1, $totalFiles - 1)
    $currentBatch = $armFiles[$batch..$endIndex]
    
    Write-Host "" -ForegroundColor White
    Write-Host "=== BATCH $([Math]::Floor($batch / $BatchSize) + 1): Processing files $($batch + 1) to $($endIndex + 1) ===" -ForegroundColor Cyan
    
    foreach ($file in $currentBatch) {
        try {
            Write-Host "[PROCESSING] $($file.Name) ($([math]::Round($file.Length / 1KB, 0)) KB)" -ForegroundColor White
            
            # Read file with timeout protection
            $content = Invoke-WithTimeout -TimeoutSeconds $TimeoutSeconds -ScriptBlock {
                param($filePath)
                Get-Content -Path $filePath -Raw -Encoding UTF8
            } -ArgumentList $file.FullName
            
            # Process SQL queries
            $result = Process-LongSqlQueries -FilePath $file.FullName -Content $content
            
            if ($result.QueriesProcessed -gt 0) {
                # Save modified ARM template
                $outputFilePath = Join-Path $OutputPath $file.Name
                Set-Content -Path $outputFilePath -Value $result.Content -Encoding UTF8
                
                $script:ExternalizedQueries += $result.QueriesProcessed
                Write-Host "[SUCCESS] Externalized $($result.QueriesProcessed) queries from $($file.Name)" -ForegroundColor Green
            } else {
                Write-Host "[SKIP] No long queries found in $($file.Name)" -ForegroundColor Gray
            }
            
            $script:ProcessedFiles++
        }
        catch {
            Write-Host "[ERROR] Failed to process $($file.Name): $($_.Exception.Message)" -ForegroundColor Red
        }
    }
    
    # Memory cleanup after each batch
    [System.GC]::Collect()
    Start-Sleep -Milliseconds 500
}

# Generate summary report
Write-Host "" -ForegroundColor White
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "EXTERNALIZATION SUMMARY" -ForegroundColor Yellow
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "Files processed: $script:ProcessedFiles / $totalFiles" -ForegroundColor White
Write-Host "Queries externalized: $script:ExternalizedQueries" -ForegroundColor White
Write-Host "Total size reduced: $([math]::Round($script:TotalSavedBytes / 1KB, 0)) KB" -ForegroundColor White
Write-Host "Output directory: $OutputPath" -ForegroundColor White
Write-Host "SQL files directory: $SqlOutputPath" -ForegroundColor White

if ($script:ExternalizedQueries -gt 0) {
    Write-Host "" -ForegroundColor White
    Write-Host "[SUCCESS] SQL externalization completed successfully!" -ForegroundColor Green
    Write-Host "Next steps:" -ForegroundColor Yellow
    Write-Host "1. Review the modified ARM templates in: $OutputPath" -ForegroundColor Gray
    Write-Host "2. Review the externalized SQL files in: $SqlOutputPath" -ForegroundColor Gray
    Write-Host "3. Test the modified templates before deployment" -ForegroundColor Gray
    Write-Host "4. Update pipeline parameters to include SQL file references" -ForegroundColor Gray
} else {
    Write-Host "[INFO] No long SQL queries found. Templates are already optimized." -ForegroundColor Green
}

Write-Host "=========================================" -ForegroundColor Cyan
