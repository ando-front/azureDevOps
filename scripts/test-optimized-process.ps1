#!/usr/bin/env powershell
# Simple test script for SQL externalization process

param(
    [Parameter(Mandatory=$true)]
    [string]$TemplatePath,
    
    [Parameter(Mandatory=$false)]
    [string]$OutputPath = ".\output_test",
    
    [Parameter(Mandatory=$false)]
    [switch]$TestMode
)

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "SQL Externalization Test Process" -ForegroundColor Yellow
Write-Host "=========================================" -ForegroundColor Cyan

# Validate input path
if (-not (Test-Path $TemplatePath)) {
    Write-Host "[ERROR] Template path not found: $TemplatePath" -ForegroundColor Red
    exit 1
}

Write-Host "[INFO] Source template path: $TemplatePath" -ForegroundColor Green
Write-Host "[INFO] Output path: $OutputPath" -ForegroundColor Green
Write-Host "[INFO] Test mode: $TestMode" -ForegroundColor Green

# Find ARM template files
$armFiles = Get-ChildItem -Path $TemplatePath -Filter "*.json" -Recurse
Write-Host "[INFO] Found $($armFiles.Count) JSON files" -ForegroundColor Cyan

# Analyze files for long SQL queries
$longQueries = @()
$totalSize = 0

foreach ($file in $armFiles) {
    try {
        $content = Get-Content -Path $file.FullName -Raw -Encoding UTF8
        $totalSize += $content.Length
        
        # Simple regex to find SQL queries over 1000 characters
        $sqlMatches = [regex]::Matches($content, '"sqlReaderQuery":\s*"([^"]{1000,})"')
        
        if ($sqlMatches.Count -gt 0) {
            $longQueries += @{
                File = $file.FullName
                Count = $sqlMatches.Count
                Size = $content.Length
            }
            Write-Host "[FOUND] $($file.Name): $($sqlMatches.Count) long SQL queries" -ForegroundColor Yellow
        }
    }
    catch {
        Write-Host "[WARNING] Could not process $($file.Name): $($_.Exception.Message)" -ForegroundColor Yellow
    }
}

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "ANALYSIS SUMMARY" -ForegroundColor Yellow
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "Total JSON files: $($armFiles.Count)" -ForegroundColor White
Write-Host "Total content size: $([math]::Round($totalSize / 1MB, 2)) MB" -ForegroundColor White
Write-Host "Files with long queries: $($longQueries.Count)" -ForegroundColor White

if ($longQueries.Count -gt 0) {
    Write-Host "" -ForegroundColor White
    Write-Host "FILES REQUIRING EXTERNALIZATION:" -ForegroundColor Yellow
    foreach ($query in $longQueries) {
        $fileName = Split-Path $query.File -Leaf
        Write-Host "  - $fileName ($($query.Count) queries, $([math]::Round($query.Size / 1KB, 0)) KB)" -ForegroundColor White
    }
    
    if (-not $TestMode) {
        Write-Host "" -ForegroundColor White
        Write-Host "[INFO] Ready to process files. Rerun without -TestMode to execute." -ForegroundColor Green
    }
} else {
    Write-Host "[INFO] No long SQL queries found. Templates appear to be optimized." -ForegroundColor Green
}

Write-Host "=========================================" -ForegroundColor Cyan
