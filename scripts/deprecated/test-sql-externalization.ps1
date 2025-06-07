# Test SQL Externalization Script
param(
    [switch]$DryRun = $false
)

Write-Host "=== SQL EXTERNALIZATION TEST ===" -ForegroundColor Magenta
Write-Host "Current directory: $(Get-Location)"
Write-Host "DryRun mode: $DryRun"

$ArmTemplateDirectory = "src\dev\arm_template"
$SqlDirectory = "sql\e2e_queries"

Write-Host "Checking ARM template directory: $ArmTemplateDirectory"
if (Test-Path $ArmTemplateDirectory) {
    Write-Host "✓ ARM template directory found" -ForegroundColor Green
    $armFiles = Get-ChildItem -Path $ArmTemplateDirectory -Filter "*.json" -Recurse
    Write-Host "Found $($armFiles.Count) ARM template files"
    
    foreach ($file in $armFiles) {
        Write-Host "  - $($file.Name)"
    }
}
else {
    Write-Host "✗ ARM template directory not found" -ForegroundColor Red
}

Write-Host "Checking SQL directory: $SqlDirectory"
if (Test-Path $SqlDirectory) {
    Write-Host "✓ SQL directory found" -ForegroundColor Green
    $sqlFiles = Get-ChildItem -Path $SqlDirectory -Filter "*.sql" -Recurse
    Write-Host "Found $($sqlFiles.Count) SQL files"
}
else {
    Write-Host "✗ SQL directory not found" -ForegroundColor Red
}

# Test reading one ARM template file
if ($armFiles.Count -gt 0) {
    $testFile = $armFiles[0]
    Write-Host "Testing file read: $($testFile.Name)"
    try {
        $content = Get-Content -Path $testFile.FullName -Raw
        Write-Host "File size: $($content.Length) characters"
        
        # Look for SQL queries
        $sqlPattern = '"sqlReaderQuery":\s*"([^"]*(?:\\.[^"]*)*)"'
        $matches = [regex]::Matches($content, $sqlPattern)
        Write-Host "Found $($matches.Count) SQL queries in this file"
        
        foreach ($match in $matches) {
            $query = $match.Groups[1].Value
            $cleanQuery = $query -replace '\\n', "`n" -replace '\\t', "`t" -replace '\\"', '"'
            $queryLength = $cleanQuery.Length
            Write-Host "  Query length: $queryLength characters"
        }
    }
    catch {
        Write-Host "Error reading file: $($_.Exception.Message)" -ForegroundColor Red
    }
}

Write-Host "Test completed"
