# Main execution script for optimized SQL externalization
param(
    [ValidateSet("analyze", "replace", "both", "test")]
    [string]$Operation = "both",
    [int]$TimeoutSeconds = 60,
    [int]$BatchSize = 5,
    [switch]$DryRun = $false,
    [switch]$Verbose = $false
)

Write-Host "╔══════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║              OPTIMIZED SQL EXTERNALIZATION SUITE            ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# Configuration
$config = @{
    ArmTemplateDirectory = "src\dev\arm_template"
    SqlDirectory = "sql\e2e_queries" 
    OutputDirectory = "build\arm_template"
    TimeoutSeconds = $TimeoutSeconds
    BatchSize = $BatchSize
    ReportsDirectory = "reports"
}

# Ensure directories exist
foreach ($dir in @($config.SqlDirectory, $config.OutputDirectory, $config.ReportsDirectory)) {
    if (-not (Test-Path $dir)) {
        New-Item -Path $dir -ItemType Directory -Force | Out-Null
        Write-Host "📁 Created directory: $dir" -ForegroundColor Green
    }
}

function Show-SystemInfo {
    Write-Host "🖥️  SYSTEM INFORMATION" -ForegroundColor Yellow
    Write-Host "PowerShell Version: $($PSVersionTable.PSVersion)" -ForegroundColor Gray
    Write-Host "Operating System: $([System.Environment]::OSVersion.VersionString)" -ForegroundColor Gray
    Write-Host "Available Memory: $(Get-WmiObject -Class Win32_OperatingSystem | ForEach-Object {[math]::Round($_.FreePhysicalMemory/1MB, 2)}) GB" -ForegroundColor Gray
    Write-Host "Current Time: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Gray
    Write-Host ""
}

function Test-Prerequisites {
    Write-Host "🔍 CHECKING PREREQUISITES" -ForegroundColor Yellow
    
    $checks = @(
        @{ Name = "ARM Template Directory"; Path = $config.ArmTemplateDirectory },
        @{ Name = "SQL Directory"; Path = $config.SqlDirectory }
    )
    
    $allPassed = $true
    
    foreach ($check in $checks) {
        if (Test-Path $check.Path) {
            $itemCount = (Get-ChildItem -Path $check.Path -Recurse).Count
            Write-Host "[OK] $($check.Name): $($check.Path) ($itemCount items)" -ForegroundColor Green
        } else {
            Write-Host "[ERROR] $($check.Name): $($check.Path) - NOT FOUND" -ForegroundColor Red
            $allPassed = $false
        }
    }
    
    # Check for required scripts
    $requiredScripts = @(
        "optimized-sql-externalization.ps1",
        "optimized-arm-replacement.ps1"
    )
    
    foreach ($script in $requiredScripts) {
        $scriptPath = Join-Path "scripts" $script
        if (Test-Path $scriptPath) {
            Write-Host "✅ Script available: $script" -ForegroundColor Green
        } else {
            Write-Host "❌ Script missing: $script" -ForegroundColor Red
            $allPassed = $false
        }
    }
    
    Write-Host ""
    return $allPassed
}

function Invoke-AnalysisPhase {
    Write-Host "🔬 ANALYSIS PHASE" -ForegroundColor Magenta
    Write-Host "Analyzing SQL queries in ARM templates..." -ForegroundColor Yellow
    
    $analysisParams = @{
        ArmTemplateDirectory = $config.ArmTemplateDirectory
        SqlDirectory = $config.SqlDirectory
        TimeoutSeconds = $config.TimeoutSeconds
        BatchSize = $config.BatchSize
        DryRun = $DryRun
        Verbose = $Verbose
    }
    
    try {
        $scriptPath = Join-Path "scripts" "optimized-sql-externalization.ps1"
        $results = & $scriptPath @analysisParams
        
        # Generate analysis report
        if ($results -and $results.Count -gt 0) {
            $reportPath = Join-Path $config.ReportsDirectory "analysis_report_$(Get-Date -Format 'yyyyMMdd_HHmmss').json"
            $results | ConvertTo-Json -Depth 3 | Out-File -FilePath $reportPath -Encoding UTF8
            Write-Host "📊 Analysis report saved: $reportPath" -ForegroundColor Green
            
            # Summary
            $uniqueMatches = $results | Group-Object SqlFile | Measure-Object
            Write-Host "📈 ANALYSIS SUMMARY:" -ForegroundColor Cyan
            Write-Host "   Total Matches: $($results.Count)" -ForegroundColor White
            Write-Host "   Unique SQL Files: $($uniqueMatches.Count)" -ForegroundColor White
        }
        
        return $results
    }
    catch {
        Write-Error "❌ Analysis phase failed: $($_.Exception.Message)"
        return $null
    }
}

function Invoke-ReplacementPhase {
    Write-Host "🔄 REPLACEMENT PHASE" -ForegroundColor Magenta
    Write-Host "Replacing SQL queries in ARM templates..." -ForegroundColor Yellow
    
    $armFiles = Get-ChildItem -Path $config.ArmTemplateDirectory -Filter "*.json" -Recurse
    $results = @()
    
    foreach ($armFile in $armFiles) {
        Write-Host "Processing: $($armFile.Name)" -ForegroundColor Cyan
        
        $outputFile = Join-Path $config.OutputDirectory $armFile.Name
        
        $replacementParams = @{
            ArmTemplateFile = $armFile.FullName
            SqlDirectory = $config.SqlDirectory
            OutputFile = $outputFile
            TimeoutSeconds = $config.TimeoutSeconds
            DryRun = $DryRun
        }
        
        try {
            $scriptPath = Join-Path "scripts" "optimized-arm-replacement.ps1"
            $success = & $scriptPath @replacementParams
            
            $results += @{
                SourceFile = $armFile.FullName
                OutputFile = $outputFile
                Success = $success
                Timestamp = Get-Date
            }
        }
        catch {
            Write-Error "❌ Failed to process $($armFile.Name): $($_.Exception.Message)"
            $results += @{
                SourceFile = $armFile.FullName
                OutputFile = $outputFile
                Success = $false
                Error = $_.Exception.Message
                Timestamp = Get-Date
            }
        }
    }
    
    # Generate replacement report
    $reportPath = Join-Path $config.ReportsDirectory "replacement_report_$(Get-Date -Format 'yyyyMMdd_HHmmss').json"
    $results | ConvertTo-Json -Depth 3 | Out-File -FilePath $reportPath -Encoding UTF8
    Write-Host "📊 Replacement report saved: $reportPath" -ForegroundColor Green
    
    return $results
}

function Invoke-TestPhase {
    Write-Host "🧪 TEST PHASE" -ForegroundColor Magenta
    Write-Host "Running validation tests..." -ForegroundColor Yellow
    
    # Test 1: JSON validity of output files
    Write-Host "Testing JSON validity..." -ForegroundColor Cyan
    $outputFiles = Get-ChildItem -Path $config.OutputDirectory -Filter "*.json" -ErrorAction SilentlyContinue
    
    foreach ($file in $outputFiles) {
        try {
            $content = Get-Content -Path $file.FullName -Raw
            $null = ConvertFrom-Json $content
            Write-Host "✅ Valid JSON: $($file.Name)" -ForegroundColor Green
        }
        catch {
            Write-Host "❌ Invalid JSON: $($file.Name) - $($_.Exception.Message)" -ForegroundColor Red
        }
    }
    
    # Test 2: SQL file references
    Write-Host "Testing SQL file references..." -ForegroundColor Cyan
    $sqlFiles = Get-ChildItem -Path $config.SqlDirectory -Filter "*.sql" -ErrorAction SilentlyContinue
    Write-Host "Found $($sqlFiles.Count) SQL files" -ForegroundColor Blue
    
    # Test 3: Performance metrics
    Write-Host "Performance test summary..." -ForegroundColor Cyan
    $reportFiles = Get-ChildItem -Path $config.ReportsDirectory -Filter "*_report_*.json" -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending
    
    if ($reportFiles) {
        $latestReport = $reportFiles[0]
        Write-Host "Latest report: $($latestReport.Name)" -ForegroundColor Blue
        Write-Host "Report size: $([math]::Round($latestReport.Length/1KB, 2)) KB" -ForegroundColor Blue
    }
}

# Main execution flow
function Start-OptimizedExecution {
    $startTime = Get-Date
    
    Show-SystemInfo
    
    if (-not (Test-Prerequisites)) {
        Write-Error "❌ Prerequisites check failed. Please fix the issues and try again."
        return
    }
    
    Write-Host "🚀 STARTING OPTIMIZED SQL EXTERNALIZATION" -ForegroundColor Green
    Write-Host "Operation: $Operation" -ForegroundColor Yellow
    Write-Host "Timeout: $($config.TimeoutSeconds) seconds" -ForegroundColor Yellow
    Write-Host "Batch Size: $($config.BatchSize)" -ForegroundColor Yellow
    Write-Host "Dry Run: $DryRun" -ForegroundColor Yellow
    Write-Host ""
    
    try {
        switch ($Operation) {
            "analyze" {
                $analysisResults = Invoke-AnalysisPhase
            }
            "replace" {
                $replacementResults = Invoke-ReplacementPhase
            }
            "both" {
                $analysisResults = Invoke-AnalysisPhase
                if ($analysisResults) {
                    Write-Host ""
                    $replacementResults = Invoke-ReplacementPhase
                }
            }
            "test" {
                Invoke-TestPhase
            }
        }
        
        $endTime = Get-Date
        $totalTime = $endTime - $startTime
        
        Write-Host ""
        Write-Host "╔══════════════════════════════════════════════════════════════╗" -ForegroundColor Green
        Write-Host "║                    EXECUTION COMPLETED                      ║" -ForegroundColor Green
        Write-Host "╚══════════════════════════════════════════════════════════════╝" -ForegroundColor Green
        Write-Host "Total Execution Time: $($totalTime.ToString('mm\:ss'))" -ForegroundColor Cyan
        Write-Host "Reports saved in: $($config.ReportsDirectory)" -ForegroundColor Blue
        
        if (-not $DryRun) {
            Write-Host "Output files saved in: $($config.OutputDirectory)" -ForegroundColor Blue
        }
    }
    catch {
        Write-Error "❌ Execution failed: $($_.Exception.Message)"
        Write-Error $_.Exception.StackTrace
    }
}

# Execute
Start-OptimizedExecution
