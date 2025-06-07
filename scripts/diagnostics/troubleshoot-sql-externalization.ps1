# Troubleshooting script for SQL externalization issues
param(
    [ValidateSet("full", "quick", "memory", "performance", "validation")]
    [string]$DiagnosticLevel = "full",
    [switch]$FixIssues = $false,
    [switch]$GenerateReport = $true
)

Write-Host "╔═══════════════════════════════════════════════════════════════════╗" -ForegroundColor Red
Write-Host "║                 SQL EXTERNALIZATION TROUBLESHOOTER               ║" -ForegroundColor Red
Write-Host "╚═══════════════════════════════════════════════════════════════════╝" -ForegroundColor Red
Write-Host ""

$Script:DiagnosticResults = @{
    SystemInfo         = @{}
    FileSystemChecks   = @{}
    PerformanceMetrics = @{}
    ValidationResults  = @{}
    Issues             = @()
    Recommendations    = @()
    StartTime          = Get-Date
}

function Test-SystemResources {
    Write-Host "🖥️  SYSTEM RESOURCE ANALYSIS" -ForegroundColor Yellow
    
    try {
        # Memory analysis
        $os = Get-WmiObject -Class Win32_OperatingSystem
        $totalMemoryGB = [math]::Round($os.TotalVisibleMemorySize / 1MB, 2)
        $freeMemoryGB = [math]::Round($os.FreePhysicalMemory / 1MB, 2)
        $memoryUsagePercent = [math]::Round((($totalMemoryGB - $freeMemoryGB) / $totalMemoryGB) * 100, 1)
        
        $Script:DiagnosticResults.SystemInfo.TotalMemoryGB = $totalMemoryGB
        $Script:DiagnosticResults.SystemInfo.FreeMemoryGB = $freeMemoryGB
        $Script:DiagnosticResults.SystemInfo.MemoryUsagePercent = $memoryUsagePercent
        
        Write-Host "  Memory: $freeMemoryGB GB free / $totalMemoryGB GB total ($memoryUsagePercent% used)" -ForegroundColor Cyan
        
        if ($memoryUsagePercent -gt 80) {
            $Script:DiagnosticResults.Issues += "High memory usage ($memoryUsagePercent%) may cause timeouts"
            $Script:DiagnosticResults.Recommendations += "Close unnecessary applications or increase batch size limits"
        }
        
        # CPU analysis
        $cpu = Get-WmiObject -Class Win32_Processor
        $cpuUsage = (Get-Counter "\Processor(_Total)\% Processor Time" -SampleInterval 1 -MaxSamples 3 | 
            Select-Object -ExpandProperty CounterSamples | 
            Measure-Object -Property CookedValue -Average).Average
        
        $Script:DiagnosticResults.SystemInfo.CPUUsagePercent = [math]::Round($cpuUsage, 1)
        Write-Host "  CPU: $([math]::Round($cpuUsage, 1))% average usage" -ForegroundColor Cyan
        
        # Disk space analysis
        $drives = Get-WmiObject -Class Win32_LogicalDisk | Where-Object { $_.DriveType -eq 3 }
        foreach ($drive in $drives) {
            $freeSpaceGB = [math]::Round($drive.FreeSpace / 1GB, 2)
            $totalSpaceGB = [math]::Round($drive.Size / 1GB, 2)
            $freePercent = [math]::Round(($drive.FreeSpace / $drive.Size) * 100, 1)
            Write-Host "  Disk $($drive.DeviceID): $freeSpaceGB GB free / $totalSpaceGB GB total ($freePercent% free)" -ForegroundColor Cyan
            
            if ($freePercent -lt 10) {
                $Script:DiagnosticResults.Issues += "Low disk space on $($drive.DeviceID) ($freePercent% free)"
                $Script:DiagnosticResults.Recommendations += "Free up disk space on $($drive.DeviceID)"
            }
        }
        
        Write-Host "✅ System resource analysis completed" -ForegroundColor Green
    }
    catch {
        Write-Warning "⚠️ Could not complete system resource analysis: $($_.Exception.Message)"
        $Script:DiagnosticResults.Issues += "System resource analysis failed"
    }
    
    Write-Host ""
}

function Test-FileSystemIntegrity {
    Write-Host "📁 FILE SYSTEM INTEGRITY CHECK" -ForegroundColor Yellow
    
    $directories = @(
        @{ Name = "ARM Templates"; Path = "src\dev\arm_template" },
        @{ Name = "SQL Directory"; Path = "sql\e2e_queries" },
        @{ Name = "Scripts Directory"; Path = "scripts" },
        @{ Name = "Build Directory"; Path = "build\arm_template" }
    )
    
    foreach ($dir in $directories) {
        try {
            if (Test-Path $dir.Path) {
                $items = Get-ChildItem -Path $dir.Path -Recurse -ErrorAction SilentlyContinue
                $totalSize = ($items | Where-Object { -not $_.PSIsContainer } | Measure-Object -Property Length -Sum).Sum
                $totalSizeMB = [math]::Round($totalSize / 1MB, 2)
                
                $Script:DiagnosticResults.FileSystemChecks[$dir.Name] = @{
                    Exists      = $true
                    ItemCount   = $items.Count
                    TotalSizeMB = $totalSizeMB
                }
                
                Write-Host "  ✅ $($dir.Name): $($items.Count) items, $totalSizeMB MB" -ForegroundColor Green
                
                # Check for very large files that might cause issues
                $largeFiles = $items | Where-Object { $_.Length -gt 50MB }
                if ($largeFiles) {
                    foreach ($file in $largeFiles) {
                        $sizeMB = [math]::Round($file.Length / 1MB, 2)
                        Write-Host "    ⚠️ Large file detected: $($file.Name) ($sizeMB MB)" -ForegroundColor Yellow
                        $Script:DiagnosticResults.Issues += "Large file may cause processing issues: $($file.Name) ($sizeMB MB)"
                    }
                }
            }
            else {
                $Script:DiagnosticResults.FileSystemChecks[$dir.Name] = @{ Exists = $false }
                Write-Host "  ❌ $($dir.Name): Directory not found - $($dir.Path)" -ForegroundColor Red
                $Script:DiagnosticResults.Issues += "$($dir.Name) directory not found: $($dir.Path)"
                
                if ($FixIssues) {
                    try {
                        New-Item -Path $dir.Path -ItemType Directory -Force | Out-Null
                        Write-Host "    🔧 Created missing directory: $($dir.Path)" -ForegroundColor Cyan
                    }
                    catch {
                        Write-Warning "    ❌ Could not create directory: $($_.Exception.Message)"
                    }
                }
            }
        }
        catch {
            Write-Warning "  ⚠️ Error checking $($dir.Name): $($_.Exception.Message)"
            $Script:DiagnosticResults.Issues += "Error checking $($dir.Name): $($_.Exception.Message)"
        }
    }
    
    Write-Host ""
}

function Test-FileContent {
    Write-Host "📄 FILE CONTENT VALIDATION" -ForegroundColor Yellow
    
    # Check ARM template validity
    $armFiles = Get-ChildItem -Path "src\dev\arm_template" -Filter "*.json" -ErrorAction SilentlyContinue
    foreach ($armFile in $armFiles) {
        try {
            Write-Host "  Validating: $($armFile.Name)" -ForegroundColor Cyan
            
            $content = Get-Content -Path $armFile.FullName -Raw -Encoding UTF8
            $contentSizeMB = [math]::Round($content.Length / 1MB, 2)
            
            # JSON validation
            try {
                $jsonObject = ConvertFrom-Json $content
                Write-Host "    ✅ Valid JSON ($contentSizeMB MB)" -ForegroundColor Green
                
                # Count SQL queries
                $sqlMatches = [regex]::Matches($content, '"sqlReaderQuery":\s*"([^"]*(?:\\.[^"]*)*)"')
                Write-Host "    📊 SQL queries found: $($sqlMatches.Count)" -ForegroundColor Blue
                
                # Check for very long queries
                foreach ($match in $sqlMatches) {
                    $queryLength = $match.Groups[1].Value.Length
                    if ($queryLength -gt 10000) {
                        Write-Host "    ⚠️ Very long SQL query detected: $queryLength characters" -ForegroundColor Yellow
                        $Script:DiagnosticResults.Issues += "Very long SQL query in $($armFile.Name): $queryLength characters"
                    }
                }
                
                $Script:DiagnosticResults.ValidationResults[$armFile.Name] = @{
                    IsValidJson   = $true
                    SizeMB        = $contentSizeMB
                    SqlQueryCount = $sqlMatches.Count
                }
            }
            catch {
                Write-Host "    ❌ Invalid JSON: $($_.Exception.Message)" -ForegroundColor Red
                $Script:DiagnosticResults.Issues += "Invalid JSON in $($armFile.Name): $($_.Exception.Message)"
                $Script:DiagnosticResults.ValidationResults[$armFile.Name] = @{
                    IsValidJson = $false
                    SizeMB      = $contentSizeMB
                    Error       = $_.Exception.Message
                }
            }
        }
        catch {
            Write-Warning "  ⚠️ Could not read $($armFile.Name): $($_.Exception.Message)"
            $Script:DiagnosticResults.Issues += "Could not read $($armFile.Name): $($_.Exception.Message)"
        }
    }
    
    # Check SQL files
    $sqlFiles = Get-ChildItem -Path "sql\e2e_queries" -Filter "*.sql" -ErrorAction SilentlyContinue
    if ($sqlFiles) {
        Write-Host "  📊 SQL files found: $($sqlFiles.Count)" -ForegroundColor Blue
        
        $totalSqlSize = 0
        foreach ($sqlFile in $sqlFiles) {
            try {
                $content = Get-Content -Path $sqlFile.FullName -Raw -Encoding UTF8 -ErrorAction SilentlyContinue
                if ($content) {
                    $totalSqlSize += $content.Length
                }
            }
            catch {
                $Script:DiagnosticResults.Issues += "Could not read SQL file: $($sqlFile.Name)"
            }
        }
        
        $totalSqlSizeMB = [math]::Round($totalSqlSize / 1MB, 2)
        Write-Host "  📊 Total SQL content: $totalSqlSizeMB MB" -ForegroundColor Blue
    }
    
    Write-Host ""
}

function Test-PerformanceScenarios {
    Write-Host "⚡ PERFORMANCE SCENARIO TESTING" -ForegroundColor Yellow
    
    # Test JSON parsing performance
    Write-Host "  Testing JSON parsing performance..." -ForegroundColor Cyan
    $armFile = Get-ChildItem -Path "src\dev\arm_template" -Filter "*.json" | Select-Object -First 1
    
    if ($armFile) {
        $parseTests = @()
        
        for ($i = 1; $i -le 3; $i++) {
            $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
            try {
                $content = Get-Content -Path $armFile.FullName -Raw -Encoding UTF8
                $jsonObject = ConvertFrom-Json $content
                $stopwatch.Stop()
                $parseTests += $stopwatch.ElapsedMilliseconds
                Write-Host "    Test $i : $($stopwatch.ElapsedMilliseconds) ms" -ForegroundColor Blue
            }
            catch {
                $stopwatch.Stop()
                Write-Host "    Test $i : Failed - $($_.Exception.Message)" -ForegroundColor Red
                $Script:DiagnosticResults.Issues += "JSON parsing test failed: $($_.Exception.Message)"
            }
        }
        
        if ($parseTests.Count -gt 0) {
            $avgParseTime = ($parseTests | Measure-Object -Average).Average
            $Script:DiagnosticResults.PerformanceMetrics.AverageJsonParseTimeMs = $avgParseTime
            Write-Host "    Average JSON parse time: $([math]::Round($avgParseTime, 2)) ms" -ForegroundColor Green
            
            if ($avgParseTime -gt 5000) {
                $Script:DiagnosticResults.Issues += "Slow JSON parsing detected: $([math]::Round($avgParseTime, 2)) ms average"
                $Script:DiagnosticResults.Recommendations += "Consider splitting large ARM templates or increasing timeout values"
            }
        }
    }
    
    # Test regex performance
    Write-Host "  Testing regex performance..." -ForegroundColor Cyan
    if ($armFile) {
        $regexTests = @()
        
        for ($i = 1; $i -le 3; $i++) {
            $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
            try {
                $content = Get-Content -Path $armFile.FullName -Raw -Encoding UTF8
                $matches = [regex]::Matches($content, '"sqlReaderQuery":\s*"([^"]*(?:\\.[^"]*)*)"')
                $stopwatch.Stop()
                $regexTests += $stopwatch.ElapsedMilliseconds
                Write-Host "    Test $i : $($stopwatch.ElapsedMilliseconds) ms (found $($matches.Count) matches)" -ForegroundColor Blue
            }
            catch {
                $stopwatch.Stop()
                Write-Host "    Test $i : Failed - $($_.Exception.Message)" -ForegroundColor Red
            }
        }
        
        if ($regexTests.Count -gt 0) {
            $avgRegexTime = ($regexTests | Measure-Object -Average).Average
            $Script:DiagnosticResults.PerformanceMetrics.AverageRegexTimeMs = $avgRegexTime
            Write-Host "    Average regex time: $([math]::Round($avgRegexTime, 2)) ms" -ForegroundColor Green
            
            if ($avgRegexTime -gt 2000) {
                $Script:DiagnosticResults.Issues += "Slow regex processing detected: $([math]::Round($avgRegexTime, 2)) ms average"
                $Script:DiagnosticResults.Recommendations += "Consider processing files in smaller chunks"
            }
        }
    }
    
    Write-Host ""
}

function Generate-RecommendedSolutions {
    Write-Host "💡 GENERATING RECOMMENDED SOLUTIONS" -ForegroundColor Yellow
    
    # Analyze issues and provide specific recommendations
    $issueCategories = @{
        Memory        = @()
        Performance   = @()
        FileSystem    = @()
        Configuration = @()
    }
    
    foreach ($issue in $Script:DiagnosticResults.Issues) {
        if ($issue -match "memory|Memory") {
            $issueCategories.Memory += $issue
        }
        elseif ($issue -match "slow|Slow|timeout|Timeout|performance") {
            $issueCategories.Performance += $issue
        }
        elseif ($issue -match "file|File|directory|Directory") {
            $issueCategories.FileSystem += $issue
        }
        else {
            $issueCategories.Configuration += $issue
        }
    }
    
    # Memory-related recommendations
    if ($issueCategories.Memory.Count -gt 0) {
        Write-Host "  🧠 Memory Optimization Recommendations:" -ForegroundColor Cyan
        Write-Host "    • Reduce batch size to 3-5 queries per batch" -ForegroundColor White
        Write-Host "    • Increase timeout values to 60-120 seconds" -ForegroundColor White
        Write-Host "    • Close unnecessary applications before processing" -ForegroundColor White
        $Script:DiagnosticResults.Recommendations += "Apply memory optimization settings"
    }
    
    # Performance-related recommendations
    if ($issueCategories.Performance.Count -gt 0) {
        Write-Host "  ⚡ Performance Optimization Recommendations:" -ForegroundColor Cyan
        Write-Host "    • Use optimized scripts instead of enhanced versions" -ForegroundColor White
        Write-Host "    • Process files individually rather than in bulk" -ForegroundColor White
        Write-Host "    • Consider splitting very large ARM templates" -ForegroundColor White
        $Script:DiagnosticResults.Recommendations += "Apply performance optimization settings"
    }
    
    # File system recommendations
    if ($issueCategories.FileSystem.Count -gt 0) {
        Write-Host "  📁 File System Recommendations:" -ForegroundColor Cyan
        Write-Host "    • Ensure all required directories exist" -ForegroundColor White
        Write-Host "    • Validate file permissions for read/write access" -ForegroundColor White
        Write-Host "    • Clean up temporary and backup files" -ForegroundColor White
        $Script:DiagnosticResults.Recommendations += "Fix file system issues"
    }
    
    Write-Host ""
}

function Export-DiagnosticReport {
    if (-not $GenerateReport) { return }
    
    Write-Host "📋 GENERATING DIAGNOSTIC REPORT" -ForegroundColor Yellow
    
    $endTime = Get-Date
    $Script:DiagnosticResults.EndTime = $endTime
    $Script:DiagnosticResults.TotalDurationMinutes = ($endTime - $Script:DiagnosticResults.StartTime).TotalMinutes
    
    # Create reports directory if it doesn't exist
    $reportsDir = "reports"
    if (-not (Test-Path $reportsDir)) {
        New-Item -Path $reportsDir -ItemType Directory -Force | Out-Null
    }
    
    # Generate comprehensive report
    $reportPath = Join-Path $reportsDir "diagnostic_report_$(Get-Date -Format 'yyyyMMdd_HHmmss').json"
    $Script:DiagnosticResults | ConvertTo-Json -Depth 4 | Out-File -FilePath $reportPath -Encoding UTF8
    
    # Generate summary report
    $summaryPath = Join-Path $reportsDir "diagnostic_summary_$(Get-Date -Format 'yyyyMMdd_HHmmss').txt"
    $summary = @"
SQL EXTERNALIZATION DIAGNOSTIC SUMMARY
Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')
Diagnostic Level: $DiagnosticLevel

ISSUES FOUND: $($Script:DiagnosticResults.Issues.Count)
$($Script:DiagnosticResults.Issues | ForEach-Object { "• $_" } | Out-String)

RECOMMENDATIONS: $($Script:DiagnosticResults.Recommendations.Count)
$($Script:DiagnosticResults.Recommendations | ForEach-Object { "• $_" } | Out-String)

SYSTEM INFO:
• Memory Usage: $($Script:DiagnosticResults.SystemInfo.MemoryUsagePercent)%
• CPU Usage: $($Script:DiagnosticResults.SystemInfo.CPUUsagePercent)%

PERFORMANCE METRICS:
• Average JSON Parse Time: $($Script:DiagnosticResults.PerformanceMetrics.AverageJsonParseTimeMs) ms
• Average Regex Time: $($Script:DiagnosticResults.PerformanceMetrics.AverageRegexTimeMs) ms

Total Diagnostic Time: $([math]::Round($Script:DiagnosticResults.TotalDurationMinutes, 2)) minutes
"@
    
    $summary | Out-File -FilePath $summaryPath -Encoding UTF8
    
    Write-Host "  📄 Detailed report: $reportPath" -ForegroundColor Green
    Write-Host "  📄 Summary report: $summaryPath" -ForegroundColor Green
    Write-Host ""
}

# Main execution
function Start-Troubleshooting {
    Write-Host "🔍 Starting diagnostic level: $DiagnosticLevel" -ForegroundColor Green
    Write-Host "Fix issues automatically: $FixIssues" -ForegroundColor Yellow
    Write-Host ""
    
    switch ($DiagnosticLevel) {
        "quick" {
            Test-FileSystemIntegrity
        }
        "memory" {
            Test-SystemResources
        }
        "performance" {
            Test-PerformanceScenarios
        }
        "validation" {
            Test-FileContent
        }
        "full" {
            Test-SystemResources
            Test-FileSystemIntegrity
            Test-FileContent
            Test-PerformanceScenarios
        }
    }
    
    Generate-RecommendedSolutions
    Export-DiagnosticReport
    
    $duration = (Get-Date) - $Script:DiagnosticResults.StartTime
    Write-Host "╔═══════════════════════════════════════════════════════════════════╗" -ForegroundColor Green
    Write-Host "║                     DIAGNOSTIC COMPLETED                         ║" -ForegroundColor Green
    Write-Host "╚═══════════════════════════════════════════════════════════════════╝" -ForegroundColor Green
    Write-Host "Issues Found: $($Script:DiagnosticResults.Issues.Count)" -ForegroundColor $(if ($Script:DiagnosticResults.Issues.Count -eq 0) { "Green" } else { "Red" })
    Write-Host "Recommendations: $($Script:DiagnosticResults.Recommendations.Count)" -ForegroundColor Blue
    Write-Host "Duration: $($duration.ToString('mm\:ss'))" -ForegroundColor Cyan
}

# Execute
Start-Troubleshooting
