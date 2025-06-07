# Optimized ARM Template SQL Replacement Script
param(
    [string]$ArmTemplateFile = "src\dev\arm_template\ARMTemplateForFactory.json",
    [string]$SqlDirectory = "sql\e2e_queries",
    [string]$OutputFile = "build\arm_template\ARMTemplateForFactory_optimized.json",
    [int]$TimeoutSeconds = 60,
    [int]$MaxReplacementLength = 100000,
    [switch]$DryRun = $false,
    [switch]$Backup = $true
)

Write-Host "=== OPTIMIZED ARM TEMPLATE SQL REPLACEMENT ===" -ForegroundColor Magenta
Write-Host "ARM Template: $ArmTemplateFile" -ForegroundColor Yellow
Write-Host "SQL Directory: $SqlDirectory" -ForegroundColor Yellow
Write-Host "Output File: $OutputFile" -ForegroundColor Yellow
Write-Host ""

# Performance tracking
$Script:ReplacementStats = @{
    TotalReplacements = 0
    SuccessfulReplacements = 0
    FailedReplacements = 0
    TimeoutReplacements = 0
    StartTime = Get-Date
}

function Test-JsonValidity {
    param([string]$JsonContent)
    
    try {
        $null = ConvertFrom-Json $JsonContent -ErrorAction Stop
        return $true
    }
    catch {
        return $false
    }
}

function Get-SqlFileReference {
    param(
        [string]$Query,
        [string]$SqlDirectory
    )
    
    # Quick hash-based matching for performance
    $queryHash = [System.Security.Cryptography.SHA256]::Create().ComputeHash([System.Text.Encoding]::UTF8.GetBytes($Query))
    $queryHashString = [System.Convert]::ToBase64String($queryHash).Substring(0, 8)
    
    # Look for existing SQL files
    $sqlFiles = Get-ChildItem -Path $SqlDirectory -Filter "*.sql"
    
    foreach ($sqlFile in $sqlFiles) {
        try {
            $fileContent = Get-Content -Path $sqlFile.FullName -Raw -Encoding UTF8
            $fileHash = [System.Security.Cryptography.SHA256]::Create().ComputeHash([System.Text.Encoding]::UTF8.GetBytes($fileContent))
            $fileHashString = [System.Convert]::ToBase64String($fileHash).Substring(0, 8)
            
            # Check for exact match
            if ($queryHashString -eq $fileHashString) {
                return $sqlFile.Name
            }
            
            # Check for content similarity (quick check)
            $normalizedQuery = $Query -replace '\s+', ' ' | Out-String
            $normalizedFile = $fileContent -replace '\s+', ' ' | Out-String
            
            if ($normalizedQuery.Trim() -eq $normalizedFile.Trim()) {
                return $sqlFile.Name
            }
        }
        catch {
            # Skip problematic files
            continue
        }
    }
    
    # Create new file if no match found
    $fileName = "query_$queryHashString.sql"
    $filePath = Join-Path $SqlDirectory $fileName
    
    try {
        if (-not $DryRun) {
            $Query | Out-File -FilePath $filePath -Encoding UTF8
            Write-Host "  ➕ Created new SQL file: $fileName" -ForegroundColor Green
        }
        return $fileName
    }
    catch {
        Write-Warning "⚠️ Could not create SQL file: $fileName"
        return $null
    }
}

function Optimize-ArmTemplateReplacement {
    param([string]$ArmTemplateFile, [string]$SqlDirectory, [string]$OutputFile)
    
    try {
        # Validate input file
        if (-not (Test-Path $ArmTemplateFile)) {
            Write-Error "❌ ARM template file not found: $ArmTemplateFile"
            return $false
        }
        
        # Create output directory if needed
        $outputDir = Split-Path $OutputFile -Parent
        if ($outputDir -and -not (Test-Path $outputDir)) {
            New-Item -Path $outputDir -ItemType Directory -Force | Out-Null
        }
        
        # Create SQL directory if needed
        if (-not (Test-Path $SqlDirectory)) {
            New-Item -Path $SqlDirectory -ItemType Directory -Force | Out-Null
        }
        
        Write-Host "🔄 Loading ARM template..." -ForegroundColor Yellow
        
        # Read ARM template with timeout protection
        $armContent = $null
        $loadJob = Start-Job -ScriptBlock {
            param($FilePath)
            Get-Content -Path $FilePath -Raw -Encoding UTF8
        } -ArgumentList $ArmTemplateFile
        
        if (Wait-Job -Job $loadJob -Timeout $TimeoutSeconds) {
            $armContent = Receive-Job -Job $loadJob
            Remove-Job -Job $loadJob
        } else {
            Stop-Job -Job $loadJob
            Remove-Job -Job $loadJob
            Write-Error "❌ Timeout loading ARM template"
            return $false
        }
        
        if (-not $armContent) {
            Write-Error "❌ Could not load ARM template content"
            return $false
        }
        
        Write-Host "✅ ARM template loaded successfully" -ForegroundColor Green
        
        # Backup original if requested
        if ($Backup -and -not $DryRun) {
            $backupFile = "$ArmTemplateFile.backup_$(Get-Date -Format 'yyyyMMdd_HHmmss')"
            Copy-Item -Path $ArmTemplateFile -Destination $backupFile
            Write-Host "💾 Backup created: $backupFile" -ForegroundColor Cyan
        }
        
        # Process SQL replacements in chunks to handle large files
        Write-Host "🔍 Identifying SQL queries for replacement..." -ForegroundColor Yellow
        
        $sqlPattern = '"sqlReaderQuery":\s*"([^"]*(?:\\.[^"]*)*)"'
        $matches = [regex]::Matches($armContent, $sqlPattern)
        
        Write-Host "📊 Found $($matches.Count) SQL queries to process" -ForegroundColor Cyan
        
        if ($matches.Count -eq 0) {
            Write-Host "ℹ️ No SQL queries found in ARM template" -ForegroundColor Blue
            return $false
        }
        
        $modifiedContent = $armContent
        $processedQueries = 0
        
        # Process matches in reverse order to maintain string positions
        $reversedMatches = $matches | Sort-Object { $_.Index } -Descending
        
        foreach ($match in $reversedMatches) {
            $Script:ReplacementStats.TotalReplacements++
            $processedQueries++
            
            try {
                $rawQuery = $match.Groups[1].Value
                $cleanQuery = $rawQuery -replace '\\n', "`n" -replace '\\t', "`t" -replace '\\"', '"' -replace '\\\\', '\'
                
                # Skip very long queries to avoid timeout
                if ($cleanQuery.Length -gt $MaxReplacementLength) {
                    Write-Host "  ⚠️ Skipping very long query ($($cleanQuery.Length) chars) - Query #$processedQueries" -ForegroundColor Yellow
                    $Script:ReplacementStats.TimeoutReplacements++
                    continue
                }
                
                # Skip very short queries
                if ($cleanQuery.Length -lt 50) {
                    Write-Host "  ⚠️ Skipping short query ($($cleanQuery.Length) chars) - Query #$processedQueries" -ForegroundColor Yellow
                    continue
                }
                
                Write-Host "  🔄 Processing query #$processedQueries (length: $($cleanQuery.Length))..." -ForegroundColor Cyan
                
                # Get or create SQL file reference with timeout
                $sqlFileJob = Start-Job -ScriptBlock {
                    param($Query, $SqlDir)
                    # Include the function definition in the job
                    function Get-SqlFileReference {
                        param([string]$Query, [string]$SqlDirectory)
                        
                        $queryHash = [System.Security.Cryptography.SHA256]::Create().ComputeHash([System.Text.Encoding]::UTF8.GetBytes($Query))
                        $queryHashString = [System.Convert]::ToBase64String($queryHash).Substring(0, 8)
                        
                        $sqlFiles = Get-ChildItem -Path $SqlDirectory -Filter "*.sql" -ErrorAction SilentlyContinue
                        
                        foreach ($sqlFile in $sqlFiles) {
                            try {
                                $fileContent = Get-Content -Path $sqlFile.FullName -Raw -Encoding UTF8
                                $normalizedQuery = $Query -replace '\s+', ' '
                                $normalizedFile = $fileContent -replace '\s+', ' '
                                
                                if ($normalizedQuery.Trim() -eq $normalizedFile.Trim()) {
                                    return $sqlFile.Name
                                }
                            }
                            catch { continue }
                        }
                        
                        $fileName = "query_$queryHashString.sql"
                        $filePath = Join-Path $SqlDirectory $fileName
                        
                        try {
                            $Query | Out-File -FilePath $filePath -Encoding UTF8
                            return $fileName
                        }
                        catch {
                            return $null
                        }
                    }
                    
                    return Get-SqlFileReference -Query $Query -SqlDirectory $SqlDir
                } -ArgumentList $cleanQuery, $SqlDirectory
                
                $sqlFileName = $null
                if (Wait-Job -Job $sqlFileJob -Timeout ($TimeoutSeconds / 4)) {
                    $sqlFileName = Receive-Job -Job $sqlFileJob
                    Remove-Job -Job $sqlFileJob
                } else {
                    Stop-Job -Job $sqlFileJob
                    Remove-Job -Job $sqlFileJob
                    Write-Host "    ⏱️ Timeout processing query #$processedQueries" -ForegroundColor Red
                    $Script:ReplacementStats.TimeoutReplacements++
                    continue
                }
                
                if ($sqlFileName) {
                    # Create replacement string
                    $replacement = '"sqlReaderQuery": "[loadTextContent(''../sql/e2e_queries/' + $sqlFileName + ''')]"'
                    
                    # Replace in content
                    $originalMatch = $match.Value
                    $modifiedContent = $modifiedContent.Remove($match.Index, $match.Length).Insert($match.Index, $replacement)
                    
                    Write-Host "    ✅ Replaced with reference to: $sqlFileName" -ForegroundColor Green
                    $Script:ReplacementStats.SuccessfulReplacements++
                } else {
                    Write-Host "    ❌ Failed to create SQL file reference" -ForegroundColor Red
                    $Script:ReplacementStats.FailedReplacements++
                }
            }
            catch {
                Write-Host "    ❌ Error processing query #$processedQueries : $($_.Exception.Message)" -ForegroundColor Red
                $Script:ReplacementStats.FailedReplacements++
            }
            
            # Progress update
            if ($processedQueries % 5 -eq 0) {
                $progress = ($processedQueries / $matches.Count) * 100
                $elapsed = (Get-Date) - $Script:ReplacementStats.StartTime
                Write-Host "    📈 Progress: $processedQueries/$($matches.Count) ($($progress.ToString('F1'))%) - Elapsed: $($elapsed.ToString('mm\:ss'))" -ForegroundColor Blue
            }
        }
        
        # Validate JSON before saving
        Write-Host "🔍 Validating modified JSON..." -ForegroundColor Yellow
        if (-not (Test-JsonValidity -JsonContent $modifiedContent)) {
            Write-Error "❌ Modified ARM template contains invalid JSON"
            return $false
        }
        
        # Save modified content
        if (-not $DryRun) {
            Write-Host "💾 Saving optimized ARM template..." -ForegroundColor Yellow
            $modifiedContent | Out-File -FilePath $OutputFile -Encoding UTF8
            Write-Host "✅ Optimized ARM template saved to: $OutputFile" -ForegroundColor Green
        } else {
            Write-Host "🔍 Dry run completed - no files modified" -ForegroundColor Blue
        }
        
        return $true
    }
    catch {
        Write-Error "❌ Fatal error in ARM template replacement: $($_.Exception.Message)"
        return $false
    }
}

# Execute optimization
Write-Host "🚀 Starting ARM template optimization..." -ForegroundColor Green
$success = Optimize-ArmTemplateReplacement -ArmTemplateFile $ArmTemplateFile -SqlDirectory $SqlDirectory -OutputFile $OutputFile

# Summary report
$elapsed = (Get-Date) - $Script:ReplacementStats.StartTime
Write-Host ""
Write-Host "=== REPLACEMENT SUMMARY ===" -ForegroundColor Magenta
Write-Host "Total Replacements Attempted: $($Script:ReplacementStats.TotalReplacements)" -ForegroundColor White
Write-Host "Successful Replacements: $($Script:ReplacementStats.SuccessfulReplacements)" -ForegroundColor Green
Write-Host "Failed Replacements: $($Script:ReplacementStats.FailedReplacements)" -ForegroundColor Red
Write-Host "Timeout Replacements: $($Script:ReplacementStats.TimeoutReplacements)" -ForegroundColor Yellow
Write-Host "Total Processing Time: $($elapsed.ToString('mm\:ss'))" -ForegroundColor Cyan

if ($Script:ReplacementStats.TotalReplacements -gt 0) {
    $successRate = ($Script:ReplacementStats.SuccessfulReplacements / $Script:ReplacementStats.TotalReplacements) * 100
    Write-Host "Success Rate: $($successRate.ToString('F1'))%" -ForegroundColor Blue
}

if ($success) {
    Write-Host "🎯 ARM template optimization completed successfully!" -ForegroundColor Green
} else {
    Write-Host "⚠️ ARM template optimization completed with issues" -ForegroundColor Yellow
}
