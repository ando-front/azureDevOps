# Optimized SQL Externalization Script with Timeout Handling and Batch Processing
param(
    [string]$ArmTemplateDirectory = "src\dev\arm_template",
    [string]$SqlDirectory = "sql\e2e_queries", 
    [string]$OutputDirectory = "build\arm_template",
    [int]$TimeoutSeconds = 30,
    [int]$BatchSize = 10,
    [int]$MaxQueryLength = 50000,
    [switch]$DryRun = $false,
    [switch]$Verbose = $false
)

Write-Host "=== OPTIMIZED SQL EXTERNALIZATION PROCESSOR ===" -ForegroundColor Magenta
Write-Host "ARM Template Directory: $ArmTemplateDirectory" -ForegroundColor Yellow
Write-Host "SQL Directory: $SqlDirectory" -ForegroundColor Yellow
Write-Host "Timeout: $TimeoutSeconds seconds" -ForegroundColor Yellow
Write-Host "Batch Size: $BatchSize" -ForegroundColor Yellow
Write-Host "Max Query Length: $MaxQueryLength chars" -ForegroundColor Yellow
Write-Host ""

# Performance metrics
$Script:ProcessingStats = @{
    TotalQueries     = 0
    ProcessedQueries = 0
    MatchedQueries   = 0
    TimeoutQueries   = 0
    SkippedQueries   = 0
    StartTime        = Get-Date
}

# Function to measure execution time with timeout
function Invoke-WithTimeout {
    param(
        [scriptblock]$ScriptBlock,
        [int]$TimeoutSeconds = 30,
        [string]$OperationName = "Operation"
    )
    
    $job = Start-Job -ScriptBlock $ScriptBlock
    $completed = Wait-Job -Job $job -Timeout $TimeoutSeconds
    
    if ($completed) {
        $result = Receive-Job -Job $job
        Remove-Job -Job $job
        return $result
    }
    else {
        Write-Warning "⚠️ Timeout: $OperationName exceeded $TimeoutSeconds seconds"
        Stop-Job -Job $job
        Remove-Job -Job $job
        return $null
    }
}

# Quick SQL normalization for large queries
function Get-QuickSqlHash {
    param([string]$Query)
    
    if ($Query.Length -gt $MaxQueryLength) {
        # For very long queries, use a hash-based approach
        $hasher = [System.Security.Cryptography.SHA256]::Create()
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($Query)
        $hash = $hasher.ComputeHash($bytes)
        return [System.Convert]::ToBase64String($hash).Substring(0, 16)
    }
    
    # Quick normalization for shorter queries
    $normalized = $Query -replace '--[^\r\n]*', ''
    $normalized = $normalized -replace '/\*[\s\S]*?\*/', ''
    $normalized = $normalized -replace '\s+', ' '
    return $normalized.Trim().Substring(0, [Math]::Min(1000, $normalized.Length))
}

# Fast keyword extraction
function Get-SqlKeywords {
    param([string]$Query, [int]$MaxKeywords = 10)
    
    $commonKeywords = @('SELECT', 'FROM', 'WHERE', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 
        'GROUP BY', 'ORDER BY', 'HAVING', 'UNION', 'INSERT', 'UPDATE', 'DELETE')
    
    $foundKeywords = @()
    foreach ($keyword in $commonKeywords) {
        if ($Query -match "\b$keyword\b" -and $foundKeywords.Count -lt $MaxKeywords) {
            $foundKeywords += $keyword
        }
    }
    return $foundKeywords
}

# Optimized query extraction
function Extract-SqlQueriesOptimized {
    param([string]$ArmContent)
    
    $extractOperation = {
        param($Content)
        
        $queries = @()
        # More efficient regex pattern
        $sqlPattern = '"sqlReaderQuery":\s*"([^"]*(?:\\.[^"]*)*)"'
        
        # Process in chunks to avoid memory issues
        $chunkSize = 100000
        $contentLength = $Content.Length
        $startPos = 0
        
        while ($startPos -lt $contentLength) {
            $endPos = [Math]::Min($startPos + $chunkSize, $contentLength)
            $chunk = $Content.Substring($startPos, $endPos - $startPos)
            
            $matches = [regex]::Matches($chunk, $sqlPattern)
            foreach ($match in $matches) {
                $rawQuery = $match.Groups[1].Value
                $cleanQuery = $rawQuery -replace '\\n', "`n" -replace '\\t', "`t" -replace '\\"', '"' -replace '\\\\', '\'
                $queries += @{
                    Query    = $cleanQuery
                    Position = $startPos + $match.Index
                    Length   = $cleanQuery.Length
                }
            }
            
            $startPos = $endPos - 1000  # Overlap to catch queries spanning chunks
        }
        
        return $queries
    }
    
    $result = Invoke-WithTimeout -ScriptBlock { $extractOperation.Invoke($ArmContent) } -TimeoutSeconds $TimeoutSeconds -OperationName "Query Extraction"
    return $result
}

# Fast file matching with caching
$Script:SqlFileCache = @{}

function Find-MatchingSqlFileOptimized {
    param(
        [hashtable]$QueryInfo,
        [array]$SqlFiles
    )
    
    $query = $QueryInfo.Query
    $queryLength = $QueryInfo.Length
    
    # Skip extremely long queries that might cause timeout
    if ($queryLength -gt $MaxQueryLength) {
        Write-Host "  ⚠️ Skipping very long query ($queryLength chars)" -ForegroundColor Yellow
        $Script:ProcessingStats.SkippedQueries++
        return $null
    }
    
    $queryHash = Get-QuickSqlHash -Query $query
    $queryKeywords = Get-SqlKeywords -Query $query
    
    $bestMatch = $null
    $bestScore = 0
    
    foreach ($sqlFile in $SqlFiles) {
        try {
            # Use cached file content if available
            if (-not $Script:SqlFileCache.ContainsKey($sqlFile.FullName)) {
                $fileContent = Get-Content -Path $sqlFile.FullName -Raw -Encoding UTF8 -ErrorAction SilentlyContinue
                if ($fileContent) {
                    $Script:SqlFileCache[$sqlFile.FullName] = @{
                        Content  = $fileContent
                        Hash     = Get-QuickSqlHash -Query $fileContent
                        Keywords = Get-SqlKeywords -Query $fileContent
                        Length   = $fileContent.Length
                    }
                }
            }
            
            $fileInfo = $Script:SqlFileCache[$sqlFile.FullName]
            if (-not $fileInfo) { continue }
            
            # Quick scoring
            $score = 0
            
            # Hash comparison for exact matches
            if ($queryHash -eq $fileInfo.Hash) {
                $score += 100
            }
            
            # Keyword matching
            $matchingKeywords = 0
            foreach ($keyword in $queryKeywords) {
                if ($fileInfo.Keywords -contains $keyword) {
                    $matchingKeywords++
                }
            }
            $score += $matchingKeywords * 10
            
            # Length similarity
            $lengthRatio = if ($fileInfo.Length -gt 0) { 
                [Math]::Min($queryLength, $fileInfo.Length) / [Math]::Max($queryLength, $fileInfo.Length) 
            }
            else { 0 }
            $score += $lengthRatio * 20
            
            if ($score -gt $bestScore -and $score -gt 30) {
                $bestScore = $score
                $bestMatch = @{
                    File             = $sqlFile
                    Score            = $score
                    MatchingKeywords = $matchingKeywords
                    QueryLength      = $queryLength
                    FileLength       = $fileInfo.Length
                }
            }
        }
        catch {
            if ($Verbose) {
                Write-Warning "Error processing $($sqlFile.Name): $($_.Exception.Message)"
            }
        }
    }
    
    return $bestMatch
}

# Process queries in batches
function Process-QueriesBatch {
    param(
        [array]$Queries,
        [array]$SqlFiles,
        [string]$ArmFileName
    )
    
    $batchMatches = @()
    $batchNumber = 1
    
    for ($i = 0; $i -lt $Queries.Count; $i += $BatchSize) {
        $batch = $Queries[$i..[Math]::Min($i + $BatchSize - 1, $Queries.Count - 1)]
        
        Write-Host "  Processing batch $batchNumber ($($batch.Count) queries)..." -ForegroundColor Cyan
        
        foreach ($queryInfo in $batch) {
            $Script:ProcessingStats.TotalQueries++
            
            if ($queryInfo.Length -lt 100) {
                Write-Host "    Skipping short query (length: $($queryInfo.Length))" -ForegroundColor Gray
                $Script:ProcessingStats.SkippedQueries++
                continue
            }
            
            $matchOperation = {
                param($QueryInfo, $SqlFiles)
                return Find-MatchingSqlFileOptimized -QueryInfo $QueryInfo -SqlFiles $SqlFiles
            }
            
            $match = Invoke-WithTimeout -ScriptBlock { $matchOperation.Invoke($queryInfo, $SqlFiles) } -TimeoutSeconds ($TimeoutSeconds / 2) -OperationName "Query Matching"
            
            if ($match -eq $null) {
                $Script:ProcessingStats.TimeoutQueries++
                continue
            }
            
            $Script:ProcessingStats.ProcessedQueries++
            
            if ($match) {
                $Script:ProcessingStats.MatchedQueries++
                $batchMatches += @{
                    ArmFile          = $ArmFileName
                    SqlFile          = $match.File.Name
                    Score            = $match.Score
                    MatchingKeywords = $match.MatchingKeywords
                    QueryLength      = $match.QueryLength
                    FileLength       = $match.FileLength
                    Position         = $queryInfo.Position
                }
                
                Write-Host "    ✅ Match found: $($match.File.Name) (score: $($match.Score))" -ForegroundColor Green
            }
        }
        
        $batchNumber++
        
        # Progress update
        $elapsed = (Get-Date) - $Script:ProcessingStats.StartTime
        $progress = ($Script:ProcessingStats.ProcessedQueries / $Script:ProcessingStats.TotalQueries) * 100
        Write-Host "    Progress: $($Script:ProcessingStats.ProcessedQueries)/$($Script:ProcessingStats.TotalQueries) queries ($($progress.ToString('F1'))%) - Elapsed: $($elapsed.ToString('mm\:ss'))" -ForegroundColor Blue
    }
    
    return $batchMatches
}

# Main processing function
function Start-OptimizedProcessing {
    try {
        # Validate directories
        if (-not (Test-Path $ArmTemplateDirectory)) {
            Write-Error "❌ ARM template directory not found: $ArmTemplateDirectory"
            return
        }
        
        if (-not (Test-Path $SqlDirectory)) {
            Write-Error "❌ SQL directory not found: $SqlDirectory"
            return
        }
        
        # Get files
        $armFiles = Get-ChildItem -Path $ArmTemplateDirectory -Filter "*.json" -Recurse
        $sqlFiles = Get-ChildItem -Path $SqlDirectory -Filter "*.sql" | Where-Object { 
            -not $_.Name.Contains("backup") -and -not $_.Name.Contains("test") 
        }
        
        Write-Host "📁 Found $($armFiles.Count) ARM template files" -ForegroundColor Green
        Write-Host "📁 Found $($sqlFiles.Count) SQL files" -ForegroundColor Green
        Write-Host ""
        
        # Pre-load SQL files cache
        Write-Host "🔄 Pre-loading SQL file cache..." -ForegroundColor Yellow
        foreach ($sqlFile in $sqlFiles) {
            try {
                $fileContent = Get-Content -Path $sqlFile.FullName -Raw -Encoding UTF8 -ErrorAction SilentlyContinue
                if ($fileContent) {
                    $Script:SqlFileCache[$sqlFile.FullName] = @{
                        Content  = $fileContent
                        Hash     = Get-QuickSqlHash -Query $fileContent
                        Keywords = Get-SqlKeywords -Query $fileContent
                        Length   = $fileContent.Length
                    }
                }
            }
            catch {
                Write-Warning "⚠️ Could not cache $($sqlFile.Name): $($_.Exception.Message)"
            }
        }
        Write-Host "✅ Cached $($Script:SqlFileCache.Count) SQL files" -ForegroundColor Green
        Write-Host ""
        
        $allMatches = @()
        
        foreach ($armFile in $armFiles) {
            Write-Host "🔍 Processing ARM template: $($armFile.Name)" -ForegroundColor Magenta
            
            try {
                $armContent = Get-Content -Path $armFile.FullName -Raw -Encoding UTF8
                $queries = Extract-SqlQueriesOptimized -ArmContent $armContent
                
                if ($queries) {
                    Write-Host "  📊 Found $($queries.Count) SQL queries" -ForegroundColor Cyan
                    $fileMatches = Process-QueriesBatch -Queries $queries -SqlFiles $sqlFiles -ArmFileName $armFile.Name
                    $allMatches += $fileMatches
                }
                else {
                    Write-Host "  ⚠️ No queries found or extraction timed out" -ForegroundColor Yellow
                }
            }
            catch {
                Write-Error "❌ Error processing $($armFile.Name): $($_.Exception.Message)"
            }
            
            Write-Host ""
        }
        
        # Summary report
        $elapsed = (Get-Date) - $Script:ProcessingStats.StartTime
        Write-Host "=== PROCESSING SUMMARY ===" -ForegroundColor Magenta
        Write-Host "Total Queries Found: $($Script:ProcessingStats.TotalQueries)" -ForegroundColor White
        Write-Host "Queries Processed: $($Script:ProcessingStats.ProcessedQueries)" -ForegroundColor Green
        Write-Host "Matches Found: $($Script:ProcessingStats.MatchedQueries)" -ForegroundColor Green
        Write-Host "Queries Skipped: $($Script:ProcessingStats.SkippedQueries)" -ForegroundColor Yellow
        Write-Host "Timeout Queries: $($Script:ProcessingStats.TimeoutQueries)" -ForegroundColor Red
        Write-Host "Total Processing Time: $($elapsed.ToString('mm\:ss'))" -ForegroundColor Cyan
        
        if ($Script:ProcessingStats.ProcessedQueries -gt 0) {
            $matchRate = ($Script:ProcessingStats.MatchedQueries / $Script:ProcessingStats.ProcessedQueries) * 100
            Write-Host "Match Rate: $($matchRate.ToString('F1'))%" -ForegroundColor Blue
        }
        
        # Export results
        if ($allMatches.Count -gt 0 -and -not $DryRun) {
            $reportPath = "sql_externalization_results_$(Get-Date -Format 'yyyyMMdd_HHmmss').json"
            $allMatches | ConvertTo-Json -Depth 3 | Out-File -FilePath $reportPath -Encoding UTF8
            Write-Host "📄 Results exported to: $reportPath" -ForegroundColor Green
        }
        
        return $allMatches
    }
    catch {
        Write-Error "❌ Fatal error: $($_.Exception.Message)"
        Write-Error $_.Exception.StackTrace
    }
}

# Execute main processing
$results = Start-OptimizedProcessing

Write-Host ""
Write-Host "🎯 Optimization complete!" -ForegroundColor Green
