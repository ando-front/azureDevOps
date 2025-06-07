# Enhanced SQL Externalization Script with Working Implementation
param(
    [string]$ArmTemplateDirectory = "src\dev\arm_template",
    [string]$SqlDirectory = "sql\e2e_queries", 
    [string]$OutputDirectory = "build\arm_template",
    [switch]$DryRun = $false
)

Write-Host "=== SQL QUERY EXTERNALIZATION ANALYSIS ===" -ForegroundColor Magenta
Write-Host "ARM Template Directory: $ArmTemplateDirectory"
Write-Host "SQL Directory: $SqlDirectory"
Write-Host ""

# Function to normalize SQL for comparison
function Normalize-SqlQuery {
    param([string]$Query)
    
    # Remove comments, extra whitespace, and normalize
    $normalized = $Query -replace '--[^\r\n]*', ''  # Remove single-line comments
    $normalized = $normalized -replace '/\*[\s\S]*?\*/', ''  # Remove multi-line comments  
    $normalized = $normalized -replace '\r?\n', ' '  # Replace newlines with spaces
    $normalized = $normalized -replace '\s+', ' '  # Normalize multiple spaces
    $normalized = $normalized.Trim()
    
    return $normalized
}

# Function to extract queries from ARM template
function Extract-SqlQueries {
    param([string]$ArmContent)
    
    $queries = @()
    $sqlPattern = '"sqlReaderQuery":\s*"([^"]*(?:\\.[^"]*)*)"'
    $matches = [regex]::Matches($ArmContent, $sqlPattern)
    
    foreach ($match in $matches) {
        $rawQuery = $match.Groups[1].Value
        # Unescape JSON string
        $cleanQuery = $rawQuery -replace '\\n', "`n" -replace '\\t', "`t" -replace '\\"', '"' -replace '\\\\', '\'
        $queries += $cleanQuery
    }
    
    return $queries
}

# Function to find matching SQL file
function Find-MatchingSqlFile {
    param(
        [string]$Query,
        [array]$SqlFiles
    )
    
    $normalizedQuery = Normalize-SqlQuery -Query $Query
    $queryLength = $normalizedQuery.Length
    
    Write-Host "  Analyzing query (length: $queryLength)..."
    
    foreach ($sqlFile in $SqlFiles) {
        try {
            $sqlContent = Get-Content -Path $sqlFile.FullName -Raw -Encoding UTF8
            $normalizedSql = Normalize-SqlQuery -Query $sqlContent
            
            if ($normalizedSql.Length -eq 0) { continue }
            
            # Calculate similarity based on content matching
            $similarity = 0
            if ($normalizedQuery.Contains($normalizedSql.Substring(0, [math]::Min(200, $normalizedSql.Length))) -or
                $normalizedSql.Contains($normalizedQuery.Substring(0, [math]::Min(200, $normalizedQuery.Length)))) {
                $similarity = 1
            }
            
            # Check for keyword matches
            $keywordMatches = 0
            $keywords = @("SELECT", "FROM", "WHERE", "JOIN", "AS CLIENT_KEY", "LIV0EU", "WEBHIS")
            foreach ($keyword in $keywords) {
                if ($normalizedQuery.Contains($keyword) -and $normalizedSql.Contains($keyword)) {
                    $keywordMatches++
                }
            }
            
            if ($keywordMatches -ge 3 -or $similarity -gt 0) {
                Write-Host "    ✓ Potential match: $($sqlFile.Name) (keywords: $keywordMatches, similarity: $similarity)" -ForegroundColor Green
                return @{
                    File        = $sqlFile
                    Similarity  = $similarity
                    Keywords    = $keywordMatches
                    QueryLength = $queryLength
                    FileLength  = $normalizedSql.Length
                }
            }
        }
        catch {
            Write-Warning "Error reading $($sqlFile.Name): $($_.Exception.Message)"
        }
    }
    
    return $null
}

# Main processing
try {
    # Validate directories
    if (-not (Test-Path $ArmTemplateDirectory)) {
        Write-Error "ARM template directory not found: $ArmTemplateDirectory"
        return
    }
    
    if (-not (Test-Path $SqlDirectory)) {
        Write-Error "SQL directory not found: $SqlDirectory"
        return
    }
    
    # Get files
    $armFiles = Get-ChildItem -Path $ArmTemplateDirectory -Filter "*.json" -Recurse
    $sqlFiles = Get-ChildItem -Path $SqlDirectory -Filter "*.sql" | Where-Object { -not $_.Name.Contains("backup") -and -not $_.Name.Contains("test") }
    
    Write-Host "Found $($armFiles.Count) ARM template files"
    Write-Host "Found $($sqlFiles.Count) SQL files"
    Write-Host ""
    
    $totalQueries = 0
    $matchedQueries = 0
    $allMatches = @()
    
    foreach ($armFile in $armFiles) {
        Write-Host "Processing ARM template: $($armFile.Name)" -ForegroundColor Cyan
        
        try {
            $armContent = Get-Content -Path $armFile.FullName -Raw -Encoding UTF8
            $queries = Extract-SqlQueries -ArmContent $armContent
            
            Write-Host "  Found $($queries.Count) SQL queries"
            $totalQueries += $queries.Count
            
            foreach ($query in $queries) {
                if ($query.Length -lt 100) {
                    Write-Host "  Skipping short query (length: $($query.Length))"
                    continue
                }
                
                $match = Find-MatchingSqlFile -Query $query -SqlFiles $sqlFiles
                
                if ($match) {
                    $matchedQueries++
                    $allMatches += @{
                        ArmFile     = $armFile.Name
                        SqlFile     = $match.File.Name
                        Similarity  = $match.Similarity
                        Keywords    = $match.Keywords
                        QueryLength = $match.QueryLength
                        FileLength  = $match.FileLength
                    }
                }
                else {
                    Write-Host "  ⚠ No match found for query (length: $($query.Length))" -ForegroundColor Yellow
                }
            }
        }
        catch {
            Write-Error "Error processing $($armFile.Name): $($_.Exception.Message)"
        }
        
        Write-Host ""
    }
    
    # Summary report
    Write-Host "=== ANALYSIS SUMMARY ===" -ForegroundColor Magenta
    Write-Host "Total ARM template files: $($armFiles.Count)"
    Write-Host "Total SQL queries found: $totalQueries"
    Write-Host "Successfully matched queries: $matchedQueries" -ForegroundColor Green
    Write-Host "Unmatched queries: $($totalQueries - $matchedQueries)" -ForegroundColor Yellow
    
    if ($totalQueries -gt 0) {
        $successRate = [math]::Round(($matchedQueries / $totalQueries) * 100, 2)
        Write-Host "Match rate: $successRate%" -ForegroundColor Cyan
    }
    
    Write-Host ""
    Write-Host "=== DETAILED MATCHES ===" -ForegroundColor Magenta
    
    foreach ($match in $allMatches) {
        Write-Host "ARM: $($match.ArmFile) → SQL: $($match.SqlFile)" -ForegroundColor Green
        Write-Host "  Query: $($match.QueryLength) chars, File: $($match.FileLength) chars, Keywords: $($match.Keywords)"
    }
    
    if ($DryRun) {
        Write-Host ""
        Write-Warning "DRY RUN MODE - No files were modified"
    }
    
}
catch {
    Write-Error "Script execution failed: $($_.Exception.Message)"
}

Write-Host ""
Write-Host "Analysis completed!" -ForegroundColor Green
