
# Script to externalize large inline SQL queries from ARM templates

param(
    [string]$ArmTemplateDirectory = "src\dev\arm_template",
    [string]$SqlDirectory = "sql\e2e_queries",
    [string]$OutputDirectory = "build\arm_template",
    [switch]$DryRun = $false,
    [switch]$AnalyzeUnmatched = $false,
    [switch]$Verbose = $false
)

# Color-coded message functions
function Write-Success {
    param([string]$Message)
    Write-Host "✓ $Message" -ForegroundColor Green
}

function Write-Info {
    param([string]$Message)
    Write-Host "ℹ  $Message" -ForegroundColor Blue
}

function Write-Warning {
    param([string]$Message)
    Write-Host "⚠  $Message" -ForegroundColor Yellow
}

function Write-Error {
    param([string]$Message)
    Write-Host "✗ $Message" -ForegroundColor Red
}

function Write-Verbose-Custom {
    param([string]$Message)
    if ($Verbose) {
        Write-Host "🔍 $Message" -ForegroundColor Cyan
    }
}

# SQL file reference mapping
$SqlFileMapping = @{
    "client_dm_main" = @{
        "SqlFile" = "client_dm_main.sql"
        "SearchPatterns" = @("IF連携", "outputDT.*varchar.*20", "client_dm", "DM_IF")
        "Description" = "Customer DM main query"
        "MinLength" = 300
        "MaxLength" = 100000
    }
    "client_dm_marketing" = @{
        "SqlFile" = "marketing_client_dm_comprehensive.sql"
        "SearchPatterns" = @("marketing_client_dm", "client_dm_comprehensive", "marketing.*dm", "comprehensive.*client")
        "Description" = "Customer DM marketing comprehensive query"
        "MinLength" = 500
        "MaxLength" = 80000
    }
    "payment_alert_main" = @{
        "SqlFile" = "payment_alert_main.sql"
        "SearchPatterns" = @("payment_alert", "payment.*alert", "alert.*payment")
        "Description" = "Payment alert main query"
        "MinLength" = 200
        "MaxLength" = 50000
    }
    "usage_services_main" = @{
        "SqlFile" = "usage_services_main.sql"
        "SearchPatterns" = @("usage_services", "service.*usage", "usage.*service")
        "Description" = "Usage services main query"
        "MinLength" = 150
        "MaxLength" = 60000
    }
    "moving_promotion_list" = @{
        "SqlFile" = "moving_promotion_list_main.sql"
        "SearchPatterns" = @("moving_promotion", "promotion.*list", "moving.*list")
        "Description" = "Moving promotion list query"
        "MinLength" = 100
        "MaxLength" = 40000
    }
    "client_dna_large" = @{
        "SqlFile" = "client_dna_large_main.sql"
        "SearchPatterns" = @("顧客DNA", "client_dna", "marketing.*trn_client_dna", "omni_ods_marketing_trn_client_dna")
        "Description" = "Large customer DNA queries"
        "MinLength" = 30000
        "MaxLength" = 100000
    }
    "electricity_contract_thanks" = @{
        "SqlFile" = "electricity_contract_thanks_main.sql"
        "SearchPatterns" = @("electricity.*contract.*thanks", "electric.*contract.*thanks", "MA向け.*電気契約", "ElectricityContractThanks")
        "Description" = "Electricity contract thanks scenario queries"
        "MinLength" = 1000
        "MaxLength" = 50000
    }
}

# Function to find inline SQL queries in ARM templates
function Find-InlineSqlQueries {
    param([string]$FilePath)
    
    Write-Info "Analyzing ARM template: $FilePath"
    $content = Get-Content -Path $FilePath -Raw -Encoding UTF8
    
    if (-not $content) {
        Write-Warning "File is empty or could not be read: $FilePath"
        return @()
    }
    
    $sqlQueryPattern = '"sqlReaderQuery":\s*("[^"]*"|{\s*"value":\s*"[^"]*"\s*})'
    $regexMatches = [regex]::Matches($content, $sqlQueryPattern, [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
    
    $queries = @()
    
    foreach ($match in $regexMatches) {
        $fullMatch = $match.Value
        $sqlContent = ""
        
        if ($fullMatch -match '"sqlReaderQuery":\s*"([^"]*)"') {
            $sqlContent = $Matches[1]
        }
        elseif ($fullMatch -match '"sqlReaderQuery":\s*{\s*"value":\s*"([^"]*)"\s*}') {
            $sqlContent = $Matches[1]
        }
        
        if ($sqlContent) {
            $sqlContent = $sqlContent -replace '\\n', "`n" -replace '\\r', "`r" -replace '\\"', '"' -replace '\\\\', '\'
            $queryLength = $sqlContent.Length
            
            Write-Verbose-Custom "Found SQL query at position $($match.Index) (length: $queryLength characters)"
            
            if ($queryLength -gt 100) {
                $queries += @{
                    "Content" = $sqlContent
                    "Length" = $queryLength
                    "MatchStart" = $match.Index
                    "MatchLength" = $match.Length
                    "FullMatch" = $fullMatch
                }
                
                if ($queryLength -gt 10000) {
                    Write-Info "Large query found (length: $queryLength characters)"
                }
                elseif ($queryLength -gt 50000) {
                    Write-Warning "Very large query found (length: $queryLength characters) - consider manual review"
                }
            }
            else {
                Write-Verbose-Custom "Skipping small query (length: $queryLength characters)"
            }
        }
    }
    
    return $queries
}

# Function to test if SQL query matches patterns
function Test-SqlQueryMatch {
    param([string]$SqlContent, [hashtable]$MappingConfig)
    
    if ($SqlContent.Length -lt $MappingConfig.MinLength) { return $false }
    if ($SqlContent.Length -gt $MappingConfig.MaxLength) { return $false }
    
    $patternMatches = 0
    foreach ($pattern in $MappingConfig.SearchPatterns) {
        if ($SqlContent -match $pattern) {
            $patternMatches++
        }
    }
    
    return $patternMatches -gt 0
}

# Function to create externalized ARM template
function Create-ExternalizedArmTemplate {
    param([string]$OriginalFilePath, [array]$QueryMatches, [string]$OutputDirectory)
    
    if ($DryRun) {
        Write-Info "DRY RUN: Would create externalized version of $OriginalFilePath"
        return
    }
    
    if (-not (Test-Path $OutputDirectory)) {
        New-Item -ItemType Directory -Path $OutputDirectory -Force | Out-Null
    }
    
    $content = Get-Content -Path $OriginalFilePath -Raw -Encoding UTF8
    $outputFileName = Split-Path $OriginalFilePath -Leaf
    $outputPath = Join-Path $OutputDirectory $outputFileName
    
    foreach ($match in $QueryMatches) {
        $originalQuery = $match.FullMatch
        $externalReference = '"sqlReaderQuery": {"value": "@{linkedService().properties.typeProperties.connectionString}+' + "'" + 'SELECT * FROM OPENROWSET(BULK ''' + $match.SqlFile + ''', SINGLE_CLOB) AS [Query]' + "'" + '"}'
        $content = $content -replace [regex]::Escape($originalQuery), $externalReference
    }
    
    $content | Out-File -FilePath $outputPath -Encoding UTF8
    Write-Success "Created externalized template: $outputPath"
}

# Function to analyze unmatched queries
function Analyze-UnmatchedQueries {
    param([array]$UnmatchedQueries)
    
    if ($UnmatchedQueries.Count -eq 0) {
        Write-Success "All queries were successfully matched!"
        return
    }
    
    Write-Warning "Found $($UnmatchedQueries.Count) unmatched queries:"
    
    foreach ($query in $UnmatchedQueries) {
        Write-Host ""
        Write-Host "Query Length: $($query.Length) characters" -ForegroundColor Yellow
        Write-Host "First 200 characters:" -ForegroundColor Yellow
        $preview = $query.Content.Substring(0, [Math]::Min(200, $query.Content.Length))
        Write-Host $preview -ForegroundColor Gray
        Write-Host "..." -ForegroundColor Gray
        
        $suggestions = @()
        if ($query.Content -match "SELECT.*FROM.*WHERE") { $suggestions += "Standard SELECT query" }
        if ($query.Content -match "INSERT.*INTO") { $suggestions += "INSERT operation" }
        if ($query.Content -match "UPDATE.*SET") { $suggestions += "UPDATE operation" }
        if ($query.Content -match "DELETE.*FROM") { $suggestions += "DELETE operation" }
        if ($query.Content -match "UNION") { $suggestions += "UNION query" }
        if ($query.Content -match "JOIN") { $suggestions += "JOIN operation" }
        
        if ($suggestions.Count -gt 0) {
            Write-Host "Detected patterns: $($suggestions -join ', ')" -ForegroundColor Cyan
        }
    }
}

# Main execution function
function Main {
    Write-Info "Starting SQL Query Externalization Process"
    Write-Info "ARM Template Directory: $ArmTemplateDirectory"
    Write-Info "SQL Directory: $SqlDirectory"
    Write-Info "Output Directory: $OutputDirectory"
    
    if ($DryRun) {
        Write-Warning "DRY RUN MODE - No files will be modified"
    }
    
    if (-not (Test-Path $ArmTemplateDirectory)) {
        Write-Error "ARM template directory not found: $ArmTemplateDirectory"
        return
    }
    
    if (-not (Test-Path $SqlDirectory)) {
        Write-Error "SQL directory not found: $SqlDirectory"
        return
    }
    
    $armFiles = Get-ChildItem -Path $ArmTemplateDirectory -Filter "*.json" -Recurse
    
    if ($armFiles.Count -eq 0) {
        Write-Warning "No ARM template files found in: $ArmTemplateDirectory"
        return
    }
    
    Write-Info "Found $($armFiles.Count) ARM template files"
    
    $totalQueries = 0
    $matchedQueries = 0
    $unmatchedQueries = @()
    
    foreach ($armFile in $armFiles) {
        Write-Host ""
        Write-Info "Processing: $($armFile.Name)"
        
        $queries = Find-InlineSqlQueries -FilePath $armFile.FullName
        $totalQueries += $queries.Count
        
        if ($queries.Count -eq 0) {
            Write-Info "No SQL queries found in this file"
            continue
        }
        
        Write-Info "Found $($queries.Count) SQL queries in this file"
        
        $fileMatches = @()
        
        foreach ($query in $queries) {
            $matched = $false
            
            foreach ($mappingKey in $SqlFileMapping.Keys) {
                $mapping = $SqlFileMapping[$mappingKey]
                
                if (Test-SqlQueryMatch -SqlContent $query.Content -MappingConfig $mapping) {
                    Write-Success "Matched query to $($mapping.SqlFile) ($($mapping.Description))"
                    
                    $query | Add-Member -NotePropertyName "SqlFile" -NotePropertyValue $mapping.SqlFile
                    $query | Add-Member -NotePropertyName "MappingKey" -NotePropertyValue $mappingKey
                    $query | Add-Member -NotePropertyName "Description" -NotePropertyValue $mapping.Description
                    
                    $fileMatches += $query
                    $matchedQueries++
                    $matched = $true
                    break
                }
            }
            
            if (-not $matched) {
                Write-Warning "No matching SQL file found for query (length: $($query.Length))"
                $unmatchedQueries += $query
            }
        }
        
        if ($fileMatches.Count -gt 0) {
            Create-ExternalizedArmTemplate -OriginalFilePath $armFile.FullName -QueryMatches $fileMatches -OutputDirectory $OutputDirectory
        }
    }
    
    Write-Host ""
    Write-Host "=== EXTERNALIZATION SUMMARY ===" -ForegroundColor Magenta
    Write-Host "Total ARM template files processed: $($armFiles.Count)"
    Write-Host "Total SQL queries found: $totalQueries"
    Write-Host "Successfully matched queries: $matchedQueries" -ForegroundColor Green
    Write-Host "Unmatched queries: $($unmatchedQueries.Count)" -ForegroundColor Yellow
    
    if ($matchedQueries -gt 0) {
        $successRate = [math]::Round(($matchedQueries / $totalQueries) * 100, 2)
        Write-Host "Success rate: $successRate%" -ForegroundColor Cyan
    }
    
    if ($AnalyzeUnmatched -and $unmatchedQueries.Count -gt 0) {
        Write-Host ""
        Write-Host "=== UNMATCHED QUERY ANALYSIS ===" -ForegroundColor Magenta
        Analyze-UnmatchedQueries -UnmatchedQueries $unmatchedQueries
    }
    
    Write-Info "Externalization process completed"
}

# Run the main function
Main
