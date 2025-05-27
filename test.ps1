# Azure DevOps Testing Environment - PowerShell Script
param(
    [Parameter(Position = 0)]
    [ValidateSet("build", "up", "down", "test", "test-unit", "test-specific", "clean", "logs", "status", "help")]
    [string]$Command = "help",
    
    [Parameter(Position = 1)]
    [string]$TestName = ""
)

function Show-Help {
    Write-Host "Available commands:" -ForegroundColor Green
    Write-Host "  build      - Build Docker images"
    Write-Host "  up         - Start all services"
    Write-Host "  down       - Stop all services"
    Write-Host "  test       - Run all tests"
    Write-Host "  test-unit  - Run unit tests only"
    Write-Host "  test-specific <test-name> - Run specific test"
    Write-Host "  clean      - Clean up containers and volumes"
    Write-Host "  logs       - Show service logs"
    Write-Host "  status     - Show service status"
    Write-Host ""
    Write-Host "Example usage:" -ForegroundColor Yellow
    Write-Host "  .\test.ps1 build"
    Write-Host "  .\test.ps1 test"
    Write-Host "  .\test.ps1 test-specific test_pi_Insert_ClientDmBx.py"
}

function Build-Images {
    Write-Host "Building Docker images..." -ForegroundColor Yellow
    docker-compose build --no-cache
}

function Start-Services {
    Write-Host "Starting services..." -ForegroundColor Yellow
    docker-compose up -d
    Write-Host "Waiting for services to be ready..." -ForegroundColor Yellow
    Start-Sleep -Seconds 10
    
    # Check if Azurite is ready
    $ready = docker-compose exec azurite curl -f http://localhost:10000/devstoreaccount1 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✓ Azurite is ready!" -ForegroundColor Green
    }
    else {
        Write-Host "⚠ Azurite may not be ready yet" -ForegroundColor Yellow
    }
}

function Stop-Services {
    Write-Host "Stopping services..." -ForegroundColor Yellow
    docker-compose down
}

function Run-AllTests {
    Start-Services
    Write-Host "Running all tests..." -ForegroundColor Yellow
    docker-compose exec pytest-test bash -c @"
source /opt/venv/bin/activate && \
export PYTHONPATH=/tests && \
export AZURITE_HOST=azurite && \
cd /tests && \
python -m pytest unit/ -v --tb=short
"@
}

function Run-UnitTests {
    Start-Services
    Write-Host "Running unit tests..." -ForegroundColor Yellow
    docker-compose exec pytest-test bash -c @"
source /opt/venv/bin/activate && \
export PYTHONPATH=/tests && \
export AZURITE_HOST=azurite && \
cd /tests && \
python -m pytest unit/ -v
"@
}

function Run-SpecificTest {
    param([string]$TestName)
    if (-not $TestName) {
        Write-Host "Error: Test name is required for test-specific command" -ForegroundColor Red
        return
    }
    
    Start-Services
    Write-Host "Running specific test: $TestName" -ForegroundColor Yellow
    docker-compose exec pytest-test bash -c @"
source /opt/venv/bin/activate && \
export PYTHONPATH=/tests && \
export AZURITE_HOST=azurite && \
cd /tests && \
python -m pytest unit/$TestName -v -s
"@
}

function Clean-Environment {
    Write-Host "Cleaning up environment..." -ForegroundColor Yellow
    docker-compose down -v
    docker system prune -f
}

function Show-Logs {
    docker-compose logs -f
}

function Show-Status {
    docker-compose ps
}

# Main switch
switch ($Command) {
    "build" { Build-Images }
    "up" { Start-Services }
    "down" { Stop-Services }
    "test" { Run-AllTests }
    "test-unit" { Run-UnitTests }
    "test-specific" { Run-SpecificTest -TestName $TestName }
    "clean" { Clean-Environment }
    "logs" { Show-Logs }
    "status" { Show-Status }
    "help" { Show-Help }
    default { Show-Help }
}
