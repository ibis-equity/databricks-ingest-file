# setup-databricks-secrets.ps1
# One-time script to create a Databricks secret scope and store Databricks credentials.
# Run this once, then use load-databricks-secrets.ps1 for day-to-day use.
#
# Usage:
#   .\setup-databricks-secrets.ps1
#   .\setup-databricks-secrets.ps1 -ScopeName ingest-secrets

param(
    [string]$ScopeName = "ingest-secrets"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# 1) Verify Databricks CLI availability.
if (-not (Get-Command databricks -ErrorAction SilentlyContinue)) {
    Write-Error "Databricks CLI not found. Install it first (pip install databricks-cli)."
    exit 1
}

# 2) Verify CLI auth by attempting to list scopes.
Write-Host "Checking Databricks CLI authentication..." -ForegroundColor Cyan
$scopesOutput = databricks secrets list-scopes 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Error "Databricks CLI is not authenticated. Configure DATABRICKS_HOST and DATABRICKS_TOKEN or ~/.databrickscfg first."
    exit 1
}

# 3) Ensure secret scope exists.
if ($scopesOutput -notmatch "\b$ScopeName\b") {
    Write-Host "Creating Databricks secret scope '$ScopeName'..." -ForegroundColor Cyan
    $null = databricks secrets create-scope --scope $ScopeName 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to create scope '$ScopeName'."
        exit 1
    }
    Write-Host "Secret scope created." -ForegroundColor Green
} else {
    Write-Host "Secret scope '$ScopeName' already exists." -ForegroundColor Yellow
}

# 4) Prompt for secrets.
Write-Host ""
Write-Host "Enter Databricks credentials to store in scope '$ScopeName'." -ForegroundColor Cyan
Write-Host "(Values will NOT be shown on screen)" -ForegroundColor DarkGray

$hostname = Read-Host "DATABRICKS_SERVER_HOSTNAME (e.g. dbc-xxxx.cloud.databricks.com)"
$httpPath = Read-Host "DATABRICKS_HTTP_PATH (e.g. /sql/1.0/warehouses/xxxx)"
$tokenSecure = Read-Host "DATABRICKS_TOKEN (dapi...)" -AsSecureString
$tokenPlain = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
    [Runtime.InteropServices.Marshal]::SecureStringToBSTR($tokenSecure)
)

if ([string]::IsNullOrWhiteSpace($hostname) -or [string]::IsNullOrWhiteSpace($httpPath) -or [string]::IsNullOrWhiteSpace($tokenPlain)) {
    Write-Error "Hostname, HTTP path, and token are all required."
    exit 1
}

# 5) Write secrets to Databricks secret scope.
Write-Host ""
Write-Host "Storing secrets in Databricks scope '$ScopeName'..." -ForegroundColor Cyan

$null = databricks secrets put --scope $ScopeName --key DATABRICKS_SERVER_HOSTNAME --string-value $hostname 2>&1
if ($LASTEXITCODE -ne 0) { Write-Error "Failed storing DATABRICKS_SERVER_HOSTNAME"; exit 1 }

$null = databricks secrets put --scope $ScopeName --key DATABRICKS_HTTP_PATH --string-value $httpPath 2>&1
if ($LASTEXITCODE -ne 0) { Write-Error "Failed storing DATABRICKS_HTTP_PATH"; exit 1 }

$null = databricks secrets put --scope $ScopeName --key DATABRICKS_TOKEN --string-value $tokenPlain 2>&1
if ($LASTEXITCODE -ne 0) { Write-Error "Failed storing DATABRICKS_TOKEN"; exit 1 }

Write-Host ""
Write-Host "Done! Secrets stored in Databricks scope '$ScopeName'." -ForegroundColor Green
Write-Host "Run '.\load-databricks-secrets.ps1' to load them and start the ingest script." -ForegroundColor Green

