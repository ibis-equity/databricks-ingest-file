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

function Invoke-DatabricksCli {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments
    )

    $previousPreference = $ErrorActionPreference
    $previousPyWarnings = $env:PYTHONWARNINGS

    try {
        # Databricks CLI 0.18 can emit urllib3 FutureWarning on stderr; do not treat that as a hard failure.
        $ErrorActionPreference = "Continue"
        $env:PYTHONWARNINGS = "ignore::FutureWarning"
        $rawOutput = & databricks @Arguments 2>&1
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousPreference
        $env:PYTHONWARNINGS = $previousPyWarnings
    }

    $textOutput = ($rawOutput | ForEach-Object { $_.ToString() }) -join "`n"
    return [PSCustomObject]@{
        ExitCode = $exitCode
        Output   = $textOutput
    }
}

# 1) Verify Databricks CLI availability.
if (-not (Get-Command databricks -ErrorAction SilentlyContinue)) {
    Write-Error "Databricks CLI not found. Install it first (pip install databricks-cli)."
    exit 1
}

# 2) Verify CLI auth by attempting to list scopes.
Write-Host "Checking Databricks CLI authentication..." -ForegroundColor Cyan
$scopesResult = Invoke-DatabricksCli -Arguments @("secrets", "list-scopes")
if ($scopesResult.ExitCode -ne 0) {
    Write-Error "Databricks CLI is not authenticated. Configure DATABRICKS_HOST and DATABRICKS_TOKEN or ~/.databrickscfg first."
    exit 1
}

# 3) Ensure secret scope exists.
if ($scopesResult.Output -notmatch "\b$ScopeName\b") {
    Write-Host "Creating Databricks secret scope '$ScopeName'..." -ForegroundColor Cyan
    $createScopeResult = Invoke-DatabricksCli -Arguments @("secrets", "create-scope", "--scope", $ScopeName)
    if ($createScopeResult.ExitCode -ne 0) {
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

$putHostResult = Invoke-DatabricksCli -Arguments @("secrets", "put", "--scope", $ScopeName, "--key", "DATABRICKS_SERVER_HOSTNAME", "--string-value", $hostname)
if ($putHostResult.ExitCode -ne 0) { Write-Error "Failed storing DATABRICKS_SERVER_HOSTNAME"; exit 1 }

$putPathResult = Invoke-DatabricksCli -Arguments @("secrets", "put", "--scope", $ScopeName, "--key", "DATABRICKS_HTTP_PATH", "--string-value", $httpPath)
if ($putPathResult.ExitCode -ne 0) { Write-Error "Failed storing DATABRICKS_HTTP_PATH"; exit 1 }

$putTokenResult = Invoke-DatabricksCli -Arguments @("secrets", "put", "--scope", $ScopeName, "--key", "DATABRICKS_TOKEN", "--string-value", $tokenPlain)
if ($putTokenResult.ExitCode -ne 0) { Write-Error "Failed storing DATABRICKS_TOKEN"; exit 1 }

Write-Host ""
Write-Host "Done! Secrets stored in Databricks scope '$ScopeName'." -ForegroundColor Green
Write-Host "Run '.\load-databricks-secrets.ps1' to load them and start the ingest script." -ForegroundColor Green

