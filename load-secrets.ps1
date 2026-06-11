# load-secrets.ps1
# Loads Databricks credentials from Azure Key Vault into environment variables,
# then runs main.py with any arguments you pass through.
#
# Usage:
#   .\load-secrets.ps1                                          # continuous event mode
#   .\load-secrets.ps1 --once                                   # run once
#   .\load-secrets.ps1 --once --dry-run                         # dry run
#   .\load-secrets.ps1 --mode poll --poll-seconds 15            # poll mode
#   .\load-secrets.ps1 -VaultName my-vault --once               # custom vault

# ── Parse -VaultName from args; pass everything else to main.py ──────────────
$VaultName = "kv-dbr-ingest-a06f24"
$MainArgs  = @()
$i = 0
# Capture only -VaultName for this script; forward all other args to Python unchanged.
while ($i -lt $args.Count) {
    if ($args[$i] -eq "-VaultName" -or $args[$i] -eq "--VaultName") {
        $i++
        $VaultName = $args[$i]
    } else {
        $MainArgs += $args[$i]
    }
    $i++
}

$ErrorActionPreference = "Stop"

# ── 1. Verify az CLI is available ────────────────────────────────────────────
if (-not (Get-Command az -ErrorAction SilentlyContinue)) {
    Write-Error "Azure CLI not found. Install from https://aka.ms/installazurecliwindows"
    exit 1
}

# ── 2. Check login ────────────────────────────────────────────────────────────
Write-Host "Checking Azure login..." -ForegroundColor Cyan
$null = az account show 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "Not logged in. Running az login..." -ForegroundColor Yellow
    az login
}

# ── 3. Pull secrets from Key Vault ───────────────────────────────────────────
Write-Host "Loading secrets from Key Vault: $VaultName" -ForegroundColor Cyan

function Get-Secret($name) {
    # Query just the secret value as plain text (tsv), not JSON.
    $val = az keyvault secret show --vault-name $VaultName --name $name --query value -o tsv 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to read secret '$name' from vault '$VaultName'. Check vault name and permissions."
        exit 1
    }
    return $val
}

$env:DATABRICKS_SERVER_HOSTNAME = Get-Secret "DATABRICKS-SERVER-HOSTNAME"
$env:DATABRICKS_HTTP_PATH       = Get-Secret "DATABRICKS-HTTP-PATH"
$env:DATABRICKS_TOKEN           = Get-Secret "DATABRICKS-TOKEN"

Write-Host "Secrets loaded:" -ForegroundColor Green
Write-Host "  DATABRICKS_SERVER_HOSTNAME = $env:DATABRICKS_SERVER_HOSTNAME"
Write-Host "  DATABRICKS_HTTP_PATH       = $env:DATABRICKS_HTTP_PATH"
# Show only the first few token characters so logs do not expose full credentials.
Write-Host "  DATABRICKS_TOKEN           = $($env:DATABRICKS_TOKEN.Substring(0, [Math]::Min(8, $env:DATABRICKS_TOKEN.Length)))..." -ForegroundColor DarkGray

# ── 4. Run main.py ────────────────────────────────────────────────────────────
$defaultArgs = @(
    "--source-dir", ".\inbox",
    "--pattern", "*.csv",
    "--schema-name", "etl-bronze-pipeline-data",
    "--table-prefix", "bronze-ingest-"
)

# Defaults first, then user-provided args so callers can append run-mode flags (e.g., --once).
$allArgs = $defaultArgs + $MainArgs

Write-Host ""
Write-Host "Running: python .\main.py $allArgs" -ForegroundColor Cyan
python .\main.py @allArgs
