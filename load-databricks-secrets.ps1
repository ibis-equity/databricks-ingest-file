# load-databricks-secrets.ps1
# Loads Databricks credentials from Databricks Secret Scope into environment variables,
# then runs main.py with any arguments you pass through.
#
# Usage:
#   .\load-databricks-secrets.ps1                                          # continuous event mode
#   .\load-databricks-secrets.ps1 --once                                   # run once
#   .\load-databricks-secrets.ps1 --once --dry-run                         # dry run
#   .\load-databricks-secrets.ps1 --mode poll --poll-seconds 15            # poll mode
#   .\load-databricks-secrets.ps1 -ScopeName ingest-secrets --once          # custom scope

# Parse -ScopeName from args; pass everything else to main.py
$ScopeName = "ingest-secrets"
$MainArgs = @()
$i = 0
while ($i -lt $args.Count) {
    if ($args[$i] -eq "-ScopeName" -or $args[$i] -eq "--ScopeName") {
        $i++
        if ($i -ge $args.Count) {
            Write-Error "Missing value for -ScopeName"
            exit 1
        }
        $ScopeName = $args[$i]
    } else {
        $MainArgs += $args[$i]
    }
    $i++
}

$ErrorActionPreference = "Stop"

# 1) Verify Python availability.
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    Write-Error "Python is required to fetch Databricks secrets via SDK."
    exit 1
}

# 2) Pull secrets from Databricks Secret Scope via SDK.
Write-Host "Loading secrets from Databricks scope: $ScopeName" -ForegroundColor Cyan

$secretJson = python -u -c "import json,sys; from databricks.sdk import WorkspaceClient; from databricks.sdk.errors.platform import PermissionDenied, ResourceDoesNotExist; scope=sys.argv[1]; w=WorkspaceClient();
try:
    data={
        'DATABRICKS_SERVER_HOSTNAME': w.dbutils.secrets.get(scope, 'DATABRICKS_SERVER_HOSTNAME'),
        'DATABRICKS_HTTP_PATH': w.dbutils.secrets.get(scope, 'DATABRICKS_HTTP_PATH'),
        'DATABRICKS_TOKEN': w.dbutils.secrets.get(scope, 'DATABRICKS_TOKEN')
    }
    print(json.dumps(data))
except (PermissionDenied, ResourceDoesNotExist) as e:
    print(f'ERROR: {e}')
    sys.exit(2)
" $ScopeName 2>&1

if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to load secrets from scope '$ScopeName'. Ensure DATABRICKS_HOST and DATABRICKS_TOKEN (or ~/.databrickscfg) are configured and you have access to the scope. Details: $secretJson"
    exit 1
}

$secrets = $secretJson | ConvertFrom-Json
$env:DATABRICKS_SERVER_HOSTNAME = $secrets.DATABRICKS_SERVER_HOSTNAME
$env:DATABRICKS_HTTP_PATH = $secrets.DATABRICKS_HTTP_PATH
$env:DATABRICKS_TOKEN = $secrets.DATABRICKS_TOKEN

Write-Host "Secrets loaded:" -ForegroundColor Green
Write-Host "  DATABRICKS_SERVER_HOSTNAME = $env:DATABRICKS_SERVER_HOSTNAME"
Write-Host "  DATABRICKS_HTTP_PATH       = $env:DATABRICKS_HTTP_PATH"
Write-Host "  DATABRICKS_TOKEN           = $($env:DATABRICKS_TOKEN.Substring(0, [Math]::Min(8, $env:DATABRICKS_TOKEN.Length)))..." -ForegroundColor DarkGray

# 3) Run main.py
$defaultArgs = @(
    "--source-dir", ".\inbox",
    "--pattern", "*.csv",
    "--schema-name", "etl-bronze-pipeline-data",
    "--table-prefix", "bronze-ingest-"
)

$allArgs = $defaultArgs + $MainArgs

Write-Host ""
Write-Host "Running: python .\main.py $allArgs" -ForegroundColor Cyan
python .\main.py @allArgs

