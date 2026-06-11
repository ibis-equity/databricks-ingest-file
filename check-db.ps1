# check-db.ps1
# Convenience wrapper for databricks_healthcheck.py.
# Examples:
#   .\check-db.ps1
#   .\check-db.ps1 --vault-name kv-dbr-ingest-a06f24
#   .\check-db.ps1 --query "SELECT current_timestamp()"

param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Args
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$healthcheckPath = Join-Path $scriptDir "databricks_healthcheck.py"

if (-not (Test-Path $healthcheckPath)) {
    Write-Error "Could not find databricks_healthcheck.py at $healthcheckPath"
    exit 1
}

python $healthcheckPath @Args
exit $LASTEXITCODE

