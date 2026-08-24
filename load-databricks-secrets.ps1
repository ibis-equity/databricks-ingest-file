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

# Parse -ScopeName / -ProfileName from args; pass everything else to main.py
$ScopeName = "ingest-secrets"
$ProfileName = "DEFAULT"
$ProfileSpecified = $false
if ($env:DATABRICKS_CONFIG_PROFILE) {
    $ProfileName = $env:DATABRICKS_CONFIG_PROFILE
    $ProfileSpecified = $true
}
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
    } elseif ($args[$i] -eq "-ProfileName" -or $args[$i] -eq "--ProfileName") {
        $i++
        if ($i -ge $args.Count) {
            Write-Error "Missing value for -ProfileName"
            exit 1
        }
        $ProfileName = $args[$i]
        $ProfileSpecified = $true
    } else {
        $MainArgs += $args[$i]
    }
    $i++
}

$ErrorActionPreference = "Stop"

# 1) Resolve Python interpreter.
$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoPython = Join-Path $RepoRoot ".venv\Scripts\python.exe"

if (Test-Path $RepoPython) {
    $PythonCmd = $RepoPython
} else {
    $PythonCmd = (Get-Command python -ErrorAction SilentlyContinue).Source
}

if (-not $PythonCmd) {
    Write-Error "Python is required to fetch Databricks secrets via SDK."
    exit 1
}

# 2) Resolve Databricks profile.
if (Get-Command databricks -ErrorAction SilentlyContinue) {
    $previousPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $profilesRaw = & databricks auth profiles -o json 2>&1
    } finally {
        $ErrorActionPreference = $previousPreference
    }
    $profilesText = ($profilesRaw | ForEach-Object { $_.ToString() }) -join "`n"
    $jsonStart = $profilesText.IndexOf("{")
    if ($jsonStart -ge 0) {
        $profilesJsonText = $profilesText.Substring($jsonStart)
        try {
            $profiles = $profilesJsonText | ConvertFrom-Json
            $profileMatch = $profiles.profiles | Where-Object { $_.name -eq $ProfileName } | Select-Object -First 1
            $ValidProfiles = @($profiles.profiles | Where-Object { $_.valid -eq $true } | ForEach-Object { $_.name })
            if ($profileMatch -and $profileMatch.valid) {
                # keep current profile
            } elseif ($ProfileSpecified) {
                Write-Error "Databricks profile '$ProfileName' is not valid. Run: databricks auth profiles"
                exit 1
            } else {
                $fallback = $profiles.profiles | Where-Object { $_.valid -eq $true } | Select-Object -First 1
                if ($fallback) {
                    $ProfileName = $fallback.name
                    Write-Host "Using Databricks profile '$ProfileName' (DEFAULT is not valid)." -ForegroundColor Yellow
                }
            }
        } catch {
            # If profile inspection fails, continue with requested profile.
        }
    }
}

# 2) Pull secrets from Databricks Secret Scope via SDK.
$candidateProfiles = @($ProfileName)
if (-not $ProfileSpecified -and $ValidProfiles) {
    $candidateProfiles = @($ValidProfiles)
}

$secrets = $null
$allErrors = @()
foreach ($candidate in $candidateProfiles) {
    Write-Host "Loading secrets from Databricks scope: $ScopeName (profile: $candidate)" -ForegroundColor Cyan
    $env:DATABRICKS_CONFIG_PROFILE = $candidate
    Remove-Item Env:DATABRICKS_TOKEN -ErrorAction SilentlyContinue
    Remove-Item Env:DATABRICKS_HOST -ErrorAction SilentlyContinue

    $previousPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $secretRaw = & $PythonCmd -u -c "import json,sys; from databricks.sdk import WorkspaceClient; from databricks.sdk.errors.platform import PermissionDenied, ResourceDoesNotExist; scope=sys.argv[1]; w=WorkspaceClient();
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
        $pythonExitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousPreference
    }

    $secretText = ($secretRaw | ForEach-Object { $_.ToString() }) -join "`n"
    if ($pythonExitCode -ne 0) {
        $allErrors += "[$candidate] $secretText"
        continue
    }

    try {
        $secrets = $secretText | ConvertFrom-Json
        $ProfileName = $candidate
        break
    } catch {
        $allErrors += "[$candidate] Loaded output was not valid JSON. Details: $secretText"
    }
}

if (-not $secrets) {
    Write-Error "Failed to load secrets from scope '$ScopeName'. Tried profiles: $($candidateProfiles -join ', '). Details: $($allErrors -join ' | ')"
    exit 1
}
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
Write-Host "Running: $PythonCmd .\main.py $allArgs" -ForegroundColor Cyan
& $PythonCmd .\main.py @allArgs
