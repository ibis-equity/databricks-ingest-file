# setup-keyvault.ps1
# One-time script to create an Azure Key Vault and store Databricks credentials.
# Run this once, then use load-secrets.ps1 for day-to-day use.
#
# Usage:
#   .\setup-keyvault.ps1

param(
    [string]$VaultName      = "kv-dbr-ingest-a06f24",
    [string]$ResourceGroup  = "rg-databricks-ingest",
    [string]$Location       = "eastus"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ── 1. Login ──────────────────────────────────────────────────────────────────
Write-Host "Checking Azure login..." -ForegroundColor Cyan
$account = az account show 2>&1
if ($LASTEXITCODE -ne 0) {
    az login
}

# ── 2. Create resource group (if it doesn't exist) ───────────────────────────
Write-Host "Ensuring resource group '$ResourceGroup' exists..." -ForegroundColor Cyan
az group create --name $ResourceGroup --location $Location --output none

# ── 3. Create Key Vault (if it doesn't exist) ────────────────────────────────
Write-Host "Ensuring Key Vault '$VaultName' exists..." -ForegroundColor Cyan
$prevPref = $ErrorActionPreference
$ErrorActionPreference = "Continue"
$null = az keyvault show --name $VaultName --resource-group $ResourceGroup 2>&1
$vaultExists = ($LASTEXITCODE -eq 0)
$ErrorActionPreference = $prevPref

if (-not $vaultExists) {
    Write-Host "Creating Key Vault '$VaultName'..." -ForegroundColor Cyan
    az keyvault create --name $VaultName --resource-group $ResourceGroup --location $Location --output none
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to create Key Vault. Check your Azure subscription and permissions."
        exit 1
    }
    Write-Host "Key Vault created." -ForegroundColor Green
} else {
    Write-Host "Key Vault already exists." -ForegroundColor Yellow
}

# ── 4. Grant current user access (RBAC) ──────────────────────────────────────
Write-Host "Granting current user 'Key Vault Secrets Officer' role..." -ForegroundColor Cyan
$vaultId = az keyvault show --name $VaultName --resource-group $ResourceGroup --query id -o tsv
$userId  = az ad signed-in-user show --query id -o tsv
az role assignment create --assignee $userId --role "Key Vault Secrets Officer" --scope $vaultId --output none

# ── 5. Prompt for secrets and store them ─────────────────────────────────────
Write-Host ""
Write-Host "Enter your Databricks credentials to store in Key Vault." -ForegroundColor Cyan
Write-Host "(Values will NOT be shown on screen)" -ForegroundColor DarkGray

$hostname  = Read-Host "DATABRICKS_SERVER_HOSTNAME (e.g. adb-xxx.azuredatabricks.net)"
$httpPath  = Read-Host "DATABRICKS_HTTP_PATH (e.g. /sql/1.0/warehouses/xxx)"
$token     = Read-Host "DATABRICKS_TOKEN (dapi...)" -AsSecureString
$tokenPlain = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
                [Runtime.InteropServices.Marshal]::SecureStringToBSTR($token))

Write-Host ""
Write-Host "Storing secrets in '$VaultName'..." -ForegroundColor Cyan

az keyvault secret set --vault-name $VaultName --name "DATABRICKS-SERVER-HOSTNAME" --value $hostname  --output none
az keyvault secret set --vault-name $VaultName --name "DATABRICKS-HTTP-PATH"       --value $httpPath  --output none
az keyvault secret set --vault-name $VaultName --name "DATABRICKS-TOKEN"           --value $tokenPlain --output none

Write-Host ""
Write-Host "Done! Secrets stored in Key Vault '$VaultName'." -ForegroundColor Green
Write-Host "Run '.\load-secrets.ps1' to load them and start the ingest script." -ForegroundColor Green

