# Databricks Folder Ingest to Delta

This project watches a local folder for input files and uploads each new file into a Databricks Delta table.

## Supported input files

- `.csv`
- `.json` (JSON Lines)
- `.parquet`

## What it does

- Detects files in `SOURCE_DIR` matching `FILE_PATTERN`
- Reads data into a pandas DataFrame
- Adds metadata columns:
  - `_ingested_at` (UTC timestamp)
  - `_source_file` (filename)
- Creates a unique Delta table for each upload using:
  - schema: `etl-bronze-pipelne-data`
  - table prefix: `bronze-ingest-`
  - timestamp suffix: `YYYYMMDD_HHMMSS_microseconds`
  - source file suffix: sanitized file stem
  - deterministic hash suffix: 8-char SHA1 from file stem
  - optional full-name cap: `MAX_FULL_TABLE_NAME_LENGTH` (default `255`)
- Inserts rows into the generated Delta table
- Tracks processed files in a local state file
- Moves each successfully uploaded file to a processed folder
- Listens for file-drop events in the source folder (default run mode)

## Install

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Required Databricks environment variables

```powershell
$env:DATABRICKS_SERVER_HOSTNAME = "adb-xxxxxxxxxxxxxxxx.xx.azuredatabricks.net"
$env:DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/xxxxxxxxxxxxxxxx"
$env:DATABRICKS_TOKEN = "dapi..."
```

## Secrets Manager (Azure Key Vault) — recommended

Store credentials safely in Azure Key Vault instead of hardcoding them.

### One-time setup (creates vault, stores all 3 secrets)

```powershell
.\setup-keyvault.ps1
```

### Load secrets and run (daily use)

```powershell
# Continuous event listener (default)
.\load-secrets.ps1

# Run once
.\load-secrets.ps1 --once

# Dry run (no Databricks upload)
.\load-secrets.ps1 --once --dry-run

# Custom vault name
.\load-secrets.ps1 -VaultName my-vault --once
```

## Run once

```powershell
python .\main.py --source-dir .\inbox --pattern "*.csv" --schema-name "etl-bronze-pipelne-data" --table-prefix "bronze-ingest-" --once
```

## Run continuously (event listener, default)

```powershell
python .\main.py --source-dir .\inbox --pattern "*.csv" --schema-name "etl-bronze-pipelne-data" --table-prefix "bronze-ingest-"
```

## Run continuously (event listener with subfolders)

```powershell
python .\main.py --source-dir .\inbox --pattern "*.csv" --watch-recursive
```

## Run continuously (event listener with subfolder depth limit)

```powershell
python .\main.py --source-dir .\inbox --pattern "*.csv" --watch-recursive --watch-depth 1
```

## Run continuously (poll fallback)

```powershell
python .\main.py --source-dir .\inbox --pattern "*.csv" --mode poll --poll-seconds 15
```

## Dry run (no Databricks upload)

```powershell
python .\main.py --source-dir .\inbox --pattern "*.csv" --once --dry-run
```

## Optional table naming env vars

```powershell
$env:SCHEMA_NAME = "etl-bronze-pipelne-data"
$env:TABLE_PREFIX = "bronze-ingest-"
$env:MAX_FULL_TABLE_NAME_LENGTH = "255"
$env:PROCESSED_DIR = ".\\processed"
$env:RUN_MODE = "event"
$env:WATCH_RECURSIVE = "true"
$env:WATCH_DEPTH = "1"
```

## Processed folder location

```powershell
python .\main.py --source-dir .\inbox --processed-dir .\processed --once
```

## Troubleshooting: reproduce file hash token

Use this to generate the same 8-character SHA1 hash suffix used in table names for a file stem.

```powershell
python -c "import hashlib; stem='sample'; print(hashlib.sha1(stem.encode('utf-8')).hexdigest()[:8])"
```

Or pass the stem as an argument:

```powershell
python -c "import hashlib,sys; stem=sys.argv[1]; print(hashlib.sha1(stem.encode('utf-8')).hexdigest()[:8])" sample2
```

## Notes

- The script currently infers SQL column types from pandas dtypes.
- For large files, increase `--batch-size` or use Databricks Auto Loader for production-scale ingestion.

