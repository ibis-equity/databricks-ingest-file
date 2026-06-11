from __future__ import annotations

import argparse
import os
import subprocess
from dataclasses import dataclass
from shutil import which

from databricks import sql

DEFAULT_VAULT_NAME = "kv-dbr-ingest-a06f24"
DEFAULT_QUERY = "SELECT 1 AS ok"


@dataclass(frozen=True)
class ConnectionSettings:
    server_hostname: str
    http_path: str
    token: str


def _normalize_host(hostname: str) -> str:
    hostname = hostname.strip()
    if hostname.startswith("http://") or hostname.startswith("https://"):
        return hostname
    return f"https://{hostname}"


def _read_secret_from_key_vault(vault_name: str, secret_name: str) -> str:
    if not which("az"):
        raise RuntimeError("Azure CLI (az) is required to load secrets from Key Vault")

    result = subprocess.run(
        [
            "az",
            "keyvault",
            "secret",
            "show",
            "--vault-name",
            vault_name,
            "--name",
            secret_name,
            "--query",
            "value",
            "-o",
            "tsv",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to read secret '{secret_name}' from vault '{vault_name}': {result.stderr.strip() or result.stdout.strip()}"
        )
    value = result.stdout.strip()
    if not value:
        raise RuntimeError(f"Secret '{secret_name}' from vault '{vault_name}' was empty")
    return value
def load_connection_settings(vault_name: str | None = None) -> ConnectionSettings:
    hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    token = os.getenv("DATABRICKS_TOKEN")

    if hostname and http_path and token:
        return ConnectionSettings(hostname, http_path, token)

    vault = vault_name or os.getenv("DATABRICKS_KEYVAULT_NAME") or DEFAULT_VAULT_NAME
    return ConnectionSettings(
        server_hostname=_read_secret_from_key_vault(vault, "DATABRICKS-SERVER-HOSTNAME"),
        http_path=_read_secret_from_key_vault(vault, "DATABRICKS-HTTP-PATH"),
        token=_read_secret_from_key_vault(vault, "DATABRICKS-TOKEN"),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify Databricks SQL connectivity.")
    parser.add_argument("--vault-name", default=None, help="Azure Key Vault name to read secrets from if env vars are missing.")
    parser.add_argument("--query", default=DEFAULT_QUERY, help="SQL query used for the connectivity check.")
    return parser.parse_args()


def run_check(settings: ConnectionSettings, query: str) -> int:
    conn = sql.connect(
        server_hostname=_normalize_host(settings.server_hostname),
        http_path=settings.http_path,
        access_token=settings.token,
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            row = cursor.fetchone()
            print(f"Databricks SQL connectivity OK: {row[0] if row else 'no rows returned'}")
    finally:
        conn.close()
    return 0


def main() -> int:
    args = parse_args()
    settings = load_connection_settings(args.vault_name)
    return run_check(settings, args.query)


if __name__ == "__main__":
    raise SystemExit(main())


