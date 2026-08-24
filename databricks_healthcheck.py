from __future__ import annotations  # Enable postponed evaluation of type annotations.

import argparse  # Parse command-line flags for this utility script.
import os  # Read environment variables.
import subprocess  # Execute Azure CLI commands for Key Vault lookup.
from dataclasses import dataclass  # Define a lightweight typed container for connection settings.
from shutil import which  # Check whether external executables (like az) are available.

from databricks import sql  # Use the Databricks SQL connector to open and test a connection.
from databricks.sdk import WorkspaceClient  # Use Databricks Workspace API for cluster status checks.
from databricks.sdk.errors.platform import NotFound, PermissionDenied  # Provide precise failures for cluster checks.

DEFAULT_VAULT_NAME = "kv-dbr-ingest-a06f24"  # Fallback Key Vault name when no vault is provided.
DEFAULT_QUERY = "SELECT 1 AS ok"  # Default query used to validate SQL connectivity.
READY_CLUSTER_STATES = {"RUNNING", "RESIZING"}  # Treat active and scaling-up clusters as ready.


@dataclass(frozen=True)  # Make settings immutable after creation for safer handling.
class ConnectionSettings:
    server_hostname: str  # Databricks workspace host (with or without scheme).
    http_path: str  # Databricks SQL warehouse HTTP path.
    token: str  # Personal access token used for Databricks SQL authentication.


def _normalize_host(hostname: str) -> str:
    hostname = hostname.strip()  # Remove accidental leading/trailing whitespace.
    if hostname.startswith("http://") or hostname.startswith("https://"):  # Keep host as-is if scheme already exists.
        return hostname  # Return the already-normalized host.
    return f"https://{hostname}"  # Otherwise default to HTTPS scheme.


def _read_secret_from_key_vault(vault_name: str, secret_name: str) -> str:
    if not which("az"):  # Ensure Azure CLI is installed and on PATH.
        raise RuntimeError("Azure CLI (az) is required to load secrets from Key Vault")  # Fail fast when az is unavailable.

    result = subprocess.run(  # Ask Azure Key Vault for a single secret value.
        [
            "az",  # Azure CLI executable.
            "keyvault",  # Key Vault command group.
            "secret",  # Secret subcommand group.
            "show",  # Read one secret by name.
            "--vault-name",  # Flag for which vault to query.
            vault_name,  # Vault name argument value.
            "--name",  # Flag for which secret key to read.
            secret_name,  # Secret key argument value.
            "--query",  # Limit response to a JMESPath selection.
            "value",  # Select only the secret value property.
            "-o",  # Choose output format.
            "tsv",  # Output plain text (no JSON wrapper).
        ],
        check=False,  # Handle non-zero exit codes manually with custom errors.
        capture_output=True,  # Capture stdout/stderr instead of printing directly.
        text=True,  # Decode output as text strings.
    )
    if result.returncode != 0:  # If az failed, raise an informative error.
        raise RuntimeError(
            f"Failed to read secret '{secret_name}' from vault '{vault_name}': {result.stderr.strip() or result.stdout.strip()}"
        )
    value = result.stdout.strip()  # Trim whitespace/newlines from returned secret text.
    if not value:  # Ensure the secret exists and is non-empty.
        raise RuntimeError(f"Secret '{secret_name}' from vault '{vault_name}' was empty")
    return value  # Return the resolved secret value.


def load_connection_settings(vault_name: str | None = None) -> ConnectionSettings:
    hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")  # Try env var for host first.
    http_path = os.getenv("DATABRICKS_HTTP_PATH")  # Try env var for SQL warehouse HTTP path.
    token = os.getenv("DATABRICKS_TOKEN")  # Try env var for token.

    if hostname and http_path and token:  # Prefer env-based config when all required values are present.
        return ConnectionSettings(hostname, http_path, token)

    vault = vault_name or os.getenv("DATABRICKS_KEYVAULT_NAME") or DEFAULT_VAULT_NAME  # Resolve vault in priority order.
    return ConnectionSettings(  # Fall back to loading all required values from Key Vault.
        server_hostname=_read_secret_from_key_vault(vault, "DATABRICKS-SERVER-HOSTNAME"),  # Read host secret.
        http_path=_read_secret_from_key_vault(vault, "DATABRICKS-HTTP-PATH"),  # Read HTTP path secret.
        token=_read_secret_from_key_vault(vault, "DATABRICKS-TOKEN"),  # Read token secret.
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify Databricks SQL connectivity.")  # Create CLI parser.
    parser.add_argument("--vault-name", default=None, help="Azure Key Vault name to read secrets from if env vars are missing.")  # Optional vault override.
    parser.add_argument("--query", default=DEFAULT_QUERY, help="SQL query used for the connectivity check.")  # Optional query override.
    parser.add_argument("--cluster-id", default=os.getenv("DATABRICKS_CLUSTER_ID"), help="Optional Databricks all-purpose cluster ID to verify readiness before SQL check.")  # Optional cluster readiness target.
    return parser.parse_args()  # Return parsed arguments.


def _cluster_state_text(cluster_state: object) -> str:
    if cluster_state is None:  # Handle missing state from API response.
        return "UNKNOWN"  # Return a stable unknown marker.
    return str(getattr(cluster_state, "value", cluster_state)).upper()  # Normalize enum/string state to uppercase text.


def check_cluster_readiness(settings: ConnectionSettings, cluster_id: str) -> None:
    workspace = WorkspaceClient(  # Initialize workspace API client with same host/token used for SQL check.
        host=_normalize_host(settings.server_hostname),  # Ensure host is HTTPS URL.
        token=settings.token,  # Use PAT from env or Key Vault.
    )
    try:
        cluster = workspace.clusters.get(cluster_id=cluster_id)  # Fetch cluster metadata by ID.
    except PermissionDenied as exc:
        raise RuntimeError(f"Permission denied while checking cluster '{cluster_id}': {exc}") from exc  # Surface auth/ACL problems clearly.
    except NotFound as exc:
        raise RuntimeError(f"Cluster '{cluster_id}' was not found.") from exc  # Surface invalid cluster IDs clearly.

    state_text = _cluster_state_text(cluster.state)  # Convert state to normalized text for comparison/output.
    if state_text not in READY_CLUSTER_STATES:  # Fail if cluster is not in a ready state.
        message_text = getattr(cluster, "state_message", None)  # Read Databricks state details when available.
        detail = f" ({message_text})" if message_text else ""  # Build optional details suffix.
        raise RuntimeError(f"Cluster '{cluster_id}' is not ready: state={state_text}{detail}")  # Raise actionable readiness error.
    print(f"Cluster readiness OK: {cluster_id} state={state_text}")  # Print success signal for readiness phase.


def run_check(settings: ConnectionSettings, query: str) -> int:
    conn = sql.connect(  # Open a Databricks SQL connection.
        server_hostname=_normalize_host(settings.server_hostname),  # Ensure host includes scheme.
        http_path=settings.http_path,  # Use provided SQL warehouse HTTP path.
        access_token=settings.token,  # Use provided PAT token.
    )
    try:  # Always close the connection, even if query execution fails.
        with conn.cursor() as cursor:  # Open cursor for query execution.
            cursor.execute(query)  # Execute healthcheck SQL query.
            row = cursor.fetchone()  # Read the first row (if any).
            print(f"Databricks SQL connectivity OK: {row[0] if row else 'no rows returned'}")  # Print success signal.
    finally:
        conn.close()  # Close connection resource.
    return 0  # Return zero exit code for successful run.


def main() -> int:
    args = parse_args()  # Parse user-provided command-line options.
    settings = load_connection_settings(args.vault_name)  # Resolve connection settings from env or Key Vault.
    if args.cluster_id:  # Run readiness check only when a cluster ID is provided.
        check_cluster_readiness(settings, args.cluster_id)  # Validate cluster state before executing SQL query.
    return run_check(settings, args.query)  # Execute health check and forward its exit code.


if __name__ == "__main__":
    raise SystemExit(main())  # Run main when executed as a script and propagate exit status.
