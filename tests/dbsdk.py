from __future__ import annotations

import argparse

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.platform import NotFound, PermissionDenied


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="List paths through Databricks SDK")
    parser.add_argument(
        "--target",
        choices=["workspace", "dbfs", "files"],
        default="workspace",
        help="API target: workspace objects, dbfs utilities, or files API.",
    )
    parser.add_argument("--path", default="/", help="Path to list (default: /)")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        client = WorkspaceClient()
    except ValueError as exc:
        print(f"Authentication/config error: {exc}")
        print("Set DATABRICKS_HOST and DATABRICKS_TOKEN, or configure ~/.databrickscfg")
        return 1

    try:
        if args.target == "workspace":
            items = list(client.workspace.list(args.path))
        elif args.target == "files":
            items = list(client.files.list_directory_contents(args.path))
        else:
            items = list(client.dbutils.fs.ls(args.path))
    except PermissionDenied as exc:
        print(f"Permission denied for {args.target} path '{args.path}': {exc}")
        if args.target == "dbfs" and args.path == "/":
            print("Tip: Public DBFS root '/' is disabled. Use --target workspace --path /")
        return 1
    except NotFound as exc:
        print(f"Path not found: {args.path}: {exc}")
        return 1
    except Exception as exc:  # noqa: BLE001
        if args.target == "files" and args.path.rstrip("/") == "/Volumes":
            print("Files API requires a full volume path like /Volumes/<catalog>/<schema>/<volume>")
            print(f"Original error: {exc}")
            return 1
        raise

    for item in items:
        print(item.path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
