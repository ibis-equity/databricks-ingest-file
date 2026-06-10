from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import math
import os
import time
from queue import Empty, Queue
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Iterator, TypeVar
T = TypeVar("T")



import pandas as pd
from databricks import sql
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer


@dataclass(frozen=True)
class AppConfig:
    source_dir: Path
    file_pattern: str
    schema_name: str
    table_prefix: str
    max_full_table_name_length: int
    state_file: Path
    processed_dir: Path
    mode: str
    watch_recursive: bool
    watch_depth: int | None
    poll_seconds: int
    batch_size: int
    dry_run: bool
    once: bool


def parse_args() -> AppConfig:
    watch_depth_env = os.getenv("WATCH_DEPTH")
    parser = argparse.ArgumentParser(
        description="Detect files in a folder and upload them to a Databricks Delta table."
    )
    parser.add_argument("--source-dir", default=os.getenv("SOURCE_DIR", "./inbox"))
    parser.add_argument("--pattern", default=os.getenv("FILE_PATTERN", "*.csv"))
    parser.add_argument(
        "--schema-name",
        default=os.getenv("SCHEMA_NAME", "etl-bronze-pipelne-data"),
    )
    parser.add_argument(
        "--table-prefix",
        default=os.getenv("TABLE_PREFIX", "bronze-ingest-"),
    )
    parser.add_argument(
        "--max-full-table-name-length",
        type=int,
        default=int(os.getenv("MAX_FULL_TABLE_NAME_LENGTH", "255")),
    )
    parser.add_argument(
        "--state-file",
        default=os.getenv("STATE_FILE", "./.processed_files.json"),
    )
    parser.add_argument(
        "--processed-dir",
        default=os.getenv("PROCESSED_DIR"),
        help="Directory where uploaded files are moved. Defaults to <source-dir>/processed.",
    )
    parser.add_argument(
        "--mode",
        choices=["event", "poll"],
        default=os.getenv("RUN_MODE", "event"),
        help="Use 'event' to listen for file system events or 'poll' for interval scanning.",
    )
    parser.add_argument(
        "--watch-recursive",
        action="store_true",
        default=os.getenv("WATCH_RECURSIVE", "false").lower() == "true",
        help="Watch source subfolders too.",
    )
    parser.add_argument(
        "--watch-depth",
        type=int,
        default=int(watch_depth_env) if watch_depth_env is not None else None,
        help="Max subfolder depth when --watch-recursive is enabled (0 means source folder only).",
    )
    parser.add_argument("--poll-seconds", type=int, default=int(os.getenv("POLL_SECONDS", "15")))
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("BATCH_SIZE", "500")))
    parser.add_argument("--dry-run", action="store_true", help="Parse files without uploading.")
    parser.add_argument("--once", action="store_true", help="Run one scan and exit.")
    args = parser.parse_args()

    if args.watch_depth is not None and args.watch_depth < 0:
        parser.error("--watch-depth must be >= 0")

    return AppConfig(
        source_dir=Path(args.source_dir).resolve(),
        file_pattern=args.pattern,
        schema_name=args.schema_name,
        table_prefix=args.table_prefix,
        max_full_table_name_length=args.max_full_table_name_length,
        state_file=Path(args.state_file).resolve(),
        processed_dir=(
            Path(args.processed_dir).resolve()
            if args.processed_dir
            else (Path(args.source_dir).resolve() / "processed")
        ),
        mode=args.mode,
        watch_recursive=args.watch_recursive,
        watch_depth=args.watch_depth,
        poll_seconds=args.poll_seconds,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
        once=args.once,
    )


def load_state(path: Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        return set(json.loads(path.read_text(encoding="utf-8")))
    except json.JSONDecodeError:
        return set()


def save_state(path: Path, processed: set[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(sorted(processed), indent=2), encoding="utf-8")


def directory_depth(root: Path, target: Path) -> int | None:
    try:
        relative = target.resolve().relative_to(root.resolve())
    except ValueError:
        return None
    return max(len(relative.parts) - 1, 0)


def within_depth(root: Path, target: Path, watch_depth: int | None) -> bool:
    if watch_depth is None:
        return True
    depth = directory_depth(root, target)
    return depth is not None and depth <= watch_depth


def discover_files(
    source_dir: Path,
    pattern: str,
    processed: set[str],
    recursive: bool,
    watch_depth: int | None,
) -> list[Path]:
    source_dir.mkdir(parents=True, exist_ok=True)
    iterator = source_dir.rglob(pattern) if recursive else source_dir.glob(pattern)
    files = sorted(
        [
            path
            for path in iterator
            if path.is_file()
            and str(path.resolve()) not in processed
            and within_depth(source_dir, path, watch_depth)
        ],
        key=lambda p: p.stat().st_mtime,
    )
    return files


def matches_pattern(path: Path, pattern: str) -> bool:
    return fnmatch.fnmatch(path.name, pattern)


def read_input_file(file_path: Path) -> pd.DataFrame:
    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(file_path)
    elif suffix == ".json":
        df = pd.read_json(file_path, lines=True)
    elif suffix == ".parquet":
        df = pd.read_parquet(file_path)
    else:
        raise ValueError(f"Unsupported file type: {suffix}")

    if df.empty:
        raise ValueError(f"No rows found in {file_path.name}")

    df.columns = [sanitize_column_name(col) for col in df.columns]
    df["_ingested_at"] = datetime.now(UTC)
    df["_source_file"] = str(file_path.name)
    return df


def sanitize_column_name(name: object) -> str:
    raw = str(name).strip().lower().replace(" ", "_")
    cleaned = "".join(ch if (ch.isalnum() or ch == "_") else "_" for ch in raw)
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    cleaned = cleaned.strip("_")
    return cleaned or "col"


def quote_ident(identifier: str) -> str:
    return ".".join(f"`{part}`" for part in identifier.split("."))


def build_unique_table_name(cfg: AppConfig, file_path: Path) -> str:
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S_%f")
    file_token = sanitize_column_name(file_path.stem)[:40]
    file_hash = hashlib.sha1(file_path.stem.encode("utf-8")).hexdigest()[:8]
    base_name = f"{cfg.table_prefix}{timestamp}"
    table_name = f"{base_name}_{file_token}_{file_hash}" if file_token else f"{base_name}_{file_hash}"

    max_table_name_length = cfg.max_full_table_name_length - len(cfg.schema_name) - 1
    timestamp_hash_core = f"{timestamp}_{file_hash}"
    if max_table_name_length < len(timestamp_hash_core):
        raise ValueError(
            "MAX_FULL_TABLE_NAME_LENGTH is too small for schema + timestamp + hash naming pattern"
        )

    if len(table_name) > max_table_name_length:
        table_name = f"{base_name}_{file_hash}"
    if len(table_name) > max_table_name_length:
        prefix_budget = max_table_name_length - len(timestamp_hash_core)
        table_name = f"{cfg.table_prefix[:prefix_budget]}{timestamp_hash_core}"

    return f"{cfg.schema_name}.{table_name}"


def to_sql_type(dtype: object) -> str:
    dtype_text = str(dtype)
    if "int" in dtype_text:
        return "BIGINT"
    if "float" in dtype_text:
        return "DOUBLE"
    if "bool" in dtype_text:
        return "BOOLEAN"
    if "datetime" in dtype_text:
        return "TIMESTAMP"
    return "STRING"


def sql_literal(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, float) and math.isnan(value):
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float, Decimal)):
        return str(value)
    if isinstance(value, datetime):
        return f"TIMESTAMP '{value.strftime('%Y-%m-%d %H:%M:%S')}'"
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    text = str(value).replace("'", "''")
    return f"'{text}'"


def connect_databricks() -> sql.client.Connection:
    hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    token = os.getenv("DATABRICKS_TOKEN")

    if not hostname or not http_path or not token:
        missing = [
            key
            for key, value in {
                "DATABRICKS_SERVER_HOSTNAME": hostname,
                "DATABRICKS_HTTP_PATH": http_path,
                "DATABRICKS_TOKEN": token,
            }.items()
            if not value
        ]
        raise RuntimeError(f"Missing required Databricks environment variables: {', '.join(missing)}")

    return sql.connect(server_hostname=hostname, http_path=http_path, access_token=token)


def ensure_table(cursor: sql.client.Cursor, table_name: str, df: pd.DataFrame) -> None:
    column_defs = ", ".join(
        f"`{column}` {to_sql_type(dtype)}" for column, dtype in df.dtypes.items()
    )
    create_sql = f"CREATE TABLE IF NOT EXISTS {quote_ident(table_name)} ({column_defs}) USING DELTA"
    cursor.execute(create_sql)


def ensure_schema(cursor: sql.client.Cursor, schema_name: str) -> None:
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(schema_name)}")


def batched(iterable: Iterable[T], batch_size: int) -> Iterator[list[T]]:
    batch: list[T] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def insert_dataframe(cursor: sql.client.Cursor, table_name: str, df: pd.DataFrame, batch_size: int) -> None:
    columns = list(df.columns)
    column_list = ", ".join(f"`{column}`" for column in columns)

    for rows in batched(df.itertuples(index=False, name=None), batch_size):
        value_sql = ", ".join(
            "(" + ", ".join(sql_literal(value) for value in row) + ")" for row in rows
        )
        insert_sql = f"INSERT INTO {quote_ident(table_name)} ({column_list}) VALUES {value_sql}"
        cursor.execute(insert_sql)


def process_file(file_path: Path, cfg: AppConfig, conn: sql.client.Connection | None) -> None:
    df = read_input_file(file_path)
    target_table = build_unique_table_name(cfg, file_path)
    print(f"Detected {file_path.name} ({len(df)} rows)")

    if cfg.dry_run:
        print(f"Dry-run mode: skipping Databricks upload to {target_table}")
        return

    if conn is None:
        raise RuntimeError("Databricks connection not initialized")

    with conn.cursor() as cursor:
        ensure_schema(cursor, cfg.schema_name)
        ensure_table(cursor, target_table, df)
        insert_dataframe(cursor, target_table, df, cfg.batch_size)
    print(f"Uploaded {file_path.name} to {target_table}")


def move_to_processed(file_path: Path, processed_dir: Path) -> None:
    processed_dir.mkdir(parents=True, exist_ok=True)
    destination = processed_dir / file_path.name
    if destination.exists():
        stamp = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
        destination = processed_dir / f"{file_path.stem}_{stamp}{file_path.suffix}"
    file_path.replace(destination)


def wait_for_file_ready(file_path: Path, retries: int = 10, delay: float = 1.0) -> bool:
    """Return True once the file can be opened exclusively, False if it never becomes ready."""
    for attempt in range(retries):
        try:
            with file_path.open("rb"):
                return True
        except OSError:
            if attempt < retries - 1:
                print(f"File {file_path.name} is locked, retrying in {delay}s… (attempt {attempt + 1}/{retries})")
                time.sleep(delay)
    return False


def process_candidate_file(
    file_path: Path,
    cfg: AppConfig,
    conn: sql.client.Connection | None,
    processed: set[str],
) -> None:
    resolved_path = str(file_path.resolve())
    if resolved_path in processed:
        return
    if not file_path.exists() or not file_path.is_file():
        return

    if not wait_for_file_ready(file_path):
        raise PermissionError(f"File {file_path.name} remained locked after retries — skipping.")

    process_file(file_path, cfg, conn)
    if not cfg.dry_run:
        move_to_processed(file_path, cfg.processed_dir)
    processed.add(resolved_path)
    save_state(cfg.state_file, processed)


class DropEventHandler(FileSystemEventHandler):
    def __init__(
        self,
        source_dir: Path,
        pattern: str,
        queue: Queue[Path],
        recursive: bool,
        watch_depth: int | None,
    ) -> None:
        super().__init__()
        self.source_dir = source_dir
        self.pattern = pattern
        self.queue = queue
        self.recursive = recursive
        self.watch_depth = watch_depth

    def _enqueue(self, candidate: str) -> None:
        path = Path(candidate).resolve()
        if self.recursive:
            try:
                path.relative_to(self.source_dir)
            except ValueError:
                return
        elif path.parent != self.source_dir:
            return
        depth = directory_depth(self.source_dir, path)
        if depth is None:
            return
        if self.watch_depth is not None and depth > self.watch_depth:
            return
        if matches_pattern(path, self.pattern):
            self.queue.put(path)

    def on_created(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        if isinstance(event.src_path, bytes):
            self._enqueue(event.src_path.decode("utf-8", errors="ignore"))
        elif isinstance(event.src_path, str):
            self._enqueue(event.src_path)

    def on_moved(self, event: FileSystemEvent) -> None:
        destination = getattr(event, "dest_path", None)
        if event.is_directory:
            return
        if isinstance(destination, bytes):
            self._enqueue(destination.decode("utf-8", errors="ignore"))
        elif isinstance(destination, str):
            self._enqueue(destination)


def run_polling(cfg: AppConfig, conn: sql.client.Connection | None, processed: set[str]) -> None:
    while True:
        files = discover_files(
            cfg.source_dir,
            cfg.file_pattern,
            processed,
            cfg.watch_recursive,
            cfg.watch_depth,
        )
        if files:
            print(f"Found {len(files)} file(s) to process")
        for file_path in files:
            try:
                process_candidate_file(file_path, cfg, conn, processed)
            except Exception as exc:  # noqa: BLE001
                print(f"Failed to process {file_path.name}: {exc}")

        if cfg.once:
            break


        time.sleep(cfg.poll_seconds)


def run_event_listener(cfg: AppConfig, conn: sql.client.Connection | None, processed: set[str]) -> None:
    startup_files = discover_files(
        cfg.source_dir,
        cfg.file_pattern,
        processed,
        cfg.watch_recursive,
        cfg.watch_depth,
    )
    if startup_files:
        print(f"Found {len(startup_files)} file(s) to process")
    for file_path in startup_files:
        try:
            process_candidate_file(file_path, cfg, conn, processed)
        except Exception as exc:  # noqa: BLE001
            print(f"Failed to process {file_path.name}: {exc}")

    if cfg.once:
        return

    event_queue: Queue[Path] = Queue()
    observer = Observer()
    observer.schedule(
        DropEventHandler(
            cfg.source_dir,
            cfg.file_pattern,
            event_queue,
            cfg.watch_recursive,
            cfg.watch_depth,
        ),
        str(cfg.source_dir),
        recursive=cfg.watch_recursive,
    )
    observer.start()
    print(f"Listening for file drop events in {cfg.source_dir}")

    try:
        while True:
            try:
                candidate = event_queue.get(timeout=1)
            except Empty:
                continue

            try:
                process_candidate_file(candidate, cfg, conn, processed)
            except Exception as exc:  # noqa: BLE001
                print(f"Failed to process {candidate.name}: {exc}")
    finally:
        observer.stop()
        observer.join()


def run(cfg: AppConfig) -> None:
    processed = load_state(cfg.state_file)
    conn: sql.client.Connection | None = None

    cfg.source_dir.mkdir(parents=True, exist_ok=True)

    if not cfg.dry_run:
        conn = connect_databricks()

    try:
        if cfg.mode == "event":
            run_event_listener(cfg, conn, processed)
        else:
            run_polling(cfg, conn, processed)
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    run(parse_args())
