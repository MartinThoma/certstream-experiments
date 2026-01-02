#!/usr/bin/env python3
"""Analytics for certificate stores"""
import argparse
import logging
import os
import sqlite3

logger = logging.getLogger(__name__)


def human_readable_size(num_bytes: int) -> str:
    """Convert byte counts into a human-friendly string"""
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(num_bytes)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TB"


def sqlite_stats(db_path: str) -> dict[str, str | None]:
    """Collect entry count and longest domain info from SQLite"""
    if not os.path.exists(db_path):
        return {"error": f"SQLite DB not found at {db_path}"}

    size_bytes = os.path.getsize(db_path)

    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM certificates")
        count = cur.fetchone()[0]
        cur.execute("SELECT domain FROM certificates ORDER BY LENGTH(domain) DESC LIMIT 1")
        row = cur.fetchone()
        conn.close()
        longest_domain = row[0] if row else None
    except Exception as exc:  # noqa: BLE001
        return {"error": f"SQLite query failed: {exc}"}

    return {
        "size_bytes": size_bytes,
        "count": count,
        "longest_domain": longest_domain,
    }


def rocksdb_stats(db_path: str) -> dict[str, str | None]:
    """Collect entry count and longest domain info from RocksDB"""
    try:
        import rocksdb  # type: ignore
    except ModuleNotFoundError:
        return {"error": "python-rocksdb is not installed"}

    if not os.path.exists(db_path):
        return {"error": f"RocksDB path not found at {db_path}"}

    size_bytes = 0
    for root, _, files in os.walk(db_path):
        for file in files:
            size_bytes += os.path.getsize(os.path.join(root, file))

    try:
        options = rocksdb.Options(create_if_missing=False)
        db = rocksdb.DB(db_path, options, read_only=True)
        it = db.iteritems()
        it.seek_to_first()

        count = 0
        longest_domain = None
        for key, _ in it:
            count += 1
            domain = key.decode(errors="ignore")
            if longest_domain is None or len(domain) > len(longest_domain):
                longest_domain = domain

    except Exception as exc:  # noqa: BLE001
        return {"error": f"RocksDB query failed: {exc}"}

    return {
        "size_bytes": size_bytes,
        "count": count,
        "longest_domain": longest_domain,
    }


def print_stats(label: str, stats: dict[str, str | None]) -> None:
    """Print metrics in a single line"""
    if "error" in stats:
        print(f"{label}: ERROR - {stats['error']}")
        return

    size_bytes = int(stats.get("size_bytes", 0))
    count = stats.get("count", 0)
    longest = stats.get("longest_domain") or "<none>"
    print(
        f"{label}: size={size_bytes} bytes ({human_readable_size(size_bytes)}), "
        f"entries={count}, longest_domain={longest}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Show stats for certificate stores")
    parser.add_argument(
        "--store",
        choices=["sqlite", "rocksdb", "both"],
        default="both",
        help="Which store(s) to inspect",
    )
    parser.add_argument(
        "--sqlite-path",
        default="certstream.db",
        help="Path to SQLite database file",
    )
    parser.add_argument(
        "--rocksdb-path",
        default="certstream_rocksdb",
        help="Path to RocksDB directory",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    if args.store in ("sqlite", "both"):
        print_stats("SQLite", sqlite_stats(args.sqlite_path))

    if args.store in ("rocksdb", "both"):
        print_stats("RocksDB", rocksdb_stats(args.rocksdb_path))


if __name__ == "__main__":
    main()
