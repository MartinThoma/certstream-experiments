#!/usr/bin/env python3
"""
Utility script to query the CertStream SQLite database
"""
import argparse
import json
import sqlite3
import sys

from tabulate import tabulate


def get_database_stats(db_path: str) -> dict:
    """Get statistics about the database"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM certificates")
        total_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(DISTINCT domain) FROM certificates")
        unique_domains = cursor.fetchone()[0]

        cursor.execute("SELECT MAX(updated_at) FROM certificates")
        latest_update = cursor.fetchone()[0]

        conn.close()

        return {
            "total_entries": total_count,
            "unique_domains": unique_domains,
            "latest_update": latest_update,
        }
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return {}


def search_domain(db_path: str, domain: str) -> dict:
    """Search for a specific domain"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT domain, data, created_at, updated_at
            FROM certificates
            WHERE domain LIKE ?
            LIMIT 10
        """,
            (f"%{domain}%",),
        )

        results = cursor.fetchall()
        conn.close()

        if not results:
            print(f"No results found for '{domain}'")
            return {}

        for domain, data_json, created_at, updated_at in results:
            data = json.loads(data_json)
            print(f"\nDomain: {domain}")
            print(f"Created: {created_at}")
            print(f"Updated: {updated_at}")
            print(f"Data: {json.dumps(data, indent=2)}")

        return {}
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return {}


def list_recent(db_path: str, limit: int = 20) -> None:
    """List the most recently updated certificates"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT domain, created_at, updated_at
            FROM certificates
            ORDER BY updated_at DESC
            LIMIT ?
        """,
            (limit,),
        )

        results = cursor.fetchall()
        conn.close()

        if not results:
            print("No certificates found in database")
            return

        headers = ["Domain", "Created", "Updated"]
        print(tabulate(results, headers=headers, tablefmt="grid"))
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="Query CertStream SQLite database")
    parser.add_argument("--db", default="certstream.db", help="Path to SQLite database")
    parser.add_argument("--stats", action="store_true", help="Show database statistics")
    parser.add_argument("--search", type=str, help="Search for a domain")
    parser.add_argument(
        "--recent", type=int, default=20, help="Show recent certificates (default: 20)"
    )

    args = parser.parse_args()

    if args.stats:
        stats = get_database_stats(args.db)
        print("Database Statistics:")
        print(f"  Total entries: {stats.get('total_entries', 0)}")
        print(f"  Unique domains: {stats.get('unique_domains', 0)}")
        print(f"  Latest update: {stats.get('latest_update', 'N/A')}")
    elif args.search:
        search_domain(args.db, args.search)
    else:
        list_recent(args.db, args.recent)


if __name__ == "__main__":
    main()
