"""SQLite implementation of certificate storage"""
import json
import logging
import sqlite3
from typing import Any

from certificate_store import CertificateStore

logger = logging.getLogger(__name__)


class SQLiteCertificateStore(CertificateStore):
    """SQLite implementation of certificate storage"""

    def __init__(self, db_path: str = "certstream.db"):
        """Initialize SQLite database"""
        self.db_path = db_path
        self.init_database()

    def init_database(self) -> None:
        """Create the certificates table if it doesn't exist"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS certificates (
                domain TEXT PRIMARY KEY,
                data TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Enable write-ahead logging for better concurrency
        conn.execute("PRAGMA journal_mode=WAL")
        conn.commit()
        conn.close()
        logger.info(f"SQLite database initialized at {self.db_path}")

    def store_certificate(self, domain: str, data: dict[str, Any]) -> bool:
        """Store or update a certificate entry in SQLite"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Convert dict to JSON string
            data_json = json.dumps(data)

            # Use INSERT OR REPLACE to handle updates
            cursor.execute(
                """
                INSERT OR REPLACE INTO certificates (domain, data, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            """,
                (domain, data_json),
            )

            conn.commit()
            conn.close()

            logger.debug(f"Stored certificate for domain: {domain}")
            return True
        except Exception as e:
            logger.error(f"Error storing certificate for {domain}: {e}")
            return False

    def get_certificate(self, domain: str) -> dict[str, Any] | None:
        """Retrieve a certificate entry from SQLite"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT data FROM certificates WHERE domain = ?
            """,
                (domain,),
            )

            result = cursor.fetchone()
            conn.close()

            if result:
                return json.loads(result[0])
            return None
        except Exception as e:
            logger.error(f"Error retrieving certificate for {domain}: {e}")
            return None

    def get_all_certificates(self, limit: int = 100) -> list[dict[str, Any]]:
        """Retrieve all certificates from SQLite (with optional limit)"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT domain, data, created_at, updated_at
                FROM certificates
                ORDER BY updated_at DESC
                LIMIT ?
            """,
                (limit,),
            )

            results = cursor.fetchall()
            conn.close()

            return [
                {
                    "domain": row[0],
                    "data": json.loads(row[1]),
                    "created_at": row[2],
                    "updated_at": row[3],
                }
                for row in results
            ]
        except Exception as e:
            logger.error(f"Error retrieving certificates: {e}")
            return []
