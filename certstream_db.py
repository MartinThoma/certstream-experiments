#!/usr/bin/env python3
"""
CertStream to SQLite Database - Stores certificate transparency logs in SQLite
"""
import asyncio
import json
import logging
import os
import signal
import sqlite3
from abc import ABC, abstractmethod
from typing import Any

import websockets

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class CertificateStore(ABC):
    """Abstract base class for certificate storage implementations"""

    @abstractmethod
    def init_database(self) -> None:
        """Initialize the database and schema"""
        pass

    @abstractmethod
    def store_certificate(self, domain: str, data: dict[str, Any]) -> bool:
        """Store or update a certificate entry"""
        pass

    @abstractmethod
    def get_certificate(self, domain: str) -> dict[str, Any] | None:
        """Retrieve a certificate entry by domain"""
        pass

    @abstractmethod
    def get_all_certificates(self, limit: int = 100) -> list[dict[str, Any]]:
        """Retrieve all certificates with optional limit"""
        pass


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


class CertStreamCollector:
    """Manages CertStream data collection and storage"""

    def __init__(self, store: CertificateStore, websocket_url: str):
        """Initialize the collector with a certificate store"""
        self.store = store
        self.websocket_url = websocket_url
        self.running = True
        # Register signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, sig, frame):
        """Handle interrupt signal gracefully"""
        logger.info("Shutdown signal received, closing connection...")
        self.running = False

    async def connect_and_store(self):
        """Connect to CertStream websocket and store incoming data"""
        try:
            async with websockets.connect(self.websocket_url) as websocket:
                logger.info(f"Connected to CertStream at {self.websocket_url}")

                message_count = 0
                while self.running:
                    try:
                        message = await websocket.recv()
                        data = json.loads(message)

                        # Handle different message types
                        if data.get("message_type") == "certificate_update":
                            cert_data = data.get("data", {})

                            # Extract leaf certificate info
                            leaf_cert = cert_data.get("leaf_cert", {})
                            all_domains = leaf_cert.get("all_domains", [])

                            # Store entry for each domain
                            for domain in all_domains:
                                print(domain)
                                self.store.store_certificate(
                                    domain,
                                    {
                                        "domains": all_domains,
                                        "leaf_cert": leaf_cert,
                                        "chain": cert_data.get("chain", []),
                                        "source": data.get("source", {}),
                                        "timestamp": data.get("timestamp", None),
                                    },
                                )

                            message_count += 1
                            if message_count % 1000 == 0:
                                logger.info(f"Processed {message_count} certificate updates")

                        elif data.get("message_type") == "heartbeat":
                            logger.debug("Heartbeat received")

                    except json.JSONDecodeError:
                        logger.warning("Received invalid JSON from server")
                    except asyncio.CancelledError:
                        logger.info("Task cancelled, shutting down...")
                        break
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")

        except websockets.exceptions.WebSocketException as e:
            logger.error(f"WebSocket connection error: {e}")
        except asyncio.CancelledError:
            logger.info("WebSocket connection cancelled")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            logger.info("CertStream connection closed")


async def main():
    """Main entry point"""
    # Use SQLite as the storage backend
    store = SQLiteCertificateStore(db_path="certstream.db")

    # Create collector with the store
    ws = os.environ["CERTSTREAM_WEBSOCKET_URL"]
    collector = CertStreamCollector(store, websocket_url=ws)

    try:
        await collector.connect_and_store()
    except KeyboardInterrupt:
        logger.info("Shutting down...")


if __name__ == "__main__":
    asyncio.run(main())
