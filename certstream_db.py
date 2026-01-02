#!/usr/bin/env python3
"""
CertStream to SQLite Database - Stores certificate transparency logs in SQLite
"""
import asyncio
import json
import logging
import os
import signal

import websockets

from certificate_store import CertificateStore
from rocksdb_store import RocksDBCertificateStore

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


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
    # store = SQLiteCertificateStore(db_path="certstream.db")
    store = RocksDBCertificateStore(db_path="certstream_rocksdb")

    # Create collector with the store
    ws = os.environ["CERTSTREAM_WEBSOCKET_URL"]
    collector = CertStreamCollector(store, websocket_url=ws)

    try:
        await collector.connect_and_store()
    except KeyboardInterrupt:
        logger.info("Shutting down...")


if __name__ == "__main__":
    asyncio.run(main())
