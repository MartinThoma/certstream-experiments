"""RocksDB implementation of certificate storage"""
import json
import logging
from typing import Any

import rocksdb

from certificate_store import CertificateStore

logger = logging.getLogger(__name__)


class RocksDBCertificateStore(CertificateStore):
    """RocksDB implementation of certificate storage"""

    def __init__(self, db_path: str = "certstream_rocksdb"):
        """Initialize RocksDB database"""
        self.db_path = db_path
        self.db = None
        self.init_database()

    def init_database(self) -> None:
        """Initialize and open the RocksDB database"""
        try:
            opts = rocksdb.Options(create_if_missing=True)
            self.db = rocksdb.DB(self.db_path, opts)
            logger.info(f"RocksDB database initialized at {self.db_path}")
        except Exception as e:
            logger.error(f"Error initializing RocksDB: {e}")
            raise

    def store_certificate(self, domain: str, data: dict[str, Any]) -> bool:
        """Store or update a certificate entry in RocksDB"""
        try:
            if self.db is None:
                logger.error("Database not initialized")
                return False

            # Convert dict to JSON string
            data_json = json.dumps(data)

            # Store using domain as key
            self.db.put(domain.encode(), data_json.encode())

            logger.debug(f"Stored certificate for domain: {domain}")
            return True
        except Exception as e:
            logger.error(f"Error storing certificate for {domain}: {e}")
            return False

    def get_certificate(self, domain: str) -> dict[str, Any] | None:
        """Retrieve a certificate entry from RocksDB"""
        try:
            if self.db is None:
                logger.error("Database not initialized")
                return None

            result = self.db.get(domain.encode())

            if result:
                return json.loads(result.decode())
            return None
        except Exception as e:
            logger.error(f"Error retrieving certificate for {domain}: {e}")
            return None

    def get_all_certificates(self, limit: int = 100) -> list[dict[str, Any]]:
        """Retrieve all certificates from RocksDB (with optional limit)"""
        try:
            if self.db is None:
                logger.error("Database not initialized")
                return []

            certificates = []
            it = self.db.iteritems()
            it.seek_to_first()

            count = 0
            for key, value in it:
                if count >= limit:
                    break

                try:
                    certificates.append(
                        {
                            "domain": key.decode(),
                            "data": json.loads(value.decode()),
                        }
                    )
                    count += 1
                except (json.JSONDecodeError, UnicodeDecodeError):
                    logger.warning(f"Error decoding certificate for key: {key}")
                    continue

            return certificates
        except Exception as e:
            logger.error(f"Error retrieving certificates: {e}")
            return []

    def __del__(self):
        """Close the database when the object is destroyed"""
        if self.db is not None:
            try:
                self.db.close()
            except Exception as e:
                logger.error(f"Error closing RocksDB: {e}")
