"""Abstract base class for certificate storage implementations"""
from abc import ABC, abstractmethod
from typing import Any


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
