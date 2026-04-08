"""Base importer interface."""

import logging
from abc import ABC, abstractmethod
from pathlib import Path

logger = logging.getLogger(__name__)


class BaseImporter(ABC):
    """Base class for all file importers."""

    def __init__(self, file_path: Path) -> None:
        self.file_path = file_path
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def extract(self) -> list[dict]:
        """Extract raw table rows from the file."""
        ...

    @staticmethod
    def detect_column(headers: list[str], candidates: list[str]) -> str | None:
        """Find best matching column name from candidates."""
        headers_lower = [h.lower().strip() for h in headers]
        for candidate in candidates:
            for i, h in enumerate(headers_lower):
                if candidate in h:
                    return headers[i]
        return None
