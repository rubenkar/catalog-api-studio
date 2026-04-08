"""Search indexer — placeholder for future catalog search."""

import logging

logger = logging.getLogger(__name__)


class SearchIndexer:
    """Search indexer placeholder."""

    def __init__(self) -> None:
        self._available = False

    @property
    def available(self) -> bool:
        return self._available
