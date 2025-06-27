"""Base system interface for DATASUS systems."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List

from returns.result import Result

if TYPE_CHECKING:
    from arrow_sus.metadata.core.errors import (
        DatasusDataNotFoundError,
        DatasusFileParsingError,
    )
    from arrow_sus.metadata.core.models import RemoteFile, SourceMetadata


class DatasusSystem(ABC):
    """Abstract base class for DATASUS systems."""

    @property
    @abstractmethod
    def source_metadata(self) -> SourceMetadata:
        """Source metadata for this system."""
        pass

    @abstractmethod
    async def get_files(
        self, **filters
    ) -> Result[List[RemoteFile], DatasusDataNotFoundError]:
        """Get files with filters."""
        pass

    @abstractmethod
    async def parse_file(
        self, file: RemoteFile
    ) -> Result[dict, DatasusFileParsingError]:
        """Parse file metadata."""
        pass


__all__ = ["DatasusSystem"]
