"""Monthly DATASUS system implementation like your monthly_sus_system.py."""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Set, Unpack

from returns.result import Failure, Result, Success

from arrow_sus.metadata.core.errors import (
    DatasusDataNotFoundError,
    DatasusFileParsingError,
)
from arrow_sus.metadata.systems.base import DatasusSystem
from arrow_sus.metadata.utils.dict_validation import validate_dict_keys
from arrow_sus.metadata.utils.filters import (
    VALID_LATEST_FILTERS,
    VALID_MONTHLY_FILTERS,
    LatestFilters,
    MonthlyFilters,
)
from arrow_sus.metadata.utils.monthly.monthly_file_filter import filter_monthly_files
from arrow_sus.metadata.utils.monthly.monthly_file_parser import parse_monthly_file

if TYPE_CHECKING:
    from arrow_sus.metadata.core.models import RemoteFile, SourceMetadata
    from arrow_sus.metadata.core.client import DataSUSMetadataClient


class MonthlyDatasusSystem(DatasusSystem):
    """DATASUS system with monthly file organization (SIA, SIH, CNES, etc.).

    Mirrors the structure of your monthly_sus_system.py but adapted for our models.

    Args:
        source_metadata: Metadata for this source system

    Example:
        ```python
        system = MonthlyDatasusSystem(source_metadata)
        result = await system.get_files(group="PA", uf="SP", year=2024)
        if is_successful(result):
            files = result.unwrap()
        ```
    """

    VALID_FILTERS: Set[str] = VALID_MONTHLY_FILTERS
    VALID_LATEST_FILTERS: Set[str] = VALID_LATEST_FILTERS

    def __init__(
        self, source_metadata: SourceMetadata, client: "DataSUSMetadataClient"
    ) -> None:
        self._source_metadata = source_metadata
        self._client = client

    @property
    def source_metadata(self) -> SourceMetadata:
        """Source metadata for this system."""
        return self._source_metadata

    async def get_files(
        self, **filters: Unpack[MonthlyFilters]
    ) -> Result[List[RemoteFile], DatasusDataNotFoundError]:
        """Get files with monthly filters by discovering them from FTP.

        Args:
            **filters: Filters for search
                - group: str - Group code (PA, RD, etc.)
                - uf: str | List[str] - State codes
                - year: str | int | List - Years
                - month: str | int | List - Months

        Returns:
            Result[List[RemoteFile], DatasusDataNotFoundError]:
                Success with file list or Failure with error

        Example:
            ```python
            result = await system.get_files(
                group="PA",
                uf=["SP", "RJ"],
                year=2024,
                month=[1, 2, 3]
            )
            ```
        """
        validation_result = validate_dict_keys(filters, self.VALID_FILTERS)

        match validation_result:
            case Success(validated_filters):
                # Extract source name from metadata (like "sia" from "Sistema de Informações Ambulatoriais")
                source_name = "unknown"
                name_lower = self.source_metadata.name.lower()
                if "informações ambulatoriais" in name_lower:
                    source_name = "sia"
                elif "informações hospitalares" in name_lower:
                    source_name = "sih"
                elif "estabelecimentos" in name_lower:
                    source_name = "cnes"
                elif "notificação" in name_lower:
                    source_name = "sinan"

                return await filter_monthly_files(
                    client=self._client,
                    source_name=source_name,
                    **validated_filters,
                )
            case Failure(error):
                return Failure(
                    DatasusDataNotFoundError(
                        self.source_metadata.name,
                        f"Invalid filters: {error.invalid_keys}",
                        filters,
                    )
                )

    async def get_latest_files(
        self, **filters: Unpack[LatestFilters]
    ) -> Result[List[RemoteFile], DatasusDataNotFoundError]:
        """Get latest files with filters.

        Args:
            **filters: Filters for latest files
                - group: str - Group code
                - states: str | List[str] - State codes
                - months: int - Number of months back (default 12)
                - current_year: bool - Only current year (default False)

        Returns:
            Result[List[RemoteFile], DatasusDataNotFoundError]:
                Success with file list or Failure with error
        """
        validation_result = validate_dict_keys(filters, self.VALID_LATEST_FILTERS)

        match validation_result:
            case Success(validated_filters):
                # Convert 'states' to 'uf' for compatibility
                if "states" in validated_filters:
                    validated_filters["uf"] = validated_filters.pop("states")

                # Extract source name from metadata
                source_name = "unknown"
                name_lower = self.source_metadata.name.lower()
                if "informações ambulatoriais" in name_lower:
                    source_name = "sia"
                elif "informações hospitalares" in name_lower:
                    source_name = "sih"
                elif "estabelecimentos" in name_lower:
                    source_name = "cnes"
                elif "notificação" in name_lower:
                    source_name = "sinan"

                # TODO: Implement get_latest_monthly_files utility
                # For now, delegate to regular filtering
                return await filter_monthly_files(
                    client=self._client,
                    source_name=source_name,
                    **validated_filters,
                )
            case Failure(error):
                return Failure(
                    DatasusDataNotFoundError(
                        self.source_metadata.name,
                        f"Invalid latest filters: {error.invalid_keys}",
                        filters,
                    )
                )

    async def parse_file(
        self, file: RemoteFile
    ) -> Result[dict, DatasusFileParsingError]:
        """Parse file metadata asynchronously.

        Args:
            file: File to parse metadata from

        Returns:
            Result[dict, DatasusFileParsingError]:
                Success with metadata dict or Failure with parsing error

        Example:
            ```python
            result = await system.parse_file(file)
            if is_successful(result):
                metadata = result.unwrap()
            ```
        """
        file_result = await parse_monthly_file(file)

        match file_result:
            case Success(metadata):
                return Success(metadata)
            case Failure(error):
                return Failure(
                    DatasusFileParsingError(
                        self.source_metadata.name, file.filename, str(error)
                    )
                )


__all__ = ["MonthlyDatasusSystem"]
