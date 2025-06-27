"""Yearly DATASUS system implementation like your yearly_sus_system.py."""

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
    VALID_YEARLY_FILTERS,
    YearlyFilters,
)
from arrow_sus.metadata.utils.yearly.yearly_file_filter import filter_yearly_files
from arrow_sus.metadata.utils.yearly.yearly_file_parser import parse_yearly_file

if TYPE_CHECKING:
    from arrow_sus.metadata.core.models import RemoteFile, SourceMetadata
    from arrow_sus.metadata.core.client import DataSUSMetadataClient


class YearlyDatasusSystem(DatasusSystem):
    """DATASUS system with yearly file organization (SINASC, SINAN, SIM, etc.).

    Mirrors the structure of your yearly_sus_system.py but adapted for our models.

    Yearly systems organize data by year instead of month, typically including:
    - SINASC (Declarações de nascidos vivos): DNufYYYY.dbc
    - SINAN (Notificações): DENGuf22.dbc, ACGRuf22.dbc, etc.
    - SIM (Declarações de óbito): DOufYYYY.dbc, DOEXTyy.dbc, etc.

    Args:
        source_metadata: Metadata for this source system

    Example:
        ```python
        system = YearlyDatasusSystem(source_metadata)
        result = await system.get_files(group="DN", uf="SP", year=2024)
        if is_successful(result):
            files = result.unwrap()
        ```
    """

    VALID_FILTERS: Set[str] = VALID_YEARLY_FILTERS

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
        self, **filters: Unpack[YearlyFilters]
    ) -> Result[List[RemoteFile], DatasusDataNotFoundError]:
        """Get files with yearly filters by discovering them from FTP.

        Args:
            **filters: Filters for search
                - group: str - Group code (DN, DENG, DO, etc.) - optional
                - uf: str | List[str] - State codes - optional
                - year: str | int | List - Years - optional

        Returns:
            Result[List[RemoteFile], DatasusDataNotFoundError]:
                Success with file list or Failure with error

        Example:
            ```python
            # Get all SINASC files for SP in 2024
            result = await system.get_files(uf="SP", year=2024)

            # Get specific SINAN dengue files
            result = await system.get_files(group="DENG", year=[2023, 2024])

            # Get all SIM files for multiple states
            result = await system.get_files(uf=["SP", "RJ"], year=2024)
            ```
        """
        validation_result = validate_dict_keys(filters, self.VALID_FILTERS)

        match validation_result:
            case Success(validated_filters):
                # Extract source name from metadata
                source_name = self._determine_source_name()

                return await filter_yearly_files(
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
        self, **filters: Unpack[YearlyFilters]
    ) -> Result[List[RemoteFile], DatasusDataNotFoundError]:
        """Get latest files with filters.

        For yearly systems, "latest" typically means the most recent years.
        This method applies a default filter to get files from recent years
        and then applies any additional filters.

        Args:
            **filters: Filters for latest files
                - group: str - Group code (optional)
                - uf: str | List[str] - State codes (optional)
                - year: str | int | List - Years (optional, defaults to last 3 years)

        Returns:
            Result[List[RemoteFile], DatasusDataNotFoundError]:
                Success with file list or Failure with error
        """
        validation_result = validate_dict_keys(filters, self.VALID_FILTERS)

        match validation_result:
            case Success(validated_filters):
                # If no year specified, default to last 3 years
                if "year" not in validated_filters:
                    from datetime import datetime

                    current_year = datetime.now().year
                    validated_filters["year"] = [
                        current_year - 2,
                        current_year - 1,
                        current_year,
                    ]

                # Extract source name from metadata
                source_name = self._determine_source_name()

                return await filter_yearly_files(
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
                print(f"Year: {metadata['parsed_year']}")
                print(f"UF: {metadata['parsed_uf']}")
            ```
        """
        file_result = await parse_yearly_file(file)

        match file_result:
            case Success(metadata):
                return Success(metadata)
            case Failure(error):
                return Failure(
                    DatasusFileParsingError(
                        self.source_metadata.name, file.filename, str(error)
                    )
                )

    def _determine_source_name(self) -> str:
        """Determine source name from metadata."""
        name_lower = self.source_metadata.name.lower()

        # Map actual source names from our metadata to source codes
        if "nascidos vivos" in name_lower:
            return "sinasc"
        elif "mortalidade" in name_lower:
            return "sim"
        elif "agravos de notificação" in name_lower:
            return "sinan"
        elif "atenção psicossocial" in name_lower:
            return "resp"
        elif "esquistossomose" in name_lower:
            return "pce"
        elif "olhar brasil" in name_lower:
            return "po"
        elif "colo do útero" in name_lower:
            return "siscolo"
        elif "câncer de mama" in name_lower:
            return "sismama"
        elif "territorial" in name_lower:
            return "base-territorial"
        elif "populacional" in name_lower or "ibge" in name_lower:
            return "base-populacional-ibge"
        else:
            # Fallback: try to extract from dataset if available
            if hasattr(self.source_metadata, "source"):
                return self.source_metadata.source
            else:
                return "unknown"


__all__ = ["YearlyDatasusSystem"]
