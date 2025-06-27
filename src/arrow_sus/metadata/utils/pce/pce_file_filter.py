"""PCE file discovery and filtering utilities."""

from __future__ import annotations

from typing import List, Optional, TYPE_CHECKING

from returns.result import Failure, Result, Success

from arrow_sus.metadata.io.async_ftp import AsyncFTPClient
from arrow_sus.metadata.utils.pce.pce_file_parser import parse_pce_filename

if TYPE_CHECKING:
    from arrow_sus.metadata.io.types import FileEntry
    from arrow_sus.metadata.utils.filters import PCEFilters


async def discover_pce_files(
    base_path: str,
    filters: Optional[PCEFilters] = None,
    max_files: int = 10000,
) -> Result[List[FileEntry], Exception]:
    """Discover PCE files from DATASUS FTP server.

    Args:
        base_path: Base FTP directory path
        filters: Optional filters to apply
        max_files: Maximum number of files to discover

    Returns:
        Result containing list of FileEntry objects or error

    Example:
        >>> filters = PCEFilters(uf="SP", year=2023)
        >>> result = await discover_pce_files("/dissemin/publicos/PCE/DADOS", filters)
        >>> if isinstance(result, Success):
        ...     files = result.unwrap()
        ...     print(f"Found {len(files)} PCE files")
    """
    try:
        client = AsyncFTPClient()

        # Get all files from the directory
        files_result = await client.list_directory(base_path, max_files=max_files)
        all_files = files_result

        if not all_files:
            return Success([])

        # Filter files based on PCE pattern and filters
        filtered_files = []

        for file_entry in all_files:
            filename = file_entry["filename"]

            # Parse filename to validate it's a PCE file
            parse_result = parse_pce_filename(filename)
            if isinstance(parse_result, Failure):
                continue  # Skip files that don't match PCE pattern

            parsed = parse_result.unwrap()

            # Apply filters if provided
            if filters:
                # UF filter
                if "uf" in filters and parsed.uf != filters["uf"]:
                    continue

                # Year filter
                if "year" in filters and parsed.year != filters["year"]:
                    continue

            filtered_files.append(file_entry)

        return Success(filtered_files)

    except Exception as e:
        return Failure(e)


async def get_pce_file_count(
    base_path: str,
    filters: Optional[PCEFilters] = None,
) -> Result[int, Exception]:
    """Get count of PCE files matching filters.

    Args:
        base_path: Base FTP directory path
        filters: Optional filters to apply

    Returns:
        Result containing file count or error
    """
    try:
        files_result = await discover_pce_files(base_path, filters, max_files=50000)
        if isinstance(files_result, Failure):
            return files_result

        files = files_result.unwrap()
        return Success(len(files))

    except Exception as e:
        return Failure(e)
