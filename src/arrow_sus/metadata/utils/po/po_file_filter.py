"""PO file discovery and filtering utilities."""

from __future__ import annotations

from typing import List, Optional, TYPE_CHECKING

from returns.result import Failure, Result, Success

from arrow_sus.metadata.io.async_ftp import AsyncFTPClient
from arrow_sus.metadata.utils.po.po_file_parser import parse_po_filename

if TYPE_CHECKING:
    from arrow_sus.metadata.io.types import FileEntry
    from arrow_sus.metadata.utils.filters import POFilters


async def discover_po_files(
    base_path: str,
    filters: Optional[POFilters] = None,
    max_files: int = 10000,
) -> Result[List[FileEntry], Exception]:
    """Discover PO files from DATASUS FTP server.

    Args:
        base_path: Base FTP directory path
        filters: Optional filters to apply
        max_files: Maximum number of files to discover

    Returns:
        Result containing list of FileEntry objects or error

    Example:
        >>> filters = POFilters(uf="BR", year=2023)
        >>> result = await discover_po_files("/dissemin/publicos/PAINEL_ONCOLOGIA/DADOS", filters)
        >>> if isinstance(result, Success):
        ...     files = result.unwrap()
        ...     print(f"Found {len(files)} PO files")
    """
    try:
        client = AsyncFTPClient()

        # Get all files from the directory
        files_result = await client.list_directory(base_path, max_files=max_files)
        all_files = files_result

        if not all_files:
            return Success([])

        # Filter files based on PO pattern and filters
        filtered_files = []

        for file_entry in all_files:
            filename = file_entry["filename"]

            # Parse filename to validate it's a PO file
            parse_result = parse_po_filename(filename)
            if isinstance(parse_result, Failure):
                continue  # Skip files that don't match PO pattern

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


async def get_po_file_count(
    base_path: str,
    filters: Optional[POFilters] = None,
) -> Result[int, Exception]:
    """Get count of PO files matching filters.

    Args:
        base_path: Base FTP directory path
        filters: Optional filters to apply

    Returns:
        Result containing file count or error
    """
    try:
        files_result = await discover_po_files(base_path, filters, max_files=50000)
        if isinstance(files_result, Failure):
            return files_result

        files = files_result.unwrap()
        return Success(len(files))

    except Exception as e:
        return Failure(e)
