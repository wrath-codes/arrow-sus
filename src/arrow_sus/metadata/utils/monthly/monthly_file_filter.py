"""Monthly file filtering utilities matching your eco-data implementation."""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Union

from datetime import datetime
from returns.result import Failure, Result, Success

from arrow_sus.metadata.core.errors import DatasusDataNotFoundError
from arrow_sus.metadata.core.models import UFCode, RemoteFile, DataPartition

if TYPE_CHECKING:
    from arrow_sus.metadata.core.client import DataSUSMetadataClient


def _normalize_to_list(value: Union[str, int, List[Union[str, int]]]) -> List[str]:
    """Normalize a value to a list of strings."""
    if isinstance(value, list):
        return [str(v) for v in value]
    return [str(value)]


def _normalize_uf_codes(ufs: Union[str, List[str]]) -> Result[List[UFCode], str]:
    """Normalize UF codes to UFCode enum values."""
    if isinstance(ufs, str):
        ufs = [ufs]

    try:
        return Success([UFCode(uf.lower()) for uf in ufs])
    except ValueError as e:
        return Failure(f"Invalid UF code: {e}")


def _create_remote_file_from_ftp_data(file_data: dict, source_name: str) -> RemoteFile:
    """Create RemoteFile object from FTP file data with intelligent parsing."""
    import re

    filename = file_data["filename"]

    # Try to parse filename for partition information
    partition = None
    dataset = f"{source_name}-unknown"

    # Try newer DATASUS patterns first: PAufYYMMx.dbc (where x is a/b/c etc.)
    newer_pattern = re.compile(
        r"^([A-Z]{2})([A-Z]{2})(\d{2})(\d{2})[a-zA-Z]\.(dbc|DBC|dbf|DBF)$",
        re.IGNORECASE,
    )
    match = newer_pattern.match(filename)

    if match:
        group, uf_part, year_part, month_part, extension = match.groups()
    else:
        # Try classic DATASUS patterns: PAufYYMM.dbc
        classic_pattern = re.compile(
            r"^([A-Z]{2})([A-Z]{2})(\d{2})(\d{2})\.(dbc|DBC|dbf|DBF)$"
        )
        match = classic_pattern.match(filename.upper())

        if match:
            group, uf_part, year_part, month_part, extension = match.groups()

    if match:
        # Use intelligent year parsing
        year_2digit = int(year_part)
        current_year = datetime.now().year
        century = (current_year // 100) * 100
        current_year_2digit = current_year % 100

        # If year > current year's 2-digit part, use previous century
        if year_2digit > current_year_2digit:
            century -= 100
        year_4digit = century + year_2digit

        try:
            uf_code = UFCode(uf_part.lower())
            partition = DataPartition(
                uf=uf_code, year=year_4digit, month=int(month_part)
            )
            dataset = f"{source_name}-{group.lower()}"
        except (ValueError, Exception):
            # If parsing fails, leave partition as None
            pass

    return RemoteFile(
        filename=filename,
        full_path=file_data["full_path"],
        datetime=file_data.get("datetime", datetime.now()),
        size=file_data.get("size"),
        dataset=dataset,
        partition=partition,
        preliminary=False,
    )


async def filter_monthly_files(
    client: "DataSUSMetadataClient",
    source_name: str,
    group: Union[str, List[str]] | None = None,
    uf: Union[str, List[str]] | None = None,
    year: Union[int, str, List[Union[int, str]]] | None = None,
    month: Union[int, str, List[Union[int, str]]] | None = None,
) -> Result[List[RemoteFile], DatasusDataNotFoundError]:
    """Filter monthly files by actually discovering them from FTP server.

    Uses the monthly file matching logic to query the FTP server and find
    all files that match the specified criteria in real-time.

    Args:
        client: DataSUSMetadataClient for FTP access
        source_name: Source system name (sia, sih, cnes, etc.)
        group: Group/prefix filter (PA, RD, etc.)
        uf: State code filter
        year: Year filter
        month: Month filter

    Returns:
        Result[List[RemoteFile], DatasusDataNotFoundError]:
            Success with discovered files or Failure with error
    """
    try:
        matching_files: List[RemoteFile] = []

        # Normalize filters to lists
        group_list = _normalize_to_list(group) if group else None
        year_list = [int(y) for y in _normalize_to_list(year)] if year else None
        month_list = [int(m) for m in _normalize_to_list(month)] if month else None

        # Handle UF codes with validation
        uf_codes: List[UFCode] | None = None
        if uf:
            uf_result = _normalize_uf_codes(uf)
            match uf_result:
                case Success(codes):
                    uf_codes = codes
                case Failure(error):
                    return Failure(
                        DatasusDataNotFoundError(source_name, error, {"uf": uf})
                    )

        # Map source names to FTP directory patterns
        source_directories = {
            "sia": [
                "/dissemin/publicos/SIASUS/199407_200712/Dados",
                "/dissemin/publicos/SIASUS/200801_/Dados",
            ],
            "sih": [
                "/dissemin/publicos/SIHSUS/199201_200712/Dados",
                "/dissemin/publicos/SIHSUS/200801_/Dados",
            ],
            "cnes": ["/dissemin/publicos/CNES/200508_/Dados"],
            "sinan": ["/dissemin/publicos/SINAN/Dados"],
            "sim": [
                "/dissemin/publicos/SIM/CID09/Dados",
                "/dissemin/publicos/SIM/CID10/Dados",
            ],
            "sinasc": ["/dissemin/publicos/SINASC/Dados"],
            "ciha": ["/dissemin/publicos/CIHA/Dados"],
            "resp": ["/dissemin/publicos/resp/Dados"],
        }

        directories_to_scan = source_directories.get(source_name, [])
        if not directories_to_scan:
            return Failure(
                DatasusDataNotFoundError(
                    source_name,
                    f"Unknown source system: {source_name}",
                    {"source": source_name},
                )
            )

        # Scan FTP directories directly for comprehensive file discovery
        for directory in directories_to_scan:
            try:
                # Use FTP client to list all files in directory
                ftp_files = await client.ftp_client.list_directory(directory)

                for ftp_file_data in ftp_files:
                    # Create RemoteFile object with intelligent parsing
                    remote_file = _create_remote_file_from_ftp_data(
                        ftp_file_data, source_name
                    )

                    # Apply group filter at filename level
                    if group_list:
                        matches_group = any(
                            remote_file.filename.upper().startswith(g.upper())
                            for g in group_list
                        )
                        if not matches_group:
                            continue

                    # Apply UF filter if file has partition info
                    if uf_codes and remote_file.partition:
                        if remote_file.partition.uf not in uf_codes:
                            continue

                    # Apply year filter if file has partition info
                    if year_list and remote_file.partition:
                        if remote_file.partition.year not in year_list:
                            continue

                    # Apply month filter if file has partition info
                    if month_list and remote_file.partition:
                        if remote_file.partition.month not in month_list:
                            continue

                    matching_files.append(remote_file)

            except Exception as e:
                # Continue with other directories if one fails
                print(f"Warning: Failed to scan directory {directory}: {e}")
                continue

        # Sort files by year, month, UF for consistent ordering
        matching_files.sort(
            key=lambda f: (
                f.partition.year if f.partition else 0,
                f.partition.month if f.partition else 0,
                f.partition.uf.value if f.partition and f.partition.uf else "",
                f.filename,
            )
        )

        if not matching_files:
            filters_dict = {}
            if group:
                filters_dict["group"] = group
            if uf:
                filters_dict["uf"] = uf
            if year:
                filters_dict["year"] = year
            if month:
                filters_dict["month"] = month

            return Failure(
                DatasusDataNotFoundError(
                    source_name,
                    "No files found matching the specified criteria",
                    filters_dict,
                )
            )

        return Success(matching_files)

    except Exception as e:
        return Failure(
            DatasusDataNotFoundError(
                source_name,
                f"Error discovering files: {str(e)}",
                {"group": group, "uf": uf, "year": year, "month": month},
            )
        )


__all__ = ["filter_monthly_files"]
