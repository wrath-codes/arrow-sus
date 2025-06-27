"""Monthly file parsing utilities for extracting metadata from DATASUS files."""

from __future__ import annotations

import re
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Union

from returns.result import Failure, Result, Success

if TYPE_CHECKING:
    from arrow_sus.metadata.core.models import RemoteFile


def zfill_year(year: Union[str, int]) -> Result[int, str]:
    """Convert 2-digit year to 4-digit year using intelligent logic.

    Based on eco-data's zfill_year function. For 2-digit years:
    - If year > current_year's 2-digit part: assume previous century
    - Otherwise: assume current century

    Args:
        year: Year to convert (2-digit or 4-digit)

    Returns:
        Result[int, str]: Success with 4-digit year or Failure with error

    Example:
        In 2025:
        - zfill_year("23") -> 2023 (current century)
        - zfill_year("26") -> 1926 (previous century, since 26 > 25)
    """
    try:
        year_str = str(year)

        if not year_str.isdigit():
            return Failure(f"Year must contain only digits: {year_str}")

        year_num = int(year_str)

        # Handle 2-digit years
        if year_num < 100:
            current_year = datetime.now().year
            century = (current_year // 100) * 100
            current_year_2digit = current_year % 100

            # If input year > current year's 2-digit part, use previous century
            if year_num > current_year_2digit:
                century -= 100

            full_year = century + year_num
            return Success(full_year)

        # Handle 4-digit years
        if 1900 <= year_num <= 2100:  # Reasonable range
            return Success(year_num)
        else:
            return Failure(f"Year out of reasonable range: {year_num}")

    except Exception as e:
        return Failure(f"Error converting year {year}: {str(e)}")


async def parse_monthly_file(file: RemoteFile) -> Result[Dict[str, Any], str]:
    """Parse metadata from a monthly DATASUS file.

    Extracts information like system, group, state, year, month from filename.

    Args:
        file: RemoteFile to parse

    Returns:
        Result[Dict[str, Any], str]: Success with metadata dict or Failure with error

    Example:
        ```python
        result = await parse_monthly_file(file)
        if is_successful(result):
            metadata = result.unwrap()
            print(f"System: {metadata['system']}")
            print(f"Group: {metadata['group']}")
        ```
    """
    try:
        filename = file.filename.upper()

        # Extract basic file info
        metadata = {
            "filename": file.filename,
            "size_bytes": file.size,
            "size_mb": file.size_mb,
            "full_path": file.full_path,
            "dataset": file.dataset,
        }

        # Add partition info if available
        if file.partition:
            metadata.update(
                {
                    "uf": file.partition.uf.value if file.partition.uf else None,
                    "year": file.partition.year,
                    "month": file.partition.month,
                }
            )

        # Parse filename patterns to extract system and group
        # Common patterns: PAufYYMM.dbc, RDufYYMM.dbc, etc.
        monthly_pattern = re.compile(r"^([A-Z]{2})([A-Z]{2})(\d{2})(\d{2})\.(dbc|DBC)$")
        match = monthly_pattern.match(filename)

        if match:
            group, uf_part, year_part, month_part, extension = match.groups()

            # Convert 2-digit year to 4-digit using intelligent logic
            year_result = zfill_year(year_part)
            match year_result:
                case Success(year_4digit):
                    metadata.update(
                        {
                            "system": file.dataset.split("-")[0].upper()
                            if "-" in file.dataset
                            else "UNKNOWN",
                            "group": group,
                            "parsed_year": year_4digit,
                            "parsed_month": int(month_part),
                            "parsed_uf": uf_part,
                            "extension": extension.lower(),
                            "pattern": "monthly_standard",
                        }
                    )
                case Failure(error):
                    # If year parsing fails, still include basic info but mark as unknown
                    metadata.update(
                        {
                            "system": file.dataset.split("-")[0].upper()
                            if "-" in file.dataset
                            else "UNKNOWN",
                            "group": group,
                            "parsed_year": None,
                            "parsed_month": int(month_part),
                            "parsed_uf": uf_part,
                            "extension": extension.lower(),
                            "pattern": "monthly_standard_invalid_year",
                            "year_error": error,
                        }
                    )
        else:
            # Try other common patterns
            # Pattern for files like APAC_2024_01_PA.dbc
            apac_pattern = re.compile(
                r"^([A-Z]+)_(\d{4})_(\d{2})_([A-Z]{2})\.(dbc|DBC)$"
            )
            match = apac_pattern.match(filename)

            if match:
                group, year_str, month_str, uf_part, extension = match.groups()
                metadata.update(
                    {
                        "system": file.dataset.split("-")[0].upper()
                        if "-" in file.dataset
                        else "UNKNOWN",
                        "group": group,
                        "parsed_year": int(year_str),
                        "parsed_month": int(month_str),
                        "parsed_uf": uf_part,
                        "extension": extension.lower(),
                        "pattern": "apac_style",
                    }
                )
            else:
                # Generic pattern extraction
                metadata.update(
                    {
                        "system": file.dataset.split("-")[0].upper()
                        if "-" in file.dataset
                        else "UNKNOWN",
                        "group": "UNKNOWN",
                        "pattern": "unknown",
                    }
                )

        return Success(metadata)

    except Exception as e:
        return Failure(f"Failed to parse file {file.filename}: {str(e)}")


__all__ = ["parse_monthly_file"]
