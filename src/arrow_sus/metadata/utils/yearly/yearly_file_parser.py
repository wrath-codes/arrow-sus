"""Yearly file parsing utilities for extracting metadata from DATASUS files."""

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


async def parse_yearly_file(file: RemoteFile) -> Result[Dict[str, Any], str]:
    """Parse metadata from a yearly DATASUS file.

    Extracts information like system, group, state, year from filename.
    Yearly systems include SINASC, SINAN, SIM with patterns like:
    - DNufYYYY.dbc (SINASC - Declarações de nascidos vivos)
    - DOufYYYY.dbc (SIM - Declarações de óbito)
    - DENGuf99.dbc (SINAN - with 2-digit years)
    - DNEX2024.dbc (External births - no UF)

    Args:
        file: RemoteFile to parse

    Returns:
        Result[Dict[str, Any], str]: Success with metadata dict or Failure with error

    Example:
        ```python
        result = await parse_yearly_file(file)
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
                }
            )

        # Parse filename patterns to extract system and group

        # Pattern 1: 4-digit year with UF (DNufYYYY.dbc, DOufYYYY.dbc)
        yearly_4digit_pattern = re.compile(
            r"^([A-Z]{2,4})([A-Z]{2})(\d{4})\.(dbc|DBC)$"
        )
        match = yearly_4digit_pattern.match(filename)

        if match:
            group, uf_part, year_str, extension = match.groups()
            metadata.update(
                {
                    "system": file.dataset.split("-")[0].upper()
                    if "-" in file.dataset
                    else "UNKNOWN",
                    "group": group,
                    "parsed_year": int(year_str),
                    "parsed_uf": uf_part,
                    "extension": extension.lower(),
                    "pattern": "yearly_4digit",
                }
            )
        else:
            # Pattern 2: 2-digit year with UF (DENGuf99.dbc, ACGRuf22.dbc)
            yearly_2digit_pattern = re.compile(
                r"^([A-Z]{2,4})([A-Z]{2})(\d{2})\.(dbc|DBC)$"
            )
            match = yearly_2digit_pattern.match(filename)

            if match:
                group, uf_part, year_part, extension = match.groups()

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
                                "parsed_uf": uf_part,
                                "extension": extension.lower(),
                                "pattern": "yearly_2digit",
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
                                "parsed_uf": uf_part,
                                "extension": extension.lower(),
                                "pattern": "yearly_2digit_invalid_year",
                                "year_error": error,
                            }
                        )
            else:
                # Pattern 3: 4-digit year without UF (DNEX2024.dbc, DOEXT2024.dbc)
                special_4digit_pattern = re.compile(r"^([A-Z]{4,6})(\d{4})\.(dbc|DBC)$")
                match = special_4digit_pattern.match(filename)

                if match:
                    group, year_str, extension = match.groups()
                    metadata.update(
                        {
                            "system": file.dataset.split("-")[0].upper()
                            if "-" in file.dataset
                            else "UNKNOWN",
                            "group": group,
                            "parsed_year": int(year_str),
                            "parsed_uf": None,  # No UF in this pattern
                            "extension": extension.lower(),
                            "pattern": "yearly_special_4digit",
                        }
                    )
                else:
                    # Pattern 4: 2-digit year without UF (DOEXTyy.dbc, DOFETyy.dbc)
                    special_2digit_pattern = re.compile(
                        r"^([A-Z]{4,6})(\d{2})\.(dbc|DBC)$"
                    )
                    match = special_2digit_pattern.match(filename)

                    if match:
                        group, year_part, extension = match.groups()

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
                                        "parsed_uf": None,  # No UF in this pattern
                                        "extension": extension.lower(),
                                        "pattern": "yearly_special_2digit",
                                    }
                                )
                            case Failure(error):
                                metadata.update(
                                    {
                                        "system": file.dataset.split("-")[0].upper()
                                        if "-" in file.dataset
                                        else "UNKNOWN",
                                        "group": group,
                                        "parsed_year": None,
                                        "parsed_uf": None,
                                        "extension": extension.lower(),
                                        "pattern": "yearly_special_2digit_invalid_year",
                                        "year_error": error,
                                    }
                                )
                    else:
                        # Pattern 5: External residence patterns (DORext22.dbc)
                        external_pattern = re.compile(
                            r"^([A-Z]{2,6})([A-Z]{2,4})(\d{2})\.(dbc|DBC)$"
                        )
                        match = external_pattern.match(filename)

                        if match:
                            prefix, suffix, year_part, extension = match.groups()
                            group = f"{prefix}{suffix}"

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
                                            "parsed_uf": None,
                                            "extension": extension.lower(),
                                            "pattern": "yearly_external",
                                        }
                                    )
                                case Failure(error):
                                    metadata.update(
                                        {
                                            "system": file.dataset.split("-")[0].upper()
                                            if "-" in file.dataset
                                            else "UNKNOWN",
                                            "group": group,
                                            "parsed_year": None,
                                            "parsed_uf": None,
                                            "extension": extension.lower(),
                                            "pattern": "yearly_external_invalid_year",
                                            "year_error": error,
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


__all__ = ["parse_yearly_file", "zfill_year"]
