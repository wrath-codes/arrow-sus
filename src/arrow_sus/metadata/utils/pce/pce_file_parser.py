"""PCE filename parsing utilities."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from returns.result import Failure, Result, Success

from arrow_sus.metadata.utils.yearly.yearly_file_parser import zfill_year
from arrow_sus.metadata.utils.validation import VALID_UFS


@dataclass(frozen=True)
class PCEFileInfo:
    """Information extracted from PCE filename."""

    uf: str
    year: int
    group: str = "PCE"  # PCE only has one group
    raw_filename: str = ""


def parse_pce_filename(filename: str) -> Result[PCEFileInfo, str]:
    """Parse PCE filename to extract metadata.

    PCE files follow the pattern: PCE[UF][YY].dbc
    Examples:
        - PCEAL00.dbc -> UF=AL, Year=2000
        - PCESP23.dbc -> UF=SP, Year=2023
        - PCERJ99.dbc -> UF=RJ, Year=1999

    Args:
        filename: The filename to parse

    Returns:
        Result containing PCEFileInfo or error message

    Example:
        >>> result = parse_pce_filename("PCEAL00.dbc")
        >>> if isinstance(result, Success):
        ...     info = result.unwrap()
        ...     print(f"UF: {info.uf}, Year: {info.year}")
        UF: AL, Year: 2000
    """
    # Remove extension
    name_without_ext = filename.replace(".dbc", "").replace(".DBC", "")

    # PCE pattern: PCE[UF][YY]
    # Group 1: UF (2 characters)
    # Group 2: Year (2 digits)
    pattern = r"^PCE([A-Z]{2})(\d{2})$"

    match = re.match(pattern, name_without_ext, re.IGNORECASE)
    if not match:
        return Failure(
            f"Filename '{filename}' does not match PCE pattern PCE[UF][YY].dbc"
        )

    uf_code = match.group(1).upper()
    year_2digit = match.group(2)

    # Validate UF code
    if uf_code not in VALID_UFS:
        return Failure(f"Invalid UF code '{uf_code}' in filename '{filename}'")

    # Convert 2-digit year to 4-digit year
    try:
        year_result = zfill_year(year_2digit)
        if isinstance(year_result, Failure):
            return Failure(
                f"Invalid year '{year_2digit}' in filename '{filename}': {year_result.failure()}"
            )

        year_4digit = year_result.unwrap()
    except ValueError:
        return Failure(f"Invalid year format '{year_2digit}' in filename '{filename}'")

    return Success(
        PCEFileInfo(
            uf=uf_code,
            year=year_4digit,
            group="PCE",
            raw_filename=filename,
        )
    )


def validate_pce_pattern(filename: str) -> bool:
    """Quick validation if filename matches PCE pattern.

    Args:
        filename: The filename to validate

    Returns:
        True if filename matches PCE pattern, False otherwise
    """
    result = parse_pce_filename(filename)
    return isinstance(result, Success)
