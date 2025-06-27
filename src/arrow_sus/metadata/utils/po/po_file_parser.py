"""PO filename parsing utilities."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from returns.result import Failure, Result, Success

from arrow_sus.metadata.utils.validation import VALID_UFS


@dataclass(frozen=True)
class POFileInfo:
    """Information extracted from PO filename."""

    uf: str
    year: int
    group: str = "PO"  # PO only has one group
    raw_filename: str = ""


def parse_po_filename(filename: str) -> Result[POFileInfo, str]:
    """Parse PO filename to extract metadata.

    PO files follow the pattern: PO[UF][YYYY].dbc
    Examples:
        - POBR2013.dbc -> UF=BR, Year=2013
        - POSP2023.dbc -> UF=SP, Year=2023
        - PORJ2020.dbc -> UF=RJ, Year=2020

    Args:
        filename: The filename to parse

    Returns:
        Result containing POFileInfo or error message

    Example:
        >>> result = parse_po_filename("POBR2013.dbc")
        >>> if isinstance(result, Success):
        ...     info = result.unwrap()
        ...     print(f"UF: {info.uf}, Year: {info.year}")
        UF: BR, Year: 2013
    """
    # Remove extension
    name_without_ext = filename.replace(".dbc", "").replace(".DBC", "")

    # PO pattern: PO[UF][YYYY]
    # Group 1: UF (2 characters)
    # Group 2: Year (4 digits)
    pattern = r"^PO([A-Z]{2})(\d{4})$"

    match = re.match(pattern, name_without_ext, re.IGNORECASE)
    if not match:
        return Failure(
            f"Filename '{filename}' does not match PO pattern PO[UF][YYYY].dbc"
        )

    uf_code = match.group(1).upper()
    year_str = match.group(2)

    # Validate UF code (BR is also valid for national data)
    valid_ufs = VALID_UFS | {"BR"}  # Add BR for national data
    if uf_code not in valid_ufs:
        return Failure(f"Invalid UF code '{uf_code}' in filename '{filename}'")

    # Convert year string to integer
    try:
        year_4digit = int(year_str)

        # Basic year validation (PO data seems to start from 2013)
        if year_4digit < 2013 or year_4digit > 2030:
            return Failure(
                f"Year '{year_4digit}' out of expected range (2013-2030) in filename '{filename}'"
            )

    except ValueError:
        return Failure(f"Invalid year format '{year_str}' in filename '{filename}'")

    return Success(
        POFileInfo(
            uf=uf_code,
            year=year_4digit,
            group="PO",
            raw_filename=filename,
        )
    )


def validate_po_pattern(filename: str) -> bool:
    """Quick validation if filename matches PO pattern.

    Args:
        filename: The filename to validate

    Returns:
        True if filename matches PO pattern, False otherwise
    """
    result = parse_po_filename(filename)
    return isinstance(result, Success)
