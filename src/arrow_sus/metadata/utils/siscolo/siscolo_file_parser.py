"""SISCOLO filename parsing utilities."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from returns.result import Failure, Result, Success

from arrow_sus.metadata.utils.yearly.yearly_file_parser import zfill_year
from arrow_sus.metadata.utils.validation import VALID_UFS


@dataclass(frozen=True)
class SISCOLOFileInfo:
    """Information extracted from SISCOLO filename."""

    group: str  # CC or HC
    uf: str
    year: int
    month: int
    raw_filename: str = ""


def parse_siscolo_filename(filename: str) -> Result[SISCOLOFileInfo, str]:
    """Parse SISCOLO filename to extract metadata.

    SISCOLO files follow the pattern: [CC|HC][UF][YY][MM].dbc
    Groups:
    - CC: Citopatológico de Colo de Útero
    - HC: Histopatológico de Colo de Útero

    Examples:
        - CCAC0601.dbc -> Group=CC, UF=AC, Year=2006, Month=1
        - HCSP2312.dbc -> Group=HC, UF=SP, Year=2023, Month=12
        - CCAL0705.dbc -> Group=CC, UF=AL, Year=2007, Month=5

    Args:
        filename: The filename to parse

    Returns:
        Result containing SISCOLOFileInfo or error message

    Example:
        >>> result = parse_siscolo_filename("CCAC0601.dbc")
        >>> if isinstance(result, Success):
        ...     info = result.unwrap()
        ...     print(f"Group: {info.group}, UF: {info.uf}, Year: {info.year}, Month: {info.month}")
        Group: CC, UF: AC, Year: 2006, Month: 1
    """
    # Remove extension
    name_without_ext = filename.replace(".dbc", "").replace(".DBC", "")

    # SISCOLO pattern: [CC|HC][UF][YY][MM]
    # Group 1: Group (CC or HC)
    # Group 2: UF (2 characters)
    # Group 3: Year (2 digits)
    # Group 4: Month (2 digits)
    pattern = r"^(CC|HC)([A-Z]{2})(\d{2})(\d{2})$"

    match = re.match(pattern, name_without_ext, re.IGNORECASE)
    if not match:
        return Failure(
            f"Filename '{filename}' does not match SISCOLO pattern [CC|HC][UF][YY][MM].dbc"
        )

    group_code = match.group(1).upper()
    uf_code = match.group(2).upper()
    year_2digit = match.group(3)
    month_2digit = match.group(4)

    # Validate group code
    if group_code not in ("CC", "HC"):
        return Failure(
            f"Invalid group code '{group_code}' in filename '{filename}'. Must be CC or HC"
        )

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

    # Validate and convert month
    try:
        month_int = int(month_2digit)
        if month_int < 1 or month_int > 12:
            return Failure(
                f"Invalid month '{month_2digit}' in filename '{filename}'. Must be 01-12"
            )
    except ValueError:
        return Failure(
            f"Invalid month format '{month_2digit}' in filename '{filename}'"
        )

    return Success(
        SISCOLOFileInfo(
            group=group_code,
            uf=uf_code,
            year=year_4digit,
            month=month_int,
            raw_filename=filename,
        )
    )


def validate_siscolo_pattern(filename: str) -> bool:
    """Quick validation if filename matches SISCOLO pattern.

    Args:
        filename: The filename to validate

    Returns:
        True if filename matches SISCOLO pattern, False otherwise
    """
    result = parse_siscolo_filename(filename)
    return isinstance(result, Success)
