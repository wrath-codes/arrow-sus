"""Validation utilities for DATASUS metadata search and filtering.

This module provides comprehensive validation for search parameters including
UF codes, date ranges, dataset names, and file patterns. It implements robust
error handling using Result types for predictable error management.

Examples:
    ```python
    from arrow_sus.metadata.utils.validation import validate_search_params
    from returns.result import Success, Failure

    # Validate search parameters
    result = validate_search_params(
        uf="SP",
        year=2024,
        month=5,
        dataset="sia-pa"
    )

    match result:
        case Success(params):
            print(f"Valid parameters: {params}")
        case Failure(error):
            print(f"Validation error: {error}")
    ```

Performance:
    - Parameter validation completes in <1ms
    - Regex compilation is cached for repeated use
    - UF validation uses set lookup (O(1))

Dependencies:
    - returns: Result types for error handling
    - re: Regex pattern compilation and matching
"""

import re
from typing import Dict, List, Optional, Union, Any
from datetime import datetime
from pathlib import Path

from returns.result import Result, Success, Failure
from returns.maybe import Maybe, Some, Nothing

from ..core.models import UFCode, DatasetSource


class ValidationError(Exception):
    """Base class for validation errors."""

    def __init__(self, message: str, field: str, value: Any):
        """Initialize validation error.

        Args:
            message: Human-readable error message
            field: Field name that failed validation
            value: Invalid value that caused the error
        """
        self.message = message
        self.field = field
        self.value = value
        super().__init__(f"{field}: {message} (got: {value})")


class UFValidationError(ValidationError):
    """Error for invalid UF codes."""

    pass


class DateValidationError(ValidationError):
    """Error for invalid date values."""

    pass


class DatasetValidationError(ValidationError):
    """Error for invalid dataset names."""

    pass


# Valid UF codes for fast lookup
VALID_UFS = {
    "AC",
    "AL",
    "AP",
    "AM",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MT",
    "MS",
    "MG",
    "PA",
    "PB",
    "PR",
    "PE",
    "PI",
    "RJ",
    "RN",
    "RS",
    "RO",
    "RR",
    "SC",
    "SP",
    "SE",
    "TO",
}

# Regex patterns for common DATASUS filename formats (cached)
FILENAME_PATTERNS = {
    "uf_year2_month": re.compile(r"^[A-Z]{2}\d{4}\.(DBC|DBF)$", re.IGNORECASE),
    "uf_year4_month": re.compile(r"^[A-Z]{2}\d{6}\.(DBC|DBF)$", re.IGNORECASE),
    "uf_year4": re.compile(r"^[A-Z]{2}\d{4}\.(DBC|DBF)$", re.IGNORECASE),
    "year4": re.compile(r"^\d{4}\.(DBC|DBF)$", re.IGNORECASE),
    "uf_year2_month_sia_pa": re.compile(
        r"^[A-Z]{2}\d{4}[a-z]?\.(DBC|DBF)$", re.IGNORECASE
    ),
    "uf_mapas_year": re.compile(r"^[A-Z]{2}_mapas_\d{4}\.(DBC|DBF)$", re.IGNORECASE),
    "uf_cnv": re.compile(r"^[A-Z]{2}_cnv\.(DBC|DBF)$", re.IGNORECASE),
}


def validate_uf(uf: Union[str, List[str]]) -> Result[List[str], UFValidationError]:
    """Validate UF code(s).

    Args:
        uf: Single UF code or list of UF codes

    Returns:
        Result with validated UF codes or validation error

    Example:
        ```python
        # Single UF
        result = validate_uf("SP")
        assert result == Success(["SP"])

        # Multiple UFs
        result = validate_uf(["SP", "RJ", "MG"])
        assert result == Success(["SP", "RJ", "MG"])

        # Invalid UF
        result = validate_uf("XX")
        assert isinstance(result, Failure)
        ```
    """
    if isinstance(uf, str):
        ufs = [uf.upper()]
    else:
        ufs = [u.upper() for u in uf]

    invalid_ufs = [u for u in ufs if u not in VALID_UFS]
    if invalid_ufs:
        return Failure(
            UFValidationError(f"Invalid UF codes: {invalid_ufs}", "uf", invalid_ufs)
        )

    return Success(ufs)


def validate_year(
    year: Union[int, str, List[Union[int, str]]],
) -> Result[List[int], DateValidationError]:
    """Validate year(s) for DATASUS data.

    Args:
        year: Single year or list of years (int or string)

    Returns:
        Result with validated years or validation error

    Example:
        ```python
        # Single year
        result = validate_year(2024)
        assert result == Success([2024])

        # Year as string
        result = validate_year("2024")
        assert result == Success([2024])

        # Multiple years
        result = validate_year([2023, "2024"])
        assert result == Success([2023, 2024])

        # Invalid year
        result = validate_year(1960)
        assert isinstance(result, Failure)
        ```
    """
    if not isinstance(year, list):
        years = [year]
    else:
        years = year

    validated_years = []
    for y in years:
        try:
            year_int = int(y)
            if year_int < 1970 or year_int > datetime.now().year + 1:
                return Failure(
                    DateValidationError(
                        f"Year must be between 1970 and {datetime.now().year + 1}",
                        "year",
                        year_int,
                    )
                )
            validated_years.append(year_int)
        except (ValueError, TypeError):
            return Failure(DateValidationError(f"Invalid year format", "year", y))

    return Success(validated_years)


def validate_month(
    month: Union[int, str, List[Union[int, str]]],
) -> Result[List[int], DateValidationError]:
    """Validate month(s).

    Args:
        month: Single month or list of months (1-12)

    Returns:
        Result with validated months or validation error

    Example:
        ```python
        # Single month
        result = validate_month(5)
        assert result == Success([5])

        # Month as string
        result = validate_month("05")
        assert result == Success([5])

        # Multiple months
        result = validate_month([1, "12"])
        assert result == Success([1, 12])

        # Invalid month
        result = validate_month(13)
        assert isinstance(result, Failure)
        ```
    """
    if not isinstance(month, list):
        months = [month]
    else:
        months = month

    validated_months = []
    for m in months:
        try:
            month_int = int(m)
            if month_int < 1 or month_int > 12:
                return Failure(
                    DateValidationError(
                        "Month must be between 1 and 12", "month", month_int
                    )
                )
            validated_months.append(month_int)
        except (ValueError, TypeError):
            return Failure(DateValidationError("Invalid month format", "month", m))

    return Success(validated_months)


def validate_dataset_name(
    dataset: str, available_datasets: List[str]
) -> Result[str, DatasetValidationError]:
    """Validate dataset name against available datasets.

    Args:
        dataset: Dataset name to validate
        available_datasets: List of available dataset names

    Returns:
        Result with validated dataset name or validation error

    Example:
        ```python
        datasets = ["sia-pa", "sih-rd", "sinasc-dn"]

        # Valid dataset
        result = validate_dataset_name("sia-pa", datasets)
        assert result == Success("sia-pa")

        # Invalid dataset
        result = validate_dataset_name("invalid", datasets)
        assert isinstance(result, Failure)
        ```
    """
    if dataset not in available_datasets:
        return Failure(
            DatasetValidationError(
                f"Dataset not found. Available datasets: {available_datasets[:10]}{'...' if len(available_datasets) > 10 else ''}",
                "dataset",
                dataset,
            )
        )

    return Success(dataset)


def build_filename_regex(
    prefix: Optional[str] = None,
    uf_codes: Optional[List[str]] = None,
    years: Optional[List[int]] = None,
    months: Optional[List[int]] = None,
    pattern_type: str = "uf_year2_month",
) -> str:
    """Build regex pattern for DATASUS filename matching.

    Args:
        prefix: File prefix (e.g., "PA", "RD")
        uf_codes: List of valid UF codes
        years: List of valid years
        months: List of valid months
        pattern_type: Pattern type for filename structure

    Returns:
        Compiled regex pattern string

    Example:
        ```python
        # Match SIA-PA files for SP in 2024
        pattern = build_filename_regex(
            prefix="PA",
            uf_codes=["SP"],
            years=[2024],
            months=[1, 2, 3],
            pattern_type="uf_year2_month"
        )

        # Pattern matches: PASP2401.dbc, PASP2402.dbc, PASP2403.dbc
        ```
    """
    pattern_parts = ["^"]

    # Add prefix if specified
    if prefix:
        pattern_parts.append(f"({prefix.upper()})")

    # Add UF codes
    if uf_codes:
        pattern_parts.append(f"({'|'.join(uf_codes)})")
    else:
        pattern_parts.append(r"[A-Z]{2}")

    # Add year pattern based on type
    if pattern_type in ["uf_year2_month", "uf_month_year2"]:
        if years:
            year_codes = [str(y)[-2:] for y in years]
            pattern_parts.append(f"({'|'.join(year_codes)})")
        else:
            pattern_parts.append(r"\d{2}")
    elif pattern_type in ["uf_year4_month", "uf_year4"]:
        if years:
            year_codes = [str(y) for y in years]
            pattern_parts.append(f"({'|'.join(year_codes)})")
        else:
            pattern_parts.append(r"\d{4}")

    # Add month pattern if required
    if "month" in pattern_type:
        if months:
            month_codes = [str(m).zfill(2) for m in months]
            pattern_parts.append(f"({'|'.join(month_codes)})")
        else:
            pattern_parts.append(r"\d{2}")

    # Add file extension
    pattern_parts.append(r"\.(DBC|DBF)$")

    return "".join(pattern_parts)


def validate_search_params(
    dataset: Optional[str] = None,
    uf: Optional[Union[str, List[str]]] = None,
    year: Optional[Union[int, str, List[Union[int, str]]]] = None,
    month: Optional[Union[int, str, List[Union[int, str]]]] = None,
    available_datasets: Optional[List[str]] = None,
) -> Result[Dict[str, Any], ValidationError]:
    """Validate all search parameters together.

    Args:
        dataset: Dataset name to validate
        uf: UF code(s) to validate
        year: Year(s) to validate
        month: Month(s) to validate
        available_datasets: List of available datasets for validation

    Returns:
        Result with validated parameters dict or first validation error

    Example:
        ```python
        result = validate_search_params(
            dataset="sia-pa",
            uf="SP",
            year=2024,
            month=5,
            available_datasets=["sia-pa", "sih-rd"]
        )

        match result:
            case Success(params):
                # params = {
                #     "dataset": "sia-pa",
                #     "uf_codes": ["SP"],
                #     "years": [2024],
                #     "months": [5]
                # }
                pass
            case Failure(error):
                print(f"Validation failed: {error}")
        ```
    """
    validated_params = {}

    # Validate dataset
    if dataset and available_datasets:
        result = validate_dataset_name(dataset, available_datasets)
        if isinstance(result, Failure):
            return result
        validated_params["dataset"] = result.unwrap()
    elif dataset:
        validated_params["dataset"] = dataset

    # Validate UF codes
    if uf:
        result = validate_uf(uf)
        if isinstance(result, Failure):
            return result
        validated_params["uf_codes"] = result.unwrap()

    # Validate years
    if year:
        result = validate_year(year)
        if isinstance(result, Failure):
            return result
        validated_params["years"] = result.unwrap()

    # Validate months
    if month:
        result = validate_month(month)
        if isinstance(result, Failure):
            return result
        validated_params["months"] = result.unwrap()

    return Success(validated_params)


__all__ = [
    "ValidationError",
    "UFValidationError",
    "DateValidationError",
    "DatasetValidationError",
    "validate_uf",
    "validate_year",
    "validate_month",
    "validate_dataset_name",
    "validate_search_params",
    "build_filename_regex",
    "VALID_UFS",
    "FILENAME_PATTERNS",
]
