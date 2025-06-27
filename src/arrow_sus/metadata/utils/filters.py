"""Filter types and validation for DATASUS metadata searches."""

from __future__ import annotations

from typing import List, Set, TypedDict, Union


class MonthlyFilters(TypedDict, total=False):
    """Filters for monthly DATASUS systems (SIA, SIH, CNES, etc.)."""

    group: Union[str, List[str]]
    uf: Union[str, List[str]]
    year: Union[int, str, List[Union[int, str]]]
    month: Union[int, str, List[Union[int, str]]]


class LatestFilters(TypedDict, total=False):
    """Filters for getting latest files from monthly systems."""

    group: Union[str, List[str]]
    states: Union[str, List[str]]
    months: int
    current_year: bool


class YearlyFilters(TypedDict, total=False):
    """Filters for yearly DATASUS systems (SINASC, SINAN, etc.)."""

    uf: Union[str, List[str]]
    year: Union[int, str, List[Union[int, str]]]


class SourceFilters(TypedDict, total=False):
    """General filters for source-based searches."""

    source: str
    group: Union[str, List[str]]
    uf: Union[str, List[str]]
    year: Union[int, str, List[Union[int, str]]]
    month: Union[int, str, List[Union[int, str]]]


# Valid filter keys for each filter type
VALID_MONTHLY_FILTERS: Set[str] = {"group", "uf", "year", "month"}
VALID_LATEST_FILTERS: Set[str] = {"group", "states", "months", "current_year"}
VALID_YEARLY_FILTERS: Set[str] = {"uf", "year"}
VALID_SOURCE_FILTERS: Set[str] = {"source", "group", "uf", "year", "month"}


__all__ = [
    "MonthlyFilters",
    "LatestFilters",
    "YearlyFilters",
    "SourceFilters",
    "VALID_MONTHLY_FILTERS",
    "VALID_LATEST_FILTERS",
    "VALID_YEARLY_FILTERS",
    "VALID_SOURCE_FILTERS",
]
