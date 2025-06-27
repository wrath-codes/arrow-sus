"""Custom error types for DATASUS metadata operations."""

from __future__ import annotations

from typing import Any, Dict, List


class DatasusError(Exception):
    """Base exception for DATASUS operations."""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class DatasusDataNotFoundError(DatasusError):
    """Raised when requested DATASUS data is not found."""

    def __init__(
        self, source: str, reason: str, filters: Dict[str, Any] | None = None
    ) -> None:
        self.source = source
        self.reason = reason
        self.filters = filters or {}
        message = f"Data not found in {source}: {reason}"
        if filters:
            message += f" (filters: {filters})"
        super().__init__(message)


class DatasusFileParsingError(DatasusError):
    """Raised when file parsing fails."""

    def __init__(self, source: str, filename: str, reason: str) -> None:
        self.source = source
        self.filename = filename
        self.reason = reason
        message = f"Failed to parse file {filename} from {source}: {reason}"
        super().__init__(message)


class DatasusValidationError(DatasusError):
    """Raised when validation fails."""

    def __init__(
        self, invalid_keys: List[str], valid_keys: List[str] | None = None
    ) -> None:
        self.invalid_keys = invalid_keys
        self.valid_keys = valid_keys or []
        message = f"Invalid keys: {invalid_keys}"
        if valid_keys:
            message += f". Valid keys: {valid_keys}"
        super().__init__(message)


__all__ = [
    "DatasusError",
    "DatasusDataNotFoundError",
    "DatasusFileParsingError",
    "DatasusValidationError",
]
