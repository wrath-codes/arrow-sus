"""Dictionary validation utilities for filter validation."""

from __future__ import annotations

from typing import Any, Dict, Set

from returns.result import Failure, Result, Success

from arrow_sus.metadata.core.errors import DatasusValidationError


def validate_dict_keys(
    input_dict: Dict[str, Any], valid_keys: Set[str]
) -> Result[Dict[str, Any], DatasusValidationError]:
    """Validate that dictionary keys are in the allowed set.

    Args:
        input_dict: Dictionary to validate
        valid_keys: Set of allowed keys

    Returns:
        Result with validated dict or validation error
    """
    invalid_keys = set(input_dict.keys()) - valid_keys

    if invalid_keys:
        return Failure(
            DatasusValidationError(
                invalid_keys=list(invalid_keys), valid_keys=list(valid_keys)
            )
        )

    return Success(input_dict)


__all__ = ["validate_dict_keys"]
