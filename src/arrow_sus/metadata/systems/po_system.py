"""PO (Painel de Oncologia) system implementation."""

from __future__ import annotations

from typing import Dict, List, Optional, TYPE_CHECKING

from returns.result import Failure, Result, Success

from arrow_sus.metadata.utils.filters import POFilters
from arrow_sus.metadata.utils.po.po_file_filter import discover_po_files
from arrow_sus.metadata.utils.po.po_file_parser import parse_po_filename

if TYPE_CHECKING:
    from arrow_sus.metadata.io.types import FileEntry


class PODatasusSystem:
    """PO (Painel de Oncologia) system.

    Handles files with pattern: PO[UF][YYYY].dbc
    Example: POBR2013.dbc (Brasil, 2013)
    """

    def __init__(self) -> None:
        """Initialize PO system."""
        self.system_id = "po"
        self.name = "Painel de Oncologia"
        self.base_path = "/dissemin/publicos/PAINEL_ONCOLOGIA/DADOS"

    async def discover_files(
        self, filters: Optional[POFilters] = None, max_files: int = 10000
    ) -> Result[List[FileEntry], Exception]:
        """Discover PO files with optional filtering."""
        return await discover_po_files(
            base_path=self.base_path, filters=filters, max_files=max_files
        )

    async def get_system_info(self) -> dict:
        """Get comprehensive system information."""
        try:
            # Discover all files to analyze patterns
            all_files_result = await self.discover_files(max_files=50000)
            if isinstance(all_files_result, Failure):
                return {
                    "system_id": self.system_id,
                    "name": self.name,
                    "total_files": 0,
                    "date_range": None,
                    "available_ufs": [],
                    "available_groups": [],
                    "pattern_info": "PO[UF][YYYY].dbc",
                }

            all_files = all_files_result.unwrap()

            # Parse files to extract metadata
            ufs = set()
            years = set()
            groups = set()

            for file_entry in all_files:
                parse_result = parse_po_filename(file_entry["filename"])
                if isinstance(parse_result, Success):
                    parsed = parse_result.unwrap()
                    if parsed.uf:
                        ufs.add(parsed.uf)
                    years.add(parsed.year)
                    if parsed.group:
                        groups.add(parsed.group)

            # Determine date range
            date_range = None
            if years:
                min_year = min(years)
                max_year = max(years)
                date_range = f"{min_year}-{max_year}"

            return {
                "system_id": self.system_id,
                "name": self.name,
                "total_files": len(all_files),
                "date_range": date_range,
                "available_ufs": sorted(list(ufs)),
                "available_groups": sorted(list(groups)) if groups else ["PO"],
                "pattern_info": "PO[UF][YYYY].dbc - Painel de Oncologia",
            }

        except Exception as e:
            return {
                "system_id": self.system_id,
                "name": self.name,
                "total_files": 0,
                "date_range": None,
                "available_ufs": [],
                "available_groups": [],
                "pattern_info": f"Error: {str(e)}",
            }

    def get_supported_filters(self) -> Dict[str, str]:
        """Get supported filter parameters."""
        return {
            "uf": "Brazilian state code (e.g., SP, RJ, BR for national)",
            "year": "Year (2013-2025, 4-digit)",
        }
