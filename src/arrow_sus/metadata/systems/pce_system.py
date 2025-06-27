"""PCE (Programa de Controle da Esquistossomose) system implementation."""

from __future__ import annotations

from typing import Dict, List, Optional, TYPE_CHECKING

from returns.result import Failure, Result, Success

from arrow_sus.metadata.utils.filters import PCEFilters
from arrow_sus.metadata.utils.pce.pce_file_filter import discover_pce_files
from arrow_sus.metadata.utils.pce.pce_file_parser import parse_pce_filename

if TYPE_CHECKING:
    from arrow_sus.metadata.io.types import FileEntry


class PCEDatasusSystem:
    """PCE (Programa de Controle da Esquistossomose) system.

    Handles files with pattern: PCE[UF][YY].dbc
    Example: PCEAL00.dbc (Alagoas, 2000)
    """

    def __init__(self) -> None:
        """Initialize PCE system."""
        self.system_id = "pce"
        self.name = "Programa de Controle da Esquistossomose"
        self.base_path = "/dissemin/publicos/PCE/DADOS"

    async def discover_files(
        self, filters: Optional[PCEFilters] = None, max_files: int = 10000
    ) -> Result[List[FileEntry], Exception]:
        """Discover PCE files with optional filtering."""
        return await discover_pce_files(
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
                    "pattern_info": "PCE[UF][YY].dbc",
                }

            all_files = all_files_result.unwrap()

            # Parse files to extract metadata
            ufs = set()
            years = set()
            groups = set()

            for file_entry in all_files:
                parse_result = parse_pce_filename(file_entry["filename"])
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
                "available_groups": sorted(list(groups)) if groups else ["PCE"],
                "pattern_info": "PCE[UF][YY].dbc - Programa de Controle da Esquistossomose",
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
            "uf": "Brazilian state code (e.g., SP, RJ)",
            "year": "Year (2000-2099, 2-digit or 4-digit)",
        }
