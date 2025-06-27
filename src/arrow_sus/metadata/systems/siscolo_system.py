"""SISCOLO (Sistema de Informação de Câncer do Colo do Útero) system implementation."""

from __future__ import annotations

from typing import Dict, List, Optional, TYPE_CHECKING

from returns.result import Failure, Result, Success

from arrow_sus.metadata.utils.filters import SISCOLOFilters
from arrow_sus.metadata.utils.siscolo.siscolo_file_filter import discover_siscolo_files
from arrow_sus.metadata.utils.siscolo.siscolo_file_parser import parse_siscolo_filename

if TYPE_CHECKING:
    from arrow_sus.metadata.io.types import FileEntry


class SISCOLODatasusSystem:
    """SISCOLO (Sistema de Informação de Câncer do Colo do Útero) system.

    Handles files with pattern: [CC|HC][UF][YY][MM].dbc
    Groups:
    - CC: Citopatológico de Colo de Útero
    - HC: Histopatológico de Colo de Útero

    Examples:
    - CCAC0601.dbc (CC group, Acre, 2006, January)
    - HCSP2312.dbc (HC group, São Paulo, 2023, December)
    """

    def __init__(self) -> None:
        """Initialize SISCOLO system."""
        self.system_id = "siscolo"
        self.name = "Sistema de Informação de Câncer do Colo do Útero"
        self.base_path = "/dissemin/publicos/SISCAN/SISCOLO4/Dados"

    async def discover_files(
        self, filters: Optional[SISCOLOFilters] = None, max_files: int = 10000
    ) -> Result[List[FileEntry], Exception]:
        """Discover SISCOLO files with optional filtering."""
        return await discover_siscolo_files(
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
                    "pattern_info": "[CC|HC][UF][YY][MM].dbc",
                }

            all_files = all_files_result.unwrap()

            # Parse files to extract metadata
            ufs = set()
            years = set()
            months = set()
            groups = set()

            for file_entry in all_files:
                parse_result = parse_siscolo_filename(file_entry["filename"])
                if isinstance(parse_result, Success):
                    parsed = parse_result.unwrap()
                    if parsed.uf:
                        ufs.add(parsed.uf)
                    years.add(parsed.year)
                    months.add(parsed.month)
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
                "available_groups": sorted(list(groups)),
                "available_months": sorted(list(months)),
                "pattern_info": "[CC|HC][UF][YY][MM].dbc - Sistema de Informação de Câncer do Colo do Útero",
            }

        except Exception as e:
            return {
                "system_id": self.system_id,
                "name": self.name,
                "total_files": 0,
                "date_range": None,
                "available_ufs": [],
                "available_groups": [],
                "available_months": [],
                "pattern_info": f"Error: {str(e)}",
            }

    def get_supported_filters(self) -> Dict[str, str]:
        """Get supported filter parameters."""
        return {
            "group": "Data group (CC for Citopatológico, HC for Histopatológico)",
            "uf": "Brazilian state code (e.g., SP, RJ, AC)",
            "year": "Year (2000-2099, 2-digit or 4-digit)",
            "month": "Month (1-12)",
        }
