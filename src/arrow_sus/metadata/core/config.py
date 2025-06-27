"""Configuration for DATASUS metadata system."""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


class DataSUSConfig(BaseSettings):
    """Configuration for DATASUS metadata system."""

    # FTP settings
    ftp_host: str = Field(default="ftp.datasus.gov.br", description="DATASUS FTP host")
    ftp_encoding: str = Field(default="latin-1", description="FTP encoding")

    # Performance settings
    max_connections: int = Field(default=10, description="Maximum FTP connections")
    max_concurrent_operations: int = Field(
        default=20, description="Maximum concurrent operations"
    )
    connection_timeout: int = Field(
        default=30, description="Connection timeout in seconds"
    )

    # Cache settings
    cache_ttl_hours: int = Field(default=24, description="Default cache TTL in hours")
    max_memory_cache_mb: int = Field(
        default=500, description="Maximum memory cache size in MB"
    )
    max_disk_cache_gb: int = Field(
        default=10, description="Maximum disk cache size in GB"
    )

    # Dataset configurations
    @property
    def datasets(self) -> Dict[str, Any]:
        """Get dataset configurations."""
        # Load datasets from extracted metadata
        import orjson
        from pathlib import Path

        metadata_file = (
            Path(__file__).parent.parent.parent.parent.parent
            / "extracted_metadata_by_source.json"
        )
        if metadata_file.exists():
            with open(metadata_file, "rb") as f:
                extracted_data = orjson.loads(f.read())
                # Convert the source-based format to our expected format
                datasets = {}
                for source_id, source_info in extracted_data.get("sources", {}).items():
                    for group_id, group_info in source_info.get("groups", {}).items():
                        # Create a dataset entry for each source-group combination
                        dataset_id = f"{source_id}-{group_id.lower()}"

                        # Convert group periods to dataset periods
                        periods = []
                        for period_config in group_info.get("periods", []):
                            period = {
                                "dir": f"/dissemin/publicos{period_config['directory']}",
                            }
                            # Use period-specific metadata
                            if "filename_prefix" in period_config:
                                period["filename_prefix"] = period_config[
                                    "filename_prefix"
                                ]
                            if "filename_pattern" in period_config:
                                period["filename_pattern"] = period_config[
                                    "filename_pattern"
                                ]
                            if "extension" in period_config:
                                period["extension"] = period_config["extension"]
                            periods.append(period)

                        datasets[dataset_id] = {
                            "name": f"{source_info['name']} - {group_id}",
                            "source": source_id,
                            "periods": periods,
                            "file_organization": source_info.get(
                                "file_organization", "unknown"
                            ),
                            "group": group_id,
                        }
                return datasets

        # Fallback to original small dataset list
        return {
            "sih-rd": {
                "name": "RD - AIH Reduzida",
                "source": "sih",
                "periods": [
                    {
                        "dir": "/dissemin/publicos/SIHSUS/199201_200712/Dados",
                        "filename_prefix": "RD",
                        "filename_pattern": "uf_year2_month",
                    },
                    {
                        "dir": "/dissemin/publicos/SIHSUS/200801_/Dados",
                        "filename_prefix": "RD",
                        "filename_pattern": "uf_year2_month",
                    },
                ],
            },
            "sih-er": {
                "name": "ER - AIH Rejeitadas com código de erro",
                "source": "sih",
                "periods": [
                    {
                        "dir": "/dissemin/publicos/SIHSUS/200801_/Dados",
                        "filename_prefix": "ER",
                        "filename_pattern": "uf_year2_month",
                    },
                ],
            },
            "sinasc-dn": {
                "name": "Declarações de nascidos vivos",
                "source": "sinasc",
                "periods": [
                    {
                        "dir": "/dissemin/publicos/SINASC/1994_1995/Dados/DNRES",
                        "filename_prefix": "DNR",
                        "filename_pattern": "uf_year4",
                    },
                    {
                        "dir": "/dissemin/publicos/SINASC/1996_/Dados/DNRES",
                        "filename_prefix": "DN",
                        "filename_pattern": "uf_year4",
                    },
                ],
            },
        }

    @property
    def documentation(self) -> Dict[str, Any]:
        """Get documentation configurations."""
        return {
            "sih": {
                "name": "SIH Documentation",
                "dir": ["/dissemin/publicos/SIHSUS/200801_/Doc"],
            },
            "sia": {
                "name": "SIA Documentation",
                "dir": ["/dissemin/publicos/SIASUS/200801_/Doc"],
            },
            "sim": {
                "name": "SIM Documentation",
                "dir": ["/dissemin/publicos/SIM/CID10/DOC"],
            },
            "sinasc": {
                "name": "SINASC Documentation",
                "dir": ["/dissemin/publicos/SINASC/Doc"],
            },
            "cnes": {
                "name": "CNES Documentation",
                "dir": ["/dissemin/publicos/CNES/200508_/Doc"],
            },
            "pni": {
                "name": "PNI Documentation",
                "dir": ["/dissemin/publicos/PNI/DOC"],
            },
        }

    @property
    def auxiliary_tables(self) -> Dict[str, Any]:
        """Get auxiliary table configurations."""
        return {
            "tabwin": {
                "name": "TabWin",
                "dir": ["/dissemin/publicos/TABWIN"],
            },
            "tabnet": {
                "name": "TabNet",
                "dir": ["/dissemin/publicos/TABNET"],
            },
            "cid": {
                "name": "CID Tables",
                "dir": ["/dissemin/publicos/CID"],
            },
            "cbo": {
                "name": "CBO Tables",
                "dir": ["/dissemin/publicos/CBO"],
            },
        }

    class Config:
        env_prefix = "DATASUS_"
        case_sensitive = False


class S3Config(BaseSettings):
    """S3 configuration for metadata storage."""

    bucket_name: str = Field(..., description="S3 bucket name for metadata")
    region: str = Field(default="us-east-1", description="AWS region")
    access_key_id: Optional[str] = Field(default=None, description="AWS access key ID")
    secret_access_key: Optional[str] = Field(
        default=None, description="AWS secret access key"
    )
    endpoint_url: Optional[str] = Field(
        default=None, description="Custom S3 endpoint URL"
    )

    class Config:
        env_prefix = "S3_"
        case_sensitive = False


class CacheConfig(BaseModel):
    """Cache configuration settings."""

    memory_max_size: int = Field(
        default=1000, description="Maximum memory cache entries"
    )
    memory_max_mb: int = Field(
        default=500, description="Maximum memory cache size in MB"
    )
    disk_max_gb: int = Field(default=10, description="Maximum disk cache size in GB")
    default_ttl_hours: int = Field(default=24, description="Default TTL in hours")
    cleanup_interval_minutes: int = Field(
        default=60, description="Cache cleanup interval"
    )


class PerformanceConfig(BaseModel):
    """Performance tuning configuration."""

    max_ftp_connections: int = Field(default=10, description="Maximum FTP connections")
    max_concurrent_downloads: int = Field(
        default=20, description="Maximum concurrent downloads"
    )
    connection_timeout: int = Field(
        default=30, description="Connection timeout in seconds"
    )
    retry_attempts: int = Field(default=3, description="Number of retry attempts")
    chunk_size: int = Field(default=8192, description="Download chunk size in bytes")
    enable_compression: bool = Field(
        default=True, description="Enable response compression"
    )
