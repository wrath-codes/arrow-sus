"""
DataSUS configuration module.

This module provides configuration settings for DataSUS data access,
including FTP and S3 mirror endpoints.
"""

from __future__ import annotations

from pydantic import Field, HttpUrl
from pydantic_settings import BaseSettings


class DataSusSettings(BaseSettings):
    """
    Configuration settings for DataSUS data access.

    This class manages all configuration related to DataSUS data sources,
    including FTP servers, S3 mirrors, and CDN endpoints.

    Attributes:
        datasus_ftp_host (str): DataSUS FTP server hostname.
        datasus_ftp_base_path (str): Base path for DataSUS FTP files.
        datasus_s3_mirror (HttpUrl): S3 mirror URL for DataSUS files.
        datasus_s3_cdn (HttpUrl): CDN URL for DataSUS S3 mirror.
        datasus_s3_rclone_paths_file (str): Filename for rclone paths configuration.

    Example:
        >>> settings = DataSusSettings()
        >>> print(settings.datasus_ftp_host)
        ftp.datasus.gov.br

        >>> # Override via environment variables
        >>> import os
        >>> os.environ['DATASUS_FTP_HOST'] = 'custom.ftp.host'
        >>> settings = DataSusSettings()
        >>> print(settings.datasus_ftp_host)
        custom.ftp.host

    Note:
        Values can be overridden via environment variables or .env file.
        Environment variable names should match the field names in uppercase.
    """

    datasus_ftp_host: str = Field(
        default="ftp.datasus.gov.br", description="DataSUS FTP server hostname"
    )

    datasus_ftp_base_path: str = Field(
        default="/dissemin/publicos", description="Base path for DataSUS FTP files"
    )

    datasus_s3_mirror: HttpUrl = Field(
        default="https://datasus-ftp-mirror.nyc3.digitaloceanspaces.com",
        description="S3 mirror URL for DataSUS files",
    )

    datasus_s3_cdn: HttpUrl = Field(
        default="https://datasus-ftp-mirror.nyc3.cdn.digitaloceanspaces.com",
        description="CDN URL for DataSUS S3 mirror",
    )

    datasus_s3_rclone_paths_file: str = Field(
        default="rclone_datasus_full_path.txt",
        description="Filename for rclone paths configuration",
    )

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "allow",
        "case_sensitive": False,
    }

    def get_ftp_url(self) -> str:
        """
        Construct the complete FTP URL.

        Returns:
            str: Complete FTP URL with host and base path.

        Example:
            >>> settings = DataSusSettings()
            >>> settings.get_ftp_url()
            'ftp://ftp.datasus.gov.br/dissemin/publicos'
        """
        return f"ftp://{self.datasus_ftp_host}{self.datasus_ftp_base_path}"

    def get_s3_mirror_url(self, path: str = "") -> str:
        """
        Construct S3 mirror URL with optional path.

        Args:
            path (str): Optional path to append to the base URL.

        Returns:
            str: Complete S3 mirror URL.

        Example:
            >>> settings = DataSusSettings()
            >>> settings.get_s3_mirror_url("some/file.txt")
            'https://datasus-ftp-mirror.nyc3.digitaloceanspaces.com/some/file.txt'
        """
        base_url = str(self.datasus_s3_mirror).rstrip("/")
        if path:
            path = path.lstrip("/")
            return f"{base_url}/{path}"
        return base_url

    def get_s3_cdn_url(self, path: str = "") -> str:
        """
        Construct S3 CDN URL with optional path.

        Args:
            path (str): Optional path to append to the base URL.

        Returns:
            str: Complete S3 CDN URL.

        Example:
            >>> settings = DataSusSettings()
            >>> settings.get_s3_cdn_url("some/file.txt")
            'https://datasus-ftp-mirror.nyc3.cdn.digitaloceanspaces.com/some/file.txt'
        """
        base_url = str(self.datasus_s3_cdn).rstrip("/")
        if path:
            path = path.lstrip("/")
            return f"{base_url}/{path}"
        return base_url


# Global instance for easy access
datasus_settings = DataSusSettings()


__all__ = ["DataSusSettings", "datasus_settings"]
