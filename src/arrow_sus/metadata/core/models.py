"""Pydantic models for type-safe DATASUS metadata handling.

This module defines the core data models used throughout the Arrow-SUS library
for handling DATASUS metadata. These models provide type safety, validation,
serialization, and caching capabilities for Brazilian health system data.

The models are designed to handle the complex structure of DATASUS files,
including different data sources (SIH, SIA, SIM, etc.), state partitions,
temporal organization, and various file formats.

Example:
    Basic usage of the core models:

    ```python
    from arrow_sus.metadata.core.models import (
        RemoteFile, DatasetMetadata, DataPartition,
        FileExtension, DatasetSource, UFCode
    )
    from datetime import datetime

    # Create a data partition
    partition = DataPartition(
        uf=UFCode.SP,
        year=2023,
        month=6
    )

    # Create a remote file metadata
    file_meta = RemoteFile(
        filename="RDSP2306.dbc",
        full_path="/dissemin/publicos/SIH/200801_/Dados/RDSP2306.dbc",
        datetime=datetime.now(),
        size=1_500_000,
        extension=FileExtension.DBC,
        dataset="SIH-RD",
        partition=partition
    )

    # Serialize to JSON
    json_data = file_meta.to_json_bytes()

    # Deserialize from JSON
    restored_file = RemoteFile.from_json_bytes(json_data)
    ```

Attributes:
    FileExtension: Enum of supported file extensions in DATASUS
    DatasetSource: Enum of DATASUS data sources (SIH, SIA, etc.)
    UFCode: Enum of Brazilian state codes
    DataPartition: Model for temporal and geographic data partitioning
    RemoteFile: Model for individual file metadata
    SubsystemInfo: Model for DATASUS subsystem information
    FileMetadata: Model for aggregated file statistics
    DatasetMetadata: Model for complete dataset information
    MetadataIndex: Model for the global metadata index
    CacheEntry: Model for cached data with TTL support
"""

from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import orjson
from pydantic import BaseModel, Field, computed_field, field_validator


def _serialize_for_json(data):
    """Convert data structure to be JSON serializable, handling sets.

    This utility function recursively processes nested data structures
    to ensure they can be serialized to JSON, with special handling
    for Python sets which are converted to sorted lists.

    Args:
        data: The data structure to serialize. Can be any combination
            of dict, list, tuple, set, or primitive types.

    Returns:
        A JSON-serializable version of the input data, with sets
        converted to sorted lists for consistent ordering.

    Example:
        ```python
        data = {"ufs": {"SP", "RJ", "MG"}, "years": [2020, 2021]}
        serializable = _serialize_for_json(data)
        # Result: {"ufs": ["MG", "RJ", "SP"], "years": [2020, 2021]}
        ```
    """
    if isinstance(data, dict):
        return {k: _serialize_for_json(v) for k, v in data.items()}
    elif isinstance(data, (list, tuple)):
        return [_serialize_for_json(item) for item in data]
    elif isinstance(data, set):
        return sorted(list(data))  # Convert sets to sorted lists
    return data


class FileExtension(str, Enum):
    """Supported file extensions in DATASUS FTP servers.

    This enum defines all file extensions that can be found in DATASUS
    FTP servers, including data files, documentation, and auxiliary files.
    The enum values are lowercase strings matching the actual file extensions.

    Attributes:
        DBC: Compressed database files (main data format)
        DBF: Database files in dBASE format
        PDF: Portable Document Format files (documentation)
        DOC: Microsoft Word documents (legacy format)
        DOCX: Microsoft Word documents (modern format)
        TXT: Plain text files
        ZIP: ZIP compressed archives
        RAR: RAR compressed archives
        XLS: Microsoft Excel spreadsheets (legacy format)
        XLSX: Microsoft Excel spreadsheets (modern format)
        CSV: Comma-separated values files
        HTML: HTML web pages
        HTM: HTML web pages (alternative extension)
        URL: URL shortcut files
        EXE: Executable files
        TAB: Tab-delimited text files

    Example:
        ```python
        # Check if a file is a data file
        extension = FileExtension.DBC
        is_data = extension in {FileExtension.DBC, FileExtension.DBF}
        print(f"Is data file: {is_data}")  # True

        # Get extension from filename
        filename = "RDSP2306.dbc"
        ext_str = filename.split('.')[-1].lower()
        if ext_str in [e.value for e in FileExtension]:
            extension = FileExtension(ext_str)
        ```
    """

    DBC = "dbc"
    DBF = "dbf"
    PDF = "pdf"
    DOC = "doc"
    DOCX = "docx"
    TXT = "txt"
    ZIP = "zip"
    RAR = "rar"
    XLS = "xls"
    XLSX = "xlsx"
    CSV = "csv"
    HTML = "html"
    HTM = "htm"
    URL = "url"
    EXE = "exe"
    TAB = "tab"


class DatasetSource(str, Enum):
    """DATASUS data sources representing different health information systems.

    This enum defines all major data sources available through DATASUS,
    each representing a different aspect of the Brazilian health system.
    These sources correspond to different information systems that collect
    and manage health-related data across Brazil.

    Attributes:
        SIH: Sistema de Informações Hospitalares (Hospital Information System)
            - Hospital admission and discharge data
        SIA: Sistema de Informações Ambulatoriais (Ambulatory Information System)
            - Outpatient care and ambulatory procedures data
        SIM: Sistema de Informações sobre Mortalidade (Mortality Information System)
            - Death certificates and mortality statistics
        SINASC: Sistema de Informações sobre Nascidos Vivos (Live Birth Information System)
            - Birth certificates and natality statistics
        CNES: Cadastro Nacional de Estabelecimentos de Saúde (National Health Facilities Registry)
            - Health facility registration and capacity data
        PNI: Programa Nacional de Imunizações (National Immunization Program)
            - Vaccination data and immunization coverage
        SINAN: Sistema de Informação de Agravos de Notificação (Notifiable Diseases Information System)
            - Reportable diseases and epidemiological surveillance data
        CIH: Comunicação de Internação Hospitalar (Hospital Admission Communication)
            - Real-time hospital admission notifications
        CIHA: Comunicação de Internação Hospitalar de Longa Permanência (Long-term Care Communications)
            - Long-term care facility admissions
        RESP: Cadastro Nacional de Estabelecimentos de Saúde - Dados Complementares (CNES Supplementary Data)
            - Additional health facility information
        SISPRENATAL: Sistema de Acompanhamento do Programa de Humanização no Pré-natal e Nascimento
            - Prenatal care monitoring system
        SISCOLO: Sistema de Informação do Câncer do Colo do Útero (Cervical Cancer Information System)
            - Cervical cancer screening and treatment data
        SISMAMA: Sistema de Informação do Câncer de Mama (Breast Cancer Information System)
            - Breast cancer screening and treatment data
        PCE: Programa de Controle de Esquistossomose (Schistosomiasis Control Program)
            - Schistosomiasis prevention and control data
        PO: Pesquisa de Orçamentos Familiares (Household Budget Survey)
            - Family budget and consumption data
        BASE_POPULACIONAL_IBGE: IBGE population base data
            - Demographic and population statistics from IBGE
        BASE_TERRITORIAL: Territorial base data
            - Geographic and administrative territorial information

    Example:
        ```python
        # Check if a source is related to hospital data
        source = DatasetSource.SIH
        hospital_sources = {DatasetSource.SIH, DatasetSource.CIH, DatasetSource.CIHA}
        is_hospital = source in hospital_sources

        # Get all available sources
        all_sources = list(DatasetSource)
        print(f"Available sources: {len(all_sources)}")
        ```
    """

    SIH = "sih"
    SIA = "sia"
    SIM = "sim"
    SINASC = "sinasc"
    CNES = "cnes"
    PNI = "pni"
    SINAN = "sinan"
    CIH = "cih"
    CIHA = "ciha"
    RESP = "resp"
    SISPRENATAL = "sisprenatal"
    SISCOLO = "siscolo"
    SISMAMA = "sismama"
    PCE = "pce"
    PO = "po"
    BASE_POPULACIONAL_IBGE = "base-populacional-ibge"
    BASE_TERRITORIAL = "base-territorial"


class UFCode(str, Enum):
    """Brazilian state codes (Unidades Federativas).

    This enum defines the official two-letter codes for all Brazilian states
    and the federal district, plus a special code for national-level data.
    These codes are used throughout DATASUS to organize data by geographic region.

    Attributes:
        AC: Acre
        AL: Alagoas
        AP: Amapá
        AM: Amazonas
        BA: Bahia
        CE: Ceará
        DF: Distrito Federal (Federal District)
        ES: Espírito Santo
        GO: Goiás
        MA: Maranhão
        MT: Mato Grosso
        MS: Mato Grosso do Sul
        MG: Minas Gerais
        PA: Pará
        PB: Paraíba
        PR: Paraná
        PE: Pernambuco
        PI: Piauí
        RJ: Rio de Janeiro
        RN: Rio Grande do Norte
        RS: Rio Grande do Sul
        RO: Rondônia
        RR: Roraima
        SC: Santa Catarina
        SP: São Paulo
        SE: Sergipe
        TO: Tocantins
        BR: Brazil (national level data)

    Example:
        ```python
        # Check if a state is in the Southeast region
        southeast_states = {UFCode.SP, UFCode.RJ, UFCode.MG, UFCode.ES}
        uf = UFCode.SP
        is_southeast = uf in southeast_states

        # Get all state codes except national
        states_only = [uf for uf in UFCode if uf != UFCode.BR]
        print(f"Number of states/DF: {len(states_only)}")  # 27
        ```
    """

    AC = "ac"
    AL = "al"
    AP = "ap"
    AM = "am"
    BA = "ba"
    CE = "ce"
    DF = "df"
    ES = "es"
    GO = "go"
    MA = "ma"
    MT = "mt"
    MS = "ms"
    MG = "mg"
    PA = "pa"
    PB = "pb"
    PR = "pr"
    PE = "pe"
    PI = "pi"
    RJ = "rj"
    RN = "rn"
    RS = "rs"
    RO = "ro"
    RR = "rr"
    SC = "sc"
    SP = "sp"
    SE = "se"
    TO = "to"
    BR = "br"  # Brazil (national)


class DataPartition(BaseModel):
    """Data partition information for organizing DATASUS files by time and geography.

    This model represents how DATASUS data is partitioned across temporal
    and geographic dimensions. Files are typically organized by state (UF),
    year, and sometimes month, with additional subpartitions for large datasets.

    Attributes:
        uf: Brazilian state code (UFCode). None for national-level data.
        year: Year of the data (1970-2030). None for non-temporal data.
        month: Month of the data (1-12). None for yearly data.
        subpartition: Additional partition identifier (e.g., 'a', 'b' for split files).

    Example:
        ```python
        # Monthly state-level data
        partition = DataPartition(
            uf=UFCode.SP,
            year=2023,
            month=6
        )
        print(partition.period_key)  # "2023-06"

        # Yearly national data
        partition = DataPartition(
            uf=UFCode.BR,
            year=2023
        )
        print(partition.period_key)  # "2023"

        # Data with subpartition
        partition = DataPartition(
            uf=UFCode.RJ,
            year=2023,
            month=12,
            subpartition="a"
        )
        ```
    """

    uf: Optional[UFCode] = None
    year: Optional[int] = Field(None, ge=1970, le=2030)
    month: Optional[int] = Field(None, ge=1, le=12)
    subpartition: Optional[str] = None  # Additional partition key (e.g., 'a', 'b')

    @field_validator("year")
    @classmethod
    def validate_year(cls, v: Optional[int]) -> Optional[int]:
        """Validate year is within acceptable range.

        Args:
            v: Year value to validate.

        Returns:
            The validated year value.

        Raises:
            ValueError: If year is not between 1970 and 2030.
        """
        if v is not None and (v < 1970 or v > 2030):
            raise ValueError("Year must be between 1970 and 2030")
        return v

    @computed_field
    @property
    def period_key(self) -> str:
        """Generate a standardized period key for indexing and sorting.

        Creates a string representation of the temporal partition that can be
        used for consistent indexing and sorting across different datasets.

        Returns:
            A period key string in format:
            - "YYYY-MM" for monthly data
            - "YYYY" for yearly data
            - "unknown" for data without year information

        Example:
            ```python
            # Monthly data
            partition = DataPartition(year=2023, month=6)
            assert partition.period_key == "2023-06"

            # Yearly data
            partition = DataPartition(year=2023)
            assert partition.period_key == "2023"

            # No temporal information
            partition = DataPartition()
            assert partition.period_key == "unknown"
            ```
        """
        if not self.year:
            return "unknown"
        if self.month:
            return f"{self.year}-{self.month:02d}"
        return str(self.year)


class RemoteFile(BaseModel):
    """Metadata for a remote file in DATASUS FTP servers.

    This model contains comprehensive metadata about individual files
    available in DATASUS FTP servers, including file properties,
    temporal/geographic partitioning, and data classification information.

    Attributes:
        filename: The filename without path (e.g., "RDSP2306.dbc").
        full_path: Complete FTP path to the file (e.g., "/dissemin/publicos/SIH/200801_/Dados/RDSP2306.dbc").
        datetime: File modification timestamp from FTP server.
        size: File size in bytes. None if unknown.
        extension: File extension enum. None if not recognized.
        dataset: Dataset identifier (e.g., "SIH-RD", "SIA-PA").
        partition: Temporal and geographic partition information.
        preliminary: Whether this is preliminary/provisional data.
        md5_hash: MD5 hash of file content. None if not computed.

    Example:
        ```python
        from datetime import datetime

        # Create metadata for a hospital data file
        file_meta = RemoteFile(
            filename="RDSP2306.dbc",
            full_path="/dissemin/publicos/SIH/200801_/Dados/RDSP2306.dbc",
            datetime=datetime(2023, 7, 15, 10, 30),
            size=1_500_000,
            extension=FileExtension.DBC,
            dataset="SIH-RD",
            partition=DataPartition(
                uf=UFCode.SP,
                year=2023,
                month=6
            ),
            preliminary=False
        )

        # Check file properties
        print(f"Size: {file_meta.size_mb:.1f} MB")  # Size: 1.5 MB
        print(f"Is data file: {file_meta.is_data_file}")  # True

        # Serialize to JSON
        json_bytes = file_meta.to_json_bytes()

        # Deserialize from JSON
        restored = RemoteFile.from_json_bytes(json_bytes)
        ```
    """

    filename: str
    full_path: str
    datetime: datetime
    size: Optional[int] = None
    extension: Optional[FileExtension] = None
    dataset: str
    partition: Optional[DataPartition] = None
    preliminary: bool = False
    md5_hash: Optional[str] = None

    class Config:
        # Use orjson for serialization
        json_encoders = {
            datetime: lambda v: v.isoformat(),
        }

    @computed_field
    @property
    def size_mb(self) -> Optional[float]:
        """File size in megabytes (MB).

        Converts the file size from bytes to megabytes for easier reading.
        Uses the decimal definition (1 MB = 1,000,000 bytes).

        Returns:
            File size in megabytes, or None if size is unknown.

        Example:
            ```python
            file_meta = RemoteFile(
                filename="test.dbc",
                full_path="/path/test.dbc",
                datetime=datetime.now(),
                size=2_500_000,
                dataset="test"
            )
            print(file_meta.size_mb)  # 2.5
            ```
        """
        return self.size / 1_000_000 if self.size else None

    @computed_field
    @property
    def is_data_file(self) -> bool:
        """Check if this is a data file (DBC or DBF format).

        Data files contain the actual health records, as opposed to
        documentation or auxiliary files. This property helps filter
        for files that contain analyzable data.

        Returns:
            True if the file extension is DBC or DBF, False otherwise.

        Example:
            ```python
            # Data file
            data_file = RemoteFile(
                filename="RDSP2306.dbc",
                extension=FileExtension.DBC,
                # ... other required fields
            )
            assert data_file.is_data_file == True

            # Documentation file
            doc_file = RemoteFile(
                filename="readme.pdf",
                extension=FileExtension.PDF,
                # ... other required fields
            )
            assert doc_file.is_data_file == False
            ```
        """
        return self.extension in {FileExtension.DBC, FileExtension.DBF}

    def to_json_bytes(self) -> bytes:
        """Serialize to JSON bytes using orjson for high performance.

        Uses orjson library for fast JSON serialization with consistent
        formatting (UTC timestamps, sorted keys).

        Returns:
            JSON serialized as bytes with UTF-8 encoding.

        Example:
            ```python
            file_meta = RemoteFile(...)
            json_data = file_meta.to_json_bytes()

            # Save to file
            with open("file_metadata.json", "wb") as f:
                f.write(json_data)
            ```
        """
        return orjson.dumps(
            self.model_dump(mode="json"), option=orjson.OPT_UTC_Z | orjson.OPT_SORT_KEYS
        )

    @classmethod
    def from_json_bytes(cls, data: bytes) -> "RemoteFile":
        """Deserialize from JSON bytes using orjson.

        Reconstructs a RemoteFile instance from JSON bytes created
        by the to_json_bytes method.

        Args:
            data: JSON data as bytes.

        Returns:
            RemoteFile instance with validated data.

        Raises:
            ValueError: If JSON is invalid or data doesn't match schema.

        Example:
            ```python
            # Load from file
            with open("file_metadata.json", "rb") as f:
                json_data = f.read()

            file_meta = RemoteFile.from_json_bytes(json_data)
            ```
        """
        return cls.model_validate(orjson.loads(data))


class SubsystemInfo(BaseModel):
    """Information about a DATASUS subsystem.

    This model contains metadata about a specific DATASUS subsystem,
    including its characteristics, data availability, and organization.
    Subsystems represent different health information systems within DATASUS.

    Attributes:
        name: Human-readable name of the subsystem.
        description: Detailed description of what the subsystem contains.
        source: The DatasetSource enum identifying this subsystem.
        groups: List of data groups/categories within the subsystem.
        supported_ufs: Set of state codes that have data in this subsystem.
        years_available: Set of years for which data is available.
        has_monthly_data: Whether the subsystem provides monthly data granularity.
        base_path: Base FTP path where the subsystem data is stored.

    Example:
        ```python
        # Hospital information subsystem
        sih_info = SubsystemInfo(
            name="Sistema de Informações Hospitalares",
            description="Hospital admission and discharge data",
            source=DatasetSource.SIH,
            groups=["RD", "RJ", "ER", "SP"],
            supported_ufs={UFCode.SP, UFCode.RJ, UFCode.MG},
            years_available={2020, 2021, 2022, 2023},
            has_monthly_data=True,
            base_path="/dissemin/publicos/SIH"
        )

        # Check data availability
        has_sp_data = UFCode.SP in sih_info.supported_ufs
        has_2023_data = 2023 in sih_info.years_available
        ```
    """

    name: str
    description: str
    source: DatasetSource
    groups: List[str] = Field(default_factory=list)
    supported_ufs: Set[UFCode] = Field(default_factory=set)
    years_available: Set[int] = Field(default_factory=set)
    has_monthly_data: bool = True
    base_path: str

    class Config:
        # Custom JSON encoder for sets
        json_encoders = {
            set: list,
        }


class FileMetadata(BaseModel):
    """Aggregated metadata for files in a dataset.

    This model contains summary statistics and metadata about all files
    within a specific dataset, providing a high-level overview of data
    availability, coverage, and characteristics.

    Attributes:
        dataset: Dataset identifier (e.g., "SIH-RD", "SIA-PA").
        total_files: Total number of files in the dataset.
        total_size_bytes: Combined size of all files in bytes.
        supported_ufs: Set of state codes that have data in this dataset.
        available_periods: Set of period keys (e.g., "2023-06", "2023") for available data.
        first_period: Earliest period with data. None if no temporal data.
        last_period: Latest period with data. None if no temporal data.
        last_updated: Timestamp when this metadata was last updated.
        file_extensions: Set of file extensions found in this dataset.

    Example:
        ```python
        from datetime import datetime

        # Create metadata for a hospital dataset
        metadata = FileMetadata(
            dataset="SIH-RD",
            total_files=324,
            total_size_bytes=1_500_000_000,
            supported_ufs={UFCode.SP, UFCode.RJ, UFCode.MG},
            available_periods={"2023-01", "2023-02", "2023-03"},
            first_period="2023-01",
            last_period="2023-03",
            last_updated=datetime.now(),
            file_extensions={FileExtension.DBC, FileExtension.PDF}
        )

        # Access computed properties
        print(f"Total size: {metadata.total_size_gb:.1f} GB")  # 1.5 GB
        print(f"States covered: {len(metadata.supported_ufs)}")  # 3
        ```
    """

    dataset: str
    total_files: int
    total_size_bytes: int
    supported_ufs: Set[UFCode] = Field(default_factory=set)
    available_periods: Set[str] = Field(default_factory=set)
    first_period: Optional[str] = None
    last_period: Optional[str] = None
    last_updated: datetime
    file_extensions: Set[FileExtension] = Field(default_factory=set)

    @computed_field
    @property
    def total_size_gb(self) -> float:
        """Total size in gigabytes (GB).

        Converts the total size from bytes to gigabytes for easier reading.
        Uses the decimal definition (1 GB = 1,000,000,000 bytes).

        Returns:
            Total size in gigabytes.

        Example:
            ```python
            metadata = FileMetadata(
                dataset="test",
                total_files=10,
                total_size_bytes=2_500_000_000,
                last_updated=datetime.now()
            )
            print(metadata.total_size_gb)  # 2.5
            ```
        """
        return self.total_size_bytes / 1_000_000_000

    class Config:
        json_encoders = {
            set: list,
            datetime: lambda v: v.isoformat(),
        }


class DatasetMetadata(BaseModel):
    """Complete metadata for a dataset including files and summary statistics.

    This model represents the complete metadata for a DATASUS dataset,
    combining summary statistics with detailed information about each file.
    It serves as the primary data structure for dataset information.

    Attributes:
        name: Human-readable name of the dataset.
        source: The DatasetSource enum identifying the data source.
        metadata: Aggregated metadata and statistics for the dataset.
        files: List of all RemoteFile objects in the dataset.

    Example:
        ```python
        from datetime import datetime
        from pathlib import Path

        # Create complete dataset metadata
        dataset = DatasetMetadata(
            name="Hospital Information System - Admissions",
            source=DatasetSource.SIH,
            metadata=FileMetadata(
                dataset="SIH-RD",
                total_files=2,
                total_size_bytes=3_000_000,
                supported_ufs={UFCode.SP},
                available_periods={"2023-06"},
                last_updated=datetime.now(),
                file_extensions={FileExtension.DBC}
            ),
            files=[
                RemoteFile(
                    filename="RDSP2306.dbc",
                    full_path="/dissemin/publicos/SIH/200801_/Dados/RDSP2306.dbc",
                    datetime=datetime.now(),
                    size=1_500_000,
                    extension=FileExtension.DBC,
                    dataset="SIH-RD"
                )
            ]
        )

        # Save to file
        await dataset.save_to_file(Path("sih_metadata.json"))

        # Load from file
        restored = await DatasetMetadata.load_from_file(Path("sih_metadata.json"))
        ```
    """

    name: str
    source: DatasetSource
    metadata: FileMetadata
    files: List[RemoteFile]

    def to_json_bytes(self) -> bytes:
        """Serialize to JSON bytes using orjson with pretty formatting.

        Converts the complete dataset metadata to JSON bytes using orjson
        for high performance, with indentation for readability.

        Returns:
            JSON serialized as bytes with UTF-8 encoding and pretty formatting.

        Example:
            ```python
            dataset = DatasetMetadata(...)
            json_data = dataset.to_json_bytes()

            # The resulting JSON is formatted with indentation
            print(json_data.decode('utf-8'))
            ```
        """
        data = self.model_dump(mode="json")
        serializable_data = _serialize_for_json(data)
        return orjson.dumps(
            serializable_data,
            option=orjson.OPT_UTC_Z | orjson.OPT_SORT_KEYS | orjson.OPT_INDENT_2,
        )

    @classmethod
    def from_json_bytes(cls, data: bytes) -> "DatasetMetadata":
        """Deserialize from JSON bytes using orjson.

        Reconstructs a DatasetMetadata instance from JSON bytes created
        by the to_json_bytes method.

        Args:
            data: JSON data as bytes.

        Returns:
            DatasetMetadata instance with validated data.

        Raises:
            ValueError: If JSON is invalid or data doesn't match schema.
        """
        return cls.model_validate(orjson.loads(data))

    async def save_to_file(self, path: Path) -> None:
        """Save metadata to file asynchronously.

        Saves the complete dataset metadata to a JSON file using
        asynchronous I/O for better performance with large datasets.

        Args:
            path: Path where the JSON file should be saved.

        Raises:
            OSError: If file cannot be written (permissions, disk space, etc.).

        Example:
            ```python
            from pathlib import Path

            dataset = DatasetMetadata(...)
            await dataset.save_to_file(Path("metadata/sih_rd.json"))
            ```
        """
        import aiofiles

        async with aiofiles.open(path, "wb") as f:
            await f.write(self.to_json_bytes())

    @classmethod
    async def load_from_file(cls, path: Path) -> "DatasetMetadata":
        """Load metadata from file asynchronously.

        Loads dataset metadata from a JSON file using asynchronous I/O
        for better performance with large datasets.

        Args:
            path: Path to the JSON file to load.

        Returns:
            DatasetMetadata instance loaded from the file.

        Raises:
            FileNotFoundError: If the file doesn't exist.
            ValueError: If the file contains invalid JSON or data.

        Example:
            ```python
            from pathlib import Path

            dataset = await DatasetMetadata.load_from_file(
                Path("metadata/sih_rd.json")
            )
            print(f"Loaded {len(dataset.files)} files")
            ```
        """
        import aiofiles

        async with aiofiles.open(path, "rb") as f:
            data = await f.read()
            return cls.from_json_bytes(data)


class MetadataIndex(BaseModel):
    """Index of all available metadata across DATASUS datasets.

    This model serves as the master index for all metadata in the system,
    organizing datasets into categories (data, documentation, auxiliary)
    and providing aggregate statistics across the entire DATASUS collection.

    Attributes:
        data: Dictionary mapping dataset names to FileMetadata for data files.
        documentation: Dictionary mapping dataset names to FileMetadata for documentation.
        auxiliary: Dictionary mapping dataset names to FileMetadata for auxiliary files.
        last_updated: Timestamp when the index was last updated.
        version: Version string for the metadata index format.

    Example:
        ```python
        from datetime import datetime

        # Create a metadata index
        index = MetadataIndex(
            data={
                "SIH-RD": FileMetadata(
                    dataset="SIH-RD",
                    total_files=100,
                    total_size_bytes=1_000_000_000,
                    last_updated=datetime.now()
                )
            },
            documentation={
                "SIH-DOC": FileMetadata(
                    dataset="SIH-DOC",
                    total_files=10,
                    total_size_bytes=50_000_000,
                    last_updated=datetime.now()
                )
            },
            last_updated=datetime.now(),
            version="1.0"
        )

        # Get aggregate statistics
        print(f"Total datasets: {index.total_datasets}")  # 2
        print(f"Total files: {index.total_files}")        # 110
        print(f"Total size: {index.total_size_gb:.1f} GB") # 1.1 GB

        # Serialize to JSON
        json_data = index.to_json_bytes()
        ```
    """

    data: Dict[str, FileMetadata] = Field(default_factory=dict)
    documentation: Dict[str, FileMetadata] = Field(default_factory=dict)
    auxiliary: Dict[str, FileMetadata] = Field(default_factory=dict)
    last_updated: datetime = Field(default_factory=datetime.utcnow)
    version: str = "1.0"

    @computed_field
    @property
    def total_datasets(self) -> int:
        """Total number of datasets across all categories.

        Counts the total number of unique datasets in the index,
        including data, documentation, and auxiliary datasets.

        Returns:
            Total count of datasets.

        Example:
            ```python
            index = MetadataIndex(
                data={"SIH-RD": FileMetadata(...)},
                documentation={"SIH-DOC": FileMetadata(...)}
            )
            print(index.total_datasets)  # 2
            ```
        """
        return len(self.data) + len(self.documentation) + len(self.auxiliary)

    @computed_field
    @property
    def total_files(self) -> int:
        """Total number of files across all datasets and categories.

        Sums up the file counts from all datasets in data, documentation,
        and auxiliary categories to provide a system-wide file count.

        Returns:
            Total count of files across all datasets.

        Example:
            ```python
            index = MetadataIndex(
                data={"SIH-RD": FileMetadata(total_files=100, ...)},
                documentation={"SIH-DOC": FileMetadata(total_files=10, ...)}
            )
            print(index.total_files)  # 110
            ```
        """
        return (
            sum(meta.total_files for meta in self.data.values())
            + sum(meta.total_files for meta in self.documentation.values())
            + sum(meta.total_files for meta in self.auxiliary.values())
        )

    @computed_field
    @property
    def total_size_gb(self) -> float:
        """Total size in gigabytes across all datasets and categories.

        Sums up the sizes from all datasets in data, documentation,
        and auxiliary categories to provide a system-wide size total.

        Returns:
            Total size in gigabytes across all datasets.

        Example:
            ```python
            index = MetadataIndex(
                data={"SIH-RD": FileMetadata(total_size_bytes=1_000_000_000, ...)},
                documentation={"SIH-DOC": FileMetadata(total_size_bytes=100_000_000, ...)}
            )
            print(index.total_size_gb)  # 1.1
            ```
        """
        return (
            sum(meta.total_size_gb for meta in self.data.values())
            + sum(meta.total_size_gb for meta in self.documentation.values())
            + sum(meta.total_size_gb for meta in self.auxiliary.values())
        )

    def to_json_bytes(self) -> bytes:
        """Serialize to JSON bytes using orjson with pretty formatting.

        Converts the complete metadata index to JSON bytes using orjson
        for high performance, with indentation for readability.

        Returns:
            JSON serialized as bytes with UTF-8 encoding and pretty formatting.

        Example:
            ```python
            index = MetadataIndex(...)
            json_data = index.to_json_bytes()

            # Save to file
            with open("metadata_index.json", "wb") as f:
                f.write(json_data)
            ```
        """
        data = self.model_dump(mode="json")
        serializable_data = _serialize_for_json(data)
        return orjson.dumps(
            serializable_data,
            option=orjson.OPT_UTC_Z | orjson.OPT_SORT_KEYS | orjson.OPT_INDENT_2,
        )

    @classmethod
    def from_json_bytes(cls, data: bytes) -> "MetadataIndex":
        """Deserialize from JSON bytes using orjson.

        Reconstructs a MetadataIndex instance from JSON bytes created
        by the to_json_bytes method.

        Args:
            data: JSON data as bytes.

        Returns:
            MetadataIndex instance with validated data.

        Raises:
            ValueError: If JSON is invalid or data doesn't match schema.

        Example:
            ```python
            # Load from file
            with open("metadata_index.json", "rb") as f:
                json_data = f.read()

            index = MetadataIndex.from_json_bytes(json_data)
            print(f"Loaded index with {index.total_datasets} datasets")
            ```
        """
        return cls.model_validate(orjson.loads(data))


class CacheEntry(BaseModel):
    """Cache entry for metadata with TTL (Time To Live) support.

    This model represents a cached piece of data with expiration tracking
    and size monitoring. Used by the caching system to store metadata
    objects with automatic expiration and memory management.

    Attributes:
        data: The cached data object (can be any serializable type).
        expires_at: Datetime when this cache entry expires.
        cache_key: Unique identifier for this cache entry.
        size_bytes: Approximate size of the cached data in bytes.

    Example:
        ```python
        from datetime import datetime, timedelta

        # Create a cache entry with 1 hour TTL
        cache_entry = CacheEntry(
            data={"dataset": "SIH-RD", "files": 100},
            expires_at=datetime.utcnow() + timedelta(hours=1),
            cache_key="sih_rd_metadata",
            size_bytes=1024
        )

        # Check if entry is still valid
        if not cache_entry.is_expired:
            data = cache_entry.data
            print(f"TTL remaining: {cache_entry.ttl_seconds} seconds")
        else:
            print("Cache entry has expired")
        ```
    """

    data: Any
    expires_at: datetime
    cache_key: str
    size_bytes: int = 0

    @computed_field
    @property
    def is_expired(self) -> bool:
        """Check if cache entry is expired.

        Compares the current UTC time with the expiration timestamp
        to determine if this cache entry is still valid.

        Returns:
            True if the entry has expired, False otherwise.

        Example:
            ```python
            from datetime import datetime, timedelta

            # Expired entry
            expired_entry = CacheEntry(
                data={},
                expires_at=datetime.utcnow() - timedelta(minutes=1),
                cache_key="expired"
            )
            assert expired_entry.is_expired == True

            # Valid entry
            valid_entry = CacheEntry(
                data={},
                expires_at=datetime.utcnow() + timedelta(minutes=1),
                cache_key="valid"
            )
            assert valid_entry.is_expired == False
            ```
        """
        return datetime.utcnow() > self.expires_at

    @computed_field
    @property
    def ttl_seconds(self) -> int:
        """Time to live in seconds.

        Calculates the remaining time until expiration in seconds.
        Returns 0 if the entry has already expired.

        Returns:
            Remaining seconds until expiration, or 0 if already expired.

        Example:
            ```python
            from datetime import datetime, timedelta

            # Entry expiring in 30 minutes
            entry = CacheEntry(
                data={},
                expires_at=datetime.utcnow() + timedelta(minutes=30),
                cache_key="test"
            )

            # Should be approximately 1800 seconds (30 * 60)
            ttl = entry.ttl_seconds
            print(f"Expires in {ttl} seconds")

            # Expired entry returns 0
            expired_entry = CacheEntry(
                data={},
                expires_at=datetime.utcnow() - timedelta(minutes=1),
                cache_key="expired"
            )
            assert expired_entry.ttl_seconds == 0
            ```
        """
        delta = self.expires_at - datetime.utcnow()
        return max(0, int(delta.total_seconds()))
