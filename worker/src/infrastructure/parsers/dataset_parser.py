"""Dataset parser using Polars for CSV, Excel, and JSON files."""

from enum import Enum
from io import BytesIO
from pathlib import Path
from typing import Any

import polars as pl
import structlog


logger = structlog.get_logger("pymes.worker.parser")


class FileFormat(str, Enum):
    """Supported file formats."""
    
    CSV = "csv"
    EXCEL = "xlsx"
    EXCEL_XLS = "xls"
    JSON = "json"
    PARQUET = "parquet"


class ParserError(Exception):
    """Base error for parser operations."""
    
    pass


class UnsupportedFormatError(ParserError):
    """Raised when file format is not supported."""
    
    pass


class ParseError(ParserError):
    """Raised when parsing fails."""
    
    pass


class DatasetParser:
    """Parser for dataset files using Polars.
    
    Supports CSV, Excel (xlsx/xls), JSON, and Parquet formats.
    Returns data as Polars DataFrames for efficient processing.
    """

    # Map file extensions to formats
    EXTENSION_MAP: dict[str, FileFormat] = {
        ".csv": FileFormat.CSV,
        ".xlsx": FileFormat.EXCEL,
        ".xls": FileFormat.EXCEL_XLS,
        ".json": FileFormat.JSON,
        ".parquet": FileFormat.PARQUET,
    }

    def __init__(
        self,
        csv_separator: str = ",",
        csv_quote_char: str = '"',
        csv_has_header: bool = True,
        csv_encoding: str = "utf-8",
        excel_sheet_name: str | int | None = None,
        json_orient: str = "records",
    ) -> None:
        """Initialize parser with configuration options.
        
        Args:
            csv_separator: Column separator for CSV files.
            csv_quote_char: Quote character for CSV files.
            csv_has_header: Whether CSV files have a header row.
            csv_encoding: Encoding for CSV files.
            excel_sheet_name: Sheet name or index for Excel files.
            json_orient: JSON orientation (records, columns, etc.).
        """
        self._csv_separator = csv_separator
        self._csv_quote_char = csv_quote_char
        self._csv_has_header = csv_has_header
        self._csv_encoding = csv_encoding
        self._excel_sheet_name = excel_sheet_name
        self._json_orient = json_orient

    def detect_format(self, filename: str) -> FileFormat:
        """Detect file format from filename extension.
        
        Args:
            filename: Name of the file (can include path).
            
        Returns:
            Detected file format.
            
        Raises:
            UnsupportedFormatError: If format is not supported.
        """
        path = Path(filename)
        extension = path.suffix.lower()
        
        if extension not in self.EXTENSION_MAP:
            raise UnsupportedFormatError(
                f"Unsupported file format: {extension}. "
                f"Supported formats: {list(self.EXTENSION_MAP.keys())}"
            )
        
        return self.EXTENSION_MAP[extension]

    def parse(
        self,
        data: bytes,
        filename: str,
        file_format: FileFormat | None = None,
    ) -> pl.DataFrame:
        """Parse file data into a Polars DataFrame.
        
        Args:
            data: Raw file bytes.
            filename: Name of the file (used for format detection).
            file_format: Optional explicit format (overrides detection).
            
        Returns:
            Polars DataFrame with parsed data.
            
        Raises:
            UnsupportedFormatError: If format is not supported.
            ParseError: If parsing fails.
        """
        if file_format is None:
            file_format = self.detect_format(filename)
        
        log = logger.bind(filename=filename, format=file_format.value)
        log.info("Parsing file")
        
        try:
            if file_format == FileFormat.CSV:
                return self._parse_csv(data)
            elif file_format in (FileFormat.EXCEL, FileFormat.EXCEL_XLS):
                return self._parse_excel(data)
            elif file_format == FileFormat.JSON:
                return self._parse_json(data)
            elif file_format == FileFormat.PARQUET:
                return self._parse_parquet(data)
            else:
                raise UnsupportedFormatError(f"Format not implemented: {file_format}")
        except pl.exceptions.PolarsError as e:
            log.error("Polars parsing error", error=str(e))
            raise ParseError(f"Failed to parse {filename}: {e}") from e
        except Exception as e:
            if isinstance(e, (UnsupportedFormatError, ParseError)):
                raise
            log.error("Unexpected parsing error", error=str(e))
            raise ParseError(f"Failed to parse {filename}: {e}") from e

    def _parse_csv(self, data: bytes) -> pl.DataFrame:
        """Parse CSV data."""
        return pl.read_csv(
            BytesIO(data),
            separator=self._csv_separator,
            quote_char=self._csv_quote_char,
            has_header=self._csv_has_header,
            encoding=self._csv_encoding,
            infer_schema_length=10000,  # Sample more rows for type inference
            try_parse_dates=True,
        )

    def _parse_excel(self, data: bytes) -> pl.DataFrame:
        """Parse Excel data (xlsx/xls)."""
        kwargs: dict[str, Any] = {}
        if self._excel_sheet_name is not None:
            kwargs["sheet_name"] = self._excel_sheet_name
        
        return pl.read_excel(
            BytesIO(data),
            infer_schema_length=10000,
            **kwargs,
        )

    def _parse_json(self, data: bytes) -> pl.DataFrame:
        """Parse JSON data."""
        # Polars expects JSON lines or array of objects
        return pl.read_json(BytesIO(data))

    def _parse_parquet(self, data: bytes) -> pl.DataFrame:
        """Parse Parquet data."""
        return pl.read_parquet(BytesIO(data))

    def get_schema(self, df: pl.DataFrame) -> dict[str, str]:
        """Get schema information from DataFrame.
        
        Args:
            df: Polars DataFrame.
            
        Returns:
            Dictionary mapping column names to type names.
        """
        return {col: str(dtype) for col, dtype in df.schema.items()}

    def get_stats(self, df: pl.DataFrame) -> dict[str, Any]:
        """Get basic statistics about the DataFrame.
        
        Args:
            df: Polars DataFrame.
            
        Returns:
            Dictionary with statistics.
        """
        return {
            "row_count": df.height,
            "column_count": df.width,
            "columns": df.columns,
            "schema": self.get_schema(df),
            "null_counts": {col: df[col].null_count() for col in df.columns},
            "memory_usage_bytes": df.estimated_size(),
        }

    def to_bytes(
        self,
        df: pl.DataFrame,
        file_format: FileFormat = FileFormat.PARQUET,
    ) -> bytes:
        """Convert DataFrame back to bytes.
        
        Args:
            df: Polars DataFrame.
            file_format: Output format.
            
        Returns:
            Serialized bytes.
        """
        buffer = BytesIO()
        
        if file_format == FileFormat.CSV:
            df.write_csv(buffer)
        elif file_format in (FileFormat.EXCEL, FileFormat.EXCEL_XLS):
            df.write_excel(buffer)
        elif file_format == FileFormat.JSON:
            # Polars 1.x: write_json returns string, no row_oriented param
            json_str = df.write_json()
            buffer.write(json_str.encode() if json_str else b"[]")
        elif file_format == FileFormat.PARQUET:
            df.write_parquet(buffer)
        else:
            raise UnsupportedFormatError(f"Cannot write format: {file_format}")
        
        buffer.seek(0)
        return buffer.read()

    def sample(
        self,
        df: pl.DataFrame,
        n: int = 100,
        seed: int | None = None,
    ) -> pl.DataFrame:
        """Get a sample of rows from DataFrame.
        
        Args:
            df: Source DataFrame.
            n: Number of rows to sample.
            seed: Random seed for reproducibility.
            
        Returns:
            Sampled DataFrame.
        """
        if df.height <= n:
            return df
        return df.sample(n=n, seed=seed)

    def preview(
        self,
        df: pl.DataFrame,
        n: int = 10,
    ) -> list[dict[str, Any]]:
        """Get first n rows as list of dicts for preview.
        
        Args:
            df: Source DataFrame.
            n: Number of rows to preview.
            
        Returns:
            List of row dictionaries.
        """
        return df.head(n).to_dicts()
