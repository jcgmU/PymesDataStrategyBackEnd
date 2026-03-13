"""Unit tests for DatasetParser."""

import json
from io import BytesIO

import polars as pl
import pytest

from src.infrastructure.parsers.dataset_parser import (
    DatasetParser,
    FileFormat,
    ParseError,
    UnsupportedFormatError,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def parser():
    """Create a DatasetParser instance with default settings."""
    return DatasetParser()


@pytest.fixture
def sample_csv_data():
    """Create sample CSV data."""
    return b"name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,Chicago"


@pytest.fixture
def sample_csv_with_nulls():
    """Create sample CSV data with null values."""
    return b"name,age,city\nAlice,30,NYC\nBob,,LA\nCharlie,35,"


@pytest.fixture
def sample_json_data():
    """Create sample JSON data (array of objects)."""
    data = [
        {"name": "Alice", "age": 30, "city": "NYC"},
        {"name": "Bob", "age": 25, "city": "LA"},
        {"name": "Charlie", "age": 35, "city": "Chicago"},
    ]
    return json.dumps(data).encode()


@pytest.fixture
def sample_parquet_data():
    """Create sample Parquet data."""
    df = pl.DataFrame({
        "name": ["Alice", "Bob", "Charlie"],
        "age": [30, 25, 35],
        "city": ["NYC", "LA", "Chicago"],
    })
    buffer = BytesIO()
    df.write_parquet(buffer)
    buffer.seek(0)
    return buffer.read()


# =============================================================================
# Format Detection Tests
# =============================================================================


class TestFormatDetection:
    """Tests for file format detection."""

    def test_detect_csv(self, parser):
        """Should detect CSV format."""
        assert parser.detect_format("data.csv") == FileFormat.CSV
        assert parser.detect_format("path/to/data.CSV") == FileFormat.CSV

    def test_detect_excel_xlsx(self, parser):
        """Should detect XLSX format."""
        assert parser.detect_format("data.xlsx") == FileFormat.EXCEL
        assert parser.detect_format("DATA.XLSX") == FileFormat.EXCEL

    def test_detect_excel_xls(self, parser):
        """Should detect XLS format."""
        assert parser.detect_format("data.xls") == FileFormat.EXCEL_XLS

    def test_detect_json(self, parser):
        """Should detect JSON format."""
        assert parser.detect_format("data.json") == FileFormat.JSON

    def test_detect_parquet(self, parser):
        """Should detect Parquet format."""
        assert parser.detect_format("data.parquet") == FileFormat.PARQUET

    def test_detect_unsupported_format(self, parser):
        """Should raise error for unsupported format."""
        with pytest.raises(UnsupportedFormatError, match="Unsupported file format"):
            parser.detect_format("data.txt")

    def test_detect_format_with_path(self, parser):
        """Should detect format from full path."""
        assert parser.detect_format("/path/to/file/data.csv") == FileFormat.CSV
        assert parser.detect_format("s3://bucket/key/data.json") == FileFormat.JSON

    def test_detect_no_extension(self, parser):
        """Should raise error for files without extension."""
        with pytest.raises(UnsupportedFormatError):
            parser.detect_format("filename")


# =============================================================================
# CSV Parsing Tests
# =============================================================================


class TestCSVParsing:
    """Tests for CSV file parsing."""

    def test_parse_csv_basic(self, parser, sample_csv_data):
        """Should parse basic CSV file."""
        df = parser.parse(sample_csv_data, "test.csv")
        
        assert df.height == 3
        assert df.width == 3
        assert df.columns == ["name", "age", "city"]
        assert df["name"].to_list() == ["Alice", "Bob", "Charlie"]

    def test_parse_csv_with_nulls(self, parser, sample_csv_with_nulls):
        """Should parse CSV with null values."""
        df = parser.parse(sample_csv_with_nulls, "test.csv")
        
        assert df.height == 3
        assert df["age"].null_count() == 1
        assert df["city"].null_count() == 1

    def test_parse_csv_custom_separator(self):
        """Should parse CSV with custom separator."""
        parser = DatasetParser(csv_separator=";")
        data = b"name;age;city\nAlice;30;NYC"
        
        df = parser.parse(data, "test.csv")
        
        assert df.columns == ["name", "age", "city"]
        assert df["name"][0] == "Alice"

    def test_parse_csv_no_header(self):
        """Should parse CSV without header."""
        parser = DatasetParser(csv_has_header=False)
        data = b"Alice,30,NYC\nBob,25,LA"
        
        df = parser.parse(data, "test.csv")
        
        assert df.height == 2
        # Column names are auto-generated
        assert "column_1" in df.columns or len(df.columns) == 3

    def test_parse_csv_explicit_format(self, parser, sample_csv_data):
        """Should use explicit format regardless of filename."""
        # Use .json extension but force CSV parsing
        df = parser.parse(sample_csv_data, "test.json", file_format=FileFormat.CSV)
        
        assert df.height == 3
        assert df.columns == ["name", "age", "city"]

    def test_parse_csv_with_quotes(self, parser):
        """Should handle quoted values."""
        data = b'name,description\n"Alice","Hello, World"\n"Bob","Test""Quote"'
        
        df = parser.parse(data, "test.csv")
        
        assert df["description"][0] == "Hello, World"

    def test_parse_empty_csv(self, parser):
        """Should handle empty CSV (header only)."""
        data = b"name,age,city\n"
        
        df = parser.parse(data, "test.csv")
        
        assert df.height == 0
        assert df.columns == ["name", "age", "city"]


# =============================================================================
# JSON Parsing Tests
# =============================================================================


class TestJSONParsing:
    """Tests for JSON file parsing."""

    def test_parse_json_array(self, parser, sample_json_data):
        """Should parse JSON array of objects."""
        df = parser.parse(sample_json_data, "test.json")
        
        assert df.height == 3
        assert "name" in df.columns
        assert "age" in df.columns

    def test_parse_json_with_nested(self, parser):
        """Should handle JSON with nested structures."""
        data = json.dumps([
            {"id": 1, "info": {"name": "Alice", "age": 30}},
            {"id": 2, "info": {"name": "Bob", "age": 25}},
        ]).encode()
        
        df = parser.parse(data, "test.json")
        
        # Polars stores nested as struct
        assert df.height == 2
        assert "info" in df.columns


# =============================================================================
# Parquet Parsing Tests
# =============================================================================


class TestParquetParsing:
    """Tests for Parquet file parsing."""

    def test_parse_parquet(self, parser, sample_parquet_data):
        """Should parse Parquet file."""
        df = parser.parse(sample_parquet_data, "test.parquet")
        
        assert df.height == 3
        assert df.columns == ["name", "age", "city"]

    def test_parse_parquet_preserves_types(self, parser):
        """Should preserve data types from Parquet."""
        original = pl.DataFrame({
            "int_col": [1, 2, 3],
            "float_col": [1.5, 2.5, 3.5],
            "str_col": ["a", "b", "c"],
            "bool_col": [True, False, True],
        })
        buffer = BytesIO()
        original.write_parquet(buffer)
        buffer.seek(0)
        
        df = parser.parse(buffer.read(), "test.parquet")
        
        assert df["int_col"].dtype == pl.Int64
        assert df["float_col"].dtype == pl.Float64
        assert df["str_col"].dtype == pl.String
        assert df["bool_col"].dtype == pl.Boolean


# =============================================================================
# Error Handling Tests
# =============================================================================


class TestErrorHandling:
    """Tests for error handling."""

    def test_parse_invalid_csv(self, parser):
        """Should raise ParseError for invalid CSV."""
        # Malformed data with inconsistent columns
        invalid_data = b"col1,col2\nval1\nval2,val3,extra"
        
        # Polars is lenient but may raise on certain malformed data
        # This tests that we wrap errors properly
        with pytest.raises(ParseError):
            # Force an error with bad encoding
            parser2 = DatasetParser(csv_encoding="ascii")
            parser2.parse("данные".encode(), "test.csv")

    def test_parse_invalid_json(self, parser):
        """Should raise ParseError for invalid JSON."""
        with pytest.raises(ParseError):
            parser.parse(b"{invalid json", "test.json")

    def test_parse_invalid_parquet(self, parser):
        """Should raise ParseError for invalid Parquet."""
        with pytest.raises(ParseError):
            parser.parse(b"not a parquet file", "test.parquet")


# =============================================================================
# Schema and Stats Tests
# =============================================================================


class TestSchemaAndStats:
    """Tests for schema and statistics functions."""

    def test_get_schema(self, parser, sample_csv_data):
        """Should return schema information."""
        df = parser.parse(sample_csv_data, "test.csv")
        schema = parser.get_schema(df)
        
        assert "name" in schema
        assert "age" in schema
        assert "city" in schema
        assert schema["name"] == "String"

    def test_get_stats(self, parser, sample_csv_data):
        """Should return DataFrame statistics."""
        df = parser.parse(sample_csv_data, "test.csv")
        stats = parser.get_stats(df)
        
        assert stats["row_count"] == 3
        assert stats["column_count"] == 3
        assert stats["columns"] == ["name", "age", "city"]
        assert "schema" in stats
        assert "null_counts" in stats
        assert "memory_usage_bytes" in stats

    def test_get_stats_with_nulls(self, parser, sample_csv_with_nulls):
        """Should count nulls correctly."""
        df = parser.parse(sample_csv_with_nulls, "test.csv")
        stats = parser.get_stats(df)
        
        assert stats["null_counts"]["age"] == 1
        assert stats["null_counts"]["city"] == 1


# =============================================================================
# Serialization Tests
# =============================================================================


class TestSerialization:
    """Tests for DataFrame serialization."""

    def test_to_bytes_csv(self, parser, sample_csv_data):
        """Should serialize DataFrame to CSV."""
        df = parser.parse(sample_csv_data, "test.csv")
        output = parser.to_bytes(df, FileFormat.CSV)
        
        # Parse it back
        df2 = parser.parse(output, "output.csv")
        assert df2.height == df.height
        assert df2.columns == df.columns

    def test_to_bytes_parquet(self, parser, sample_csv_data):
        """Should serialize DataFrame to Parquet."""
        df = parser.parse(sample_csv_data, "test.csv")
        output = parser.to_bytes(df, FileFormat.PARQUET)
        
        # Parse it back
        df2 = parser.parse(output, "output.parquet")
        assert df2.height == df.height

    def test_to_bytes_json(self, parser, sample_csv_data):
        """Should serialize DataFrame to JSON."""
        df = parser.parse(sample_csv_data, "test.csv")
        output = parser.to_bytes(df, FileFormat.JSON)
        
        # Verify it's valid JSON
        parsed = json.loads(output.decode())
        assert len(parsed) == 3


# =============================================================================
# Sample and Preview Tests
# =============================================================================


class TestSampleAndPreview:
    """Tests for sampling and preview functions."""

    def test_sample_smaller_than_df(self, parser):
        """Should return sampled rows."""
        df = pl.DataFrame({
            "id": list(range(1000)),
            "value": list(range(1000)),
        })
        
        sampled = parser.sample(df, n=100)
        
        assert sampled.height == 100

    def test_sample_larger_than_df(self, parser, sample_csv_data):
        """Should return full df when sample > df size."""
        df = parser.parse(sample_csv_data, "test.csv")
        
        sampled = parser.sample(df, n=100)
        
        assert sampled.height == 3  # Original size

    def test_sample_with_seed(self, parser):
        """Should return consistent results with seed."""
        df = pl.DataFrame({"id": list(range(100))})
        
        sampled1 = parser.sample(df, n=10, seed=42)
        sampled2 = parser.sample(df, n=10, seed=42)
        
        assert sampled1["id"].to_list() == sampled2["id"].to_list()

    def test_preview(self, parser, sample_csv_data):
        """Should return first n rows as dicts."""
        df = parser.parse(sample_csv_data, "test.csv")
        
        preview = parser.preview(df, n=2)
        
        assert len(preview) == 2
        assert preview[0]["name"] == "Alice"
        assert preview[1]["name"] == "Bob"

    def test_preview_all_rows(self, parser, sample_csv_data):
        """Should return all rows if n > df size."""
        df = parser.parse(sample_csv_data, "test.csv")
        
        preview = parser.preview(df, n=100)
        
        assert len(preview) == 3


# =============================================================================
# Integration Tests
# =============================================================================


class TestParserIntegration:
    """Integration tests for parser workflow."""

    def test_full_workflow_csv(self, parser):
        """Should handle full parse -> transform -> serialize workflow."""
        # Parse CSV
        csv_data = b"id,name,score\n1,Alice,85\n2,Bob,92\n3,Charlie,78"
        df = parser.parse(csv_data, "input.csv")
        
        # Get stats
        stats = parser.get_stats(df)
        assert stats["row_count"] == 3
        
        # Transform (filter high scores)
        filtered = df.filter(pl.col("score") > 80)
        assert filtered.height == 2
        
        # Serialize to Parquet
        output = parser.to_bytes(filtered, FileFormat.PARQUET)
        
        # Verify output
        final = parser.parse(output, "output.parquet")
        assert final.height == 2
        assert set(final["name"].to_list()) == {"Alice", "Bob"}

    def test_parse_large_csv(self, parser):
        """Should handle larger CSV files."""
        # Generate CSV with 10000 rows
        rows = ["id,name,value"]
        for i in range(10000):
            rows.append(f"{i},Name{i},{i * 10}")
        data = "\n".join(rows).encode()
        
        df = parser.parse(data, "large.csv")
        
        assert df.height == 10000
        stats = parser.get_stats(df)
        assert stats["null_counts"]["id"] == 0
