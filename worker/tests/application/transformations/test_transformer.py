"""Unit tests for DataTransformer."""

import polars as pl
import pytest

from src.application.transformations import (
    DataTransformer,
    TransformationType,
    TransformationConfig,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def transformer():
    """Create a DataTransformer instance."""
    return DataTransformer()


@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing."""
    return pl.DataFrame({
        "name": ["Alice", "Bob", "Charlie", "Diana"],
        "age": [30, 25, 35, 28],
        "city": ["NYC", "LA", "Chicago", "Boston"],
        "score": [85.5, 92.0, 78.5, 88.0],
    })


@pytest.fixture
def df_with_nulls():
    """Create a DataFrame with null values."""
    return pl.DataFrame({
        "name": ["Alice", None, "Charlie", "Diana"],
        "age": [30, 25, None, 28],
        "city": ["NYC", "LA", "Chicago", None],
    })


@pytest.fixture
def df_with_whitespace():
    """Create a DataFrame with whitespace."""
    return pl.DataFrame({
        "name": ["  Alice  ", "Bob  ", "  Charlie", "Diana"],
        "code": ["  ABC  ", "DEF", "  GHI  ", "JKL  "],
    })


@pytest.fixture
def df_with_duplicates():
    """Create a DataFrame with duplicates."""
    return pl.DataFrame({
        "id": [1, 2, 1, 3, 2],
        "name": ["Alice", "Bob", "Alice", "Charlie", "Bob"],
        "value": [100, 200, 100, 300, 200],
    })


# =============================================================================
# Clean Nulls Tests
# =============================================================================


class TestCleanNulls:
    """Tests for CLEAN_NULLS transformation."""

    def test_clean_nulls_all_columns(self, transformer, df_with_nulls):
        """Should remove rows with nulls in any column."""
        config = TransformationConfig(type=TransformationType.CLEAN_NULLS)
        
        result_df, result = transformer.transform(df_with_nulls, config)
        
        assert result.success is True
        assert result_df.height == 1  # Only row without any nulls
        assert result_df["name"][0] == "Alice"

    def test_clean_nulls_specific_column(self, transformer, df_with_nulls):
        """Should remove rows with nulls in specific column."""
        config = TransformationConfig(
            type=TransformationType.CLEAN_NULLS,
            columns=["name"],
        )
        
        result_df, result = transformer.transform(df_with_nulls, config)
        
        assert result.success is True
        assert result_df.height == 3  # One row with null name removed
        assert result.details["rows_removed"] == 1

    def test_clean_nulls_no_nulls(self, transformer, sample_df):
        """Should not change DataFrame without nulls."""
        config = TransformationConfig(type=TransformationType.CLEAN_NULLS)
        
        result_df, result = transformer.transform(sample_df, config)
        
        assert result.success is True
        assert result_df.height == sample_df.height


# =============================================================================
# Fill Nulls Tests
# =============================================================================


class TestFillNulls:
    """Tests for FILL_NULLS transformation."""

    def test_fill_nulls_with_literal(self, transformer, df_with_nulls):
        """Should fill nulls with literal value."""
        config = TransformationConfig(
            type=TransformationType.FILL_NULLS,
            columns=["name"],
            params={"value": "Unknown", "strategy": "literal"},
        )
        
        result_df, result = transformer.transform(df_with_nulls, config)
        
        assert result.success is True
        assert result_df["name"].null_count() == 0
        assert "Unknown" in result_df["name"].to_list()

    def test_fill_nulls_with_mean(self, transformer, df_with_nulls):
        """Should fill nulls with column mean."""
        config = TransformationConfig(
            type=TransformationType.FILL_NULLS,
            columns=["age"],
            params={"strategy": "mean"},
        )
        
        result_df, result = transformer.transform(df_with_nulls, config)
        
        assert result.success is True
        assert result_df["age"].null_count() == 0

    def test_fill_nulls_forward(self, transformer):
        """Should fill nulls with forward fill."""
        df = pl.DataFrame({"value": [1, None, None, 4, None]})
        config = TransformationConfig(
            type=TransformationType.FILL_NULLS,
            columns=["value"],
            params={"strategy": "forward"},
        )
        
        result_df, result = transformer.transform(df, config)
        
        assert result.success is True
        # Forward fill: [1, 1, 1, 4, 4]
        assert result_df["value"].to_list() == [1, 1, 1, 4, 4]


# =============================================================================
# Trim Whitespace Tests
# =============================================================================


class TestTrimWhitespace:
    """Tests for TRIM_WHITESPACE transformation."""

    def test_trim_all_string_columns(self, transformer, df_with_whitespace):
        """Should trim whitespace from all string columns."""
        config = TransformationConfig(type=TransformationType.TRIM_WHITESPACE)
        
        result_df, result = transformer.transform(df_with_whitespace, config)
        
        assert result.success is True
        assert result_df["name"].to_list() == ["Alice", "Bob", "Charlie", "Diana"]
        assert result_df["code"].to_list() == ["ABC", "DEF", "GHI", "JKL"]

    def test_trim_specific_columns(self, transformer, df_with_whitespace):
        """Should trim whitespace from specific columns only."""
        config = TransformationConfig(
            type=TransformationType.TRIM_WHITESPACE,
            columns=["name"],
        )
        
        result_df, result = transformer.transform(df_with_whitespace, config)
        
        assert result.success is True
        assert result_df["name"].to_list() == ["Alice", "Bob", "Charlie", "Diana"]
        # code should still have whitespace
        assert result_df["code"][0] == "  ABC  "


# =============================================================================
# Uppercase/Lowercase Tests
# =============================================================================


class TestCaseTransformations:
    """Tests for UPPERCASE and LOWERCASE transformations."""

    def test_uppercase(self, transformer, sample_df):
        """Should convert strings to uppercase."""
        config = TransformationConfig(
            type=TransformationType.UPPERCASE,
            columns=["name"],
        )
        
        result_df, result = transformer.transform(sample_df, config)
        
        assert result.success is True
        assert result_df["name"].to_list() == ["ALICE", "BOB", "CHARLIE", "DIANA"]

    def test_lowercase(self, transformer, sample_df):
        """Should convert strings to lowercase."""
        config = TransformationConfig(
            type=TransformationType.LOWERCASE,
            columns=["city"],
        )
        
        result_df, result = transformer.transform(sample_df, config)
        
        assert result.success is True
        assert result_df["city"].to_list() == ["nyc", "la", "chicago", "boston"]

    def test_case_all_string_columns(self, transformer, sample_df):
        """Should transform all string columns when none specified."""
        config = TransformationConfig(type=TransformationType.UPPERCASE)
        
        result_df, result = transformer.transform(sample_df, config)
        
        assert result.success is True
        assert result_df["name"][0] == "ALICE"
        assert result_df["city"][0] == "NYC"


# =============================================================================
# Remove Duplicates Tests
# =============================================================================


class TestRemoveDuplicates:
    """Tests for REMOVE_DUPLICATES transformation."""

    def test_remove_all_duplicates(self, transformer, df_with_duplicates):
        """Should remove duplicate rows."""
        config = TransformationConfig(type=TransformationType.REMOVE_DUPLICATES)
        
        result_df, result = transformer.transform(df_with_duplicates, config)
        
        assert result.success is True
        assert result_df.height == 3  # 3 unique rows
        assert result.details["duplicates_removed"] == 2

    def test_remove_duplicates_by_column(self, transformer, df_with_duplicates):
        """Should remove duplicates based on specific columns."""
        config = TransformationConfig(
            type=TransformationType.REMOVE_DUPLICATES,
            columns=["id"],
        )
        
        result_df, result = transformer.transform(df_with_duplicates, config)
        
        assert result.success is True
        assert result_df.height == 3  # 3 unique ids

    def test_remove_duplicates_keep_last(self, transformer):
        """Should keep last occurrence when specified."""
        df = pl.DataFrame({
            "id": [1, 1, 1],
            "value": ["first", "second", "last"],
        })
        config = TransformationConfig(
            type=TransformationType.REMOVE_DUPLICATES,
            columns=["id"],
            params={"keep": "last"},
        )
        
        result_df, result = transformer.transform(df, config)
        
        assert result.success is True
        assert result_df["value"][0] == "last"


# =============================================================================
# Convert Type Tests
# =============================================================================


class TestConvertType:
    """Tests for CONVERT_TYPE transformation."""

    def test_convert_to_string(self, transformer, sample_df):
        """Should convert numeric column to string."""
        config = TransformationConfig(
            type=TransformationType.CONVERT_TYPE,
            columns=["age"],
            params={"target_type": "str"},
        )
        
        result_df, result = transformer.transform(sample_df, config)
        
        assert result.success is True
        assert result_df["age"].dtype == pl.String
        assert result_df["age"][0] == "30"

    def test_convert_to_int(self, transformer):
        """Should convert float to int."""
        df = pl.DataFrame({"value": [1.5, 2.0, 3.9]})
        config = TransformationConfig(
            type=TransformationType.CONVERT_TYPE,
            columns=["value"],
            params={"target_type": "int"},
        )
        
        result_df, result = transformer.transform(df, config)
        
        assert result.success is True
        assert result_df["value"].dtype == pl.Int64

    def test_convert_invalid_type(self, transformer, sample_df):
        """Should fail for invalid target type."""
        config = TransformationConfig(
            type=TransformationType.CONVERT_TYPE,
            columns=["age"],
            params={"target_type": "invalid"},
        )
        
        result_df, result = transformer.transform(sample_df, config)
        
        assert result.success is False
        assert "Unknown target type" in result.error


# =============================================================================
# Rename Column Tests
# =============================================================================


class TestRenameColumn:
    """Tests for RENAME_COLUMN transformation."""

    def test_rename_single_column(self, transformer, sample_df):
        """Should rename a single column."""
        config = TransformationConfig(
            type=TransformationType.RENAME_COLUMN,
            params={"mapping": {"name": "full_name"}},
        )
        
        result_df, result = transformer.transform(sample_df, config)
        
        assert result.success is True
        assert "full_name" in result_df.columns
        assert "name" not in result_df.columns

    def test_rename_multiple_columns(self, transformer, sample_df):
        """Should rename multiple columns."""
        config = TransformationConfig(
            type=TransformationType.RENAME_COLUMN,
            params={"mapping": {"name": "person_name", "city": "location"}},
        )
        
        result_df, result = transformer.transform(sample_df, config)
        
        assert result.success is True
        assert "person_name" in result_df.columns
        assert "location" in result_df.columns

    def test_rename_nonexistent_column(self, transformer, sample_df):
        """Should fail for nonexistent column."""
        config = TransformationConfig(
            type=TransformationType.RENAME_COLUMN,
            params={"mapping": {"nonexistent": "new_name"}},
        )
        
        result_df, result = transformer.transform(sample_df, config)
        
        assert result.success is False
        assert "not found" in result.error


# =============================================================================
# Drop Column Tests
# =============================================================================


class TestDropColumn:
    """Tests for DROP_COLUMN transformation."""

    def test_drop_single_column(self, transformer, sample_df):
        """Should drop a single column."""
        config = TransformationConfig(
            type=TransformationType.DROP_COLUMN,
            columns=["score"],
        )
        
        result_df, result = transformer.transform(sample_df, config)
        
        assert result.success is True
        assert "score" not in result_df.columns
        assert result_df.width == 3

    def test_drop_multiple_columns(self, transformer, sample_df):
        """Should drop multiple columns."""
        config = TransformationConfig(
            type=TransformationType.DROP_COLUMN,
            columns=["age", "score"],
        )
        
        result_df, result = transformer.transform(sample_df, config)
        
        assert result.success is True
        assert "age" not in result_df.columns
        assert "score" not in result_df.columns


# =============================================================================
# Filter Rows Tests
# =============================================================================


class TestFilterRows:
    """Tests for FILTER_ROWS transformation."""

    def test_filter_equal(self, transformer, sample_df):
        """Should filter rows with equal condition."""
        config = TransformationConfig(
            type=TransformationType.FILTER_ROWS,
            params={"column": "name", "operator": "eq", "value": "Alice"},
        )
        
        result_df, result = transformer.transform(sample_df, config)
        
        assert result.success is True
        assert result_df.height == 1
        assert result_df["name"][0] == "Alice"

    def test_filter_greater_than(self, transformer, sample_df):
        """Should filter rows with greater than condition."""
        config = TransformationConfig(
            type=TransformationType.FILTER_ROWS,
            params={"column": "age", "operator": "gt", "value": 30},
        )
        
        result_df, result = transformer.transform(sample_df, config)
        
        assert result.success is True
        assert result_df.height == 1
        assert result_df["name"][0] == "Charlie"

    def test_filter_contains(self, transformer, sample_df):
        """Should filter rows containing string."""
        config = TransformationConfig(
            type=TransformationType.FILTER_ROWS,
            params={"column": "city", "operator": "contains", "value": "o"},
        )
        
        result_df, result = transformer.transform(sample_df, config)
        
        assert result.success is True
        # NYC, Chicago, Boston contain 'o'
        assert result_df.height == 2

    def test_filter_not_null(self, transformer, df_with_nulls):
        """Should filter non-null rows."""
        config = TransformationConfig(
            type=TransformationType.FILTER_ROWS,
            params={"column": "name", "operator": "not_null"},
        )
        
        result_df, result = transformer.transform(df_with_nulls, config)
        
        assert result.success is True
        assert result_df.height == 3  # One null removed

    def test_filter_missing_column(self, transformer, sample_df):
        """Should fail for missing column."""
        config = TransformationConfig(
            type=TransformationType.FILTER_ROWS,
            params={"column": "nonexistent", "operator": "eq", "value": "x"},
        )
        
        result_df, result = transformer.transform(sample_df, config)
        
        assert result.success is False


# =============================================================================
# Map Values Tests
# =============================================================================


class TestMapValues:
    """Tests for MAP_VALUES transformation."""

    def test_map_values_string(self, transformer, sample_df):
        """Should map string values."""
        config = TransformationConfig(
            type=TransformationType.MAP_VALUES,
            columns=["city"],
            params={"mapping": {"NYC": "New York City", "LA": "Los Angeles"}},
        )
        
        result_df, result = transformer.transform(sample_df, config)
        
        assert result.success is True
        assert "New York City" in result_df["city"].to_list()
        assert "Los Angeles" in result_df["city"].to_list()

    def test_map_values_with_default(self, transformer, sample_df):
        """Should use default for unmapped values."""
        config = TransformationConfig(
            type=TransformationType.MAP_VALUES,
            columns=["city"],
            params={
                "mapping": {"NYC": "New York"},
                "default": "Other",
            },
        )
        
        result_df, result = transformer.transform(sample_df, config)
        
        assert result.success is True
        cities = result_df["city"].to_list()
        assert "New York" in cities


# =============================================================================
# Transform Many Tests
# =============================================================================


class TestTransformMany:
    """Tests for multiple transformations in sequence."""

    def test_transform_chain(self, transformer, df_with_whitespace):
        """Should apply multiple transformations in order."""
        configs = [
            TransformationConfig(type=TransformationType.TRIM_WHITESPACE),
            TransformationConfig(type=TransformationType.UPPERCASE),
        ]
        
        result_df, results = transformer.transform_many(df_with_whitespace, configs)
        
        assert len(results) == 2
        assert all(r.success for r in results)
        assert result_df["name"].to_list() == ["ALICE", "BOB", "CHARLIE", "DIANA"]

    def test_transform_chain_stops_on_failure(self, transformer, sample_df):
        """Should stop chain on first failure."""
        configs = [
            TransformationConfig(
                type=TransformationType.RENAME_COLUMN,
                params={"mapping": {"nonexistent": "new"}},  # Will fail
            ),
            TransformationConfig(type=TransformationType.UPPERCASE),
        ]
        
        result_df, results = transformer.transform_many(sample_df, configs)
        
        assert len(results) == 1  # Stopped after first failure
        assert results[0].success is False


# =============================================================================
# Error Handling Tests
# =============================================================================


class TestErrorHandling:
    """Tests for error handling."""

    def test_nonexistent_column_error(self, transformer, sample_df):
        """Should fail gracefully for nonexistent column."""
        config = TransformationConfig(
            type=TransformationType.CLEAN_NULLS,
            columns=["nonexistent"],
        )
        
        result_df, result = transformer.transform(sample_df, config)
        
        assert result.success is False
        assert "not found" in result.error

    def test_transformation_result_includes_row_counts(self, transformer, df_with_nulls):
        """Should include row counts in result."""
        config = TransformationConfig(type=TransformationType.CLEAN_NULLS)
        
        result_df, result = transformer.transform(df_with_nulls, config)
        
        assert result.rows_before == 4
        assert result.rows_after == 1
