"""Data transformation operations using Polars."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import polars as pl
import structlog


logger = structlog.get_logger("pymes.worker.transformer")


class TransformationType(str, Enum):
    """Available transformation types."""
    
    # Null handling
    CLEAN_NULLS = "CLEAN_NULLS"  # Remove rows with nulls
    FILL_NULLS = "FILL_NULLS"  # Fill nulls with value
    
    # String transformations
    TRIM_WHITESPACE = "TRIM_WHITESPACE"  # Trim leading/trailing whitespace
    UPPERCASE = "UPPERCASE"  # Convert to uppercase
    LOWERCASE = "LOWERCASE"  # Convert to lowercase
    
    # Deduplication
    REMOVE_DUPLICATES = "REMOVE_DUPLICATES"  # Remove duplicate rows
    
    # Type conversions
    CONVERT_TYPE = "CONVERT_TYPE"  # Convert column type
    
    # Column operations
    RENAME_COLUMN = "RENAME_COLUMN"  # Rename a column
    DROP_COLUMN = "DROP_COLUMN"  # Drop a column
    
    # Filtering
    FILTER_ROWS = "FILTER_ROWS"  # Filter rows based on condition
    
    # Value mapping
    MAP_VALUES = "MAP_VALUES"  # Map values to other values


@dataclass
class TransformationConfig:
    """Configuration for a transformation."""
    
    type: TransformationType
    columns: list[str] | None = None  # None = all applicable columns
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class TransformationResult:
    """Result of a transformation."""
    
    transformation: TransformationType
    rows_before: int
    rows_after: int
    columns_affected: list[str]
    success: bool
    error: str | None = None
    details: dict[str, Any] = field(default_factory=dict)


class TransformationError(Exception):
    """Error during transformation."""
    
    pass


class DataTransformer:
    """Applies transformations to Polars DataFrames.
    
    This class provides a set of data cleaning and transformation operations
    commonly needed in ETL pipelines.
    """

    def __init__(self) -> None:
        """Initialize the transformer."""
        self._handlers = {
            TransformationType.CLEAN_NULLS: self._clean_nulls,
            TransformationType.FILL_NULLS: self._fill_nulls,
            TransformationType.TRIM_WHITESPACE: self._trim_whitespace,
            TransformationType.UPPERCASE: self._uppercase,
            TransformationType.LOWERCASE: self._lowercase,
            TransformationType.REMOVE_DUPLICATES: self._remove_duplicates,
            TransformationType.CONVERT_TYPE: self._convert_type,
            TransformationType.RENAME_COLUMN: self._rename_column,
            TransformationType.DROP_COLUMN: self._drop_column,
            TransformationType.FILTER_ROWS: self._filter_rows,
            TransformationType.MAP_VALUES: self._map_values,
        }

    def transform(
        self,
        df: pl.DataFrame,
        config: TransformationConfig,
    ) -> tuple[pl.DataFrame, TransformationResult]:
        """Apply a single transformation to the DataFrame.
        
        Args:
            df: Input DataFrame.
            config: Transformation configuration.
            
        Returns:
            Tuple of (transformed DataFrame, result).
        """
        log = logger.bind(
            transformation=config.type.value,
            columns=config.columns,
        )
        log.info("Applying transformation")
        
        handler = self._handlers.get(config.type)
        if handler is None:
            error = f"Unknown transformation type: {config.type}"
            log.error(error)
            return df, TransformationResult(
                transformation=config.type,
                rows_before=df.height,
                rows_after=df.height,
                columns_affected=[],
                success=False,
                error=error,
            )
        
        rows_before = df.height
        
        try:
            result_df, columns_affected, details = handler(df, config)
            
            result = TransformationResult(
                transformation=config.type,
                rows_before=rows_before,
                rows_after=result_df.height,
                columns_affected=columns_affected,
                success=True,
                details=details,
            )
            
            log.info(
                "Transformation completed",
                rows_before=rows_before,
                rows_after=result_df.height,
                columns_affected=columns_affected,
            )
            
            return result_df, result
            
        except Exception as e:
            log.error("Transformation failed", error=str(e))
            return df, TransformationResult(
                transformation=config.type,
                rows_before=rows_before,
                rows_after=df.height,
                columns_affected=[],
                success=False,
                error=str(e),
            )

    def transform_many(
        self,
        df: pl.DataFrame,
        configs: list[TransformationConfig],
    ) -> tuple[pl.DataFrame, list[TransformationResult]]:
        """Apply multiple transformations in sequence.
        
        Args:
            df: Input DataFrame.
            configs: List of transformation configurations.
            
        Returns:
            Tuple of (final DataFrame, list of results).
        """
        results: list[TransformationResult] = []
        current_df = df
        
        for config in configs:
            current_df, result = self.transform(current_df, config)
            results.append(result)
            
            # Stop on first failure
            if not result.success:
                logger.warning(
                    "Transformation chain stopped due to failure",
                    failed_at=config.type.value,
                )
                break
        
        return current_df, results

    def _get_columns(
        self,
        df: pl.DataFrame,
        specified: list[str] | None,
        dtype_filter: type | None = None,
    ) -> list[str]:
        """Get columns to operate on.
        
        Args:
            df: DataFrame.
            specified: User-specified columns (None = all).
            dtype_filter: Optional dtype filter (e.g., pl.String).
            
        Returns:
            List of column names.
        """
        if specified:
            # Validate specified columns exist
            missing = set(specified) - set(df.columns)
            if missing:
                raise TransformationError(f"Columns not found: {missing}")
            columns = specified
        else:
            columns = df.columns
        
        if dtype_filter:
            columns = [
                col for col in columns
                if df[col].dtype == dtype_filter
            ]
        
        return columns

    # =========================================================================
    # Transformation Handlers
    # =========================================================================

    def _clean_nulls(
        self,
        df: pl.DataFrame,
        config: TransformationConfig,
    ) -> tuple[pl.DataFrame, list[str], dict[str, Any]]:
        """Remove rows containing null values."""
        columns = self._get_columns(df, config.columns)
        
        # Count nulls before
        null_counts = {col: df[col].null_count() for col in columns}
        
        # Drop rows with nulls in specified columns
        result = df.drop_nulls(subset=columns)
        
        rows_removed = df.height - result.height
        
        return result, columns, {
            "null_counts_before": null_counts,
            "rows_removed": rows_removed,
        }

    def _fill_nulls(
        self,
        df: pl.DataFrame,
        config: TransformationConfig,
    ) -> tuple[pl.DataFrame, list[str], dict[str, Any]]:
        """Fill null values with a specified value or strategy."""
        columns = self._get_columns(df, config.columns)
        
        fill_value = config.params.get("value")
        strategy = config.params.get("strategy", "literal")  # literal, mean, median, forward, backward
        
        null_counts_before = {col: df[col].null_count() for col in columns}
        
        result = df
        for col in columns:
            if strategy == "literal" and fill_value is not None:
                result = result.with_columns(pl.col(col).fill_null(fill_value))
            elif strategy == "mean":
                result = result.with_columns(pl.col(col).fill_null(pl.col(col).mean()))
            elif strategy == "median":
                result = result.with_columns(pl.col(col).fill_null(pl.col(col).median()))
            elif strategy == "forward":
                result = result.with_columns(pl.col(col).fill_null(strategy="forward"))
            elif strategy == "backward":
                result = result.with_columns(pl.col(col).fill_null(strategy="backward"))
        
        null_counts_after = {col: result[col].null_count() for col in columns}
        
        return result, columns, {
            "null_counts_before": null_counts_before,
            "null_counts_after": null_counts_after,
            "strategy": strategy,
            "fill_value": fill_value,
        }

    def _trim_whitespace(
        self,
        df: pl.DataFrame,
        config: TransformationConfig,
    ) -> tuple[pl.DataFrame, list[str], dict[str, Any]]:
        """Trim leading and trailing whitespace from string columns."""
        columns = self._get_columns(df, config.columns, dtype_filter=pl.String)
        
        result = df
        for col in columns:
            result = result.with_columns(pl.col(col).str.strip_chars())
        
        return result, columns, {}

    def _uppercase(
        self,
        df: pl.DataFrame,
        config: TransformationConfig,
    ) -> tuple[pl.DataFrame, list[str], dict[str, Any]]:
        """Convert string columns to uppercase."""
        columns = self._get_columns(df, config.columns, dtype_filter=pl.String)
        
        result = df
        for col in columns:
            result = result.with_columns(pl.col(col).str.to_uppercase())
        
        return result, columns, {}

    def _lowercase(
        self,
        df: pl.DataFrame,
        config: TransformationConfig,
    ) -> tuple[pl.DataFrame, list[str], dict[str, Any]]:
        """Convert string columns to lowercase."""
        columns = self._get_columns(df, config.columns, dtype_filter=pl.String)
        
        result = df
        for col in columns:
            result = result.with_columns(pl.col(col).str.to_lowercase())
        
        return result, columns, {}

    def _remove_duplicates(
        self,
        df: pl.DataFrame,
        config: TransformationConfig,
    ) -> tuple[pl.DataFrame, list[str], dict[str, Any]]:
        """Remove duplicate rows."""
        columns = config.columns  # None means all columns
        keep = config.params.get("keep", "first")  # first, last, none
        
        if columns:
            result = df.unique(subset=columns, keep=keep)
        else:
            result = df.unique(keep=keep)
        
        duplicates_removed = df.height - result.height
        
        return result, columns or df.columns, {
            "duplicates_removed": duplicates_removed,
            "keep": keep,
        }

    def _convert_type(
        self,
        df: pl.DataFrame,
        config: TransformationConfig,
    ) -> tuple[pl.DataFrame, list[str], dict[str, Any]]:
        """Convert column data types."""
        columns = self._get_columns(df, config.columns)
        target_type = config.params.get("target_type", "str")
        
        type_map = {
            "str": pl.String,
            "string": pl.String,
            "int": pl.Int64,
            "int64": pl.Int64,
            "int32": pl.Int32,
            "float": pl.Float64,
            "float64": pl.Float64,
            "bool": pl.Boolean,
            "boolean": pl.Boolean,
            "date": pl.Date,
            "datetime": pl.Datetime,
        }
        
        polars_type = type_map.get(target_type.lower())
        if polars_type is None:
            raise TransformationError(f"Unknown target type: {target_type}")
        
        result = df
        for col in columns:
            result = result.with_columns(pl.col(col).cast(polars_type))
        
        return result, columns, {"target_type": target_type}

    def _rename_column(
        self,
        df: pl.DataFrame,
        config: TransformationConfig,
    ) -> tuple[pl.DataFrame, list[str], dict[str, Any]]:
        """Rename columns."""
        mapping = config.params.get("mapping", {})
        if not mapping:
            raise TransformationError("No column mapping provided")
        
        # Validate source columns exist
        missing = set(mapping.keys()) - set(df.columns)
        if missing:
            raise TransformationError(f"Columns not found: {missing}")
        
        result = df.rename(mapping)
        
        return result, list(mapping.keys()), {"mapping": mapping}

    def _drop_column(
        self,
        df: pl.DataFrame,
        config: TransformationConfig,
    ) -> tuple[pl.DataFrame, list[str], dict[str, Any]]:
        """Drop columns from DataFrame."""
        columns = self._get_columns(df, config.columns)
        
        result = df.drop(columns)
        
        return result, columns, {"dropped": columns}

    def _filter_rows(
        self,
        df: pl.DataFrame,
        config: TransformationConfig,
    ) -> tuple[pl.DataFrame, list[str], dict[str, Any]]:
        """Filter rows based on condition."""
        column = config.params.get("column")
        operator = config.params.get("operator", "eq")  # eq, ne, gt, lt, gte, lte, contains, not_null
        value = config.params.get("value")
        
        if not column:
            raise TransformationError("No column specified for filter")
        if column not in df.columns:
            raise TransformationError(f"Column not found: {column}")
        
        col_expr = pl.col(column)
        
        if operator == "eq":
            condition = col_expr == value
        elif operator == "ne":
            condition = col_expr != value
        elif operator == "gt":
            condition = col_expr > value
        elif operator == "lt":
            condition = col_expr < value
        elif operator == "gte":
            condition = col_expr >= value
        elif operator == "lte":
            condition = col_expr <= value
        elif operator == "contains":
            condition = col_expr.str.contains(str(value))
        elif operator == "not_null":
            condition = col_expr.is_not_null()
        elif operator == "is_null":
            condition = col_expr.is_null()
        else:
            raise TransformationError(f"Unknown operator: {operator}")
        
        result = df.filter(condition)
        
        return result, [column], {
            "operator": operator,
            "value": value,
            "rows_matched": result.height,
        }

    def _map_values(
        self,
        df: pl.DataFrame,
        config: TransformationConfig,
    ) -> tuple[pl.DataFrame, list[str], dict[str, Any]]:
        """Map values in a column to other values."""
        columns = self._get_columns(df, config.columns)
        mapping = config.params.get("mapping", {})
        default = config.params.get("default")  # Value for unmapped
        
        if not mapping:
            raise TransformationError("No value mapping provided")
        
        result = df
        for col in columns:
            # Use replace for value mapping
            result = result.with_columns(
                pl.col(col).replace(mapping, default=default)
            )
        
        return result, columns, {"mapping": mapping, "default": default}
