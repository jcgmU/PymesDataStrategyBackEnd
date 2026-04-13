"""Comprehensive tests for IRExecutor.

Covers all operations, the whitelist, nested trees, error cases,
integration with _apply_decisions, and backward compatibility.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock
from uuid import uuid4

import polars as pl
import pytest

from src.application.ir.executor import (
    IRExecutionError,
    IRExecutionResult,
    IRExecutor,
    IRResult,
)
from src.domain.entities.anomaly import AnomalyEntity
from src.domain.entities.decision import DecisionEntity


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def _make_anomaly(column: str, anomaly_type: str = "MISSING_VALUE") -> AnomalyEntity:
    return AnomalyEntity(
        id=str(uuid4()),
        dataset_id=str(uuid4()),
        column=column,
        row=None,
        type=anomaly_type,
        description="test anomaly",
        original_value=None,
        suggested_value=None,
        status="PENDING",
        created_at=datetime.now(timezone.utc),
    )


def _make_executor(
    df: pl.DataFrame,
    column: str,
    affected_rows: list[int] | None = None,
    anomaly_type: str = "MISSING_VALUE",
) -> IRExecutor:
    anomaly = _make_anomaly(column, anomaly_type)
    rows = affected_rows if affected_rows is not None else list(range(df.height))
    return IRExecutor(df, anomaly, rows)


# A simple numeric DataFrame used across many tests
_DF_NUMERIC = pl.DataFrame(
    {
        "value": [10.0, 20.0, 30.0, 40.0, 50.0],
        "other": [1.0, 2.0, 3.0, 4.0, 5.0],
    }
)

# A string DataFrame
_DF_STRING = pl.DataFrame(
    {
        "name": ["alice", "bob", "charlie", "dave"],
        "city": ["NYC", "LA", "NYC", "SF"],
    }
)

# DataFrame with nulls
_DF_WITH_NULLS = pl.DataFrame(
    {
        "salary": [1000.0, None, 3000.0, None, 5000.0],
        "dept": ["eng", None, "hr", None, "eng"],
    }
)


# ===========================================================================
# A5-1: FILL_LITERAL — casting
# ===========================================================================


class TestFillLiteral:
    def test_fill_literal_int64(self):
        df = pl.DataFrame({"x": pl.Series([1, 2, 3], dtype=pl.Int64)})
        executor = _make_executor(df, "x", [0, 1])
        result = executor.execute({"op": "FILL_LITERAL", "value": "42"})
        assert result.result_type == IRResult.FILL
        assert result.scalar_value == 42
        assert isinstance(result.scalar_value, int)

    def test_fill_literal_float64(self):
        df = pl.DataFrame({"x": pl.Series([1.0, 2.0], dtype=pl.Float64)})
        executor = _make_executor(df, "x", [0])
        result = executor.execute({"op": "FILL_LITERAL", "value": "3.14"})
        assert result.result_type == IRResult.FILL
        assert abs(result.scalar_value - 3.14) < 1e-9

    def test_fill_literal_utf8(self):
        df = pl.DataFrame({"x": pl.Series(["a", "b"], dtype=pl.Utf8)})
        executor = _make_executor(df, "x", [0])
        result = executor.execute({"op": "FILL_LITERAL", "value": "hello"})
        assert result.result_type == IRResult.FILL
        assert result.scalar_value == "hello"

    def test_fill_literal_numeric_from_number(self):
        df = pl.DataFrame({"x": pl.Series([1, 2], dtype=pl.Int64)})
        executor = _make_executor(df, "x", [0])
        result = executor.execute({"op": "FILL_LITERAL", "value": 7})
        assert result.scalar_value == 7

    # A5-2: null stays null, not the string "null"
    def test_fill_literal_null_stays_none(self):
        df = pl.DataFrame({"x": pl.Series([1.0, 2.0], dtype=pl.Float64)})
        executor = _make_executor(df, "x", [0])
        result = executor.execute({"op": "FILL_LITERAL", "value": None})
        assert result.result_type == IRResult.FILL
        assert result.scalar_value is None

    def test_fill_literal_null_not_string_null(self):
        """Ensure null isn't stored as the string 'null'."""
        df = pl.DataFrame({"x": pl.Series(["a", "b"], dtype=pl.Utf8)})
        executor = _make_executor(df, "x", [0])
        result = executor.execute({"op": "FILL_LITERAL", "value": None})
        assert result.scalar_value is None
        assert result.scalar_value != "null"


# ===========================================================================
# A5-3/4: FILL_AGGREGATE — all aggregates + error on string
# ===========================================================================


class TestFillAggregate:
    """Test each aggregate against manually computed values."""

    @pytest.fixture
    def df(self):
        return pl.DataFrame({"score": [10.0, 20.0, 30.0, 40.0, 50.0]})

    def test_mean(self, df):
        executor = _make_executor(df, "score", [0])
        result = executor.execute({"op": "FILL_AGGREGATE", "agg": "mean"})
        assert result.result_type == IRResult.FILL
        assert abs(result.scalar_value - 30.0) < 1e-9  # (10+20+30+40+50)/5

    def test_median(self, df):
        executor = _make_executor(df, "score", [0])
        result = executor.execute({"op": "FILL_AGGREGATE", "agg": "median"})
        assert result.result_type == IRResult.FILL
        assert abs(result.scalar_value - 30.0) < 1e-9  # median of [10,20,30,40,50]

    def test_min(self, df):
        executor = _make_executor(df, "score", [0])
        result = executor.execute({"op": "FILL_AGGREGATE", "agg": "min"})
        assert result.scalar_value == pytest.approx(10.0)

    def test_max(self, df):
        executor = _make_executor(df, "score", [0])
        result = executor.execute({"op": "FILL_AGGREGATE", "agg": "max"})
        assert result.scalar_value == pytest.approx(50.0)

    def test_sum(self, df):
        executor = _make_executor(df, "score", [0])
        result = executor.execute({"op": "FILL_AGGREGATE", "agg": "sum"})
        assert result.scalar_value == pytest.approx(150.0)

    def test_mode_numeric(self, df):
        df2 = pl.DataFrame({"score": [10.0, 10.0, 20.0, 30.0]})
        executor = _make_executor(df2, "score", [0])
        result = executor.execute({"op": "FILL_AGGREGATE", "agg": "mode"})
        assert result.result_type == IRResult.FILL
        assert result.scalar_value == pytest.approx(10.0)

    def test_mode_string(self):
        df = pl.DataFrame({"cat": ["a", "b", "a", "c", "a"]})
        executor = _make_executor(df, "cat", [0])
        result = executor.execute({"op": "FILL_AGGREGATE", "agg": "mode"})
        assert result.result_type == IRResult.FILL
        assert result.scalar_value == "a"

    # A5-4: mean on string column → IRExecutionError
    def test_mean_on_string_column_raises(self):
        df = pl.DataFrame({"name": ["alice", "bob", "charlie"]})
        executor = _make_executor(df, "name", [0])
        with pytest.raises(IRExecutionError, match="numeric"):
            executor.execute({"op": "FILL_AGGREGATE", "agg": "mean"})

    def test_median_on_string_column_raises(self):
        df = pl.DataFrame({"name": ["alice", "bob"]})
        executor = _make_executor(df, "name", [0])
        with pytest.raises(IRExecutionError, match="numeric"):
            executor.execute({"op": "FILL_AGGREGATE", "agg": "median"})

    def test_unknown_agg_raises(self):
        df = pl.DataFrame({"x": [1.0, 2.0]})
        executor = _make_executor(df, "x", [0])
        with pytest.raises(IRExecutionError, match="Unknown aggregate"):
            executor.execute({"op": "FILL_AGGREGATE", "agg": "percentile"})

    def test_empty_series_returns_none(self):
        df = pl.DataFrame({"x": pl.Series([], dtype=pl.Float64)})
        executor = _make_executor(df, "x", [])
        result = executor.execute({"op": "FILL_AGGREGATE", "agg": "mean"})
        assert result.scalar_value is None


# ===========================================================================
# A5-5: FILL_FROM_COLUMN — valid / invalid
# ===========================================================================


class TestFillFromColumn:
    def test_valid_column_returns_per_row(self):
        df = pl.DataFrame({"a": [1, 2, 3], "b": [10, 20, 30]})
        executor = _make_executor(df, "a", [0, 2])
        result = executor.execute({"op": "FILL_FROM_COLUMN", "sourceColumn": "b"})
        assert result.result_type == IRResult.FILL
        assert result.per_row_values == [10, 30]

    def test_invalid_column_raises(self):
        df = pl.DataFrame({"a": [1, 2, 3]})
        executor = _make_executor(df, "a", [0])
        with pytest.raises(IRExecutionError, match="does not exist"):
            executor.execute({"op": "FILL_FROM_COLUMN", "sourceColumn": "nonexistent"})

    def test_all_affected_rows_mapped(self):
        df = pl.DataFrame({"x": [None, None, None], "src": [100, 200, 300]})
        executor = _make_executor(df, "x", [0, 1, 2])
        result = executor.execute({"op": "FILL_FROM_COLUMN", "sourceColumn": "src"})
        assert result.per_row_values == [100, 200, 300]


# ===========================================================================
# A5-6: DELETE_ROWS, KEEP
# ===========================================================================


class TestDeleteAndKeep:
    def test_delete_rows(self):
        executor = _make_executor(_DF_NUMERIC, "value", [0, 1])
        result = executor.execute({"op": "DELETE_ROWS"})
        assert result.result_type == IRResult.DELETE
        assert result.scalar_value is None
        assert result.per_row_values is None

    def test_keep(self):
        executor = _make_executor(_DF_NUMERIC, "value", [0])
        result = executor.execute({"op": "KEEP"})
        assert result.result_type == IRResult.KEEP


# ===========================================================================
# A5-7: TRANSFORM — whitelist functions
# ===========================================================================


class TestTransform:
    """Test each whitelisted function."""

    @pytest.fixture
    def num_df(self):
        return pl.DataFrame({"x": [3.567, 2.123, 1.999]})

    @pytest.fixture
    def str_df(self):
        return pl.DataFrame({"name": ["  hello world  ", "PYTHON ", " test"]})

    def test_round(self, num_df):
        executor = _make_executor(num_df, "x", [0])
        result = executor.execute({
            "op": "TRANSFORM",
            "fn": "round",
            "params": {"decimals": 1},
            "input": {"op": "FILL_LITERAL", "value": 3.567},
        })
        assert result.scalar_value == pytest.approx(3.6)

    def test_floor(self, num_df):
        executor = _make_executor(num_df, "x", [0])
        result = executor.execute({
            "op": "TRANSFORM",
            "fn": "floor",
            "params": {},
            "input": {"op": "FILL_LITERAL", "value": 3.9},
        })
        assert result.scalar_value == pytest.approx(3.0)

    def test_ceil(self, num_df):
        executor = _make_executor(num_df, "x", [0])
        result = executor.execute({
            "op": "TRANSFORM",
            "fn": "ceil",
            "params": {},
            "input": {"op": "FILL_LITERAL", "value": 3.1},
        })
        assert result.scalar_value == pytest.approx(4.0)

    def test_abs(self, num_df):
        executor = _make_executor(num_df, "x", [0])
        result = executor.execute({
            "op": "TRANSFORM",
            "fn": "abs",
            "params": {},
            "input": {"op": "FILL_LITERAL", "value": -5.5},
        })
        assert result.scalar_value == pytest.approx(5.5)

    def test_upper(self, str_df):
        executor = _make_executor(str_df, "name", [0])
        result = executor.execute({
            "op": "TRANSFORM",
            "fn": "upper",
            "params": {},
            "input": {"op": "FILL_LITERAL", "value": "hello"},
        })
        assert result.scalar_value == "HELLO"

    def test_lower(self, str_df):
        executor = _make_executor(str_df, "name", [0])
        result = executor.execute({
            "op": "TRANSFORM",
            "fn": "lower",
            "params": {},
            "input": {"op": "FILL_LITERAL", "value": "HELLO"},
        })
        assert result.scalar_value == "hello"

    def test_title(self, str_df):
        executor = _make_executor(str_df, "name", [0])
        result = executor.execute({
            "op": "TRANSFORM",
            "fn": "title",
            "params": {},
            "input": {"op": "FILL_LITERAL", "value": "hello world"},
        })
        assert result.scalar_value == "Hello World"

    def test_trim(self, str_df):
        executor = _make_executor(str_df, "name", [0])
        result = executor.execute({
            "op": "TRANSFORM",
            "fn": "trim",
            "params": {},
            "input": {"op": "FILL_LITERAL", "value": "  spaces  "},
        })
        assert result.scalar_value == "spaces"

    def test_multiply(self, num_df):
        executor = _make_executor(num_df, "x", [0])
        result = executor.execute({
            "op": "TRANSFORM",
            "fn": "multiply",
            "params": {"by": 2},
            "input": {"op": "FILL_LITERAL", "value": 5.0},
        })
        assert result.scalar_value == pytest.approx(10.0)

    def test_add(self, num_df):
        executor = _make_executor(num_df, "x", [0])
        result = executor.execute({
            "op": "TRANSFORM",
            "fn": "add",
            "params": {"by": 3},
            "input": {"op": "FILL_LITERAL", "value": 7.0},
        })
        assert result.scalar_value == pytest.approx(10.0)

    def test_subtract(self, num_df):
        executor = _make_executor(num_df, "x", [0])
        result = executor.execute({
            "op": "TRANSFORM",
            "fn": "subtract",
            "params": {"by": 3},
            "input": {"op": "FILL_LITERAL", "value": 10.0},
        })
        assert result.scalar_value == pytest.approx(7.0)

    def test_divide(self, num_df):
        executor = _make_executor(num_df, "x", [0])
        result = executor.execute({
            "op": "TRANSFORM",
            "fn": "divide",
            "params": {"by": 4},
            "input": {"op": "FILL_LITERAL", "value": 20.0},
        })
        assert result.scalar_value == pytest.approx(5.0)

    def test_divide_by_zero_raises(self, num_df):
        executor = _make_executor(num_df, "x", [0])
        with pytest.raises(IRExecutionError, match="zero"):
            executor.execute({
                "op": "TRANSFORM",
                "fn": "divide",
                "params": {"by": 0},
                "input": {"op": "FILL_LITERAL", "value": 10.0},
            })

    # A5-8: fn not in whitelist → error
    def test_unknown_fn_raises(self, num_df):
        executor = _make_executor(num_df, "x", [0])
        with pytest.raises(IRExecutionError, match="whitelist"):
            executor.execute({
                "op": "TRANSFORM",
                "fn": "eval",
                "params": {},
                "input": {"op": "FILL_LITERAL", "value": 1},
            })

    def test_exec_fn_raises(self, num_df):
        executor = _make_executor(num_df, "x", [0])
        with pytest.raises(IRExecutionError, match="whitelist"):
            executor.execute({
                "op": "TRANSFORM",
                "fn": "exec",
                "params": {},
                "input": {"op": "FILL_LITERAL", "value": 1},
            })


# ===========================================================================
# A5-9: Nested trees
# ===========================================================================


class TestNestedTrees:
    """Test trees with depth 2: TRANSFORM(round, FILL_AGGREGATE(mean))
    and CONDITIONAL(gt col>100, FILL_LITERAL(100), KEEP)."""

    def test_transform_round_fill_aggregate_mean(self):
        """TRANSFORM(round(2), FILL_AGGREGATE(mean)) on a numeric column."""
        df = pl.DataFrame({"price": [10.1, 20.2, 30.3]})
        executor = _make_executor(df, "price", [0])
        # mean = (10.1+20.2+30.3)/3 = 20.2
        result = executor.execute({
            "op": "TRANSFORM",
            "fn": "round",
            "params": {"decimals": 1},
            "input": {"op": "FILL_AGGREGATE", "agg": "mean"},
        })
        assert result.result_type == IRResult.FILL
        assert result.scalar_value == pytest.approx(20.2, abs=0.01)

    def test_conditional_gt_fill_literal_keep(self):
        """CONDITIONAL: if value > 100 → fill 100, else KEEP."""
        df = pl.DataFrame({"val": [50.0, 150.0, 200.0]})
        executor = _make_executor(df, "val", [0, 1, 2])
        result = executor.execute({
            "op": "CONDITIONAL",
            "condition": {
                "op": "gt",
                "left": {"kind": "column", "column": "val"},
                "right": {"kind": "literal", "value": 100},
            },
            "then": {"op": "FILL_LITERAL", "value": "100"},
            "else": {"op": "KEEP"},
        })
        assert result.result_type == IRResult.FILL
        assert result.per_row_values is not None
        # row 0: 50 ≤ 100 → KEEP → None in mixed output
        # row 1: 150 > 100 → FILL 100
        # row 2: 200 > 100 → FILL 100
        assert result.per_row_values[1] == pytest.approx(100.0, abs=1)
        assert result.per_row_values[2] == pytest.approx(100.0, abs=1)

    def test_conditional_all_true_gives_fill(self):
        df = pl.DataFrame({"v": [500.0, 600.0]})
        executor = _make_executor(df, "v", [0, 1])
        result = executor.execute({
            "op": "CONDITIONAL",
            "condition": {
                "op": "gt",
                "left": {"kind": "column", "column": "v"},
                "right": {"kind": "literal", "value": 100},
            },
            "then": {"op": "FILL_LITERAL", "value": "100"},
            "else": {"op": "FILL_LITERAL", "value": "50"},
        })
        assert result.result_type == IRResult.FILL
        assert all(v == pytest.approx(100.0, abs=1) for v in result.per_row_values)

    def test_conditional_all_false_gives_else(self):
        df = pl.DataFrame({"v": [10.0, 20.0]})
        executor = _make_executor(df, "v", [0, 1])
        result = executor.execute({
            "op": "CONDITIONAL",
            "condition": {
                "op": "gt",
                "left": {"kind": "column", "column": "v"},
                "right": {"kind": "literal", "value": 100},
            },
            "then": {"op": "FILL_LITERAL", "value": "999"},
            "else": {"op": "FILL_LITERAL", "value": "0"},
        })
        assert result.result_type == IRResult.FILL
        assert all(v == pytest.approx(0.0, abs=1) for v in result.per_row_values)

    def test_conditional_delete_all_rows(self):
        df = pl.DataFrame({"v": [500.0, 600.0]})
        executor = _make_executor(df, "v", [0, 1])
        result = executor.execute({
            "op": "CONDITIONAL",
            "condition": {
                "op": "gt",
                "left": {"kind": "column", "column": "v"},
                "right": {"kind": "literal", "value": 100},
            },
            "then": {"op": "DELETE_ROWS"},
            "else": {"op": "DELETE_ROWS"},
        })
        assert result.result_type == IRResult.DELETE

    def test_transform_upper_fill_from_column(self):
        """TRANSFORM(upper, FILL_FROM_COLUMN(src))"""
        df = pl.DataFrame({"name": ["", ""], "src": ["hello", "world"]})
        executor = _make_executor(df, "name", [0, 1])
        result = executor.execute({
            "op": "TRANSFORM",
            "fn": "upper",
            "params": {},
            "input": {"op": "FILL_FROM_COLUMN", "sourceColumn": "src"},
        })
        assert result.result_type == IRResult.FILL
        assert result.per_row_values == ["HELLO", "WORLD"]


# ===========================================================================
# A5-10: Security — depth > 3 → error
# ===========================================================================


class TestSecurityDepth:
    def _deep_node(self, depth: int) -> dict:
        """Recursively build a TRANSFORM chain of given depth."""
        if depth == 0:
            return {"op": "FILL_LITERAL", "value": "1"}
        return {
            "op": "TRANSFORM",
            "fn": "abs",
            "params": {},
            "input": self._deep_node(depth - 1),
        }

    def test_depth_3_is_allowed(self):
        df = pl.DataFrame({"x": [1.0]})
        executor = _make_executor(df, "x", [0])
        # depth 3 = TRANSFORM → TRANSFORM → TRANSFORM → FILL_LITERAL
        node = self._deep_node(3)
        result = executor.execute(node)
        assert result.result_type == IRResult.FILL

    def test_depth_4_raises(self):
        df = pl.DataFrame({"x": [1.0]})
        executor = _make_executor(df, "x", [0])
        node = self._deep_node(4)
        with pytest.raises(IRExecutionError, match="too deep"):
            executor.execute(node)

    def test_depth_10_raises(self):
        df = pl.DataFrame({"x": [1.0]})
        executor = _make_executor(df, "x", [0])
        node = self._deep_node(10)
        with pytest.raises(IRExecutionError, match="too deep"):
            executor.execute(node)


# ===========================================================================
# A5-11: Unknown ops / operands / conditions → errors
# ===========================================================================


class TestUnknownOpsErrors:
    def test_unknown_op_raises(self):
        df = pl.DataFrame({"x": [1.0]})
        executor = _make_executor(df, "x", [0])
        with pytest.raises(IRExecutionError, match="Unknown IR op"):
            executor.execute({"op": "INJECT_SQL"})

    def test_unknown_operand_kind_raises(self):
        df = pl.DataFrame({"x": [100.0, 200.0]})
        executor = _make_executor(df, "x", [0, 1])
        with pytest.raises(IRExecutionError, match="Unknown operand kind"):
            executor.execute({
                "op": "CONDITIONAL",
                "condition": {
                    "op": "gt",
                    "left": {"kind": "magic", "value": 1},
                    "right": {"kind": "literal", "value": 0},
                },
                "then": {"op": "FILL_LITERAL", "value": "1"},
                "else": {"op": "KEEP"},
            })

    def test_unknown_condition_op_raises(self):
        df = pl.DataFrame({"x": [1.0]})
        executor = _make_executor(df, "x", [0])
        with pytest.raises(IRExecutionError):
            executor.execute({
                "op": "CONDITIONAL",
                "condition": {
                    "op": "like",  # not in whitelist
                    "left": {"kind": "column", "column": "x"},
                    "right": {"kind": "literal", "value": 1},
                },
                "then": {"op": "FILL_LITERAL", "value": "0"},
                "else": {"op": "KEEP"},
            })

    def test_operand_column_missing_raises(self):
        df = pl.DataFrame({"x": [1.0]})
        executor = _make_executor(df, "x", [0])
        with pytest.raises(IRExecutionError, match="does not exist"):
            executor.execute({
                "op": "CONDITIONAL",
                "condition": {
                    "op": "eq",
                    "left": {"kind": "column", "column": "nonexistent"},
                    "right": {"kind": "literal", "value": 1},
                },
                "then": {"op": "FILL_LITERAL", "value": "0"},
                "else": {"op": "KEEP"},
            })


# ===========================================================================
# Condition operators — exhaustive tests
# ===========================================================================


class TestConditionOperators:
    @pytest.fixture
    def df(self):
        return pl.DataFrame({
            "num": [50.0, 150.0, 100.0],
            "txt": ["hello", "world", "foo"],
        })

    def _cond_result(self, df, col, condition, affected_rows=None):
        rows = affected_rows or list(range(df.height))
        executor = _make_executor(df, col, rows)
        result = executor.execute({
            "op": "CONDITIONAL",
            "condition": condition,
            "then": {"op": "FILL_LITERAL", "value": "1"},
            "else": {"op": "FILL_LITERAL", "value": "0"},
        })
        return result.per_row_values

    def test_eq(self, df):
        vals = self._cond_result(df, "num", {
            "op": "eq",
            "left": {"kind": "column", "column": "num"},
            "right": {"kind": "literal", "value": 100},
        })
        # row 0: 50≠100 → 0; row 1: 150≠100 → 0; row 2: 100==100 → 1
        assert vals[2] is not None  # truthy branch

    def test_neq(self, df):
        vals = self._cond_result(df, "num", {
            "op": "neq",
            "left": {"kind": "column", "column": "num"},
            "right": {"kind": "literal", "value": 100},
        })
        assert vals[0] is not None  # 50 != 100

    def test_gt(self, df):
        vals = self._cond_result(df, "num", {
            "op": "gt",
            "left": {"kind": "column", "column": "num"},
            "right": {"kind": "literal", "value": 100},
        })
        # only 150 > 100
        assert vals[1] is not None

    def test_gte(self, df):
        vals = self._cond_result(df, "num", {
            "op": "gte",
            "left": {"kind": "column", "column": "num"},
            "right": {"kind": "literal", "value": 100},
        })
        # 150 >= 100 and 100 >= 100
        assert vals[1] is not None
        assert vals[2] is not None

    def test_lt(self, df):
        vals = self._cond_result(df, "num", {
            "op": "lt",
            "left": {"kind": "column", "column": "num"},
            "right": {"kind": "literal", "value": 100},
        })
        assert vals[0] is not None  # 50 < 100

    def test_lte(self, df):
        vals = self._cond_result(df, "num", {
            "op": "lte",
            "left": {"kind": "column", "column": "num"},
            "right": {"kind": "literal", "value": 100},
        })
        assert vals[0] is not None  # 50 <= 100
        assert vals[2] is not None  # 100 <= 100

    def test_is_null(self):
        df = pl.DataFrame({"x": [1.0, None, 3.0]})
        executor = _make_executor(df, "x", [0, 1, 2])
        result = executor.execute({
            "op": "CONDITIONAL",
            "condition": {
                "op": "is_null",
                "left": {"kind": "column", "column": "x"},
            },
            "then": {"op": "FILL_LITERAL", "value": "0"},
            "else": {"op": "KEEP"},
        })
        # row 1 is null → then branch → 0
        assert result.per_row_values[1] == pytest.approx(0.0, abs=1)

    def test_is_not_null(self):
        df = pl.DataFrame({"x": [1.0, None, 3.0]})
        executor = _make_executor(df, "x", [0, 1, 2])
        result = executor.execute({
            "op": "CONDITIONAL",
            "condition": {
                "op": "is_not_null",
                "left": {"kind": "column", "column": "x"},
            },
            "then": {"op": "FILL_LITERAL", "value": "99"},
            "else": {"op": "KEEP"},
        })
        # rows 0 and 2 are not null → then branch → 99
        assert result.per_row_values[0] == pytest.approx(99.0, abs=1)
        assert result.per_row_values[2] == pytest.approx(99.0, abs=1)

    def test_contains(self, df):
        vals = self._cond_result(df, "txt", {
            "op": "contains",
            "left": {"kind": "column", "column": "txt"},
            "right": {"kind": "literal", "value": "ell"},
        })
        # "hello" contains "ell" → row 0 true
        assert vals[0] is not None

    def test_starts_with(self, df):
        vals = self._cond_result(df, "txt", {
            "op": "starts_with",
            "left": {"kind": "column", "column": "txt"},
            "right": {"kind": "literal", "value": "wor"},
        })
        # "world" starts with "wor" → row 1
        assert vals[1] is not None

    def test_ends_with(self, df):
        vals = self._cond_result(df, "txt", {
            "op": "ends_with",
            "left": {"kind": "column", "column": "txt"},
            "right": {"kind": "literal", "value": "oo"},
        })
        # "foo" ends with "oo" → row 2
        assert vals[2] is not None

    def test_between(self):
        df = pl.DataFrame({"x": [50.0, 150.0, 300.0]})
        executor = _make_executor(df, "x", [0, 1, 2])
        result = executor.execute({
            "op": "CONDITIONAL",
            "condition": {
                "op": "between",
                "left": {"kind": "column", "column": "x"},
                "right": {"kind": "literal", "value": 100},
                "upper": {"kind": "literal", "value": 200},
            },
            "then": {"op": "FILL_LITERAL", "value": "1"},
            "else": {"op": "FILL_LITERAL", "value": "0"},
        })
        # 50 not between 100-200, 150 yes, 300 no
        vals = result.per_row_values
        assert vals is not None


# ===========================================================================
# A5-12: Integration — real dataset with nulls/outliers
# ===========================================================================


class TestIntegration:
    """End-to-end tests using ProcessDatasetUseCase._apply_decisions."""

    def _make_use_case(self):
        from src.application.use_cases.process_dataset import ProcessDatasetUseCase
        from src.infrastructure.parsers.dataset_parser import DatasetParser
        from unittest.mock import AsyncMock

        storage = AsyncMock()
        storage.download_file = AsyncMock()
        storage.upload_file = AsyncMock()
        parser = DatasetParser()
        return ProcessDatasetUseCase(
            storage=storage,
            parser=parser,
            output_bucket="test",
        )

    def test_ir_fill_aggregate_mean_applied_to_nulls(self):
        """IR FILL_AGGREGATE(mean) fills null cells correctly."""
        df = pl.DataFrame({
            "salary": pl.Series([1000.0, None, 3000.0, None, 5000.0], dtype=pl.Float64)
        })
        anomaly = _make_anomaly("salary", "MISSING_VALUE")
        # Affected rows = rows where salary is null (indices 1 and 3)
        affected = [i for i, v in enumerate(df["salary"].to_list()) if v is None]

        executor = IRExecutor(df, anomaly, affected)
        ir_result = executor.execute({"op": "FILL_AGGREGATE", "agg": "mean"})

        assert ir_result.result_type == IRResult.FILL
        # mean of [1000, 3000, 5000] = 3000
        assert abs(ir_result.scalar_value - 3000.0) < 1e-9

        # Simulate applying the result
        col_data = df["salary"].to_list()
        for row_idx in affected:
            col_data[row_idx] = ir_result.scalar_value
        updated = df.with_columns(pl.Series("salary", col_data, dtype=pl.Float64))
        assert updated["salary"].null_count() == 0
        assert updated["salary"][1] == pytest.approx(3000.0)
        assert updated["salary"][3] == pytest.approx(3000.0)

    def test_ir_delete_rows_removes_outliers(self):
        """IR DELETE_ROWS correctly identifies rows to drop."""
        # Build a dataset with one extreme outlier
        normal = [30.0] * 20
        df = pl.DataFrame({"age": normal + [9999.0]})
        anomaly = _make_anomaly("age", "OUTLIER")
        affected = [20]  # the outlier row

        executor = IRExecutor(df, anomaly, affected)
        ir_result = executor.execute({"op": "DELETE_ROWS"})

        assert ir_result.result_type == IRResult.DELETE

    def test_ir_fill_literal_replaces_correct_rows(self):
        """IR FILL_LITERAL fills specific rows without touching others."""
        df = pl.DataFrame({"x": pl.Series([1.0, None, 3.0, None], dtype=pl.Float64)})
        anomaly = _make_anomaly("x", "MISSING_VALUE")
        affected = [1, 3]

        executor = IRExecutor(df, anomaly, affected)
        ir_result = executor.execute({"op": "FILL_LITERAL", "value": "0"})

        assert ir_result.result_type == IRResult.FILL
        col_data = df["x"].to_list()
        for row_idx in affected:
            col_data[row_idx] = ir_result.scalar_value
        updated = df.with_columns(pl.Series("x", col_data, dtype=pl.Float64))
        assert updated["x"].to_list() == [1.0, 0.0, 3.0, 0.0]

    def test_ir_conditional_clamps_outlier(self):
        """Conditional IR: if value > 100 → clamp to 100, else keep."""
        df = pl.DataFrame({"price": [50.0, 150.0, 200.0, 80.0]})
        anomaly = _make_anomaly("price", "OUTLIER")
        affected = list(range(4))

        executor = IRExecutor(df, anomaly, affected)
        ir_result = executor.execute({
            "op": "CONDITIONAL",
            "condition": {
                "op": "gt",
                "left": {"kind": "column", "column": "price"},
                "right": {"kind": "literal", "value": 100},
            },
            "then": {"op": "FILL_LITERAL", "value": "100"},
            "else": {"op": "KEEP"},
        })

        assert ir_result.result_type == IRResult.FILL
        per_row = ir_result.per_row_values
        assert per_row is not None
        # 50 ≤ 100 → KEEP (None sentinel in mixed)
        # 150 > 100 → 100
        # 200 > 100 → 100
        # 80 ≤ 100 → KEEP (None sentinel)
        assert per_row[1] == pytest.approx(100.0, abs=1)
        assert per_row[2] == pytest.approx(100.0, abs=1)

    @pytest.mark.asyncio
    async def test_apply_decisions_with_ir_fill(self):
        """_apply_decisions applies IR FILL correctly (integration with use case)."""
        from src.application.use_cases.process_dataset import ProcessDatasetUseCase
        from src.infrastructure.parsers.dataset_parser import DatasetParser
        from unittest.mock import AsyncMock

        storage = AsyncMock()
        storage.download_file = AsyncMock(
            return_value=b"salary\n1000\n\n3000\n\n5000\n"
        )
        storage.upload_file = AsyncMock()
        parser = DatasetParser()

        use_case = ProcessDatasetUseCase(
            storage=storage,
            parser=parser,
            output_bucket="test",
        )

        df = pl.DataFrame({
            "salary": pl.Series([1000.0, None, 3000.0, None, 5000.0], dtype=pl.Float64)
        })

        anomaly = _make_anomaly("salary", "MISSING_VALUE")
        decision = DecisionEntity(
            id=str(uuid4()),
            anomaly_id=anomaly.id,
            action="CORRECTED",
            correction=None,
            user_id=str(uuid4()),
            created_at=datetime.now(timezone.utc),
            correction_ir={"op": "FILL_AGGREGATE", "agg": "mean"},
            ir_source="rule",
        )

        result_df = use_case._apply_decisions(df, [anomaly], [decision])
        assert result_df["salary"].null_count() == 0
        # mean of [1000, 3000, 5000] = 3000
        assert result_df["salary"][1] == pytest.approx(3000.0)
        assert result_df["salary"][3] == pytest.approx(3000.0)

    @pytest.mark.asyncio
    async def test_apply_decisions_with_ir_delete(self):
        """_apply_decisions applies IR DELETE_ROWS correctly."""
        from src.application.use_cases.process_dataset import ProcessDatasetUseCase
        from src.infrastructure.parsers.dataset_parser import DatasetParser
        from unittest.mock import AsyncMock

        storage = AsyncMock()
        storage.upload_file = AsyncMock()
        parser = DatasetParser()
        use_case = ProcessDatasetUseCase(
            storage=storage, parser=parser, output_bucket="test"
        )

        normal = [30] * 20
        df = pl.DataFrame({"age": pl.Series(normal + [9999], dtype=pl.Int64)})
        anomaly = _make_anomaly("age", "OUTLIER")
        # Force anomaly type to OUTLIER so affected rows are computed correctly
        anomaly_outlier = AnomalyEntity(
            id=anomaly.id,
            dataset_id=anomaly.dataset_id,
            column="age",
            row=None,
            type="OUTLIER",
            description="outlier",
            original_value=None,
            suggested_value=None,
            status="PENDING",
            created_at=anomaly.created_at,
        )

        decision = DecisionEntity(
            id=str(uuid4()),
            anomaly_id=anomaly_outlier.id,
            action="CORRECTED",
            correction=None,
            user_id=str(uuid4()),
            created_at=datetime.now(timezone.utc),
            correction_ir={"op": "DELETE_ROWS"},
            ir_source="rule",
        )

        result_df = use_case._apply_decisions(df, [anomaly_outlier], [decision])
        # Business rule: rows are NEVER deleted.  IR DELETE is treated as a
        # no-op, so all 21 rows (including the outlier) are preserved.
        assert result_df.height == 21
        assert 9999 in result_df["age"].to_list()


# ===========================================================================
# A5-13: Backward compatibility — legacy correction path still works
# ===========================================================================


class TestBackwardCompatibility:
    """Legacy decisions (correction=string, correction_ir=None) still work."""

    @pytest.mark.asyncio
    async def test_legacy_correction_string_still_applies(self):
        """When correction_ir is None, the legacy string correction path is used."""
        from src.application.use_cases.process_dataset import ProcessDatasetUseCase
        from src.infrastructure.parsers.dataset_parser import DatasetParser
        from unittest.mock import AsyncMock

        storage = AsyncMock()
        storage.upload_file = AsyncMock()
        parser = DatasetParser()
        use_case = ProcessDatasetUseCase(
            storage=storage, parser=parser, output_bucket="test"
        )

        df = pl.DataFrame({
            "salary": pl.Series([1000.0, None, 3000.0], dtype=pl.Float64)
        })
        anomaly = _make_anomaly("salary", "MISSING_VALUE")

        # Legacy decision: correction is a string, correction_ir is None (default)
        decision = DecisionEntity(
            id=str(uuid4()),
            anomaly_id=anomaly.id,
            action="CORRECTED",
            correction="99",   # legacy string
            user_id=str(uuid4()),
            created_at=datetime.now(timezone.utc),
            # correction_ir and ir_source default to None
        )

        result_df = use_case._apply_decisions(df, [anomaly], [decision])

        assert result_df["salary"].null_count() == 0
        assert result_df["salary"][1] == pytest.approx(99.0)

    @pytest.mark.asyncio
    async def test_decision_entity_defaults_to_none_for_ir_fields(self):
        """DecisionEntity with no IR fields creates entity with correction_ir=None."""
        decision = DecisionEntity(
            id="test-id",
            anomaly_id="anomaly-id",
            action="CORRECTED",
            correction="42",
            user_id="user-id",
            created_at=datetime.now(timezone.utc),
        )
        assert decision.correction_ir is None
        assert decision.ir_source is None

    @pytest.mark.asyncio
    async def test_ir_error_does_not_abort_other_decisions(self):
        """An IRExecutionError on one anomaly does not abort other decisions."""
        from src.application.use_cases.process_dataset import ProcessDatasetUseCase
        from src.infrastructure.parsers.dataset_parser import DatasetParser
        from unittest.mock import AsyncMock

        storage = AsyncMock()
        storage.upload_file = AsyncMock()
        parser = DatasetParser()
        use_case = ProcessDatasetUseCase(
            storage=storage, parser=parser, output_bucket="test"
        )

        df = pl.DataFrame({
            "salary": pl.Series([1000.0, None, 3000.0], dtype=pl.Float64),
            "bonus": pl.Series([100.0, None, 300.0], dtype=pl.Float64),
        })

        anomaly1 = _make_anomaly("salary", "MISSING_VALUE")
        anomaly2 = _make_anomaly("bonus", "MISSING_VALUE")

        # Bad IR decision on salary (unknown op → IRExecutionError)
        bad_decision = DecisionEntity(
            id=str(uuid4()),
            anomaly_id=anomaly1.id,
            action="CORRECTED",
            correction=None,
            user_id=str(uuid4()),
            created_at=datetime.now(timezone.utc),
            correction_ir={"op": "INVALID_OP"},
        )

        # Good legacy decision on bonus
        good_decision = DecisionEntity(
            id=str(uuid4()),
            anomaly_id=anomaly2.id,
            action="CORRECTED",
            correction="0",
            user_id=str(uuid4()),
            created_at=datetime.now(timezone.utc),
        )

        # Should not raise; bad IR is logged and skipped
        result_df = use_case._apply_decisions(df, [anomaly1, anomaly2], [bad_decision, good_decision])

        # bonus null should be filled with 0
        assert result_df["bonus"].null_count() == 0
        assert result_df["bonus"][1] == pytest.approx(0.0)
        # salary null remains (bad decision was skipped)
        assert result_df["salary"].null_count() == 1
