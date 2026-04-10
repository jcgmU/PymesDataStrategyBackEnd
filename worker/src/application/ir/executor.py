"""IR (Intermediate Representation) executor for the Worker.

Executes a validated IR tree over a Polars DataFrame to apply user corrections
to anomalous rows.  All operations use a closed whitelist — no eval, no exec,
no string-to-code paths of any kind.

IR schema reference: .planning/specs/2026-04-09-edit-natural-language-design.md §2
Executor spec:       .planning/specs/2026-04-09-edit-natural-language-design.md §5
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

import polars as pl

if TYPE_CHECKING:
    from src.domain.entities.anomaly import AnomalyEntity


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


class IRResult(Enum):
    """High-level outcome of executing one IR node."""

    FILL = "fill"
    DELETE = "delete"
    KEEP = "keep"


@dataclass
class IRExecutionResult:
    """Result returned by ``IRExecutor.execute()``.

    Exactly one of ``scalar_value`` or ``per_row_values`` is set when
    ``result_type == IRResult.FILL``.
    """

    result_type: IRResult
    scalar_value: Any | None = field(default=None)
    per_row_values: list[Any] | None = field(default=None)


class IRExecutionError(Exception):
    """Raised when the IR tree cannot be executed (bad op, bad type, etc.)."""


# ---------------------------------------------------------------------------
# Numeric / string dtype helpers
# ---------------------------------------------------------------------------

_NUMERIC_DTYPES = (
    pl.Int8, pl.Int16, pl.Int32, pl.Int64,
    pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
    pl.Float32, pl.Float64,
)

_STRING_DTYPES = (pl.Utf8, pl.String)


def _is_numeric(dtype: pl.DataType) -> bool:
    return isinstance(dtype, _NUMERIC_DTYPES)


def _is_string(dtype: pl.DataType) -> bool:
    return isinstance(dtype, _STRING_DTYPES)


# ---------------------------------------------------------------------------
# Executor
# ---------------------------------------------------------------------------


class IRExecutor:
    """Executes a validated IR tree over a Polars DataFrame.

    Args:
        df:            The full dataset DataFrame.
        anomaly:       The anomaly entity that triggered this decision.
        affected_rows: Row indices (0-based) that the anomaly covers.
    """

    MAX_DEPTH = 3

    # Whitelist of TRANSFORM functions mapped to (lambda df_series, params) -> value.
    # Each entry is a callable that takes (value, params) and returns the transformed value.
    # "value" is the scalar or per-row Python value coming from the inner node.
    _TRANSFORM_FNS: dict[str, Any] = {}  # populated after class definition

    def __init__(
        self,
        df: pl.DataFrame,
        anomaly: "AnomalyEntity",
        affected_rows: list[int],
    ) -> None:
        self._df = df
        self._anomaly = anomaly
        self._col = anomaly.column
        self._dtype = df[anomaly.column].dtype
        self._affected_rows = affected_rows

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def execute(self, node: dict, depth: int = 0) -> IRExecutionResult:
        """Execute a single IR node (recursive for TRANSFORM / CONDITIONAL).

        Args:
            node:  IR node dict (validated by the backend before storage).
            depth: Current recursion depth; raises ``IRExecutionError`` if > MAX_DEPTH.

        Returns:
            ``IRExecutionResult`` describing the action to take.

        Raises:
            ``IRExecutionError``: On any invalid op, type mismatch, or depth violation.
        """
        if depth > self.MAX_DEPTH:
            raise IRExecutionError(
                f"IR tree too deep (>{self.MAX_DEPTH}); possible infinite nesting"
            )

        op = node.get("op")
        if op == "FILL_LITERAL":
            return self._fill_literal(node)
        if op == "FILL_AGGREGATE":
            return self._fill_aggregate(node)
        if op == "FILL_FROM_COLUMN":
            return self._fill_from_column(node)
        if op == "DELETE_ROWS":
            return IRExecutionResult(IRResult.DELETE)
        if op == "KEEP":
            return IRExecutionResult(IRResult.KEEP)
        if op == "TRANSFORM":
            return self._transform(node, depth)
        if op == "CONDITIONAL":
            return self._conditional(node, depth)
        raise IRExecutionError(f"Unknown IR op: {op!r}")

    # ------------------------------------------------------------------
    # Terminal operations
    # ------------------------------------------------------------------

    def _fill_literal(self, node: dict) -> IRExecutionResult:
        """Return a scalar literal cast to the column dtype."""
        raw = node.get("value")

        # Explicit null (JSON null → Python None)
        if raw is None:
            return IRExecutionResult(IRResult.FILL, scalar_value=None)

        # Cast to column dtype
        casted = self._cast_to_dtype(raw, self._dtype)
        return IRExecutionResult(IRResult.FILL, scalar_value=casted)

    def _fill_aggregate(self, node: dict) -> IRExecutionResult:
        """Compute a column-level aggregate and return as scalar."""
        agg = node.get("agg")
        series = self._df[self._col].drop_nulls()

        if agg == "mode":
            # mode works on any dtype
            if series.len() == 0:
                return IRExecutionResult(IRResult.FILL, scalar_value=None)
            mode_result = series.mode()
            return IRExecutionResult(IRResult.FILL, scalar_value=mode_result[0])

        # All other aggregates require numeric dtype
        if not _is_numeric(self._dtype):
            raise IRExecutionError(
                f"FILL_AGGREGATE.{agg} requires a numeric column; "
                f"column {self._col!r} has dtype {self._dtype}"
            )

        if series.len() == 0:
            return IRExecutionResult(IRResult.FILL, scalar_value=None)

        float_series = series.cast(pl.Float64)

        if agg == "mean":
            value = float_series.mean()
        elif agg == "median":
            value = float_series.median()
        elif agg == "min":
            value = float_series.min()
        elif agg == "max":
            value = float_series.max()
        elif agg == "sum":
            value = float_series.sum()
        else:
            raise IRExecutionError(f"Unknown aggregate function: {agg!r}")

        # Cast back to column dtype so downstream can write without type errors
        casted = self._cast_to_dtype(value, self._dtype)
        return IRExecutionResult(IRResult.FILL, scalar_value=casted)

    def _fill_from_column(self, node: dict) -> IRExecutionResult:
        """Return per-row values from another column for each affected row."""
        source_col = node.get("sourceColumn")
        if source_col not in self._df.columns:
            raise IRExecutionError(
                f"FILL_FROM_COLUMN: source column {source_col!r} does not exist. "
                f"Available columns: {self._df.columns}"
            )
        col_data = self._df[source_col].to_list()
        per_row = [col_data[i] for i in self._affected_rows]
        return IRExecutionResult(IRResult.FILL, per_row_values=per_row)

    # ------------------------------------------------------------------
    # TRANSFORM
    # ------------------------------------------------------------------

    def _transform(self, node: dict, depth: int) -> IRExecutionResult:
        """Apply a whitelisted function to the result of the inner node."""
        fn_name = node.get("fn")
        params = node.get("params") or {}
        input_node = node.get("input")

        if fn_name not in _TRANSFORM_WHITELIST:
            raise IRExecutionError(
                f"TRANSFORM fn {fn_name!r} is not in the whitelist. "
                f"Allowed: {sorted(_TRANSFORM_WHITELIST)}"
            )

        inner = self.execute(input_node, depth + 1)
        if inner.result_type != IRResult.FILL:
            # DELETE / KEEP pass through unchanged — transform has nothing to apply
            return inner

        fn = _TRANSFORM_WHITELIST[fn_name]

        if inner.scalar_value is not None:
            try:
                new_val = fn(inner.scalar_value, params)
            except (TypeError, ValueError, ZeroDivisionError) as exc:
                raise IRExecutionError(
                    f"TRANSFORM.{fn_name} failed on value {inner.scalar_value!r}: {exc}"
                ) from exc
            return IRExecutionResult(IRResult.FILL, scalar_value=new_val)

        if inner.per_row_values is not None:
            new_vals: list[Any] = []
            for v in inner.per_row_values:
                try:
                    new_vals.append(fn(v, params))
                except (TypeError, ValueError, ZeroDivisionError) as exc:
                    raise IRExecutionError(
                        f"TRANSFORM.{fn_name} failed on per-row value {v!r}: {exc}"
                    ) from exc
            return IRExecutionResult(IRResult.FILL, per_row_values=new_vals)

        # Both scalar and per_row are None (unusual — treat as null fill)
        return IRExecutionResult(IRResult.FILL, scalar_value=None)

    # ------------------------------------------------------------------
    # CONDITIONAL
    # ------------------------------------------------------------------

    def _conditional(self, node: dict, depth: int) -> IRExecutionResult:
        """Evaluate condition per affected row, mix results from both branches."""
        condition = node.get("condition")
        then_node = node.get("then")
        else_node = node.get("else")

        # Evaluate condition for each affected row → list[bool]
        cond_results = [
            self._eval_condition(condition, row_idx)
            for row_idx in self._affected_rows
        ]

        # Execute both branches
        then_result = self.execute(then_node, depth + 1)
        else_result = self.execute(else_node, depth + 1)

        # If both branches are non-FILL (DELETE/KEEP), mix is straightforward
        # but in practice mixed DELETE+FILL is unusual; handle the common cases.
        then_vals = self._result_to_per_row_list(then_result)
        else_vals = self._result_to_per_row_list(else_result)

        # _SENTINEL_DELETE / _SENTINEL_KEEP — special objects for non-FILL branches
        mixed: list[Any] = []
        for i, cond_true in enumerate(cond_results):
            mixed.append(then_vals[i] if cond_true else else_vals[i])

        # Determine if all are the same special sentinel
        all_delete = all(v is _SENTINEL_DELETE for v in mixed)
        all_keep = all(v is _SENTINEL_KEEP for v in mixed)
        if all_delete:
            return IRExecutionResult(IRResult.DELETE)
        if all_keep:
            return IRExecutionResult(IRResult.KEEP)

        # Mixed or FILL: replace sentinels with None (null) to keep series valid
        final: list[Any] = [
            None if v in (_SENTINEL_DELETE, _SENTINEL_KEEP) else v
            for v in mixed
        ]
        return IRExecutionResult(IRResult.FILL, per_row_values=final)

    def _result_to_per_row_list(self, result: IRExecutionResult) -> list[Any]:
        """Expand an IRExecutionResult to a per-row list aligned with affected_rows."""
        n = len(self._affected_rows)
        if result.result_type == IRResult.DELETE:
            return [_SENTINEL_DELETE] * n
        if result.result_type == IRResult.KEEP:
            return [_SENTINEL_KEEP] * n
        # FILL
        if result.per_row_values is not None:
            return list(result.per_row_values)
        # scalar — broadcast
        return [result.scalar_value] * n

    # ------------------------------------------------------------------
    # Condition evaluation
    # ------------------------------------------------------------------

    def _eval_condition(self, cond: dict, row_idx: int) -> bool:
        """Evaluate a condition dict for a single row index.

        Supported ops: eq, neq, gt, gte, lt, lte, is_null, is_not_null,
                       contains, starts_with, ends_with, between.
        """
        op = cond.get("op")
        left = self._resolve_operand(cond["left"], row_idx)

        if op == "is_null":
            return left is None
        if op == "is_not_null":
            return left is not None

        if op == "between":
            right = self._resolve_operand(cond["right"], row_idx)
            upper = self._resolve_operand(cond["upper"], row_idx)
            try:
                return float(right) <= float(left) <= float(upper)
            except (TypeError, ValueError) as exc:
                raise IRExecutionError(
                    f"CONDITIONAL.between requires numeric operands; got {left!r}, {right!r}, {upper!r}"
                ) from exc

        right = self._resolve_operand(cond["right"], row_idx)

        if op == "eq":
            return left == right
        if op == "neq":
            return left != right
        if op == "contains":
            return str(right) in str(left)
        if op == "starts_with":
            return str(left).startswith(str(right))
        if op == "ends_with":
            return str(left).endswith(str(right))

        # Numeric comparisons
        if op in {"gt", "gte", "lt", "lte"}:
            try:
                l_f, r_f = float(left), float(right)
            except (TypeError, ValueError) as exc:
                raise IRExecutionError(
                    f"CONDITIONAL.{op} requires numeric operands; got {left!r} and {right!r}"
                ) from exc
            if op == "gt":
                return l_f > r_f
            if op == "gte":
                return l_f >= r_f
            if op == "lt":
                return l_f < r_f
            if op == "lte":
                return l_f <= r_f

        raise IRExecutionError(f"Unknown condition op: {op!r}")

    def _resolve_operand(self, operand: dict, row_idx: int) -> Any:
        """Resolve an operand to a Python scalar for a given row index.

        Supported kinds: literal, column, aggregate.
        """
        kind = operand.get("kind")

        if kind == "literal":
            return operand["value"]

        if kind == "column":
            col_name = operand["column"]
            if col_name not in self._df.columns:
                raise IRExecutionError(
                    f"Operand column {col_name!r} does not exist. "
                    f"Available: {self._df.columns}"
                )
            return self._df[col_name][row_idx]

        if kind == "aggregate":
            col_name = operand.get("column", self._col)
            agg = operand.get("agg")
            if col_name not in self._df.columns:
                raise IRExecutionError(
                    f"Aggregate operand column {col_name!r} does not exist."
                )
            series = self._df[col_name].drop_nulls().cast(pl.Float64)
            if series.len() == 0:
                return None
            if agg == "mean":
                return float(series.mean())
            if agg == "median":
                return float(series.median())
            if agg == "min":
                return float(series.min())
            if agg == "max":
                return float(series.max())
            if agg == "sum":
                return float(series.sum())
            if agg == "mode":
                return self._df[col_name].drop_nulls().mode()[0]
            raise IRExecutionError(f"Unknown aggregate in operand: {agg!r}")

        raise IRExecutionError(f"Unknown operand kind: {kind!r}")

    # ------------------------------------------------------------------
    # Dtype casting helper
    # ------------------------------------------------------------------

    @staticmethod
    def _cast_to_dtype(value: Any, dtype: pl.DataType) -> Any:
        """Cast *value* to the Python type that Polars expects for *dtype*.

        Returns None unchanged.  On cast failure returns the raw value so
        downstream Polars insertion will raise a descriptive error.
        """
        if value is None:
            return None

        integer_types = (
            pl.Int8, pl.Int16, pl.Int32, pl.Int64,
            pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
        )
        float_types = (pl.Float32, pl.Float64)

        try:
            if isinstance(dtype, integer_types):
                return int(float(value))
            if isinstance(dtype, float_types):
                return float(value)
            if isinstance(dtype, (pl.Utf8, pl.String)):
                return str(value)
        except (ValueError, TypeError):
            pass

        return value


# ---------------------------------------------------------------------------
# Sentinel objects for conditional branch mixing
# ---------------------------------------------------------------------------

class _Sentinel:
    def __init__(self, name: str) -> None:
        self._name = name

    def __repr__(self) -> str:
        return f"<{self._name}>"


_SENTINEL_DELETE = _Sentinel("DELETE")
_SENTINEL_KEEP = _Sentinel("KEEP")


# ---------------------------------------------------------------------------
# TRANSFORM whitelist
# Mapping: fn_name -> callable(value: Any, params: dict) -> Any
# NO eval, NO exec, NO string concatenation of code.
# ---------------------------------------------------------------------------

def _fn_round(v: Any, params: dict) -> Any:
    decimals = int(params.get("decimals", 0))
    return round(float(v), decimals)


def _fn_floor(v: Any, params: dict) -> Any:
    return float(math.floor(float(v)))


def _fn_ceil(v: Any, params: dict) -> Any:
    return float(math.ceil(float(v)))


def _fn_abs(v: Any, params: dict) -> Any:
    return abs(float(v))


def _fn_upper(v: Any, params: dict) -> Any:
    return str(v).upper()


def _fn_lower(v: Any, params: dict) -> Any:
    return str(v).lower()


def _fn_title(v: Any, params: dict) -> Any:
    return str(v).title()


def _fn_trim(v: Any, params: dict) -> Any:
    return str(v).strip()


def _fn_multiply(v: Any, params: dict) -> Any:
    by = params.get("by")
    if by is None:
        raise IRExecutionError("TRANSFORM.multiply requires params.by")
    return float(v) * float(by)


def _fn_add(v: Any, params: dict) -> Any:
    by = params.get("by")
    if by is None:
        raise IRExecutionError("TRANSFORM.add requires params.by")
    return float(v) + float(by)


def _fn_subtract(v: Any, params: dict) -> Any:
    by = params.get("by")
    if by is None:
        raise IRExecutionError("TRANSFORM.subtract requires params.by")
    return float(v) - float(by)


def _fn_divide(v: Any, params: dict) -> Any:
    by = params.get("by")
    if by is None:
        raise IRExecutionError("TRANSFORM.divide requires params.by")
    divisor = float(by)
    if divisor == 0:
        raise ZeroDivisionError("TRANSFORM.divide: division by zero")
    return float(v) / divisor


_TRANSFORM_WHITELIST: dict[str, Any] = {
    "round": _fn_round,
    "floor": _fn_floor,
    "ceil": _fn_ceil,
    "abs": _fn_abs,
    "upper": _fn_upper,
    "lower": _fn_lower,
    "title": _fn_title,
    "trim": _fn_trim,
    "multiply": _fn_multiply,
    "add": _fn_add,
    "subtract": _fn_subtract,
    "divide": _fn_divide,
}
