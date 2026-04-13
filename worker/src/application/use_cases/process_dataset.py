"""Process Dataset use case - orchestrates ETL pipeline with HITL support."""

import asyncio
import os
from contextlib import suppress
from dataclasses import dataclass, field
from io import BytesIO
from typing import Any
from uuid import UUID, uuid4

import httpx

import re
import polars as pl
import structlog

from src.application.ir import IRExecutionError, IRExecutor, IRResult
from src.application.transformations import (
    DataTransformer,
    TransformationType,
    TransformationConfig,
    TransformationResult,
)
from src.domain.entities.anomaly import AnomalyEntity
from src.domain.entities.decision import DecisionEntity
from src.domain.ports.repositories.job_repository import JobRepository
from src.domain.ports.services.ai_suggestion_service import AiSuggestionService
from src.domain.ports.services.storage_service import StorageService
from src.domain.value_objects.job_status import JobStatus
from src.infrastructure.parsers.dataset_parser import (
    DatasetParser,
    FileFormat,
)


logger = structlog.get_logger("pymes.worker.use_cases.process_dataset")

# ── Shared dtype/keyword constants (used by _detect_anomalies & _apply_decisions) ──
_NUMERIC_DTYPES = (
    pl.Int8, pl.Int16, pl.Int32, pl.Int64,
    pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
    pl.Float32, pl.Float64,
)
_STRING_DTYPES = (pl.Utf8, pl.String, pl.Categorical)
_DATE_DTYPES = (pl.Date, pl.Datetime)
_EMAIL_KW = ("email", "correo", "mail", "e-mail", "e_mail")
_PHONE_KW = ("phone", "tel", "telefono", "fono", "celular", "movil", "móvil")
_ID_KW = ("id", "codigo", "código", "code", "uuid", "clave", "key")
_NAME_KW = ("nombre", "name", "apellido", "surname", "fullname", "first_name", "last_name")
_ADDRESS_KW = ("direccion", "dirección", "address", "domicilio", "calle", "street")
_PLACEHOLDERS = frozenset({
    "n/a", "na", "null", "none", "undefined",
    "pendiente", "tbd", "todo", "s/d", "sin dato", "sin datos",
    "desconocido", "unknown", "0000", "xxxxxx",
    "campo en blanco", "véase otra columna", "ver otra columna",
    "no aplica", "no disponible",
    "xxx", "xx", "x", "-", "--", "---",
})
_DATE_MIN_YEAR = 1900
_DATE_MAX_YEAR = 2030

# How long to wait between polling DB for decisions
_HITL_POLL_INTERVAL_SECONDS = 5
# Maximum wait time (safety valve): 30 minutes
_HITL_MAX_WAIT_SECONDS = 1800


@dataclass
class ProcessDatasetInput:
    """Input for ProcessDataset use case."""

    dataset_id: UUID
    job_id: UUID
    source_key: str  # S3/MinIO key for input file
    filename: str  # Original filename (for format detection)
    transformations: list[dict[str, Any]]  # List of transformation configs
    output_format: str = "parquet"  # Output format


@dataclass
class ProcessDatasetOutput:
    """Output from ProcessDataset use case."""

    success: bool
    dataset_id: UUID
    job_id: UUID
    output_key: str | None = None
    rows_processed: int = 0
    columns_count: int = 0
    transformation_results: list[dict[str, Any]] | None = None
    error: str | None = None
    preview: list[dict[str, Any]] | None = None
    schema: dict[str, str] | None = None
    anomalies_detected: int = 0
    decisions_applied: int = 0


class ProcessDatasetUseCase:
    """Orchestrates the ETL pipeline for dataset processing.

    Full HITL flow:
    1. Update job status → PROCESSING
    2. Download raw file from storage
    3. Parse into DataFrame
    4. Apply transformations
    5. Detect anomalies
    6. If anomalies found:
       a. Save anomalies to DB
       b. Update job status → AWAITING_REVIEW
       c. Poll until all anomalies have decisions
       d. Read and apply decisions (DISCARDED → drop row, CORRECTED → apply value)
    7. Upload processed result to storage
    8. Update job status → COMPLETED
    9. On any error → FAILED
    """

    def __init__(
        self,
        storage: StorageService,
        parser: DatasetParser | None = None,
        transformer: DataTransformer | None = None,
        output_bucket: str = "processed-datasets",
        job_repository: JobRepository | None = None,
        hitl_poll_interval: float = _HITL_POLL_INTERVAL_SECONDS,
        hitl_max_wait: float = _HITL_MAX_WAIT_SECONDS,
        ai_suggestion_service: AiSuggestionService | None = None,
    ) -> None:
        """Initialize the use case.

        Args:
            storage: Storage service for file operations.
            parser: Dataset parser (creates default if None).
            transformer: Data transformer (creates default if None).
            output_bucket: Bucket for processed output files.
            job_repository: Optional persistence port for DB operations.
                            If None, HITL and status updates are skipped.
            hitl_poll_interval: Seconds between polling for decisions.
            hitl_max_wait: Maximum seconds to wait for human decisions.
            ai_suggestion_service: Optional AI suggestion service. When
                                   provided, suggestions are generated directly
                                   from the worker (Opción C). When ``None``
                                   the legacy n8n webhook path is used as
                                   fallback (if configured).
        """
        self._storage = storage
        self._parser = parser or DatasetParser()
        self._transformer = transformer or DataTransformer()
        self._output_bucket = output_bucket
        self._job_repo = job_repository
        self._hitl_poll_interval = hitl_poll_interval
        self._hitl_max_wait = hitl_max_wait
        self._ai_suggestion_service = ai_suggestion_service

    async def execute(self, input_data: ProcessDatasetInput) -> ProcessDatasetOutput:
        """Execute the dataset processing pipeline.

        Args:
            input_data: Processing input parameters.

        Returns:
            Processing output with results.
        """
        log = logger.bind(
            dataset_id=str(input_data.dataset_id),
            job_id=str(input_data.job_id),
            source_key=input_data.source_key,
        )
        log.info("Starting dataset processing")

        job_id_str = str(input_data.job_id)
        dataset_id_str = str(input_data.dataset_id)

        try:
            # ---------------------------------------------------------------
            # Step 1: Mark job as PROCESSING
            # ---------------------------------------------------------------
            await self._update_status(job_id_str, JobStatus.PROCESSING)

            # ---------------------------------------------------------------
            # Step 2: Download file from storage
            # ---------------------------------------------------------------
            log.info("Downloading source file")
            source_bucket, source_key = self._parse_storage_path(input_data.source_key)
            file_data = await self._storage.download_file(source_bucket, source_key)

            # ---------------------------------------------------------------
            # Step 3: Parse file into DataFrame
            # ---------------------------------------------------------------
            log.info("Parsing file", filename=input_data.filename)
            df = self._parser.parse(file_data, input_data.filename)

            rows_initial = df.height
            log.info("File parsed", rows=rows_initial, columns=df.width)

            # ---------------------------------------------------------------
            # Step 4: Apply transformations
            # ---------------------------------------------------------------
            transformation_results = []

            if input_data.transformations:
                log.info("Applying transformations", count=len(input_data.transformations))

                configs = self._build_transformation_configs(input_data.transformations)
                df, results = self._transformer.transform_many(df, configs)

                transformation_results = [
                    {
                        "type": r.transformation.value,
                        "success": r.success,
                        "rows_before": r.rows_before,
                        "rows_after": r.rows_after,
                        "columns_affected": r.columns_affected,
                        "error": r.error,
                        "details": r.details,
                    }
                    for r in results
                ]

                failures = [r for r in results if not r.success]
                if failures:
                    error_msg = f"Transformation failed: {failures[0].error}"
                    log.error(error_msg)
                    await self._update_status(
                        job_id_str, JobStatus.FAILED, error=error_msg
                    )
                    return ProcessDatasetOutput(
                        success=False,
                        dataset_id=input_data.dataset_id,
                        job_id=input_data.job_id,
                        transformation_results=transformation_results,
                        error=error_msg,
                    )

            # ---------------------------------------------------------------
            # Step 5: Detect anomalies or load existing
            # ---------------------------------------------------------------
            anomalies = []
            if self._job_repo is not None:
                anomalies = await self._job_repo.get_anomalies(dataset_id_str)
            
            is_new_anomalies = False
            if not anomalies:
                anomalies = self._detect_anomalies(df, dataset_id_str)
                is_new_anomalies = bool(anomalies)

            anomalies_detected = len(anomalies)
            decisions_applied = 0

            # ---------------------------------------------------------------
            # Step 6: HITL flow (only if repository available + anomalies)
            # ---------------------------------------------------------------
            if anomalies and self._job_repo is not None:
                log.info("Anomalies detected — entering HITL flow", count=anomalies_detected)

                # 6a. Save anomalies to DB only if they are newly detected
                if is_new_anomalies:
                    await self._job_repo.save_anomalies(dataset_id_str, anomalies)

                # 6b. Update job → AWAITING_REVIEW
                await self._update_status(job_id_str, JobStatus.AWAITING_REVIEW)

                # 6b+. Generate AI suggestions (fire-and-forget)
                # Opción C: call Gemini directly from the worker when the service
                # is configured; fall back to the legacy n8n webhook otherwise.
                if self._ai_suggestion_service is not None:
                    asyncio.create_task(
                        self._generate_ai_suggestions(anomalies, df, job_id_str)
                    )
                else:
                    asyncio.create_task(
                        self._notify_n8n_for_suggestions(dataset_id_str, anomalies, df)
                    )

                # 6c. Poll until all decisions are in
                df, decisions_applied = await self._wait_for_decisions_and_apply(
                    df=df,
                    dataset_id_str=dataset_id_str,
                    job_id_str=job_id_str,
                    anomalies=anomalies,
                    log=log,
                )

                # Back to PROCESSING after decisions applied
                await self._update_status(job_id_str, JobStatus.PROCESSING)

            # ---------------------------------------------------------------
            # Step 7: Serialize and upload result
            # ---------------------------------------------------------------
            output_format = self._get_output_format(input_data.output_format)
            output_data = self._parser.to_bytes(df, output_format)

            output_key = self._generate_output_key(
                input_data.dataset_id,
                input_data.job_id,
                output_format,
            )

            log.info("Uploading processed file", output_key=output_key)
            await self._storage.upload_file(
                bucket=self._output_bucket,
                key=output_key,
                data=BytesIO(output_data),
                content_type=self._get_content_type(output_format),
            )

            # Step 8: Generate preview and schema
            preview = self._parser.preview(df, n=10)
            schema = self._parser.get_schema(df)

            result_meta = {
                "output_key": output_key,
                "rows_processed": df.height,
                "columns_count": df.width,
                "anomalies_detected": anomalies_detected,
                "decisions_applied": decisions_applied,
            }

            # Step 9: Update to COMPLETED
            await self._update_status(
                job_id_str,
                JobStatus.COMPLETED,
                result=result_meta,
            )

            log.info(
                "Processing complete",
                rows_processed=df.height,
                columns=df.width,
                output_key=output_key,
                anomalies=anomalies_detected,
            )

            return ProcessDatasetOutput(
                success=True,
                dataset_id=input_data.dataset_id,
                job_id=input_data.job_id,
                output_key=output_key,
                rows_processed=df.height,
                columns_count=df.width,
                transformation_results=transformation_results,
                preview=preview,
                schema=schema,
                anomalies_detected=anomalies_detected,
                decisions_applied=decisions_applied,
            )

        except Exception as e:
            log.error("Processing failed", error=str(e), error_type=type(e).__name__)
            await self._update_status(job_id_str, JobStatus.FAILED, error=str(e))
            return ProcessDatasetOutput(
                success=False,
                dataset_id=input_data.dataset_id,
                job_id=input_data.job_id,
                error=str(e),
            )

    # =========================================================================
    # Private: HITL helpers
    # =========================================================================

    async def _wait_for_decisions_and_apply(
        self,
        df: pl.DataFrame,
        dataset_id_str: str,
        job_id_str: str,
        anomalies: list[AnomalyEntity],
        log: Any,
    ) -> tuple[pl.DataFrame, int]:
        """Poll DB until all anomalies have decisions, then apply them.

        Returns:
            Tuple of (updated DataFrame, number of decisions applied).
        """
        assert self._job_repo is not None

        elapsed = 0.0
        while elapsed < self._hitl_max_wait:
            pending = await self._job_repo.count_pending_anomalies(dataset_id_str)
            if pending == 0:
                break
            log.info("Waiting for human decisions", pending=pending, elapsed_s=elapsed)
            await asyncio.sleep(self._hitl_poll_interval)
            elapsed += self._hitl_poll_interval
        else:
            log.warning(
                "HITL wait timeout — proceeding without all decisions",
                max_wait=self._hitl_max_wait,
            )

        decisions = await self._job_repo.get_decisions(dataset_id_str)
        df = self._apply_decisions(df, anomalies, decisions)
        return df, len(decisions)

    def _detect_anomalies(
        self,
        df: pl.DataFrame,
        dataset_id: str,
    ) -> list[AnomalyEntity]:
        """Detect anomalies in the transformed DataFrame.

        Heuristics applied (one anomaly record per affected column):
        - MISSING_VALUE : any null in any column
        - OUTLIER       : numeric values beyond 3 standard deviations from the mean
        - DUPLICATE     : repeated values in high-uniqueness columns (ID, email, phone…)
        - FORMAT_INVALID: values that violate expected format patterns (email, phone)
        - INCONSISTENT  : values outside the dominant category set of low-cardinality columns
        - DATE_OUTLIER  : dates with years outside a reasonable human range (1900–2030)
        """
        anomalies: list[AnomalyEntity] = []

        _numeric_fill_dtypes = _NUMERIC_DTYPES
        _string_fill_dtypes = _STRING_DTYPES
        _email_kw = _EMAIL_KW
        _phone_kw = _PHONE_KW
        _id_kw = _ID_KW
        _name_kw = _NAME_KW

        # ── 1. MISSING VALUES ─────────────────────────────────────────────────
        for col in df.columns:
            null_count = df[col].is_null().sum()
            if null_count == 0:
                continue

            non_null = df[col].drop_nulls()
            dtype = df[col].dtype

            if dtype in _numeric_fill_dtypes:
                fill_value = "0" if non_null.len() == 0 else str(non_null.cast(pl.Float64).mean())
            elif dtype in _string_fill_dtypes or dtype == pl.Boolean:
                if non_null.len() == 0:
                    fill_value = ""
                else:
                    counts = non_null.value_counts(sort=True)
                    fill_value = str(counts[col][0])
            else:
                fill_value = "null"

            # Override fill_value to None for columns where auto-fill is inappropriate
            if any(kw in col.lower() for kw in _PHONE_KW):
                fill_value = None
            elif any(kw in col.lower() for kw in _ADDRESS_KW):
                fill_value = None

            anomalies.append(
                AnomalyEntity.create(
                    id=str(uuid4()),
                    dataset_id=dataset_id,
                    column=col,
                    row=int(str(null_count)),
                    anomaly_type="MISSING_VALUE",
                    description=f"Null values in column '{col}'",
                    original_value=None,
                    suggested_value=fill_value,
                )
            )

        # ── 2. NUMERIC OUTLIERS (Z-score > 3) ────────────────────────────────
        for col in df.columns:
            if df[col].dtype not in _NUMERIC_DTYPES:
                continue
            series = df[col].cast(pl.Float64).drop_nulls()
            if series.len() < 4:
                continue
            mean = series.mean()
            std = series.std()
            if std is None or std == 0:
                continue

            mean_f = float(str(mean))  # type: ignore[arg-type]
            std_f = float(str(std))    # type: ignore[arg-type]
            outliers_mask = ((df[col].cast(pl.Float64) - mean_f).abs() / std_f) > 3.0
            outliers_count = int(str(outliers_mask.sum()))

            if outliers_count > 0:
                anomalies.append(
                    AnomalyEntity.create(
                        id=str(uuid4()),
                        dataset_id=dataset_id,
                        column=col,
                        row=outliers_count,
                        anomaly_type="OUTLIER",
                        description=f"Outliers detected in column '{col}'",
                        original_value=None,
                        suggested_value=str(mean),
                    )
                )

        # ── 3. DUPLICATES ─────────────────────────────────────────────────────
        # Flag columns where duplicates are unexpected:
        #   a) Keyword-based: column name suggests a unique identifier (ID, email, phone…)
        #   b) Generic: String columns where ≥98% of values are unique AND at least
        #      one value appears more than twice (to avoid flagging natural coincidences)
        # Date/Datetime columns are excluded — duplicate birthdays/dates are normal.

        for col in df.columns:
            # Skip date columns — birthday/date duplicates are statistically expected
            if df[col].dtype in (pl.Date, pl.Datetime):
                continue

            non_null = df[col].drop_nulls()
            if non_null.len() < 4:
                continue

            col_lower = col.lower()
            is_id_like = (
                any(kw in col_lower for kw in _id_kw)
                or any(kw in col_lower for kw in _email_kw)
                or any(kw in col_lower for kw in _phone_kw)
            )
            _is_dup_email = any(kw in col_lower for kw in _email_kw)
            _is_dup_phone = any(kw in col_lower for kw in _phone_kw)

            # Normalizar valores para comparación de duplicados:
            # email → lowercase+strip; phone → solo dígitos
            if _is_dup_email:
                comparison_series = non_null.str.strip_chars().str.to_lowercase()
            elif _is_dup_phone:
                comparison_series = non_null.str.replace_all(r"[\s\-\(\)\+\.]", "")
            else:
                comparison_series = non_null

            unique_ratio = comparison_series.n_unique() / comparison_series.len()

            # For ID/email/phone columns use a lower threshold (0.80);
            # for generic columns require near-perfect uniqueness (0.98) and
            # at least one value appearing 3+ times to avoid flagging common names.
            if is_id_like:
                if unique_ratio < 0.80:
                    continue
            else:
                if unique_ratio < 0.98:
                    continue

            vc = comparison_series.alias(col).value_counts(sort=True)
            dups = vc.filter(pl.col("count") > 1)
            if len(dups) == 0:
                continue

            # Extra occurrences beyond the first legitimate one
            dup_instance_count = int(str((dups["count"] - 1).sum()))
            example = str(dups[col][0])

            anomalies.append(
                AnomalyEntity.create(
                    id=str(uuid4()),
                    dataset_id=dataset_id,
                    column=col,
                    row=dup_instance_count,
                    anomaly_type="DUPLICATE",
                    description=(
                        f"Duplicate values in column '{col}': "
                        f"{dup_instance_count} extra instance(s) across "
                        f"{len(dups)} repeated value(s). Example: '{example}'"
                    ),
                    original_value=example,
                    suggested_value="DUPLICADO",
                )
            )

        # ── 4. FORMAT_INVALID (email & phone columns) ─────────────────────────
        for col in df.columns:
            if df[col].dtype not in _string_fill_dtypes:
                continue
            col_lower = col.lower()
            non_null = df[col].drop_nulls()
            if non_null.len() == 0:
                continue

            invalid_mask: pl.Series | None = None
            format_hint = ""

            if any(kw in col_lower for kw in _email_kw):
                # Valid email: something@something.something
                valid = non_null.str.contains(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
                invalid_mask = ~valid
                format_hint = "email address"

            elif any(kw in col_lower for kw in _phone_kw):
                # Strip common separators; result must be 7–13 digits,
                # not all-zeros, and area code must not be "00"
                cleaned = non_null.str.replace_all(r"[\s\-\(\)\+\.]", "")
                valid_length = cleaned.str.contains(r"^\d{7,13}$")
                not_all_zeros = ~cleaned.str.contains(r"^0+$")
                not_area_00 = ~non_null.str.strip_chars().str.contains(r"^\(?00\)?[\s\-]")
                valid = valid_length & not_all_zeros & not_area_00
                invalid_mask = ~valid
                format_hint = "phone number"

            if invalid_mask is None:
                continue

            invalid_count = int(str(invalid_mask.sum()))
            if invalid_count == 0:
                continue

            example = str(non_null.filter(invalid_mask)[0])
            if format_hint == "email address":
                _fmt_suggested = "EMAIL_INVALIDO"
            elif format_hint == "phone number":
                _fmt_suggested = "TEL_INVALIDO"
            else:
                _fmt_suggested = "FORMAT_INVALIDO"
            anomalies.append(
                AnomalyEntity.create(
                    id=str(uuid4()),
                    dataset_id=dataset_id,
                    column=col,
                    row=invalid_count,
                    anomaly_type="FORMAT_INVALID",
                    description=(
                        f"Invalid {format_hint} format in column '{col}': "
                        f"{invalid_count} value(s) do not match the expected pattern. "
                        f"Example: '{example}'"
                    ),
                    original_value=example,
                    suggested_value=_fmt_suggested,
                )
            )

        # ── 5. INCONSISTENT VALUES (categorical columns) ─────────────────────
        # For low-cardinality string columns (<=15 unique values, <10% unique ratio),
        # flag values that appear rarely compared to the dominant set.
        for col in df.columns:
            if df[col].dtype not in _string_fill_dtypes:
                continue
            non_null = df[col].drop_nulls()
            total = non_null.len()
            if total < 10:
                continue
            n_unique = non_null.n_unique()
            if n_unique > 15 or (n_unique / total) >= 0.10:
                continue  # Not a low-cardinality column

            vc = non_null.value_counts(sort=True)
            # "Common" = appears in at least 1% of rows or at least 2 times
            threshold = max(2, int(total * 0.01))
            rare = vc.filter(pl.col("count") < threshold)
            if len(rare) == 0:
                continue

            rare_count = int(str(rare["count"].sum()))
            common_values = vc.filter(pl.col("count") >= threshold)[col].to_list()
            example_rare = str(rare[col][0])

            anomalies.append(
                AnomalyEntity.create(
                    id=str(uuid4()),
                    dataset_id=dataset_id,
                    column=col,
                    row=rare_count,
                    anomaly_type="INCONSISTENT",
                    description=(
                        f"Unexpected values in categorical column '{col}': "
                        f"{rare_count} value(s) fall outside the expected set "
                        f"{common_values[:5]}. Example: '{example_rare}'"
                    ),
                    original_value=example_rare,
                    suggested_value=str(common_values[0]) if common_values else None,
                )
            )

        # ── 6. DATE OUTLIERS (years outside a reasonable human range) ─────────
        for col in df.columns:
            if df[col].dtype not in _DATE_DTYPES:
                continue
            non_null = df[col].drop_nulls()
            if non_null.len() == 0:
                continue

            years = non_null.dt.year()
            out_of_range = (years < _DATE_MIN_YEAR) | (years > _DATE_MAX_YEAR)
            bad_count = int(str(out_of_range.sum()))
            if bad_count == 0:
                continue

            example_date = str(non_null.filter(out_of_range)[0])
            anomalies.append(
                AnomalyEntity.create(
                    id=str(uuid4()),
                    dataset_id=dataset_id,
                    column=col,
                    row=bad_count,
                    anomaly_type="OUTLIER",
                    description=(
                        f"Impossible dates in column '{col}': "
                        f"{bad_count} value(s) have years outside the range "
                        f"{_DATE_MIN_YEAR}–{_DATE_MAX_YEAR}. "
                        f"Example: '{example_date}'"
                    ),
                    original_value=example_date,
                    suggested_value=None,
                )
            )

        # ── 7. WHITESPACE_ONLY ────────────────────────────────────────────────
        try:
            for col in df.columns:
                if df[col].dtype not in _string_fill_dtypes:
                    continue
                non_null = df[col].drop_nulls()
                if non_null.len() == 0:
                    continue
                whitespace_mask = non_null.str.strip_chars().str.len_chars() == 0
                ws_count = int(str(whitespace_mask.sum()))
                if ws_count == 0:
                    continue
                example = str(non_null.filter(whitespace_mask)[0])
                anomalies.append(
                    AnomalyEntity.create(
                        id=str(uuid4()),
                        dataset_id=dataset_id,
                        column=col,
                        row=ws_count,
                        anomaly_type="WHITESPACE_ONLY",
                        description=(
                            f"La columna '{col}' tiene {ws_count} celda(s) con solo "
                            f"espacios/tabuladores/saltos de línea. Parecen llenas pero "
                            f"están vacías funcionalmente."
                        ),
                        original_value=repr(example),
                        suggested_value=None,
                    )
                )
        except Exception:
            pass

        # ── 8. CROSS_FIELD_SWAP ───────────────────────────────────────────────
        try:
            _date_pattern_re = re.compile(
                r"^\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}$"
                r"|^\d{4}[\/\-]\d{1,2}[\/\-]\d{1,2}$"
            )
            _phone_digit_re = re.compile(r"^[\+\s\-\(\)]*\d[\d\s\-\.\(\)]{6,19}$")

            for col in df.columns:
                if df[col].dtype not in _string_fill_dtypes:
                    continue
                col_lower = col.lower()
                non_null = df[col].drop_nulls()
                if non_null.len() == 0:
                    continue

                swap_type: str | None = None
                swap_count = 0
                example_val: str | None = None

                is_name_col = any(kw in col_lower for kw in _name_kw)
                is_phone_col = any(kw in col_lower for kw in _phone_kw)

                if is_name_col:
                    email_mask = non_null.str.contains(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
                    email_count = int(str(email_mask.sum()))
                    if email_count > 0:
                        swap_type = "EMAIL_EN_NOMBRE"
                        swap_count = email_count
                        example_val = str(non_null.filter(email_mask)[0])

                    if swap_type is None:
                        sample = non_null.head(500).to_list()
                        date_hits = [v for v in sample if v and _date_pattern_re.match(str(v))]
                        if date_hits:
                            swap_type = "FECHA_EN_NOMBRE"
                            swap_count = len(date_hits)
                            example_val = date_hits[0]

                if swap_type is None and not is_phone_col:
                    sample = non_null.head(500).to_list()
                    phone_hits = [
                        v for v in sample
                        if v and _phone_digit_re.match(str(v).strip())
                        and len(re.sub(r"\D", "", str(v))) >= 7
                    ]
                    if phone_hits and len(phone_hits) / max(len(sample), 1) > 0.1:
                        swap_type = "TELEFONO_EN_COLUMNA_INCORRECTA"
                        swap_count = len(phone_hits)
                        example_val = phone_hits[0]

                # Swap: phone number placed in an email column
                is_email_col = any(kw in col_lower for kw in _EMAIL_KW)
                if swap_type is None and is_email_col:
                    _phone_re_swap = re.compile(r"^\+?[\d\s\-\(\)]{7,20}$")
                    phone_hits_email = [
                        v for v in non_null.head(500).to_list()
                        if v and _phone_re_swap.match(str(v).strip()) and "@" not in str(v)
                    ]
                    if phone_hits_email:
                        swap_type = "TELEFONO_EN_EMAIL"
                        swap_count = len(phone_hits_email)
                        example_val = str(phone_hits_email[0])

                # Swap: email or other non-address data placed in an address column
                is_address_col = any(kw in col_lower for kw in _ADDRESS_KW)
                if swap_type is None and is_address_col:
                    email_in_addr = [
                        v for v in non_null.head(500).to_list()
                        if v and re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", str(v))
                    ]
                    if email_in_addr:
                        swap_type = "EMAIL_EN_DIRECCION"
                        swap_count = len(email_in_addr)
                        example_val = str(email_in_addr[0])

                # Swap: ID pattern (e.g. C0999) placed in address column
                if swap_type is None and is_address_col:
                    _id_in_addr_re = re.compile(r"^[A-Z]\d{4}$")
                    id_in_addr = [
                        v for v in non_null.head(500).to_list()
                        if v and _id_in_addr_re.match(str(v).strip())
                    ]
                    if id_in_addr:
                        swap_type = "ID_EN_DIRECCION"
                        swap_count = len(id_in_addr)
                        example_val = str(id_in_addr[0])

                # Swap: locality pattern (e.g. "Billings, MT 59101") in name column
                if swap_type is None and is_name_col:
                    _locality_in_name_re = re.compile(r"^[A-Za-z\s]+,\s*[A-Z]{2}\s+\d{5}$")
                    locality_in_name = [
                        v for v in non_null.head(500).to_list()
                        if v and _locality_in_name_re.match(str(v).strip())
                    ]
                    if locality_in_name:
                        swap_type = "LOCALIDAD_EN_NOMBRE"
                        swap_count = len(locality_in_name)
                        example_val = str(locality_in_name[0])

                if swap_type is None:
                    continue

                anomalies.append(
                    AnomalyEntity.create(
                        id=str(uuid4()),
                        dataset_id=dataset_id,
                        column=col,
                        row=swap_count,
                        anomaly_type="CROSS_FIELD_SWAP",
                        description=(
                            f"La columna '{col}' parece contener datos del tipo incorrecto "
                            f"({swap_type}): {swap_count} valor(es) parecen pertenecer a otro campo. "
                            f"Ejemplo: '{example_val}'"
                        ),
                        original_value=example_val,
                        suggested_value="VALOR_EN_CAMPO_INCORRECTO",
                    )
                )
        except Exception:
            pass

        # ── 9. SUSPICIOUS_PLACEHOLDER ─────────────────────────────────────────
        try:
            for col in df.columns:
                if df[col].dtype not in _string_fill_dtypes:
                    continue
                non_null = df[col].drop_nulls()
                if non_null.len() == 0:
                    continue
                normalized = non_null.str.strip_chars().str.to_lowercase()
                placeholder_mask = normalized.is_in(list(_PLACEHOLDERS))
                ph_count = int(str(placeholder_mask.sum()))
                if ph_count == 0:
                    continue
                example = str(non_null.filter(placeholder_mask)[0])
                anomalies.append(
                    AnomalyEntity.create(
                        id=str(uuid4()),
                        dataset_id=dataset_id,
                        column=col,
                        row=ph_count,
                        anomaly_type="SUSPICIOUS_PLACEHOLDER",
                        description=(
                            f"La columna '{col}' tiene {ph_count} valor(es) que son "
                            f"marcadores de posición o sustitutos vacíos (ej: 'N/A', 'pendiente', "
                            f"'desconocido'). Ejemplo: '{example}'"
                        ),
                        original_value=example,
                        suggested_value="PENDIENTE_COMPLETAR",
                    )
                )
        except Exception:
            pass

        # ── 10. LEADING_TRAILING_WHITESPACE ───────────────────────────────────
        try:
            for col in df.columns:
                if df[col].dtype not in _string_fill_dtypes:
                    continue
                non_null = df[col].drop_nulls()
                if non_null.len() == 0:
                    continue
                stripped = non_null.str.strip_chars()
                has_content = stripped.str.len_chars() > 0
                has_padding = non_null != stripped
                padding_mask = has_content & has_padding
                pad_count = int(str(padding_mask.sum()))
                if pad_count == 0:
                    continue
                example_raw = str(non_null.filter(padding_mask)[0])
                example_fixed = str(stripped.filter(padding_mask)[0])
                anomalies.append(
                    AnomalyEntity.create(
                        id=str(uuid4()),
                        dataset_id=dataset_id,
                        column=col,
                        row=pad_count,
                        anomaly_type="LEADING_TRAILING_WHITESPACE",
                        description=(
                            f"La columna '{col}' tiene {pad_count} valor(es) con espacios "
                            f"al inicio o al final, lo que puede romper joins y búsquedas. "
                            f"Ejemplo: {repr(example_raw)}"
                        ),
                        original_value=example_raw,
                        suggested_value=example_fixed,
                    )
                )
        except Exception:
            pass

        # ── 11. DATE_LOGICAL ──────────────────────────────────────────────────
        try:
            _START_KW = ("nacimiento", "birth", "inicio", "start", "apertura", "desde", "from")
            # "alta" is always an end/registration date (posterior to birth), so it lives only in _END_KW
            _END_KW   = ("vencimiento", "expiry", "expiracion", "fin", "end", "alta", "baja", "cierre", "hasta")

            def _col_tokens(col_name: str):
                """Split column name into lowercase word tokens to avoid substring false matches."""
                return set(re.split(r"[\s_\-]+", col_name.lower()))

            date_cols = [c for c in df.columns if df[c].dtype in _DATE_DTYPES]

            if len(date_cols) >= 2:
                start_cols = [c for c in date_cols if any(kw in _col_tokens(c) for kw in _START_KW)]
                end_cols   = [c for c in date_cols if any(kw in _col_tokens(c) for kw in _END_KW)]

                if not start_cols or not end_cols:
                    # Without clear keyword signals, skip to avoid false positives
                    start_cols = []
                    end_cols   = []

                for sc in start_cols:
                    for ec in end_cols:
                        if sc == ec:
                            continue
                        both_non_null = df[sc].is_not_null() & df[ec].is_not_null()
                        sub = df.filter(both_non_null)
                        if sub.height == 0:
                            continue
                        sc_date = sub[sc].cast(pl.Date)
                        ec_date = sub[ec].cast(pl.Date)
                        inversion_mask = sc_date > ec_date
                        inv_count = int(str(inversion_mask.sum()))
                        if inv_count == 0:
                            continue
                        example_start = str(sub[sc].filter(inversion_mask)[0])
                        example_end   = str(sub[ec].filter(inversion_mask)[0])
                        anomalies.append(
                            AnomalyEntity.create(
                                id=str(uuid4()),
                                dataset_id=dataset_id,
                                column=sc,
                                row=inv_count,
                                anomaly_type="DATE_LOGICAL",
                                description=(
                                    f"Inconsistencia lógica de fechas: '{sc}' es posterior a '{ec}' "
                                    f"en {inv_count} fila(s). Se esperaba que '{sc}' fuera anterior. "
                                    f"Ejemplo: {sc}={example_start}, {ec}={example_end}"
                                ),
                                original_value=f"{sc}={example_start} | {ec}={example_end}",
                                suggested_value=None,
                            )
                        )
        except Exception:
            pass

        # ── 12. NUMERIC_ROUND_NUMBER ──────────────────────────────────────────
        try:
            for col in df.columns:
                if df[col].dtype not in _numeric_fill_dtypes:
                    continue
                col_lower = col.lower()
                if any(kw in col_lower for kw in _id_kw):
                    continue
                series = df[col].cast(pl.Float64).drop_nulls()
                n = series.len()
                if n < 10:
                    continue
                mean_val = series.mean()
                std_val  = series.std()
                if mean_val is None or std_val is None or float(str(mean_val)) == 0:
                    continue
                mean_f = float(str(mean_val))
                std_f  = float(str(std_val))
                if std_f < abs(mean_f) * 0.1:
                    continue
                divisor = 1000.0 if abs(mean_f) >= 1000 else 100.0
                round_mask = (series % divisor) == 0.0
                round_count = int(str(round_mask.sum()))
                round_ratio = round_count / n
                if round_ratio <= 0.40:
                    continue
                example = str(series.filter(round_mask)[0])
                anomalies.append(
                    AnomalyEntity.create(
                        id=str(uuid4()),
                        dataset_id=dataset_id,
                        column=col,
                        row=round_count,
                        anomaly_type="NUMERIC_ROUND_NUMBER",
                        description=(
                            f"La columna '{col}' tiene {round_count} valor(es) ({round_ratio:.0%}) "
                            f"que son múltiplos exactos de {int(divisor)}, lo que sugiere "
                            f"estimaciones o datos ficticios. Ejemplo: '{example}'"
                        ),
                        original_value=example,
                        suggested_value=None,
                    )
                )
        except Exception:
            pass

        # ── 13. LOW_VARIANCE ──────────────────────────────────────────────────
        try:
            for col in df.columns:
                col_lower = col.lower()
                if any(kw in col_lower for kw in _id_kw):
                    continue

                dtype = df[col].dtype

                if dtype in _numeric_fill_dtypes:
                    series = df[col].cast(pl.Float64).drop_nulls()
                    if series.len() <= 10:
                        continue
                    mean_val = series.mean()
                    std_val  = series.std()
                    if mean_val is None or std_val is None:
                        continue
                    mean_f = float(str(mean_val))
                    std_f  = float(str(std_val))
                    if mean_f == 0:
                        continue
                    cv = std_f / abs(mean_f)
                    if cv >= 0.01:
                        continue
                    anomalies.append(
                        AnomalyEntity.create(
                            id=str(uuid4()),
                            dataset_id=dataset_id,
                            column=col,
                            row=series.len(),
                            anomaly_type="LOW_VARIANCE",
                            description=(
                                f"La columna numérica '{col}' tiene variación casi nula "
                                f"(coeficiente de variación={cv:.4f}), lo que puede indicar "
                                f"que no fue llenada correctamente. Media={mean_f:.4f}"
                            ),
                            original_value=str(round(mean_f, 4)),
                            suggested_value=None,
                        )
                    )

                elif dtype in _string_fill_dtypes:
                    non_null = df[col].drop_nulls()
                    if non_null.len() <= 10:
                        continue
                    total = non_null.len()
                    vc = non_null.value_counts(sort=True)
                    top_count = int(str(vc["count"][0]))
                    top_ratio = top_count / total
                    if top_ratio < 0.95:
                        continue
                    top_value = str(vc[col][0])
                    anomalies.append(
                        AnomalyEntity.create(
                            id=str(uuid4()),
                            dataset_id=dataset_id,
                            column=col,
                            row=total,
                            anomaly_type="LOW_VARIANCE",
                            description=(
                                f"La columna de texto '{col}' tiene el {top_ratio:.0%} de sus "
                                f"valores idénticos al valor '{top_value}', lo que sugiere que "
                                f"no fue llenada correctamente."
                            ),
                            # Store the dominant value so apply handler can use the
                            # original detection result even if the DataFrame was mutated.
                            original_value=top_value,
                            suggested_value="GRUPO_BAJO_VARIANZA",
                        )
                    )
        except Exception:
            pass

        # ── 14. OUTLIER_IQR ───────────────────────────────────────────────────
        try:
            cols_with_zscore_outlier = {
                a.column for a in anomalies if a.type == "OUTLIER"
            }

            for col in df.columns:
                if df[col].dtype not in _numeric_fill_dtypes:
                    continue
                if col in cols_with_zscore_outlier:
                    continue
                series = df[col].cast(pl.Float64).drop_nulls()
                if series.len() < 4:
                    continue
                q1 = series.quantile(0.25)
                q3 = series.quantile(0.75)
                if q1 is None or q3 is None:
                    continue
                q1_f = float(str(q1))
                q3_f = float(str(q3))
                iqr = q3_f - q1_f
                if iqr == 0:
                    continue
                lower = q1_f - 1.5 * iqr
                upper = q3_f + 1.5 * iqr
                outlier_mask = (series < lower) | (series > upper)
                outlier_count = int(str(outlier_mask.sum()))
                if outlier_count == 0:
                    continue
                median_val = series.median()
                median_str = str(median_val) if median_val is not None else "None"
                example = str(series.filter(outlier_mask)[0])
                anomalies.append(
                    AnomalyEntity.create(
                        id=str(uuid4()),
                        dataset_id=dataset_id,
                        column=col,
                        row=outlier_count,
                        anomaly_type="OUTLIER_IQR",
                        description=(
                            f"Outliers por IQR en columna '{col}': {outlier_count} valor(es) "
                            f"fuera del rango [{lower:.2f}, {upper:.2f}] "
                            f"(Q1={q1_f:.2f}, Q3={q3_f:.2f}, IQR={iqr:.2f}). "
                            f"Ejemplo: '{example}'"
                        ),
                        original_value=example,
                        suggested_value=median_str,
                    )
                )
        except Exception:
            pass

        # ── 15. SEQUENCE_GAP ──────────────────────────────────────────────────
        try:
            from collections import Counter as _Counter
            _seq_re = re.compile(r"^([A-Za-z\-_]*)(\d+)$")

            for col in df.columns:
                if df[col].dtype not in _string_fill_dtypes:
                    continue
                non_null = df[col].drop_nulls()
                n = non_null.len()
                if n < 4:
                    continue
                unique_ratio = non_null.n_unique() / n
                if unique_ratio < 0.95:
                    continue

                values = non_null.to_list()
                matches = [_seq_re.fullmatch(str(v)) for v in values]
                valid_matches = [m for m in matches if m is not None]
                if len(valid_matches) / n < 0.80:
                    continue

                prefix_counts = _Counter(m.group(1) for m in valid_matches)
                dominant_prefix, dominant_count = prefix_counts.most_common(1)[0]
                if dominant_count / n < 0.80:
                    continue

                dominant_nums = sorted(
                    int(m.group(2))
                    for m in valid_matches
                    if m.group(1) == dominant_prefix
                )
                if len(dominant_nums) < 2:
                    continue

                # Use IQR fence to define the canonical cluster range.
                # p5-p95 was too permissive for dense sequences (e.g. C0001–C0999
                # where C0500/C0750 fell inside the p5-p95 window).
                # IQR fence (Q1 - 1.5*IQR, Q3 + 1.5*IQR) correctly isolates outlier IDs.
                dominant_sorted = dominant_nums  # already sorted above
                _ds = dominant_sorted
                _n = len(_ds)
                _q1 = _ds[max(0, int(_n * 0.25))]
                _q3 = _ds[min(_n - 1, int(_n * 0.75))]
                _iqr = _q3 - _q1
                if _iqr > 0:
                    _fence_low = _q1 - 1.5 * _iqr
                    _fence_high = _q3 + 1.5 * _iqr
                    inliers = [n for n in _ds if _fence_low <= n <= _fence_high]
                    canonical_min = inliers[0] if inliers else _ds[0]
                    canonical_max = inliers[-1] if inliers else _ds[-1]
                else:
                    # IQR == 0: nearly all values identical — use p10-p90 as
                    # tighter fallback (full range would include outliers).
                    _p10_idx = max(0, int(_n * 0.10))
                    _p90_idx = min(_n - 1, int(_n * 0.90))
                    canonical_min = _ds[_p10_idx]
                    canonical_max = _ds[_p90_idx]

                out_of_range_nums = [
                    n for n in dominant_sorted
                    if n < canonical_min or n > canonical_max
                ]

                if out_of_range_nums:
                    zero_pad = len(str(dominant_sorted[0]))
                    out_of_range_example = (
                        f"{dominant_prefix}{str(out_of_range_nums[0]).zfill(zero_pad)}"
                    )
                    structured_original = (
                        f"{dominant_prefix}|{canonical_min}|{canonical_max}|{out_of_range_example}"
                    )
                    gap_count = len(out_of_range_nums)
                    first_gap_label = out_of_range_example
                else:
                    # No out-of-range values with percentile method; fall back to gap detection
                    min_num = dominant_sorted[0]
                    max_num = dominant_sorted[-1]
                    expected_set = set(range(min_num, max_num + 1))
                    actual_set = set(dominant_sorted)
                    gaps = sorted(expected_set - actual_set)
                    if not gaps:
                        continue
                    zero_pad = len(str(min_num))
                    out_of_range_example = f"{dominant_prefix}{str(gaps[0]).zfill(zero_pad)}"
                    structured_original = (
                        f"{dominant_prefix}|{min_num}|{max_num}|{out_of_range_example}"
                    )
                    gap_count = len(gaps)
                    first_gap_label = out_of_range_example
                    canonical_min = min_num
                    canonical_max = max_num

                anomalies.append(
                    AnomalyEntity.create(
                        id=str(uuid4()),
                        dataset_id=dataset_id,
                        column=col,
                        row=gap_count,
                        anomaly_type="SEQUENCE_GAP",
                        description=(
                            f"La columna '{col}' parece secuencial (prefijo '{dominant_prefix}') "
                            f"pero tiene {gap_count} hueco(s) entre "
                            f"{dominant_prefix}{canonical_min} "
                            f"y {dominant_prefix}{canonical_max}. "
                            f"Primer hueco/fuera-de-rango: '{first_gap_label}'"
                        ),
                        original_value=structured_original,
                        suggested_value="ID_FUERA_SECUENCIA",
                    )
                )
        except Exception:
            pass

        return anomalies

    def _apply_decisions(
        self,
        df: pl.DataFrame,
        anomalies: list[AnomalyEntity],
        decisions: list[DecisionEntity],
    ) -> pl.DataFrame:
        """Apply human decisions to the DataFrame.

        Anomalies are grouped by column (one anomaly per affected column), so
        decisions must be applied to ALL rows affected by that column anomaly:

        - APPROVED  → apply the anomaly's pre-calculated suggested value
        - CORRECTED → fill every affected cell in the column with the correction value
        - DISCARDED → reject the suggestion, keep original value (NO-OP)

        Business rule: the output dataset must ALWAYS preserve every row from the
        input.  Rows are NEVER deleted during anomaly correction.

        For MISSING_VALUE: affected rows = rows where the column is null.
        For OUTLIER:       affected rows = rows where |z-score| > 3.
        """
        if not decisions:
            return df

        decision_map: dict[str, DecisionEntity] = {d.anomaly_id: d for d in decisions}
        anomaly_map: dict[str, AnomalyEntity] = {a.id: a for a in anomalies}

        # Snapshot of the original DataFrame before any corrections are applied.
        # Used by the DUPLICATE handler so that prior column mutations don't affect
        # duplicate detection.
        original_df_snapshot = df.clone()

        result = df

        # ── Row-finder dispatcher (v8) ────────────────────────────────────────
        # Each function returns the list of row indices affected by its anomaly type.
        # All finders read from original_df_snapshot to prevent cross-handler interference.

        def _find_missing(sc: "pl.Series", col: str, anomaly: "AnomalyEntity", snap: "pl.DataFrame") -> "list[int]":
            return [i for i, is_null in enumerate(sc.is_null().to_list()) if is_null]

        def _find_outlier(sc: "pl.Series", col: str, anomaly: "AnomalyEntity", snap: "pl.DataFrame") -> "list[int]":
            rows: list[int] = []
            if sc.dtype in _NUMERIC_DTYPES:
                series = sc.cast(pl.Float64).drop_nulls()
                if series.len() < 4:
                    return []
                mean_v = float(series.mean())  # type: ignore[arg-type]
                std_v = float(series.std())    # type: ignore[arg-type]
                if std_v == 0:
                    return []
                z_scores = ((sc.cast(pl.Float64) - mean_v).abs() / std_v).to_list()
                rows = [i for i, z in enumerate(z_scores) if z is not None and z > 3.0]
            elif sc.dtype in _DATE_DTYPES:
                for i, v in enumerate(sc.to_list()):
                    if v is None:
                        continue
                    try:
                        if v.year < _DATE_MIN_YEAR or v.year > _DATE_MAX_YEAR:
                            rows.append(i)
                    except AttributeError:
                        pass
            return rows

        def _find_duplicate(sc: "pl.Series", col: str, anomaly: "AnomalyEntity", snap: "pl.DataFrame") -> "list[int]":
            snap_values = snap[col].to_list() if col in snap.columns else []
            _dup_col_lower = col.lower()
            _dup_is_email = any(kw in _dup_col_lower for kw in _EMAIL_KW)
            _dup_is_phone = any(kw in _dup_col_lower for kw in _PHONE_KW)
            val_count: dict = {}
            for sv in snap_values:
                if sv is not None:
                    if _dup_is_email:
                        k = str(sv).strip().lower()
                    elif _dup_is_phone:
                        k = re.sub(r"[\s\-\(\)\+\.]", "", str(sv))
                    else:
                        k = str(sv)
                    val_count[k] = val_count.get(k, 0) + 1
            dup_keys = {k for k, cnt in val_count.items() if cnt > 1}
            rows: list[int] = []
            if dup_keys:
                values = snap[col].to_list() if col in snap.columns else sc.to_list()
                seen: dict = {}
                for i, v in enumerate(values):
                    if v is None:
                        continue
                    raw = str(v).removesuffix("_DUP")
                    if _dup_is_email:
                        base = raw.strip().lower()
                    elif _dup_is_phone:
                        base = re.sub(r"[\s\-\(\)\+\.]", "", raw)
                    else:
                        base = raw
                    if base in dup_keys:
                        if base in seen:
                            rows.append(i)
                        else:
                            seen[base] = i
            return rows

        def _find_format_invalid(sc: "pl.Series", col: str, anomaly: "AnomalyEntity", snap: "pl.DataFrame") -> "list[int]":
            # BUG FIX v8: removed `if decision.is_approved: continue` — suggested_value
            # ("EMAIL_INVALIDO" / "TEL_INVALIDO") is applied via the normal is_approved path.
            rows: list[int] = []
            if sc.dtype in _STRING_DTYPES:
                col_lower = col.lower()
                for i, v in enumerate(sc.to_list()):
                    if v is None:
                        continue
                    if any(kw in col_lower for kw in _EMAIL_KW):
                        if not re.fullmatch(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", str(v)):
                            rows.append(i)
                    elif any(kw in col_lower for kw in _PHONE_KW):
                        cleaned = re.sub(r"[\s\-\(\)\+\.]", "", str(v))
                        raw = str(v).strip()
                        if (
                            not re.fullmatch(r"\d{7,13}", cleaned)
                            or bool(re.fullmatch(r"0+", cleaned))
                            or bool(re.match(r"^\(?00\)?[\s\-]", raw))
                        ):
                            rows.append(i)
            return rows

        def _find_inconsistent(sc: "pl.Series", col: str, anomaly: "AnomalyEntity", snap: "pl.DataFrame") -> "list[int]":
            if sc.dtype not in _STRING_DTYPES:
                return []
            non_null = sc.drop_nulls()
            total = non_null.len()
            if total < 10:
                return []
            vc = non_null.value_counts(sort=True)
            threshold = max(2, int(total * 0.01))
            rare_df = vc.filter(pl.col("count") < threshold)
            if len(rare_df) == 0:
                return []
            rare_values = set(rare_df[col].to_list())
            return [i for i, v in enumerate(sc.to_list()) if v is not None and v in rare_values]

        def _find_whitespace(sc: "pl.Series", col: str, anomaly: "AnomalyEntity", snap: "pl.DataFrame") -> "list[int]":
            if sc.dtype not in _STRING_DTYPES:
                return []
            return [
                i for i, v in enumerate(sc.to_list())
                if v is not None and str(v).strip() == "" and len(str(v)) > 0
            ]

        def _find_cross_swap(sc: "pl.Series", col: str, anomaly: "AnomalyEntity", snap: "pl.DataFrame") -> "list[int]":
            rows: list[int] = []
            if sc.dtype not in _STRING_DTYPES:
                return rows
            col_lower = col.lower()
            is_name_col    = any(kw in col_lower for kw in _NAME_KW)
            is_phone_col   = any(kw in col_lower for kw in _PHONE_KW)
            is_email_col   = any(kw in col_lower for kw in _EMAIL_KW)
            is_address_col = any(kw in col_lower for kw in _ADDRESS_KW)
            _date_pat      = re.compile(r"^\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}$|^\d{4}[\/\-]\d{1,2}[\/\-]\d{1,2}$")
            _phone_pat     = re.compile(r"^[\+\s\-\(\)]*\d[\d\s\-\.\(\)]{6,19}$")
            _phone_re_local = re.compile(r"^\+?[\d\s\-\(\)]{7,20}$")
            _addr_kw       = ["street", "ave", "blvd", "road", "st.", "nj", "mt", "dr."]
            for i, v in enumerate(sc.to_list()):
                if v is None:
                    continue
                s = str(v).strip()
                if is_name_col:
                    if re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", s):
                        rows.append(i); continue
                    if _date_pat.match(s):
                        rows.append(i); continue
                if is_email_col:
                    if _phone_re_local.match(s) and "@" not in s:
                        rows.append(i); continue
                    if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", s):
                        if any(kw in s.lower() for kw in _addr_kw):
                            rows.append(i); continue
                if is_address_col:
                    if re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", s):
                        rows.append(i); continue
                    if re.match(r"^[A-Z]\d{4}$", s):
                        rows.append(i); continue
                # BUG FIX v8: handle email/address value in phone column
                if is_phone_col:
                    if re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", s):
                        rows.append(i); continue
                    if any(kw in s.lower() for kw in _addr_kw):
                        rows.append(i); continue
                if is_name_col:
                    if re.match(r"^[A-Za-z\s]+,\s*[A-Z]{2}\s+\d{5}$", s):
                        rows.append(i); continue
                if not is_phone_col:
                    if _phone_pat.match(s) and len(re.sub(r"\D", "", s)) >= 7:
                        rows.append(i)
            return rows

        def _find_suspicious(sc: "pl.Series", col: str, anomaly: "AnomalyEntity", snap: "pl.DataFrame") -> "list[int]":
            snap_ph = snap[col].to_list() if col in snap.columns else []
            # BUG FIX v8: include numeric zero ("0", "0.0") as placeholder values
            return [
                i for i in range(len(snap_ph))
                if snap_ph[i] is not None
                and (
                    str(snap_ph[i]).strip().lower() in _PLACEHOLDERS
                    or str(snap_ph[i]).strip() in ("0", "0.0")
                )
            ]

        def _find_leading(sc: "pl.Series", col: str, anomaly: "AnomalyEntity", snap: "pl.DataFrame") -> "list[int]":
            if sc.dtype not in _STRING_DTYPES:
                return []
            return [
                i for i, v in enumerate(sc.to_list())
                if v is not None and str(v) != str(v).strip() and len(str(v).strip()) > 0
            ]

        def _find_date_logical(sc: "pl.Series", col: str, anomaly: "AnomalyEntity", snap: "pl.DataFrame") -> "list[int]":
            rows: list[int] = []
            m = re.search(r"es posterior a '([^']+)'", anomaly.description)
            if not m:
                return rows
            ec = m.group(1)
            if ec not in snap.columns:
                return rows
            _snap_ec = snap[ec]
            bn_list = (sc.is_not_null() & _snap_ec.is_not_null()).to_list()
            sc_vals = sc.to_list()
            ec_vals = _snap_ec.to_list()
            for i in range(len(sc_vals)):
                if bn_list[i] and sc_vals[i] is not None and ec_vals[i] is not None:
                    try:
                        if sc_vals[i] > ec_vals[i]:
                            rows.append(i)
                    except TypeError:
                        pass
            return rows

        def _find_round_number(sc: "pl.Series", col: str, anomaly: "AnomalyEntity", snap: "pl.DataFrame") -> "list[int]":
            if sc.dtype not in _NUMERIC_DTYPES:
                return []
            series = sc.cast(pl.Float64)
            non_null_series = series.drop_nulls()
            if non_null_series.len() < 10:
                return []
            mean_val = non_null_series.mean()
            if mean_val is None or float(str(mean_val)) == 0:
                return []
            mean_f = float(str(mean_val))
            divisor = 1000.0 if abs(mean_f) >= 1000 else 100.0
            return [i for i, v in enumerate(series.to_list()) if v is not None and v % divisor == 0.0]

        def _find_low_variance(sc: "pl.Series", col: str, anomaly: "AnomalyEntity", snap: "pl.DataFrame") -> "list[int]":
            if sc.dtype not in _STRING_DTYPES:
                # Numeric LOW_VARIANCE is audit-only; no automatic correction.
                return []
            dominant_value = anomaly.original_value
            if dominant_value is None:
                non_null = sc.drop_nulls()
                if non_null.len() > 0:
                    vc = non_null.value_counts(sort=True)
                    dominant_value = vc[col][0]
            if dominant_value is None:
                return []
            return [
                i for i, v in enumerate(sc.to_list())
                if v is not None and str(v).strip() == str(dominant_value).strip()
            ]

        def _find_outlier_iqr(sc: "pl.Series", col: str, anomaly: "AnomalyEntity", snap: "pl.DataFrame") -> "list[int]":
            if sc.dtype not in _NUMERIC_DTYPES:
                return []
            series = sc.cast(pl.Float64).drop_nulls()
            if series.len() < 4:
                return []
            q1 = series.quantile(0.25)
            q3 = series.quantile(0.75)
            if q1 is None or q3 is None:
                return []
            q1_f, q3_f = float(str(q1)), float(str(q3))
            iqr = q3_f - q1_f
            if iqr <= 0:
                return []
            lower, upper = q1_f - 1.5 * iqr, q3_f + 1.5 * iqr
            return [
                i for i, v in enumerate(sc.cast(pl.Float64).to_list())
                if v is not None and (v < lower or v > upper)
            ]

        def _find_sequence_gap(sc: "pl.Series", col: str, anomaly: "AnomalyEntity", snap: "pl.DataFrame") -> "list[int]":
            _seq_pat2 = re.compile(r"^([A-Za-z\-_]*)(\d+)$")
            parts = (anomaly.original_value or "").split("|")
            if len(parts) < 3:
                return []
            prefix = parts[0]
            try:
                seq_min, seq_max = int(parts[1]), int(parts[2])
            except (ValueError, IndexError):
                return []
            rows: list[int] = []
            for i, v in enumerate(sc.to_list()):
                if v is None:
                    continue
                m_v = _seq_pat2.fullmatch(str(v))
                if m_v and m_v.group(1) == prefix:
                    num = int(m_v.group(2))
                    if num < seq_min or num > seq_max:
                        rows.append(i)
            return rows

        _ROW_FINDERS = {
            "MISSING_VALUE":               _find_missing,
            "OUTLIER":                     _find_outlier,
            "DUPLICATE":                   _find_duplicate,
            "FORMAT_INVALID":              _find_format_invalid,
            "INCONSISTENT":                _find_inconsistent,
            "WHITESPACE_ONLY":             _find_whitespace,
            "CROSS_FIELD_SWAP":            _find_cross_swap,
            "SUSPICIOUS_PLACEHOLDER":      _find_suspicious,
            "LEADING_TRAILING_WHITESPACE": _find_leading,
            "DATE_LOGICAL":                _find_date_logical,
            "NUMERIC_ROUND_NUMBER":        _find_round_number,
            "LOW_VARIANCE":                _find_low_variance,
            "OUTLIER_IQR":                 _find_outlier_iqr,
            "SEQUENCE_GAP":                _find_sequence_gap,
        }

        for anomaly_id, decision in decision_map.items():
            anomaly = anomaly_map.get(anomaly_id)
            if anomaly is None:
                continue

            col = anomaly.column
            if col not in result.columns:
                continue

            # ── Find affected row indices via dispatcher ──────────────────────
            _snap_col = original_df_snapshot[col] if col in original_df_snapshot.columns else result[col]
            _finder = _ROW_FINDERS.get(anomaly.type)
            affected_rows: list[int] = _finder(_snap_col, col, anomaly, original_df_snapshot) if _finder else []

            if not affected_rows:
                continue

            # Special: DUPLICATE (any non-discarded decision) → append "_DUP" suffix
            if anomaly.type == "DUPLICATE" and affected_rows and not decision.is_discarded:
                col_data = result[col].to_list()
                for row_idx in affected_rows:
                    v = col_data[row_idx]
                    if v is not None and not str(v).endswith("_DUP"):
                        col_data[row_idx] = str(v) + "_DUP"
                try:
                    result = result.with_columns(pl.Series(col, col_data, dtype=result[col].dtype))
                except Exception:
                    result = result.with_columns(pl.Series(col, col_data, dtype=pl.Utf8))
                continue

            # Special: WHITESPACE_ONLY APPROVED → set to None (real null)
            if anomaly.type == "WHITESPACE_ONLY" and decision.is_approved and affected_rows:
                col_data = result[col].to_list()
                for row_idx in affected_rows:
                    col_data[row_idx] = None
                try:
                    result = result.with_columns(pl.Series(col, col_data, dtype=result[col].dtype))
                except Exception:
                    pass
                continue

            # Special handling: strip each value individually for LEADING_TRAILING_WHITESPACE
            if anomaly.type == "LEADING_TRAILING_WHITESPACE" and decision.is_approved and affected_rows:
                col_data = result[col].to_list()
                for row_idx in affected_rows:
                    if col_data[row_idx] is not None:
                        col_data[row_idx] = str(col_data[row_idx]).strip()
                try:
                    result = result.with_columns(
                        pl.Series(col, col_data, dtype=result[col].dtype)
                    )
                except Exception:
                    pass
                continue

            # ── Apply decision to all affected rows ──────────────────────────
            if decision.is_discarded:
                # DISCARDED = reject the suggestion, keep original value.
                # Business rule: NEVER delete rows from the dataset.
                continue

            elif decision.is_corrected and decision.correction_ir is not None:
                # ── IR path (new): execute structured IR tree ────────────────
                try:
                    ir_result = IRExecutor(result, anomaly, affected_rows).execute(
                        decision.correction_ir
                    )
                except IRExecutionError as exc:
                    logger.warning(
                        "ir_execution_failed",
                        error=str(exc),
                        anomaly_id=anomaly.id,
                    )
                    continue

                if ir_result.result_type == IRResult.DELETE:
                    # Business rule: NEVER delete rows. Treat DELETE as no-op.
                    structlog.get_logger().warning(
                        "ir_delete_skipped_business_rule",
                        anomaly_id=anomaly.id,
                        col=col,
                        affected_rows_count=len(affected_rows),
                    )

                elif ir_result.result_type == IRResult.FILL:
                    col_data = result[col].to_list()
                    if ir_result.scalar_value is not None or (
                        ir_result.per_row_values is None
                    ):
                        # scalar broadcast
                        for row_idx in affected_rows:
                            col_data[row_idx] = ir_result.scalar_value
                    else:
                        # per-row values
                        for row_idx, v in zip(affected_rows, ir_result.per_row_values):
                            col_data[row_idx] = v
                    try:
                        result = result.with_columns(
                            pl.Series(col, col_data, dtype=result[col].dtype)
                        )
                    except Exception:
                        structlog.get_logger().warning(
                            "ir_fill_type_mismatch_skipped",
                            col=col,
                            affected_rows=affected_rows,
                            dtype=str(result[col].dtype),
                        )
                # IRResult.KEEP → no-op

            elif decision.is_corrected and decision.correction is not None:
                col_data = result[col].to_list()
                casted = self._cast_correction(decision.correction, result[col].dtype)
                for row_idx in affected_rows:
                    col_data[row_idx] = casted
                try:
                    result = result.with_columns(
                        pl.Series(col, col_data, dtype=result[col].dtype)
                    )
                except Exception:
                    try:
                        # BUG FIX v8: fallback to Utf8 for Categorical columns that
                        # reject new string values not in their category set.
                        result = result.with_columns(
                            pl.Series(col, col_data, dtype=pl.Utf8)
                        )
                    except Exception:
                        structlog.get_logger().warning(
                            "correction_type_mismatch_skipped",
                            col=col,
                            affected_rows=affected_rows,
                            value=decision.correction,
                            dtype=str(result[col].dtype),
                        )

            elif decision.is_approved:
                # Apply the anomaly's pre-calculated suggested value
                if anomaly.suggested_value is not None:
                    col_data = result[col].to_list()
                    casted = self._cast_correction(anomaly.suggested_value, result[col].dtype)
                    for row_idx in affected_rows:
                        col_data[row_idx] = casted
                    try:
                        result = result.with_columns(
                            pl.Series(col, col_data, dtype=result[col].dtype)
                        )
                    except Exception:
                        try:
                            # BUG FIX v8: fallback to Utf8 for Categorical columns
                            # (e.g. "Grupo" LOW_VARIANCE → "GRUPO_BAJO_VARIANZA").
                            result = result.with_columns(
                                pl.Series(col, col_data, dtype=pl.Utf8)
                            )
                        except Exception:
                            structlog.get_logger().warning(
                                "approved_suggestion_type_mismatch_skipped",
                                col=col,
                                affected_rows=affected_rows,
                                value=anomaly.suggested_value,
                                dtype=str(result[col].dtype),
                            )

        # ── Universal strip: limpiar whitespace en todas las columnas string ──
        # Cubre LEADING_TRAILING_WHITESPACE que no fue tratado por su handler
        # (ej: columnas Localidad y Teléfono, filas 174 y 235).
        for _strip_col in result.columns:
            if result[_strip_col].dtype in _STRING_DTYPES:
                result = result.with_columns(
                    pl.when(pl.col(_strip_col).str.strip_chars().str.len_chars() == 0)
                    .then(None)
                    .otherwise(pl.col(_strip_col).str.strip_chars())
                    .alias(_strip_col)
                )

        # ── Post-correction validation for DATE_LOGICAL ─────────────────────
        # After all corrections, verify that any DATE_LOGICAL anomalies now
        # satisfy their constraint (start_date <= end_date).  If a corrected
        # value still violates the constraint, advance end-date by one day or null it out.
        import datetime as _dt

        def _to_date(v: object) -> "_dt.date | None":
            if isinstance(v, _dt.datetime):
                return v.date()
            if isinstance(v, _dt.date):
                return v
            if isinstance(v, str):
                try:
                    return _dt.date.fromisoformat(str(v)[:10])
                except (ValueError, TypeError):
                    return None
            return None

        for anomaly in anomalies:
            if anomaly.type != "DATE_LOGICAL":
                continue
            m = re.search(r"es posterior a '([^']+)'", anomaly.description)
            if not m:
                continue
            ec = m.group(1)
            sc = anomaly.column
            if sc not in result.columns or ec not in result.columns:
                continue
            sc_vals = result[sc].to_list()
            ec_vals = result[ec].to_list()
            changed = False
            for i in range(len(sc_vals)):
                if sc_vals[i] is not None and ec_vals[i] is not None:
                    sc_date = _to_date(sc_vals[i])
                    ec_date = _to_date(ec_vals[i])
                    if sc_date and ec_date and ec_date < sc_date:
                        try:
                            # alta (ec) must be at least 18 years after nacimiento (sc).
                            new_date = sc_date + _dt.timedelta(days=365 * 18)
                            # BUG FIX v8: Polars Datetime columns require datetime objects,
                            # not date objects — mismatched type causes silent no-op.
                            if result[ec].dtype in _DATE_DTYPES and result[ec].dtype != pl.Date:
                                ec_vals[i] = _dt.datetime(new_date.year, new_date.month, new_date.day)
                            else:
                                ec_vals[i] = new_date
                        except Exception:
                            ec_vals[i] = None
                        changed = True
            if changed:
                try:
                    result = result.with_columns(
                        pl.Series(ec, ec_vals, dtype=result[ec].dtype)
                    )
                except Exception:
                    try:
                        result = result.with_columns(
                            pl.Series(ec, ec_vals, dtype=pl.Date)
                        )
                    except Exception:
                        pass

        # ── Invariante de integridad: nunca eliminar filas ───────────────────
        assert result.shape[0] == df.shape[0], (
            f"Row count changed: {df.shape[0]} → {result.shape[0]}. "
            "Business rule violated: rows must never be deleted."
        )

        return result

    @staticmethod
    def _cast_correction(value: Any, dtype: pl.DataType) -> Any:
        """Cast a correction string to the appropriate Python type for a Polars column.

        When humans submit corrections via the UI, values arrive as strings.
        Before inserting into a typed Polars Series we must convert them.
        """
        if value is None:
            return None
        integer_types = (
            pl.Int8, pl.Int16, pl.Int32, pl.Int64,
            pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
        )
        float_types = (pl.Float32, pl.Float64)
        with suppress(ValueError, TypeError):
            if dtype in integer_types:
                return int(float(value))
            if dtype in float_types:
                return float(value)
        return value

    # =========================================================================
    # Private: status helpers
    # =========================================================================

    async def _generate_ai_suggestions(
        self,
        anomalies: list[AnomalyEntity],
        df: pl.DataFrame,
        job_id: str,
    ) -> None:
        """Generate AI suggestions directly via Gemini and persist them to DB.

        Opción C: the worker calls Gemini directly using the AiSuggestionService
        port, which has access to the full DataFrame (real Polars dtypes,
        statistics, sample values) — context that n8n never has.

        Fire-and-forget: errors per anomaly are logged but do not abort the job.
        """
        if self._ai_suggestion_service is None or self._job_repo is None:
            return

        log = logger.bind(job_id=job_id, anomalies_count=len(anomalies))
        log.info("Generating AI suggestions via Gemini")

        for anomaly in anomalies:
            try:
                suggestion = await self._ai_suggestion_service.generate_suggestion(anomaly, df)
                if suggestion is None:
                    logger.debug(
                        "No AI suggestion returned",
                        anomaly_id=anomaly.id,
                        column=anomaly.column,
                    )
                    continue

                await self._job_repo.save_ai_suggestion(
                    anomaly_id=anomaly.id,
                    action_type=suggestion.action_type,
                    value=suggestion.value,
                    reason=suggestion.reason,
                )
                logger.info(
                    "AI suggestion saved",
                    anomaly_id=anomaly.id,
                    action_type=suggestion.action_type,
                )
            except Exception as exc:
                logger.warning(
                    "AI suggestion failed for anomaly",
                    anomaly_id=anomaly.id,
                    error=str(exc),
                )

    async def _notify_n8n_for_suggestions(
        self,
        dataset_id: str,
        anomalies: list[AnomalyEntity],
        df: pl.DataFrame | None = None,
    ) -> None:
        """Call n8n webhook to trigger Gemini AI suggestions for detected anomalies.

        Fire-and-forget: errors are logged but do not affect the HITL flow.

        If the DataFrame is provided, the payload is enriched with `dtype` and
        `sampleValues` per anomaly so the downstream Gemini prompt has real
        column context (required by the v4 workflow's JSON-structured prompt).
        """
        webhook_url = os.environ.get("N8N_SUGGESTIONS_WEBHOOK_URL", "")
        if not webhook_url:
            logger.debug("N8N_SUGGESTIONS_WEBHOOK_URL not set — skipping AI suggestion trigger")
            return

        def _column_context(col: str) -> tuple[str, list[str]]:
            if df is None or col not in df.columns:
                return "unknown", []
            dtype_str = str(df[col].dtype)
            try:
                samples = df[col].drop_nulls().head(5).to_list()
                sample_strs = [str(v) for v in samples if v is not None]
            except Exception:
                sample_strs = []
            return dtype_str, sample_strs

        payload_anomalies = []
        for a in anomalies[:20]:  # limit to 20 like the API does
            dtype_str, sample_values = _column_context(a.column)
            payload_anomalies.append(
                {
                    "id": a.id,
                    "type": a.type,
                    "column": a.column,
                    "row": a.row,
                    "description": a.description,
                    "originalValue": a.original_value,
                    "suggestedValue": a.suggested_value,
                    "dtype": dtype_str,
                    "sampleValues": sample_values,
                }
            )

        payload = {
            "datasetId": dataset_id,
            "anomalies": payload_anomalies,
        }

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(webhook_url, json=payload)
                if resp.status_code >= 400:
                    logger.warning(
                        "n8n webhook returned error",
                        status=resp.status_code,
                        dataset_id=dataset_id,
                    )
                else:
                    logger.info(
                        "n8n notified for AI suggestions",
                        dataset_id=dataset_id,
                        anomalies_count=len(payload["anomalies"]),
                    )
        except Exception as exc:
            logger.warning(
                "Failed to notify n8n for AI suggestions",
                dataset_id=dataset_id,
                error=str(exc),
            )

    async def _update_status(
        self,
        job_id: str,
        status: JobStatus,
        result: dict[str, object] | None = None,
        error: str | None = None,
    ) -> None:
        """Update job status if repository is available."""
        if self._job_repo is None:
            return
        try:
            await self._job_repo.update_job_status(job_id, status, result=result, error=error)
        except Exception as exc:
            logger.warning(
                "Failed to update job status",
                job_id=job_id,
                status=status.value,
                error=str(exc),
            )

    # =========================================================================
    # Private: transformation helpers (unchanged from original)
    # =========================================================================

    def _build_transformation_configs(
        self,
        raw_configs: list[dict[str, Any]],
    ) -> list[TransformationConfig]:
        """Convert raw config dicts to TransformationConfig objects."""
        configs = []

        for raw in raw_configs:
            transformation_type = TransformationType(raw["type"])
            config = TransformationConfig(
                type=transformation_type,
                columns=raw.get("columns"),
                params=raw.get("params", {}),
            )
            configs.append(config)

        return configs

    def _get_output_format(self, format_str: str) -> FileFormat:
        """Convert string to FileFormat enum."""
        format_map = {
            "parquet": FileFormat.PARQUET,
            "csv": FileFormat.CSV,
            "json": FileFormat.JSON,
            "xlsx": FileFormat.EXCEL,
        }
        return format_map.get(format_str.lower(), FileFormat.PARQUET)

    def _generate_output_key(
        self,
        dataset_id: UUID,
        job_id: UUID,
        file_format: FileFormat,
    ) -> str:
        """Generate storage key for output file."""
        extension = file_format.value
        return f"processed/{dataset_id}/{job_id}/output.{extension}"

    def _get_content_type(self, file_format: FileFormat) -> str:
        """Get content type for file format."""
        content_types = {
            FileFormat.PARQUET: "application/octet-stream",
            FileFormat.CSV: "text/csv",
            FileFormat.JSON: "application/json",
            FileFormat.EXCEL: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }
        return content_types.get(file_format, "application/octet-stream")

    def _parse_storage_path(self, source_key: str) -> tuple[str, str]:
        """Parse storage path into bucket and key."""
        # Always return datasets bucket because source_key from API is the object key
        return "datasets", source_key
