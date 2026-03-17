"""Process Dataset use case - orchestrates ETL pipeline with HITL support."""

import asyncio
from contextlib import suppress
from dataclasses import dataclass, field
from io import BytesIO
from typing import Any
from uuid import UUID, uuid4

import polars as pl
import structlog

from src.application.transformations import (
    DataTransformer,
    TransformationType,
    TransformationConfig,
    TransformationResult,
)
from src.domain.entities.anomaly import AnomalyEntity
from src.domain.entities.decision import DecisionEntity
from src.domain.ports.repositories.job_repository import JobRepository
from src.domain.ports.services.storage_service import StorageService
from src.domain.value_objects.job_status import JobStatus
from src.infrastructure.parsers.dataset_parser import (
    DatasetParser,
    FileFormat,
)


logger = structlog.get_logger("pymes.worker.use_cases.process_dataset")

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
        """
        self._storage = storage
        self._parser = parser or DatasetParser()
        self._transformer = transformer or DataTransformer()
        self._output_bucket = output_bucket
        self._job_repo = job_repository
        self._hitl_poll_interval = hitl_poll_interval
        self._hitl_max_wait = hitl_max_wait

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
            # Step 5: Detect anomalies
            # ---------------------------------------------------------------
            anomalies = self._detect_anomalies(df, dataset_id_str)
            anomalies_detected = len(anomalies)
            decisions_applied = 0

            # ---------------------------------------------------------------
            # Step 6: HITL flow (only if repository available + anomalies)
            # ---------------------------------------------------------------
            if anomalies and self._job_repo is not None:
                log.info("Anomalies detected — entering HITL flow", count=anomalies_detected)

                # 6a. Save anomalies to DB
                await self._job_repo.save_anomalies(dataset_id_str, anomalies)

                # 6b. Update job → AWAITING_REVIEW
                await self._update_status(job_id_str, JobStatus.AWAITING_REVIEW)

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

        Current heuristics:
        - MISSING_VALUE: any null in any column
        - OUTLIER: numeric values beyond 3 standard deviations from the mean

        Each distinct (row, column) pair with an issue becomes one anomaly.
        """
        anomalies: list[AnomalyEntity] = []

        # Missing values
        for col in df.columns:
            null_mask = df[col].is_null()
            null_indices = [i for i, v in enumerate(null_mask.to_list()) if v]
            for row_idx in null_indices:
                anomalies.append(
                    AnomalyEntity.create(
                        id=str(uuid4()),
                        dataset_id=dataset_id,
                        column=col,
                        row=row_idx,
                        anomaly_type="MISSING_VALUE",
                        description=f"Null value in column '{col}' at row {row_idx}",
                        original_value=None,
                        suggested_value=None,
                    )
                )

        # Outliers (numeric only — Z-score > 3)
        numeric_dtypes = (
            pl.Int8, pl.Int16, pl.Int32, pl.Int64,
            pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
            pl.Float32, pl.Float64,
        )
        for col in df.columns:
            if df[col].dtype not in numeric_dtypes:
                continue
            series = df[col].cast(pl.Float64).drop_nulls()
            if series.len() < 4:  # Too few points for meaningful stats
                continue
            mean = series.mean()
            std = series.std()
            if std is None or std == 0:
                continue
            for row_idx, val in enumerate(df[col].to_list()):
                if val is None:
                    continue
                z = abs((float(val) - mean) / std)  # type: ignore[arg-type]
                if z > 3.0:
                    anomalies.append(
                        AnomalyEntity.create(
                            id=str(uuid4()),
                            dataset_id=dataset_id,
                            column=col,
                            row=row_idx,
                            anomaly_type="OUTLIER",
                            description=(
                                f"Outlier in column '{col}' at row {row_idx}: "
                                f"z-score={z:.2f}"
                            ),
                            original_value=str(val),
                            suggested_value=str(mean),
                        )
                    )

        return anomalies

    def _apply_decisions(
        self,
        df: pl.DataFrame,
        anomalies: list[AnomalyEntity],
        decisions: list[DecisionEntity],
    ) -> pl.DataFrame:
        """Apply human decisions to the DataFrame.

        Decision semantics:
        - APPROVED  → keep the row as-is (no change)
        - CORRECTED → replace the anomalous cell with DecisionEntity.correction
        - DISCARDED → drop the row entirely

        Rows with DISCARDED decisions are removed; CORRECTED cells are updated.
        """
        if not decisions:
            return df

        # Build lookup: anomaly_id → decision
        decision_map: dict[str, DecisionEntity] = {d.anomaly_id: d for d in decisions}
        # Build lookup: anomaly_id → AnomalyEntity
        anomaly_map: dict[str, AnomalyEntity] = {a.id: a for a in anomalies}

        rows_to_drop: set[int] = set()
        corrections: dict[int, dict[str, Any]] = {}  # {row_idx: {col: new_val}}

        for anomaly_id, decision in decision_map.items():
            anomaly = anomaly_map.get(anomaly_id)
            if anomaly is None or anomaly.row is None:
                continue

            if decision.is_discarded:
                rows_to_drop.add(anomaly.row)
            elif decision.is_corrected and decision.correction is not None:
                corrections.setdefault(anomaly.row, {})[anomaly.column] = decision.correction

        # Apply corrections (before dropping rows so indices stay stable)
        result = df
        for row_idx, col_values in corrections.items():
            for col, new_val in col_values.items():
                if col not in result.columns:
                    continue
                # Build a new series with the corrected value,
                # casting the correction string to the column's native type.
                col_data = result[col].to_list()
                col_data[row_idx] = self._cast_correction(new_val, result[col].dtype)
                result = result.with_columns(
                    pl.Series(col, col_data, dtype=result[col].dtype)
                )

        # Drop discarded rows
        if rows_to_drop:
            keep_mask = [i not in rows_to_drop for i in range(result.height)]
            result = result.filter(pl.Series("_keep", keep_mask))

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
                return int(value)
            if dtype in float_types:
                return float(value)
        return value

    # =========================================================================
    # Private: status helpers
    # =========================================================================

    async def _update_status(
        self,
        job_id: str,
        status: JobStatus,
        result: dict | None = None,
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
        if "/" in source_key:
            parts = source_key.split("/", 1)
            if "." not in parts[0]:
                return parts[0], parts[1]
        return "datasets", source_key
