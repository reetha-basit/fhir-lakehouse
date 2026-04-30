"""
Bronze layer ingestion.

Responsibility
--------------
Land raw FHIR NDJSON resources into Delta tables with audit metadata.
NO business transformations happen here — that's the Silver layer's job.
The goal is preserving an immutable, replayable record of what arrived
from the source, with enough metadata to reconstruct lineage.

Audit columns added
-------------------
- _ingest_timestamp : when the row landed in Bronze
- _source_file      : which NDJSON file the row came from
- _resource_type    : the FHIR resourceType (Patient, Observation, etc.)

Why preserve raw nesting?
-------------------------
FHIR resources are deeply nested. If we flatten in Bronze, we lose
information that may matter later (extensions, contained resources).
Bronze keeps the original structure intact; Silver flattens.
"""
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit

from config.config import (
    BRONZE_ENCOUNTERS,
    BRONZE_OBSERVATIONS,
    BRONZE_PATIENTS,
    RAW_ENCOUNTERS,
    RAW_OBSERVATIONS,
    RAW_PATIENTS,
    RESOURCE_ENCOUNTER,
    RESOURCE_OBSERVATION,
    RESOURCE_PATIENT,
)
from utils.logger import get_logger
from utils.spark_session import get_spark

log = get_logger(__name__)


def ingest_resource(
    spark: SparkSession,
    source_path: Path,
    target_path: Path,
    resource_type: str,
) -> DataFrame:
    """
    Read a FHIR NDJSON file and write to Bronze Delta with audit columns.

    Uses overwrite mode for simplicity; in production this would be
    append-only with a per-batch ingestion ID and idempotency keys.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    source_path : Path
        NDJSON source file. Must exist and be readable.
    target_path : Path
        Destination Delta table path.
    resource_type : str
        FHIR resourceType label (added to every row for filtering).

    Returns
    -------
    DataFrame
        The dataframe that was written, useful for downstream chaining
        or row counts in tests.
    """
    if not source_path.exists():
        raise FileNotFoundError(f"Source file missing: {source_path}")

    log.info(f"[Bronze] Ingesting {resource_type} from {source_path.name}")

    # Spark's JSON reader handles NDJSON natively (one JSON doc per line).
    # multiLine is intentionally left False — NDJSON has line-delimited records.
    raw_df = spark.read.json(str(source_path))

    # Stamp every row with provenance metadata so we can answer "where did
    # this come from and when?" without scanning logs.
    enriched_df = (
        raw_df
        .withColumn("_ingest_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_resource_type", lit(resource_type))
    )

    # mergeSchema=true lets the Delta table evolve as FHIR resources gain
    # new optional fields over time (very common in real EHR feeds).
    (
        enriched_df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .save(str(target_path))
    )

    row_count = enriched_df.count()
    log.info(f"[Bronze] Wrote {row_count} {resource_type} rows -> {target_path}")
    return enriched_df


def run() -> None:
    """Orchestrate Bronze ingestion for all FHIR resource types."""
    spark = get_spark()

    log.info("=" * 60)
    log.info("BRONZE LAYER — Raw FHIR ingestion")
    log.info("=" * 60)

    ingest_resource(spark, RAW_PATIENTS, BRONZE_PATIENTS, RESOURCE_PATIENT)
    ingest_resource(spark, RAW_OBSERVATIONS, BRONZE_OBSERVATIONS, RESOURCE_OBSERVATION)
    ingest_resource(spark, RAW_ENCOUNTERS, BRONZE_ENCOUNTERS, RESOURCE_ENCOUNTER)

    log.info("[Bronze] ✓ Complete")


if __name__ == "__main__":
    run()
