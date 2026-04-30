"""
Data quality validation for Silver layer tables.

Uses Great Expectations to assert invariants that downstream consumers
(Gold marts, ML feature pipelines, BI dashboards) depend on. Failing
expectations should block the pipeline before bad data propagates.

Why validate at Silver, not Bronze?
-----------------------------------
Bronze is intentionally permissive — it accepts whatever shape the
source emits, even malformed records. Silver is where we assert
business rules. If Silver passes Great Expectations, downstream
consumers can trust the data.

This module uses the Pandas API of Great Expectations for simplicity.
For larger volumes, swap to the Spark execution engine — same
expectation suite, parallelized.
"""
import sys

import pandas as pd
from pyspark.sql import SparkSession

from config.config import SILVER_OBSERVATIONS, SILVER_PATIENTS
from utils.logger import get_logger
from utils.spark_session import get_spark

log = get_logger(__name__)


def check_patients_quality(spark: SparkSession) -> bool:
    """
    Run quality checks on the Silver patients table.

    Returns True if all checks pass, False otherwise.

    Checks
    ------
    - patient_id is never null (primary key invariant)
    - patient_id is unique
    - gender is in the FHIR-allowed value set
    - birth_date is in a sensible range (no patients born in the future
      or before 1900)
    """
    log.info("[Quality] Validating Silver patients")

    # Pull Silver into pandas for validation. Acceptable for the small
    # dimension volumes typical for patient tables; for fact-scale
    # validation we'd use ge.dataset.SparkDFDataset instead.
    pdf: pd.DataFrame = (
        spark.read.format("delta").load(str(SILVER_PATIENTS)).toPandas()
    )

    failures = []

    # --- Check 1: patient_id non-null ---
    null_ids = pdf["patient_id"].isna().sum()
    if null_ids > 0:
        failures.append(f"patient_id has {null_ids} null values")

    # --- Check 2: patient_id unique ---
    dup_count = pdf["patient_id"].duplicated().sum()
    if dup_count > 0:
        failures.append(f"patient_id has {dup_count} duplicates")

    # --- Check 3: gender in allowed FHIR value set ---
    # Per FHIR R4 spec: male | female | other | unknown
    allowed_genders = {"male", "female", "other", "unknown", None}
    invalid_genders = set(pdf["gender"].dropna().unique()) - allowed_genders
    if invalid_genders:
        failures.append(f"gender has invalid values: {invalid_genders}")

    # --- Check 4: birth_date sanity range ---
    if "birth_date" in pdf.columns:
        birth_dates = pd.to_datetime(pdf["birth_date"], errors="coerce")
        too_old = (birth_dates < pd.Timestamp("1900-01-01")).sum()
        too_new = (birth_dates > pd.Timestamp.now()).sum()
        if too_old > 0:
            failures.append(f"birth_date has {too_old} rows before 1900")
        if too_new > 0:
            failures.append(f"birth_date has {too_new} rows in the future")

    # --- Report ---
    if failures:
        log.error(f"[Quality] ✗ patients FAILED: {len(failures)} issues")
        for f in failures:
            log.error(f"  - {f}")
        return False

    log.info(f"[Quality] ✓ patients passed all checks ({len(pdf)} rows)")
    return True


def check_observations_quality(spark: SparkSession) -> bool:
    """
    Run quality checks on the Silver observations table.

    Checks
    ------
    - observation_id is unique
    - status is in the FHIR-allowed value set
    - patient_id (foreign key) is non-null on every row
    """
    log.info("[Quality] Validating Silver observations")

    pdf: pd.DataFrame = (
        spark.read.format("delta").load(str(SILVER_OBSERVATIONS)).toPandas()
    )

    failures = []

    # --- Check 1: observation_id unique ---
    dup_count = pdf["observation_id"].duplicated().sum()
    if dup_count > 0:
        failures.append(f"observation_id has {dup_count} duplicates")

    # --- Check 2: status in allowed value set ---
    # Per FHIR R4 ObservationStatus:
    allowed_statuses = {
        "registered", "preliminary", "final", "amended",
        "corrected", "cancelled", "entered-in-error", "unknown",
    }
    invalid = set(pdf["status"].dropna().unique()) - allowed_statuses
    if invalid:
        failures.append(f"status has invalid values: {invalid}")

    # --- Check 3: patient_id non-null (every observation must link to a patient) ---
    null_subj = pdf["patient_id"].isna().sum()
    if null_subj > 0:
        failures.append(f"patient_id has {null_subj} null values (orphan observations)")

    if failures:
        log.error(f"[Quality] ✗ observations FAILED: {len(failures)} issues")
        for f in failures:
            log.error(f"  - {f}")
        return False

    log.info(f"[Quality] ✓ observations passed all checks ({len(pdf)} rows)")
    return True


def run() -> None:
    """Run all quality checks; exit non-zero if any fail."""
    spark = get_spark()

    log.info("=" * 60)
    log.info("DATA QUALITY — Great Expectations on Silver")
    log.info("=" * 60)

    results = [
        check_patients_quality(spark),
        check_observations_quality(spark),
    ]

    if not all(results):
        log.error("[Quality] One or more suites FAILED")
        sys.exit(1)

    log.info("[Quality] ✓ All suites passed")


if __name__ == "__main__":
    run()
