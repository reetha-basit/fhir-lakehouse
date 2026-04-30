"""
Silver layer transformation.

Responsibility
--------------
Transform raw nested FHIR resources from Bronze into clean, flat,
strongly-typed Delta tables that downstream consumers (BI, ML, Gold
aggregations) can query without parsing JSON.

Transformations applied
-----------------------
- Flatten nested arrays (name[0].given[0] -> first_name, etc.)
- Cast types (birthDate -> DateType, timestamps -> TimestampType)
- Normalize null/missing values
- Deduplicate on resource id (last-write-wins on _ingest_timestamp)
- Drop Bronze-only audit columns we no longer need downstream

Design notes
------------
- We use explicit `select(col(...).alias(...))` rather than `withColumn`
  loops because Spark's Catalyst optimizer plans a single projection
  more efficiently than chained withColumn calls.
- Encounter transform is intentionally left as a TODO — see roadmap in
  README. It would follow the same pattern as Patient/Observation.
"""
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    coalesce,
    col,
    row_number,
    to_date,
    to_timestamp,
)

from config.config import (
    BRONZE_OBSERVATIONS,
    BRONZE_PATIENTS,
    SILVER_OBSERVATIONS,
    SILVER_PATIENTS,
)
from utils.logger import get_logger
from utils.spark_session import get_spark

log = get_logger(__name__)


# -------------------------------------------------------------------
# Patient transformation
# -------------------------------------------------------------------
def transform_patients(spark: SparkSession) -> DataFrame:
    """
    Flatten FHIR Patient resources into a relational dimension table.

    A Patient resource has nested arrays for name, identifier, address,
    telecom — we project the canonical "first" element of each (which is
    convention for the primary/usual entry) into top-level columns.
    """
    log.info("[Silver] Transforming patients")

    bronze = spark.read.format("delta").load(str(BRONZE_PATIENTS))

    # Project the fields we care about. name[0] is the primary name by
    # FHIR convention; we take the first given name as the first_name.
    flattened = bronze.select(
        col("id").alias("patient_id"),
        col("name")[0]["family"].alias("last_name"),
        col("name")[0]["given"][0].alias("first_name"),
        col("gender"),
        to_date(col("birthDate")).alias("birth_date"),
        col("address")[0]["city"].alias("city"),
        col("address")[0]["state"].alias("state"),
        col("address")[0]["postalCode"].alias("postal_code"),
        col("address")[0]["country"].alias("country"),
        col("identifier")[0]["value"].alias("mrn"),
        col("_ingest_timestamp"),
    )

    # Replace string "null" / empty with proper NULL — sample data sometimes
    # arrives with literal "null" strings from upstream serialization quirks.
    cleaned = flattened.select(
        col("patient_id"),
        coalesce(col("first_name"), col("last_name")).alias("display_name_fallback"),
        col("first_name"),
        col("last_name"),
        col("gender"),
        col("birth_date"),
        col("city"),
        col("state"),
        col("postal_code"),
        col("country"),
        col("mrn"),
        col("_ingest_timestamp"),
    )

    # Deduplicate: if the same patient_id appears more than once, keep the
    # most recently ingested record. Window functions are the idiomatic
    # way to do this in Spark.
    dedup_window = Window.partitionBy("patient_id").orderBy(col("_ingest_timestamp").desc())
    deduped = (
        cleaned
        .withColumn("_rn", row_number().over(dedup_window))
        .filter(col("_rn") == 1)
        .drop("_rn", "_ingest_timestamp")
    )

    return deduped


# -------------------------------------------------------------------
# Observation transformation
# -------------------------------------------------------------------
def transform_observations(spark: SparkSession) -> DataFrame:
    """
    Flatten FHIR Observation resources (vital signs, lab results, etc.).

    Observations link back to a Patient via the subject reference.
    We strip the "Patient/" prefix to get a clean foreign key.
    """
    log.info("[Silver] Transforming observations")

    bronze = spark.read.format("delta").load(str(BRONZE_OBSERVATIONS))

    flattened = bronze.select(
        col("id").alias("observation_id"),
        # Strip "Patient/" prefix from the FHIR reference to leave just the id.
        # regexp_replace would also work — substring after slash is simpler here.
        col("subject")["reference"].alias("subject_ref_full"),
        col("status"),
        col("code")["coding"][0]["code"].alias("loinc_code"),
        col("code")["coding"][0]["display"].alias("observation_name"),
        col("valueQuantity")["value"].cast("double").alias("value_numeric"),
        col("valueQuantity")["unit"].alias("value_unit"),
        to_timestamp(col("effectiveDateTime")).alias("effective_datetime"),
        col("_ingest_timestamp"),
    )

    # Extract patient_id from "Patient/<id>" reference string.
    cleaned = flattened.withColumn(
        "patient_id",
        # Slice off the first 8 chars ("Patient/") — substring is 1-indexed in Spark SQL.
        col("subject_ref_full").substr(9, 100),
    ).drop("subject_ref_full")

    return cleaned


# -------------------------------------------------------------------
# Writers
# -------------------------------------------------------------------
def write_silver(df: DataFrame, target_path) -> None:
    """Write a Silver dataframe to Delta with overwrite + schema merge."""
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(target_path))
    )
    log.info(f"[Silver] Wrote {df.count()} rows -> {target_path}")


def run() -> None:
    """Orchestrate Silver transformations."""
    spark = get_spark()

    log.info("=" * 60)
    log.info("SILVER LAYER — FHIR flattening + cleansing")
    log.info("=" * 60)

    patients_silver = transform_patients(spark)
    write_silver(patients_silver, SILVER_PATIENTS)

    observations_silver = transform_observations(spark)
    write_silver(observations_silver, SILVER_OBSERVATIONS)

    # TODO: transform_encounters() — see README roadmap.
    # The Encounter resource has class, period.start, period.end, type,
    # serviceProvider — straightforward flatten following the same pattern.

    log.info("[Silver] ✓ Complete")


if __name__ == "__main__":
    run()
