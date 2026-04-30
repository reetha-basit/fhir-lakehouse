"""
Spark session factory configured for Delta Lake.

Centralizing the session builder ensures every pipeline module gets a
consistently-configured Spark context — same Delta extensions, same
catalog wiring, same shuffle settings. Calling get_spark() repeatedly
returns the same session (Spark singleton behavior).
"""
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from config.config import APP_NAME, SHUFFLE_PARTITIONS, LOG_LEVEL


def get_spark(app_name: str = APP_NAME) -> SparkSession:
    """
    Build (or fetch existing) SparkSession with Delta Lake extensions enabled.

    Delta Lake requires two specific config keys to register its SQL
    extension and catalog implementation. Without these, attempts to read
    or write Delta tables fall back to plain Parquet behavior and ACID
    guarantees are lost.

    Returns
    -------
    SparkSession
        A fully configured Spark session.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        # Delta Lake hooks
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Local-dev tuning — small shuffle partition count avoids creating
        # hundreds of tiny files for the small datasets used here.
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        # Adaptive Query Execution — lets Spark optimize plans at runtime
        # based on actual data sizes (good default for varying workloads).
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )

    # configure_spark_with_delta_pip downloads the matching Delta JARs from
    # Maven on first run and pins them to the session classpath.
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel(LOG_LEVEL)
    return spark
