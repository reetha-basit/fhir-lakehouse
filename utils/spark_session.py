"""
Spark session factory configured for Delta Lake.

Centralizing the session builder ensures every pipeline module gets a
consistently-configured Spark context — same Delta extensions, same
catalog wiring, same shuffle settings. Calling get_spark() repeatedly
returns the same session (Spark singleton behavior).
"""
import os
import sys

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from config.config import (
    APP_NAME,
    LOG_LEVEL,
    SHUFFLE_PARTITIONS,
    SPARK_EXTRA_PACKAGES,
    SPARK_IVY_DIR,
    SPARK_LOCAL_DIR,
)


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
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    SPARK_IVY_DIR.mkdir(parents=True, exist_ok=True)
    SPARK_LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    os.environ["SPARK_LOCAL_DIRS"] = str(SPARK_LOCAL_DIR)
    os.environ["TMP"] = str(SPARK_LOCAL_DIR)
    os.environ["TEMP"] = str(SPARK_LOCAL_DIR)
    java_tmp_dir = SPARK_LOCAL_DIR.as_posix()

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
        .config("spark.jars.ivy", str(SPARK_IVY_DIR))
        .config("spark.local.dir", str(SPARK_LOCAL_DIR))
        .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={java_tmp_dir}")
        .config("spark.executor.extraJavaOptions", f"-Djava.io.tmpdir={java_tmp_dir}")
        # Adaptive Query Execution — lets Spark optimize plans at runtime
        # based on actual data sizes (good default for varying workloads).
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )

    # configure_spark_with_delta_pip downloads the matching Delta JARs from
    # Maven on first run and pins them to the session classpath.
    spark = configure_spark_with_delta_pip(
        builder,
        extra_packages=SPARK_EXTRA_PACKAGES,
    ).getOrCreate()
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.file.impl", "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem")
    hadoop_conf.set(
        "fs.AbstractFileSystem.file.impl",
        "org.apache.hadoop.fs.local.BareStreamingLocalFileSystem",
    )
    spark._jvm.org.apache.hadoop.fs.FileSystem.closeAll()
    spark.sparkContext.setLogLevel(LOG_LEVEL)
    return spark
