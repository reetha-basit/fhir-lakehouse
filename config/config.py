"""
Centralized configuration for the FHIR Lakehouse pipeline.

All path resolution, layer constants, and tunable parameters live here so
individual pipeline modules stay focused on transformations rather than
environment concerns.
"""
from pathlib import Path

# -------------------------------------------------------------------
# Project root resolution
# -------------------------------------------------------------------
# Resolve relative to this file so the pipeline runs correctly regardless
# of the working directory it's invoked from.
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# -------------------------------------------------------------------
# Data layer paths (Medallion architecture)
# -------------------------------------------------------------------
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

# -------------------------------------------------------------------
# Source files (Bronze inputs)
# -------------------------------------------------------------------
RAW_PATIENTS = RAW_DIR / "patients.ndjson"
RAW_OBSERVATIONS = RAW_DIR / "observations.ndjson"
RAW_ENCOUNTERS = RAW_DIR / "encounters.ndjson"

# -------------------------------------------------------------------
# Bronze table paths
# -------------------------------------------------------------------
BRONZE_PATIENTS = BRONZE_DIR / "patients"
BRONZE_OBSERVATIONS = BRONZE_DIR / "observations"
BRONZE_ENCOUNTERS = BRONZE_DIR / "encounters"

# -------------------------------------------------------------------
# Silver table paths
# -------------------------------------------------------------------
SILVER_PATIENTS = SILVER_DIR / "patients"
SILVER_OBSERVATIONS = SILVER_DIR / "observations"
SILVER_ENCOUNTERS = SILVER_DIR / "encounters"  # TODO: implement transform

# -------------------------------------------------------------------
# Gold table paths (TODO — to be built in next iteration)
# -------------------------------------------------------------------
GOLD_ENCOUNTER_SUMMARY = GOLD_DIR / "encounter_summary"
GOLD_PATIENT_DEMOGRAPHICS = GOLD_DIR / "patient_demographics"

# -------------------------------------------------------------------
# Spark / Delta tunables
# -------------------------------------------------------------------
APP_NAME = "FHIRLakehouse"
SHUFFLE_PARTITIONS = 4  # Small for local dev; tune for production
LOG_LEVEL = "WARN"  # Suppress Spark INFO noise
SPARK_IVY_DIR = PROJECT_ROOT / ".spark-ivy"
SPARK_LOCAL_DIR = PROJECT_ROOT / ".spark-tmp"
SPARK_EXTRA_PACKAGES = ["com.sparkutils:hadoop-bare-naked-local-fs:0.2.0"]

# -------------------------------------------------------------------
# FHIR resource type constants
# -------------------------------------------------------------------
RESOURCE_PATIENT = "Patient"
RESOURCE_OBSERVATION = "Observation"
RESOURCE_ENCOUNTER = "Encounter"
