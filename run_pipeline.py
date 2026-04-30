"""
End-to-end pipeline orchestrator.

Runs Bronze -> Silver -> Quality in sequence. Designed as a
lightweight stand-in for an Airflow DAG; the production version would
wrap each step as a task with retries, alerting, and SLA monitoring.

Usage
-----
    python run_pipeline.py
"""
from pipelines import bronze_ingestion, silver_transform, gold_aggregations
from quality import expectations
from utils.logger import get_logger

log = get_logger(__name__)


def main() -> None:
    """Execute the full pipeline in order."""
    log.info("\n" + "█" * 60)
    log.info("  FHIR LAKEHOUSE — End-to-end pipeline run")
    log.info("█" * 60 + "\n")

    # --- Bronze ---
    bronze_ingestion.run()

    # --- Silver ---
    silver_transform.run()

    # --- Quality gate (Silver) ---
    # In production this would block downstream steps on failure.
    expectations.run()

    # --- Gold (currently a no-op until implemented) ---
    gold_aggregations.run()

    log.info("\n" + "█" * 60)
    log.info("  Pipeline complete ✓")
    log.info("█" * 60 + "\n")


if __name__ == "__main__":
    main()
