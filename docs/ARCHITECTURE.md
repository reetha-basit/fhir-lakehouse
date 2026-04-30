# Architecture Notes

Personal scratchpad-style notes on design decisions made while building this project. Useful as a reference and for explaining choices during reviews.

## Why Medallion (Bronze / Silver / Gold)?

Three reasons drove the choice:

1. **Replayability** — Bronze is immutable raw. If Silver logic changes, we rebuild Silver without re-ingesting from source. Source systems (EHRs) are slow, sometimes flaky, and have rate limits — minimizing re-ingestion is a real operational concern.

2. **Separation of concerns** — Bronze owns "did the data arrive correctly?", Silver owns "is the data clean and queryable?", Gold owns "is the data useful for a specific business question?". Each layer has its own validation and contract.

3. **Cost control** — Gold marts are tuned for specific consumers (BI dashboards, ML features). Without a Gold layer, every consumer queries Silver and pays the aggregation cost on every read. With Gold, we pay once.

## Why Delta Lake (vs Iceberg, Hudi, plain Parquet)?

For this project Delta won on three axes:

- **Tooling maturity in the Python/Spark ecosystem** — `delta-spark` integrates cleanly with PySpark.
- **ACID transactions** — non-negotiable for healthcare audit trails.
- **Time travel** — `VERSION AS OF` queries are powerful for debugging "what did this table look like before yesterday's run?".

Iceberg would be the strong alternative if we needed cross-engine support (Trino, Snowflake, Spark all reading the same table). For a Spark-only pipeline, Delta is simpler. Hudi is best for streaming upserts at high volume — overkill here.

## FHIR R4 Specifics That Influenced Design

A few FHIR quirks shaped how Bronze and Silver are written:

- **Deeply nested resources** — Patient has `name[].given[]`, `address[].line[]`, etc. We project the *first* element of each array in Silver, which is FHIR convention for the "primary/usual" entry. Production would handle multiple entries (e.g. previous addresses) via a separate child table.

- **References as strings** — `"subject": {"reference": "Patient/patient-001"}` rather than a foreign key. We strip the `"Patient/"` prefix in Silver to get a clean joinable key.

- **Schema evolution** — FHIR resources gain new optional fields over versions and across implementations. `mergeSchema=true` on Bronze writes prevents pipeline breakage when the EHR adds a field.

- **NDJSON for bulk export** — the FHIR `$export` operation emits NDJSON natively. Spark's JSON reader handles this with no extra config.

## Why Window Functions for Deduplication?

The Silver patient transform uses:

```python
Window.partitionBy("patient_id").orderBy(col("_ingest_timestamp").desc())
row_number().over(window) == 1
```

Two reasons over a `groupBy + max()`:

1. We keep the *full row* of the latest record, not just the latest timestamp.
2. It generalizes to keeping top-N if requirements change ("keep the last 3 versions").

The trade-off is shuffle cost — Window functions force a shuffle on the partition key. For very large tables we'd consider clustered tables or merge-on-read patterns instead.

## Trade-offs Accepted

These are conscious shortcuts for a small project:

| Shortcut | Production fix |
|----------|----------------|
| Local file paths | Object storage (ADLS/S3/GCS) via config |
| Single-node Spark | Databricks cluster or EMR |
| Overwrite mode in Bronze | Append + ingestion ID + idempotency key |
| Pandas-based Great Expectations | Spark execution engine for scale |
| Synthetic data | Real FHIR bulk export with PHI controls |
| No CI/CD | GitHub Actions: lint, test, deploy on merge |
| No orchestrator | Airflow / Prefect / Dagster DAG |

## Data Quality Strategy

Validation lives at Silver, not Bronze. Bronze is intentionally permissive — it accepts whatever shape the source emits, including malformed records. Silver is the trust boundary: if Silver passes Great Expectations, downstream consumers can rely on it.

The current expectation suite covers basic structural and value-set rules. A production suite would add:

- Statistical drift checks (row count vs prior run, distribution shift)
- Cross-table referential integrity (every observation.patient_id exists in patients)
- Domain rules (vital signs within physiological ranges)
- Freshness checks (every patient has at least one observation in last 30 days)

## Observability TODOs

Not implemented but worth noting:

- Pipeline run metadata table (run_id, start_time, end_time, row_counts per layer)
- Delta table history surfaced as a metrics dashboard
- Alerting on quality check failures (email / Slack via webhook)
- Query plan logging for Silver/Gold transforms to catch regressions

## Security Posture (if this were production)

- Field-level encryption on direct identifiers (name, MRN, SSN, DOB) at the Bronze landing
- Column-level access control: PHI columns visible only to authorized roles
- Audit logging on all reads of identified data
- SMART on FHIR OAuth2 for any API-based ingestion
- Network: ingestion happens in a private subnet, no inbound from the public internet
- Secrets: never in code; use a vault (Azure Key Vault, AWS Secrets Manager)
