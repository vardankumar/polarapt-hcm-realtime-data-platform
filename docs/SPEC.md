# SPEC.md — Polarapt HCM Real-Time Data Platform (AWS)
**Version:** v2.0 | **Updated:** 2026-10-25 00:40 PKT | **Owner:** Opus | **Deadline:** 03:00 PKT
**Source of truth:** hackathon PDF (Task 1 + Task 2 + architecture diagram). v1.x is superseded.

## 0. Architecture
```
EventBridge (3 rules, rate 1 min)
  ├─ lambda_jobs_ingest         ─┐
  ├─ lambda_benchmarks_scraper  ─┼─► S3 data-hackathon-smit-<name>/raw/<source>/YYYY/MM/DD/HHMM.json
  └─ lambda_fx_ingest           ─┘        (contract = S3 object metadata + "contract" block in file)
S3 ObjectCreated (raw/*.json) ─► SNS hcm-raw-events (standard) ─► lambda_contract_router (reads metadata)
  ─► SNS FIFO hcm-routed-events.fifo  [MessageAttribute source; MessageGroupId=source; dedup=sha256(key)]
      ├─ filter source=jobs        ─► jobs-queue.fifo        ─► Glue glue_jobs_to_snowflake ─► Snowflake HCM.TALENT.JOB_POSTINGS
      ├─ filter source=benchmarks  ─► benchmarks-queue.fifo  ─► lambda_benchmarks_transform ─► S3 processed/benchmarks/
      └─ filter source=fx          ─► fx-queue.fifo          ─► lambda_fx_to_sqlserver      ─► RDS SQL Server hcm.dbo.fx_rates
Processors are triggered by EventBridge (Lambdas: rule every minute; Glue: EventBridge Scheduler, GLUE_SCHEDULE).
Each FIFO queue has a FIFO DLQ (<source>-dlq.fifo, maxReceiveCount 3).
```

## 1. Environment
Windows + PowerShell + VS Code + uv. Real AWS account, us-east-1. Snowflake trial (AWS us-east-1).

## 2. Contract (Task 1)
Key `raw/{jobs|benchmarks|fx}/YYYY/MM/DD/HHMM.json`. S3 metadata (x-amz-meta-*):
`source`, `ingested-at`, `record-count`, `response-status`, `schema-version`. Same fields in file `contract` block; raw API body in `payload`.

## 3. Targets
| Source | Store | Shape |
|---|---|---|
| jobs | Snowflake `HCM.TALENT.JOB_POSTINGS` | 1 row / control number, MERGE upsert, FIRST_SEEN_AT/LAST_SEEN_AT, pay annualized (PA×1, PH×2087, PD×260, BW×26…) |
| benchmarks | S3 `processed/benchmarks/YYYY/MM/DD/HHMM.json` + `.csv` + `latest.json` | top 10 by median annual pay, rank 1-10, pay_year from header |
| fx | SQL Server `hcm.dbo.fx_rates` | PK (currency, rate_ts), MERGE upsert, DECIMAL(19,6) |

## 4. Config (.env)
`AWS_*`, `BUCKET_OWNER`, `MOCK_SOURCES`, `GLUE_SCHEDULE`, `USAJOBS_*`, `BLS_*`, `OXR_APP_ID`, `FX_SYMBOLS`.
Secrets in AWS Secrets Manager: `hcm/sqlserver` (auto-generated), `hcm/snowflake` (key-pair, from `snowflake_setup`).

## 5. Commands (PowerShell)
| Command | Does |
|---|---|
| `uv run python -m polarapt.deploy` | create/update everything (idempotent) |
| `uv run python -m polarapt.snowflake_setup --account <id>` | key pair + SQL for Snowsight + secret |
| `uv run python -m polarapt.status --watch 15` | live health screen |
| `uv run python -m polarapt.deploy --pause / --resume / --destroy` | ops |
| `uv run pytest -q` | parser + contract tests |

## 6. Known trade-offs (say these to judges)
- SQL Server is public on 1433 so Lambda needs no VPC/NAT (hackathon speed); production → private subnet + VPC Lambda + Secrets rotation.
- Glue Python shell installs snowflake-connector each run (~1 min); production → wheel in S3 / Glue 4 Spark job.
- OXR free plan 1,000 calls/month: every-minute ingestion is fine for the event; pause afterwards.
- FIFO: a failing message blocks its group until retried 3× → DLQ (ordering guarantee).

## 7. Changelog
- v1.0 22:30 local design · v1.1 23:10 AWS Lambda deploy
- **v2.0 00:40 rebuilt to PDF: 3 named ingest Lambdas, bucket naming, S3 metadata contract, SNS→router→SNS FIFO→SQS FIFO, S3 processed / SQL Server / Glue→Snowflake.**
