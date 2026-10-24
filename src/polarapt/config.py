"""Single source of config and resource names (SPEC v2 §4). Names follow the hackathon PDF exactly."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

ROOT = Path(os.getenv("POLARAPT_ROOT") or Path(__file__).resolve().parents[2])
FIXTURES = ROOT / "fixtures"
SOURCES = ("jobs", "benchmarks", "fx")
SOURCE_NAMES = SOURCES  # back-compat alias


def _env(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip()


@dataclass(frozen=True)
class Config:
    region: str
    owner: str
    mock: bool
    usajobs_key: str
    usajobs_ua: str
    usajobs_per_page: int
    usajobs_keyword: str
    oxr_app_id: str
    fx_symbols: tuple[str, ...]
    bls_url: str
    bls_top_n: int
    bls_ua: str
    glue_schedule: str
    bucket_override: str

    # ---------- names (PDF) ----------
    @property
    def bucket(self) -> str:
        return self.bucket_override or f"data-hackathon-smit-{self.owner}"

    ingest_lambda = {"jobs": "lambda_jobs_ingest", "benchmarks": "lambda_benchmarks_scraper", "fx": "lambda_fx_ingest"}
    router_lambda = "lambda_contract_router"
    benchmarks_lambda = "lambda_benchmarks_transform"
    fx_lambda = "lambda_fx_to_sqlserver"
    glue_job = "glue_jobs_to_snowflake"
    raw_topic = "hcm-raw-events"
    routed_topic = "hcm-routed-events.fifo"
    lambda_role = "hcm-lambda-role"
    glue_role = "hcm-glue-role"
    scheduler_role = "hcm-scheduler-role"
    glue_schedule_name = "hcm-process-jobs-glue"
    rds_id = "hcm-sqlserver"
    rds_sg = "hcm-sqlserver-sg"
    sql_secret = "hcm/sqlserver"
    snowflake_secret = "hcm/snowflake"
    sql_database = "hcm"

    @staticmethod
    def queue(source: str) -> str:
        return f"{source}-queue.fifo"

    @staticmethod
    def dlq(source: str) -> str:
        return f"{source}-dlq.fifo"

    @staticmethod
    def ingest_rule(source: str) -> str:
        return f"hcm-ingest-{source}-every-minute"

    @staticmethod
    def process_rule(source: str) -> str:
        return f"hcm-process-{source}-every-minute"


@lru_cache(maxsize=1)
def cfg() -> Config:
    owner = re.sub(r"[^a-z0-9-]", "", _env("BUCKET_OWNER", "yourname").lower()) or "yourname"
    return Config(
        region=_env("AWS_REGION", "us-east-1"),
        owner=owner,
        mock=_env("MOCK_SOURCES", "false").lower() in ("1", "true", "yes"),
        usajobs_key=_env("USAJOBS_API_KEY"),
        usajobs_ua=_env("USAJOBS_USER_AGENT"),
        usajobs_per_page=int(_env("USAJOBS_RESULTS_PER_PAGE", "500")),
        usajobs_keyword=_env("USAJOBS_KEYWORD"),
        oxr_app_id=_env("OXR_APP_ID"),
        fx_symbols=tuple(s.strip().upper() for s in _env("FX_SYMBOLS", "PKR,SAR,AED,GBP,EUR,INR").split(",") if s.strip()),
        bls_url=_env("BLS_URL", "https://www.bls.gov/ooh/highest-paying.htm"),
        bls_top_n=int(_env("BLS_TOP_N", "10")),
        bls_ua=_env("BLS_USER_AGENT", "Mozilla/5.0 (compatible; PolaraptHCM/1.0; hackathon)"),
        glue_schedule=_env("GLUE_SCHEDULE", "rate(5 minutes)"),
        bucket_override=_env("BUCKET_NAME"),
    )
