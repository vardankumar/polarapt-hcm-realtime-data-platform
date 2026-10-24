"""Task 2: lambda_fx_to_sqlserver (EventBridge every minute).
Drains fx-queue.fifo and MERGEs rates into SQL Server (Amazon RDS, SQL Server Express): hcm.dbo.fx_rates.
While the database is still being created, it does nothing, so messages wait safely in the queue.
"""
from __future__ import annotations

import json

from .aws import client
from .config import cfg
from .drain import drain
from .log import log
from .processors import fx

DDL = """
IF OBJECT_ID('dbo.fx_rates', 'U') IS NULL
CREATE TABLE dbo.fx_rates (
    currency      CHAR(3)        NOT NULL,
    rate_ts       BIGINT         NOT NULL,
    base          CHAR(3)        NOT NULL,
    rate          DECIMAL(19,6)  NOT NULL,
    as_of         DATETIME2(0)   NOT NULL,
    fetched_at    DATETIME2(0)   NOT NULL,
    raw_key       NVARCHAR(400)  NOT NULL,
    loaded_at     DATETIME2(0)   NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT pk_fx_rates PRIMARY KEY (currency, rate_ts)
);
"""

MERGE = """
MERGE dbo.fx_rates AS t
USING (SELECT %s AS currency, %s AS rate_ts, %s AS base, %s AS rate, %s AS as_of, %s AS fetched_at, %s AS raw_key) AS s
   ON t.currency = s.currency AND t.rate_ts = s.rate_ts
WHEN MATCHED THEN UPDATE SET rate = s.rate, fetched_at = s.fetched_at, raw_key = s.raw_key, loaded_at = SYSUTCDATETIME()
WHEN NOT MATCHED THEN INSERT (currency, rate_ts, base, rate, as_of, fetched_at, raw_key)
     VALUES (s.currency, s.rate_ts, s.base, s.rate, s.as_of, s.fetched_at, s.raw_key);
"""

_conn = None  # reused across warm invocations


def db_endpoint() -> str | None:
    try:
        db = client("rds").describe_db_instances(DBInstanceIdentifier=cfg().rds_id)["DBInstances"][0]
    except Exception:  # noqa: BLE001
        return None
    return db["Endpoint"]["Address"] if db.get("DBInstanceStatus") == "available" and db.get("Endpoint") else None


def connect(host: str):
    import pymssql

    sec = json.loads(client("secretsmanager").get_secret_value(SecretId=cfg().sql_secret)["SecretString"])
    with pymssql.connect(server=host, user=sec["username"], password=sec["password"], database="master",
                         port=1433, login_timeout=15, autocommit=True) as m:
        m.cursor().execute(f"IF DB_ID('{cfg().sql_database}') IS NULL CREATE DATABASE {cfg().sql_database}")
    conn = pymssql.connect(server=host, user=sec["username"], password=sec["password"],
                           database=cfg().sql_database, port=1433, login_timeout=15)
    conn.cursor().execute(DDL)
    conn.commit()
    return conn


def _sql_ts(iso_z: str) -> str:
    return iso_z.replace("T", " ").replace("Z", "")


def process_message(msg: dict) -> int:
    env = json.loads(client("s3").get_object(Bucket=msg["bucket"], Key=msg["key"])["Body"].read())
    rows = fx.parse(env, cfg().fx_symbols)
    cur = _conn.cursor()
    for r in rows:
        cur.execute(MERGE, (r["currency"], r["rate_ts"], r["base"], r["rate"], _sql_ts(r["as_of"]),
                            _sql_ts(r["fetched_at"]), msg["key"]))
    _conn.commit()
    log("fx", "merged", raw=msg["key"], rows=len(rows))
    return len(rows)


def handler(event=None, context=None):
    global _conn
    host = db_endpoint()
    if not host:
        log("fx", "waiting_for_sqlserver", detail="RDS not available yet; messages stay queued")
        return {"skipped": "sqlserver not available yet"}
    if _conn is None:
        _conn = connect(host)
    try:
        return drain(cfg().queue("fx"), process_message)
    except Exception:
        _conn = None  # force reconnect next time
        raise
