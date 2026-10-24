"""Task 2: glue_jobs_to_snowflake (AWS Glue Python shell 3.9, triggered by EventBridge Scheduler).

Drains jobs-queue.fifo, reads each raw USAJOBS file from S3, parses the postings and MERGEs them into
Snowflake HCM.TALENT.JOB_POSTINGS (upsert by control number; FIRST_SEEN_AT kept on update).
Self-contained on purpose: Glue Python shell does not get the polarapt package.
Job arguments: --QUEUE_NAME  --SECRET_NAME  --REGION
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

import boto3

try:
    from awsglue.utils import getResolvedOptions

    ARGS = getResolvedOptions(sys.argv, ["QUEUE_NAME", "SECRET_NAME", "REGION"])
except ImportError:  # local run: python jobs_to_snowflake.py --QUEUE_NAME x --SECRET_NAME y --REGION z
    import argparse

    ap = argparse.ArgumentParser()
    for a in ("QUEUE_NAME", "SECRET_NAME", "REGION"):
        ap.add_argument(f"--{a}", required=True)
    ARGS = vars(ap.parse_known_args()[0])

ANNUALIZE = {"PA": 1, "PH": 2087, "PD": 260, "PW": 52, "BW": 26, "SM": 24, "PM": 12}
COLS = ["CONTROL_NUMBER", "POSITION_ID", "TITLE", "ORGANIZATION", "DEPARTMENT", "LOCATION_DISPLAY", "CITY",
        "STATE", "COUNTRY", "LOCATION_COUNT", "PAY_MIN", "PAY_MAX", "PAY_INTERVAL", "PAY_MIN_ANNUAL",
        "PAY_MAX_ANNUAL", "OPEN_DATE", "CLOSE_DATE", "POSITION_URL", "RAW_KEY", "INGESTED_AT"]

DDL = """
CREATE TABLE IF NOT EXISTS JOB_POSTINGS (
  CONTROL_NUMBER STRING PRIMARY KEY, POSITION_ID STRING, TITLE STRING, ORGANIZATION STRING, DEPARTMENT STRING,
  LOCATION_DISPLAY STRING, CITY STRING, STATE STRING, COUNTRY STRING, LOCATION_COUNT INTEGER,
  PAY_MIN NUMBER(12,2), PAY_MAX NUMBER(12,2), PAY_INTERVAL STRING, PAY_MIN_ANNUAL NUMBER(12,0), PAY_MAX_ANNUAL NUMBER(12,0),
  OPEN_DATE TIMESTAMP_NTZ, CLOSE_DATE TIMESTAMP_NTZ, POSITION_URL STRING, RAW_KEY STRING, INGESTED_AT TIMESTAMP_NTZ,
  FIRST_SEEN_AT TIMESTAMP_NTZ, LAST_SEEN_AT TIMESTAMP_NTZ
)"""


def _num(v):
    try:
        return None if v in (None, "") else float(str(v).replace(",", ""))
    except ValueError:
        return None


def _ts(v):
    return v[:19].replace("T", " ") if v else None


def parse(env: dict) -> list:
    items = env["payload"]["SearchResult"]["SearchResultItems"]
    ingested = _ts(env.get("contract", {}).get("ingested_at") or env.get("fetched_at"))
    rows = {}
    for it in items:
        d = it.get("MatchedObjectDescriptor") or {}
        cn, title = str(it.get("MatchedObjectId") or "").strip(), (d.get("PositionTitle") or "").strip()
        if not cn or not title:
            continue
        rem = (d.get("PositionRemuneration") or [{}])[0] or {}
        code = (rem.get("RateIntervalCode") or "").upper() or None
        pmin, pmax = _num(rem.get("MinimumRange")), _num(rem.get("MaximumRange"))
        f = ANNUALIZE.get(code or "")
        locs = d.get("PositionLocation") or []
        loc = locs[0] if locs else {}
        rows[cn] = (cn, d.get("PositionID"), title, d.get("OrganizationName"), d.get("DepartmentName"),
                    d.get("PositionLocationDisplay"), loc.get("CityName"), loc.get("CountrySubDivisionCode"),
                    loc.get("CountryCode"), len(locs), pmin, pmax, code,
                    round(pmin * f) if pmin is not None and f else None,
                    round(pmax * f) if pmax is not None and f else None,
                    _ts(d.get("PublicationStartDate")), _ts(d.get("ApplicationCloseDate")), d.get("PositionURI"),
                    None, ingested)
    return list(rows.values())  # dedup within one file


def connect(sec: dict):
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization

    key = serialization.load_pem_private_key(sec["private_key"].encode(), password=None)
    der = key.private_bytes(serialization.Encoding.DER, serialization.PrivateFormat.PKCS8,
                            serialization.NoEncryption())
    conn = snowflake.connector.connect(account=sec["account"], user=sec["user"], private_key=der,
                                       role=sec["role"], warehouse=sec["warehouse"], database=sec["database"],
                                       schema=sec["schema"])
    conn.cursor().execute(DDL)
    return conn


def load(conn, rows: list, raw_key: str) -> int:
    rows = [r[:18] + (raw_key,) + r[19:] for r in rows]
    cur = conn.cursor()
    cur.execute("CREATE TEMPORARY TABLE IF NOT EXISTS JOB_STAGE LIKE JOB_POSTINGS")
    cur.execute("TRUNCATE TABLE JOB_STAGE")
    ph = ", ".join(["%s"] * len(COLS))
    cur.executemany(f"INSERT INTO JOB_STAGE ({', '.join(COLS)}) VALUES ({ph})", rows)
    sets = ", ".join(f"t.{c} = s.{c}" for c in COLS if c != "CONTROL_NUMBER")
    cur.execute(f"""
        MERGE INTO JOB_POSTINGS t USING JOB_STAGE s ON t.CONTROL_NUMBER = s.CONTROL_NUMBER
        WHEN MATCHED THEN UPDATE SET {sets}, t.LAST_SEEN_AT = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
        WHEN NOT MATCHED THEN INSERT ({', '.join(COLS)}, FIRST_SEEN_AT, LAST_SEEN_AT)
             VALUES ({', '.join('s.' + c for c in COLS)}, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)
    """)
    return len(rows)


def main():
    region = ARGS["REGION"]
    sqs, s3 = boto3.client("sqs", region_name=region), boto3.client("s3", region_name=region)
    sec = json.loads(boto3.client("secretsmanager", region_name=region)
                     .get_secret_value(SecretId=ARGS["SECRET_NAME"])["SecretString"])
    url = sqs.get_queue_url(QueueName=ARGS["QUEUE_NAME"])["QueueUrl"]
    conn = connect(sec)
    files = total = 0
    started = datetime.now(timezone.utc)
    while (datetime.now(timezone.utc) - started).seconds < 600:
        msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=2).get("Messages", [])
        if not msgs:
            break
        for m in msgs:
            body = json.loads(m["Body"])
            env = json.loads(s3.get_object(Bucket=body["bucket"], Key=body["key"])["Body"].read())
            n = load(conn, parse(env), body["key"])  # an exception stops the run; SQS retries -> DLQ after 3
            sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
            files += 1
            total += n
            print(json.dumps({"e": "loaded", "key": body["key"], "rows": n}))
    count = conn.cursor().execute("SELECT COUNT(*) FROM JOB_POSTINGS").fetchone()[0]
    print(json.dumps({"e": "done", "files": files, "rows_merged": total, "table_rows": count}))
    conn.close()


if __name__ == "__main__":
    main()
