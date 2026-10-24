"""Task 2: lambda_benchmarks_transform (EventBridge every minute).
Drains benchmarks-queue.fifo, parses the raw BLS HTML into the top-10 table and writes the processed zone:
  processed/benchmarks/YYYY/MM/DD/HHMM.json  (+ .csv)  and  processed/benchmarks/latest.json
"""
from __future__ import annotations

import csv
import io
import json

from .aws import client
from .config import cfg
from .contract import iso, processed_key, utcnow
from .drain import drain
from .log import log
from .processors import bls

COLUMNS = ["rank", "occupation", "median_annual_pay", "pay_capped", "pay_source_unit", "pay_year", "profile_url"]


def process_message(msg: dict) -> int:
    s3, bucket = client("s3"), msg["bucket"]
    env = json.loads(s3.get_object(Bucket=bucket, Key=msg["key"])["Body"].read())
    rows = bls.parse(env, top_n=cfg().bls_top_n)
    contract = {"source": "benchmarks", "raw_key": msg["key"], "ingested_at": msg.get("ingested_at"),
                "processed_at": iso(utcnow()), "record_count": len(rows), "schema_version": "1"}
    doc = json.dumps({"contract": contract, "data": rows}, indent=1).encode()
    meta = {"source": "benchmarks", "record-count": str(len(rows)), "raw-key": msg["key"]}
    out = processed_key(msg["key"])
    s3.put_object(Bucket=bucket, Key=out, Body=doc, ContentType="application/json", Metadata=meta)
    s3.put_object(Bucket=bucket, Key="processed/benchmarks/latest.json", Body=doc, ContentType="application/json",
                  Metadata=meta)
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=COLUMNS, extrasaction="ignore")
    w.writeheader()
    w.writerows(rows)
    s3.put_object(Bucket=bucket, Key=processed_key(msg["key"], "csv"), Body=buf.getvalue().encode(),
                  ContentType="text/csv", Metadata=meta)
    log("benchmarks", "processed", raw=msg["key"], out=out, rows=len(rows))
    return len(rows)


def handler(event=None, context=None):
    return drain(cfg().queue("benchmarks"), process_message)
