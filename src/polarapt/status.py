"""One-screen health view of the whole platform.  python -m polarapt.status [--watch 15]"""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone

from botocore.exceptions import ClientError

from .aws import client, queue_url
from .config import SOURCES, cfg


def _count(prefix: str) -> tuple[int, str]:
    n, last = 0, ""
    for page in client("s3").get_paginator("list_objects_v2").paginate(Bucket=cfg().bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            n += 1
            last = max(last, o["Key"])
    return n, last


def render() -> str:
    c, out = cfg(), []
    out.append(f"POLARAPT HCM PLATFORM   {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S}Z   s3://{c.bucket}")

    out.append("\nTASK 1  RAW ZONE                files  latest key  (contract from S3 metadata)")
    for s in SOURCES:
        n, last = _count(f"raw/{s}/")
        meta = ""
        if last:
            md = client("s3").head_object(Bucket=c.bucket, Key=last)["Metadata"]
            meta = f"records={md.get('record-count')} status={md.get('response-status')}"
        out.append(f"  {s:<12}{n:>6}  {last or '-'}  {meta}")

    out.append("\nTASK 2  SQS FIFO                waiting  in-flight  DLQ")
    sqs = client("sqs")
    for s in SOURCES:
        try:
            a = sqs.get_queue_attributes(QueueUrl=queue_url(c.queue(s)), AttributeNames=["All"])["Attributes"]
            d = sqs.get_queue_attributes(QueueUrl=queue_url(c.dlq(s)),
                                         AttributeNames=["ApproximateNumberOfMessages"])["Attributes"]
            flag = "  <-- check logs" if d["ApproximateNumberOfMessages"] != "0" else ""
            out.append(f"  {c.queue(s):<26}{a['ApproximateNumberOfMessages']:>8}"
                       f"{a['ApproximateNumberOfMessagesNotVisible']:>10}{d['ApproximateNumberOfMessages']:>5}{flag}")
        except ClientError:
            out.append(f"  {c.queue(s):<26} missing")

    out.append("\nTARGETS")
    n, _ = _count("processed/benchmarks/")
    try:
        latest = json.loads(client("s3").get_object(Bucket=c.bucket, Key="processed/benchmarks/latest.json")["Body"].read())
        top = latest["data"][0]
        out.append(f"  S3 processed/benchmarks   {n} files; #1 {top['occupation']} ${top['median_annual_pay']:,} "
                   f"(BLS {top['pay_year']})")
    except ClientError:
        out.append(f"  S3 processed/benchmarks   {n} files")
    try:
        db = client("rds").describe_db_instances(DBInstanceIdentifier=c.rds_id)["DBInstances"][0]
        out.append(f"  SQL Server (RDS)          {db['DBInstanceStatus']}  {db.get('Endpoint', {}).get('Address', '')}")
    except ClientError:
        out.append("  SQL Server (RDS)          not created")
    try:
        runs = client("glue").get_job_runs(JobName=c.glue_job, MaxResults=1)["JobRuns"]
        if runs:
            r = runs[0]
            err = f"  ERROR: {r.get('ErrorMessage', '')[:90]}" if r["JobRunState"] == "FAILED" else ""
            out.append(f"  Glue -> Snowflake         last run {r['JobRunState']} at {r['StartedOn']:%H:%M}{err}")
        else:
            out.append("  Glue -> Snowflake         no runs yet (needs snowflake_setup)")
    except ClientError:
        out.append("  Glue -> Snowflake         job missing")
    return "\n".join(out)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--watch", type=int, default=0)
    a = ap.parse_args()
    while True:
        try:
            text = render()
        except Exception as exc:  # network blip on the laptop: keep watching
            text = f"(could not reach AWS this round: {type(exc).__name__}; retrying)"
        if a.watch:
            print("\033[2J\033[H", end="")
        print(text, flush=True)
        if not a.watch:
            break
        time.sleep(a.watch)


if __name__ == "__main__":
    main()
