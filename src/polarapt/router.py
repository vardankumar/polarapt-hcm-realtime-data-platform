"""Task 2 step 1: lambda_contract_router.

S3 ObjectCreated -> SNS standard topic hcm-raw-events -> this Lambda.
Reads the contract from the object's S3 metadata and republishes to the SNS FIFO topic
hcm-routed-events.fifo with a `source` message attribute. SNS filter policies deliver to the right SQS FIFO queue.
"""
from __future__ import annotations

import hashlib
import json
import os
from urllib.parse import unquote_plus

from .aws import client
from .config import SOURCES
from .log import log


def route_object(bucket: str, key: str, topic_arn: str) -> str | None:
    md = client("s3").head_object(Bucket=bucket, Key=key)["Metadata"]  # S3 lower-cases metadata keys
    source = md.get("source")
    if source not in SOURCES:
        log("router", "skip_no_contract", key=key)
        return None
    body = {"bucket": bucket, "key": key, "source": source, "ingested_at": md.get("ingested-at"),
            "record_count": int(md.get("record-count", 0)), "response_status": int(md.get("response-status", 0))}
    client("sns").publish(
        TopicArn=topic_arn, Message=json.dumps(body),
        MessageGroupId=source,  # FIFO ordering per source
        MessageDeduplicationId=hashlib.sha256(key.encode()).hexdigest(),
        MessageAttributes={"source": {"DataType": "String", "StringValue": source}},
    )
    log("router", "routed", key=key, source=source, records=body["record_count"])
    return source


def handler(event, context=None):
    topic = os.environ["ROUTED_TOPIC_ARN"]
    routed = []
    for rec in event.get("Records", []):
        msg = json.loads(rec["Sns"]["Message"])
        if msg.get("Event") == "s3:TestEvent":
            continue
        for r in msg.get("Records", []):
            routed.append(route_object(r["s3"]["bucket"]["name"], unquote_plus(r["s3"]["object"]["key"]), topic))
    return {"routed": routed}
