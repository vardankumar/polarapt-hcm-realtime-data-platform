"""Raw-zone contract (PDF Task 1): key layout + metadata stored BOTH as S3 object metadata and inside the file.

Key:      raw/{source}/YYYY/MM/DD/{HHMM}.json
Metadata: source, ingested-at, record-count, response-status, schema-version
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from .config import SOURCES

SCHEMA_VERSION = "1"


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def raw_key(source: str, ts: datetime) -> str:
    return f"raw/{source}/{ts:%Y/%m/%d}/{ts:%H%M}.json"


def processed_key(raw: str, ext: str = "json") -> str:
    return "processed/" + raw.split("/", 1)[1].rsplit(".", 1)[0] + f".{ext}"


def source_from_key(key: str) -> str:
    parts = key.split("/")
    if len(parts) < 3 or parts[0] != "raw" or parts[1] not in SOURCES:
        raise ValueError(f"key outside raw zone contract: {key}")
    return parts[1]


def build(source: str, ts: datetime, record_count: int, response_status: int, extra: dict | None = None) -> dict:
    return {"source": source, "ingested_at": iso(ts), "record_count": int(record_count),
            "response_status": int(response_status), "schema_version": SCHEMA_VERSION, **(extra or {})}


def s3_metadata(contract: dict) -> dict:
    """S3 user metadata must be str->str; keys become x-amz-meta-<key>."""
    return {"source": contract["source"], "ingested-at": contract["ingested_at"],
            "record-count": str(contract["record_count"]), "response-status": str(contract["response_status"]),
            "schema-version": contract["schema_version"]}


def envelope(contract: dict, payload) -> dict:
    return {"contract": contract, "source": contract["source"], "fetched_at": contract["ingested_at"], "payload": payload}


def put_raw(source: str, payload, record_count: int, response_status: int, extra: dict | None = None) -> tuple[str, dict]:
    from .aws import client
    from .config import cfg

    ts = utcnow()
    c = build(source, ts, record_count, response_status, extra)
    key = raw_key(source, ts)
    client("s3").put_object(Bucket=cfg().bucket, Key=key, ContentType="application/json",
                            Body=json.dumps(envelope(c, payload), ensure_ascii=False).encode(),
                            Metadata=s3_metadata(c))
    return key, c
