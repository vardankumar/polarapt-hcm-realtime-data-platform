"""boto3 factories (real AWS only in v2)."""
from __future__ import annotations

import threading

import boto3

from .config import cfg

_local = threading.local()


def _session() -> boto3.session.Session:
    if not hasattr(_local, "session"):
        _local.session = boto3.session.Session(region_name=cfg().region)
    return _local.session


def client(service: str):
    return _session().client(service)


def queue_url(name: str) -> str:
    return client("sqs").get_queue_url(QueueName=name)["QueueUrl"]
