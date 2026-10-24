"""Shared SQS FIFO consumer used by the EventBridge-scheduled processors."""
from __future__ import annotations

import json
import time

from .aws import client, queue_url
from .log import log


def drain(queue_name: str, fn, budget_s: int = 50) -> dict:
    """Process messages until the queue is empty or the time budget ends.
    A message is deleted only after fn succeeds. On failure we stop (keeps FIFO order); SQS retries it,
    and after 3 receives it moves to the DLQ."""
    sqs, url = client("sqs"), queue_url(queue_name)
    t0, ok, failed = time.time(), 0, 0
    while time.time() - t0 < budget_s:
        msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1).get("Messages", [])
        if not msgs:
            break
        for m in msgs:
            try:
                fn(json.loads(m["Body"]))
                sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
                ok += 1
            except Exception as exc:  # noqa: BLE001
                failed += 1
                log(queue_name, "process_error", error=f"{type(exc).__name__}: {exc}"[:500])
                return {"processed": ok, "failed": failed}
    return {"processed": ok, "failed": failed}
