"""One-line JSON logs: easy to grep, easy to paste into a debugging chat."""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone


def log(component: str, event: str, **fields) -> None:
    rec = {"ts": datetime.now(timezone.utc).strftime("%H:%M:%S"), "c": component, "e": event, **fields}
    print(json.dumps(rec, default=str), file=sys.stderr if event.endswith("error") else sys.stdout, flush=True)
