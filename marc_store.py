import secrets
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

from pymarc import Record


@dataclass
class StoredSet:
    created_ts: float
    filename: str
    records: List[Record]


_STORE: Dict[str, StoredSet] = {}


def new_session_id() -> str:
    return secrets.token_urlsafe(24)


def put(session_id: str, filename: str, records: List[Record]) -> None:
    _STORE[session_id] = StoredSet(created_ts=time.time(), filename=filename, records=records)


def get(session_id: str) -> Optional[StoredSet]:
    return _STORE.get(session_id)


def purge_older_than(seconds: int = 3600) -> int:
    """Best-effort cleanup; returns number removed."""
    now = time.time()
    dead = [sid for sid, s in _STORE.items() if (now - s.created_ts) > seconds]
    for sid in dead:
        _STORE.pop(sid, None)
    return len(dead)
