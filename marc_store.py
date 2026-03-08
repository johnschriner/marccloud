import secrets
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

from pymarc import Record


@dataclass
class StoredSet:
    created_ts: float
    last_access_ts: float
    filename: str
    records: List[Record]
    included: List[bool]


_STORE: Dict[str, StoredSet] = {}


def new_session_id() -> str:
    return secrets.token_urlsafe(24)


def put(session_id: str, filename: str, records: List[Record]) -> None:
    now = time.time()
    _STORE[session_id] = StoredSet(
        created_ts=now,
        last_access_ts=now,
        filename=filename,
        records=records,
        included=[True] * len(records),
    )


def get(session_id: str) -> Optional[StoredSet]:
    s = _STORE.get(session_id)
    if not s:
        return None

    # keep included aligned if record count changed somehow
    if len(s.included) != len(s.records):
        s.included = [True] * len(s.records)

    return s


def touch(session_id: str) -> None:
    s = _STORE.get(session_id)
    if s:
        s.last_access_ts = time.time()


def delete(session_id: str) -> None:
    _STORE.pop(session_id, None)


def purge_older_than(seconds: int = 14400) -> int:
    """
    Remove sessions inactive longer than `seconds`.
    Default: 4 hours.
    """
    now = time.time()
    dead = [sid for sid, s in _STORE.items() if (now - s.last_access_ts) > seconds]
    for sid in dead:
        _STORE.pop(sid, None)
    return len(dead)