"""Fault-event grouping, querying and acknowledgment.

Event definition
----------------
An event is a maximal run of ``critical`` logs for one device where every
pair of chronologically adjacent logs is at most five minutes apart and the
run contains at least three logs.  A run of one or two logs is not an event.
Logs with any other severity never form, split or join events.

Event identity
--------------
The identifier is derived from the actual set of logs, not from a mutable
row::

    event_id = "evt_" + sha256(device_id, sorted(log_ids))[:32]

Adding a log that extends or merges a run therefore changes the identifier.
An acknowledgment recorded for the old identifier can no longer match (or
acknowledge) the changed event, without any background invalidation job.

Acknowledgments
---------------
``fault_event_acks`` has one row per ``(device_id, event_id)``.  A repeated
acknowledgment is ``INSERT ... ON CONFLICT DO NOTHING`` followed by a read,
so it is idempotent.

Critical-log writes and acknowledgments take the same transaction-scoped
advisory lock, keyed by device id.  The lock is taken *before* validation, so
an acknowledgment is checked against exactly the logs committed before it
(and never against a write still in flight).  A failed write rolls its log
and its lock back together.  Locks are per device, so other devices are not
blocked.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional, Sequence

from .db import connect

EVENT_GAP = timedelta(minutes=5)
MIN_EVENT_LOGS = 3
MAX_EVENT_RANGE = timedelta(hours=24)
EVENT_ID_PREFIX = "evt_"
EVENT_ID_HEX_CHARS = 32

# Advisory-lock namespaces.  The one-argument index-build key is separate.
SCHEMA_LOCK_KEY = 861_227_002
DEVICE_LOCK_NAMESPACE = 861_227_003

_ACK_COLUMNS = (
    "device_id, event_id, first_log_id, last_log_id, log_ids, "
    "acked_at, acknowledged_by"
)

ENSURE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS fault_event_acks (
    device_id       TEXT        NOT NULL,
    event_id        TEXT        NOT NULL,
    first_log_id    BIGINT      NOT NULL,
    last_log_id     BIGINT      NOT NULL,
    log_ids         BIGINT[]    NOT NULL,
    acked_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    acknowledged_by TEXT,
    PRIMARY KEY (device_id, event_id)
);
COMMENT ON TABLE fault_event_acks IS
    'Acknowledgment for one exact fault-event log set. Event ids are content
     hashes, so a changed event has a different id and never inherits an ack.';
"""


class EventChanged(Exception):
    """The supplied identifier no longer describes an existing event."""


@dataclass(frozen=True)
class LogRow:
    id: int
    occurred_at: datetime


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def to_utc(value: datetime) -> datetime:
    """Normalize a datetime to UTC; a naive value is interpreted as UTC."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def iso_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def compute_event_id(device_id: str, log_ids: Iterable[int]) -> str:
    """Stable content-derived identifier for an exact set of logs."""
    ids = sorted(set(int(i) for i in log_ids))
    payload = f"v1\x00{device_id}\x00" + ",".join(str(i) for i in ids)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return EVENT_ID_PREFIX + digest[:EVENT_ID_HEX_CHARS]


def validate_range(start: datetime, end: datetime) -> tuple[datetime, datetime]:
    start, end = to_utc(start), to_utc(end)
    if end <= start:
        raise ValueError("end must be later than start")
    if end - start > MAX_EVENT_RANGE:
        raise ValueError("time range must not exceed 24 hours")
    return start, end


def ensure_schema() -> None:
    """Create the ack table on databases initialized before this feature."""
    with connect(autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(%s)", (SCHEMA_LOCK_KEY,))
        try:
            conn.execute(ENSURE_SCHEMA_SQL)
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (SCHEMA_LOCK_KEY,))


def lock_device(conn, device_id: str) -> None:
    """Serialize critical writes and acks for one device (transaction scope).

    Uses a 64-bit advisory key derived from the device id; different devices
    take different keys and do not block each other.
    """
    conn.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, %s))",
        (device_id, DEVICE_LOCK_NAMESPACE),
    )


# ---------------------------------------------------------------------------
# Grouping
# ---------------------------------------------------------------------------

def group_events(logs: Sequence[LogRow]) -> list[list[LogRow]]:
    """Split chronologically ordered critical logs into maximal 5-minute runs."""
    groups: list[list[LogRow]] = []
    current: list[LogRow] = []
    for row in logs:
        if current and row.occurred_at - current[-1].occurred_at > EVENT_GAP:
            groups.append(current)
            current = []
        current.append(row)
    if current:
        groups.append(current)
    return groups


def _critical_rows(conn, device_id: str, start: datetime, end: datetime) -> list[LogRow]:
    rows = conn.execute(
        """
        SELECT id, occurred_at
        FROM device_fault_logs
        WHERE device_id = %s
          AND severity = 'critical'
          AND occurred_at >= %s
          AND occurred_at <= %s
        ORDER BY occurred_at, id
        """,
        (device_id, start, end),
    ).fetchall()
    return [LogRow(int(r[0]), r[1]) for r in rows]


def _extend_before(conn, device_id: str, group: list[LogRow]) -> list[LogRow]:
    """Pull the maximal run that continues before ``group`` into the group.

    Pagination uses the full (occurred_at, id) key so that runs containing
    more than one page of logs sharing a timestamp are never truncated.
    """
    cut_at, cut_id = group[0].occurred_at, group[0].id
    boundary = group[0].occurred_at  # earliest timestamp accepted so far
    while True:
        rows = conn.execute(
            """
            SELECT id, occurred_at
            FROM device_fault_logs
            WHERE device_id = %s AND severity = 'critical'
              AND (occurred_at, id) < (%s, %s)
            ORDER BY occurred_at DESC, id DESC
            LIMIT 500
            """,
            (device_id, cut_at, cut_id),
        ).fetchall()
        if not rows:
            return group
        added: list[LogRow] = []
        exhausted = False
        for raw in rows:
            row = LogRow(int(raw[0]), raw[1])
            if boundary - row.occurred_at > EVENT_GAP:
                exhausted = True
                break
            added.append(row)
            boundary = row.occurred_at
            cut_at, cut_id = row.occurred_at, row.id
        group = list(reversed(added)) + group
        if exhausted or len(rows) < 500:
            return group


def _extend_after(conn, device_id: str, group: list[LogRow]) -> list[LogRow]:
    """Pull the maximal run that continues after ``group`` into the group."""
    cut_at, cut_id = group[-1].occurred_at, group[-1].id
    boundary = group[-1].occurred_at  # latest timestamp accepted so far
    while True:
        rows = conn.execute(
            """
            SELECT id, occurred_at
            FROM device_fault_logs
            WHERE device_id = %s AND severity = 'critical'
              AND (occurred_at, id) > (%s, %s)
            ORDER BY occurred_at ASC, id ASC
            LIMIT 500
            """,
            (device_id, cut_at, cut_id),
        ).fetchall()
        if not rows:
            return group
        added: list[LogRow] = []
        exhausted = False
        for raw in rows:
            row = LogRow(int(raw[0]), raw[1])
            if row.occurred_at - boundary > EVENT_GAP:
                exhausted = True
                break
            added.append(row)
            boundary = row.occurred_at
            cut_at, cut_id = row.occurred_at, row.id
        group = group + added
        if exhausted or len(rows) < 500:
            return group


def _extend_event(conn, device_id: str, group: list[LogRow]) -> list[LogRow]:
    return _extend_after(conn, device_id, _extend_before(conn, device_id, group))


def _ack_map(conn, device_id: str, event_ids: Sequence[str]) -> dict:
    if not event_ids:
        return {}
    rows = conn.execute(
        f"""
        SELECT {_ACK_COLUMNS}
        FROM fault_event_acks
        WHERE device_id = %s AND event_id = ANY(%s)
        """,
        (device_id, list(event_ids)),
    ).fetchall()
    return {r[1]: r for r in rows}


def _event_dict(device_id: str, group: list[LogRow], ack) -> dict:
    ids = [row.id for row in group]
    event_id = compute_event_id(device_id, ids)
    return {
        "event_id": event_id,
        "device_id": device_id,
        "start": iso_z(group[0].occurred_at),
        "end": iso_z(group[-1].occurred_at),
        "first_log_id": ids[0],
        "last_log_id": ids[-1],
        "log_count": len(ids),
        "log_ids": ids,
        "acknowledged": ack is not None,
        "acknowledged_at": iso_z(ack[5]) if ack is not None else None,
        "acknowledged_by": ack[6] if ack is not None else None,
    }


# ---------------------------------------------------------------------------
# Public operations
# ---------------------------------------------------------------------------

def list_events(device_id: str, start: datetime, end: datetime) -> dict:
    """Return events with at least one log in ``[start, end]``.

    An event that straddles a window boundary is returned whole: its ``start``
    can precede the requested start and its ``end`` can exceed the requested
    end.  The window is expanded over a repeatable-read snapshot, so the
    reported log set always forms exactly one complete event and the
    ``event_id`` is identical no matter which window surfaced the event.
    """
    start, end = validate_range(start, end)
    with connect() as conn:
        conn.execute(
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
        )
        groups = [
            _extend_event(conn, device_id, group)
            for group in group_events(_critical_rows(conn, device_id, start, end))
        ]
        groups = [g for g in groups if len(g) >= MIN_EVENT_LOGS]
        event_ids = [compute_event_id(device_id, [r.id for r in g]) for g in groups]
        acks = _ack_map(conn, device_id, event_ids)
        events = [
            _event_dict(device_id, g, acks.get(eid))
            for g, eid in zip(groups, event_ids)
        ]
    return {
        "device_id": device_id,
        "start": iso_z(start),
        "end": iso_z(end),
        "count": len(events),
        "events": events,
    }


def _load_seen_logs(conn, device_id: str, log_ids: Sequence[int]) -> list[LogRow]:
    rows = conn.execute(
        """
        SELECT id, occurred_at, severity
        FROM device_fault_logs
        WHERE device_id = %s AND id = ANY(%s)
        ORDER BY occurred_at, id
        """,
        (device_id, list(log_ids)),
    ).fetchall()
    if len(rows) != len(set(log_ids)):
        raise EventChanged("one or more log ids do not belong to this device")
    if any(r[2] != "critical" for r in rows):
        raise EventChanged("the supplied log set contains non-critical logs")
    return [LogRow(int(r[0]), r[1]) for r in rows]


def _validate_exact_event(
    conn, device_id: str, event_id: str, log_ids: Sequence[int]
) -> list[LogRow]:
    ids = set(int(i) for i in log_ids)
    rows = _load_seen_logs(conn, device_id, list(ids))
    if len(rows) < MIN_EVENT_LOGS:
        raise EventChanged("an event needs at least three critical logs")
    for prev, cur in zip(rows, rows[1:]):
        if cur.occurred_at - prev.occurred_at > EVENT_GAP:
            raise EventChanged("the supplied logs are separated by more than 5 minutes")

    first_at, last_at = rows[0].occurred_at, rows[-1].occurred_at
    # No critical log may sit inside the span without being part of the set.
    inside = conn.execute(
        """
        SELECT count(*)
        FROM device_fault_logs
        WHERE device_id = %s AND severity = 'critical'
          AND occurred_at >= %s AND occurred_at <= %s
          AND NOT (id = ANY(%s))
        """,
        (device_id, first_at, last_at, list(ids)),
    ).fetchone()[0]
    if inside:
        raise EventChanged("the event now contains additional logs")

    # The nearest critical log outside the set must be more than 5 minutes
    # away at both ends; otherwise the event has been extended.
    prev = conn.execute(
        """
        SELECT occurred_at
        FROM device_fault_logs
        WHERE device_id = %s AND severity = 'critical' AND occurred_at < %s
        ORDER BY occurred_at DESC, id DESC
        LIMIT 1
        """,
        (device_id, first_at),
    ).fetchone()
    if prev is not None and first_at - prev[0] <= EVENT_GAP:
        raise EventChanged("the event has been extended before the seen logs")

    nxt = conn.execute(
        """
        SELECT occurred_at
        FROM device_fault_logs
        WHERE device_id = %s AND severity = 'critical' AND occurred_at > %s
        ORDER BY occurred_at ASC, id ASC
        LIMIT 1
        """,
        (device_id, last_at),
    ).fetchone()
    if nxt is not None and nxt[0] - last_at <= EVENT_GAP:
        raise EventChanged("the event has been extended after the seen logs")

    if compute_event_id(device_id, ids) != event_id:
        raise EventChanged("event id does not match the supplied log set")
    return rows


def acknowledge_event(
    device_id: str,
    event_id: str,
    log_ids: Sequence[int],
    acknowledged_by: Optional[str] = None,
) -> dict:
    """Acknowledge exactly the seen log set, idempotently.

    Validation and insert happen under the per-device lock, after which no
    concurrent critical write for this device can interleave.  Raises
    ``EventChanged`` if the id no longer describes the current event.
    """
    unique_ids = sorted(set(int(i) for i in log_ids))
    with connect() as conn:
        lock_device(conn, device_id)
        rows = _validate_exact_event(conn, device_id, event_id, unique_ids)
        inserted = conn.execute(
            """
            INSERT INTO fault_event_acks
                (device_id, event_id, first_log_id, last_log_id, log_ids,
                 acknowledged_by)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (device_id, event_id) DO NOTHING
            RETURNING acked_at
            """,
            (device_id, event_id, rows[0].id, rows[-1].id, unique_ids,
             acknowledged_by),
        ).fetchone()
        ack = conn.execute(
            f"""
            SELECT {_ACK_COLUMNS}
            FROM fault_event_acks
            WHERE device_id = %s AND event_id = %s
            """,
            (device_id, event_id),
        ).fetchone()
    return {
        "device_id": device_id,
        "event_id": event_id,
        "acknowledged": True,
        "already_acknowledged": inserted is None,
        "start": iso_z(rows[0].occurred_at),
        "end": iso_z(rows[-1].occurred_at),
        "first_log_id": rows[0].id,
        "last_log_id": rows[-1].id,
        "log_count": len(rows),
        "log_ids": [r.id for r in rows],
        "acknowledged_at": iso_z(ack[5]),
        "acknowledged_by": ack[6],
    }