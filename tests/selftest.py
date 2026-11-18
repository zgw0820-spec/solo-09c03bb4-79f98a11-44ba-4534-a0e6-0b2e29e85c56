#!/usr/bin/env python3
"""End-to-end self-test for the fault-log online-index platform.

Runs against two live API instances and a real PostgreSQL and verifies:

  P0  both instances up, Swagger reachable, schema present, clean slate
  P1  log write via instance 1 is readable via instance 2
  P2  12 concurrent build requests across both instances -> exactly ONE build;
      log writes keep flowing during the build; the in-progress (temporarily
      invalid) catalog entry is never deleted mid-build
  P3  further build requests are idempotent ('ready', same job, catalog-checked)
  P4a instance crash while the server-side build finishes anyway -> orphan
      claim -> failed -> next request converges to ready (no rebuild)
  P4b instance crash with the build truly interrupted -> invalid leftover ->
      orphan detection after grace -> cleanup + retry succeeds; late old-job
      writes are fenced off
  P5  startup reconcile marks the orphaned build failed; restarts keep the
      tracked state consistent with the actual index
  P6  the device/time-range query plan uses the new index
  P7  fault-event grouping, acknowledgment idempotency, and invalidation on
      backfill; 24 h range limit; concurrent critical writes vs. ack across
      both instances; a failed critical write leaves the event unchanged;
      other devices are unaffected; /logs and index build keep working

Modes:
  local   spawns/kills two local uvicorn processes (used in this sandbox)
  docker  drives `docker compose` services api1/api2

Requires psycopg (project venv). Usage:
  python3 tests/selftest.py --mode local --dsn "host=/tmp/... dbname=faultlogs user=ops"
  python3 tests/selftest.py --mode docker --dsn "postgresql://ops:ops@localhost:5433/faultlogs"
"""

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import psycopg

GRACE = float(os.environ.get("ORPHAN_GRACE_SECONDS", "15"))
INDEX_NAME = "idx_fault_logs_device_occurred"
MARKER = "test-" + uuid.uuid4().hex[:10]

RESULTS = []


def check(name, ok, detail=""):
    RESULTS.append((name, bool(ok)))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" -- {detail}" if detail else ""),
          flush=True)
    if not ok:
        raise SystemExit(f"aborting: {name} failed {detail}")


def info(msg):
    print(f"  .. {msg}", flush=True)


# ---------------------------------------------------------------------------
# HTTP / SQL helpers
# ---------------------------------------------------------------------------

def http(method, base, path, body=None, timeout=120):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode()
            return r.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            payload = json.loads(raw)
        except Exception:
            payload = {"raw": raw}
        return e.code, payload


def http_raw(base, path, timeout=30):
    with urllib.request.urlopen(base + path, timeout=timeout) as r:
        return r.status, r.read().decode(errors="replace")


class Db:
    def __init__(self, dsn):
        self.dsn = dsn

    def q(self, sql, params=None):
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            cur = conn.execute(sql, params or ())
            if cur.description is None:  # DDL/DML without RETURNING
                return []
            return cur.fetchall()

    def rowcount(self, sql, params=None):
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            return conn.execute(sql, params or ()).rowcount


def index_catalog(db):
    rows = db.q(
        """
        SELECT i.indisvalid, i.indisready, pg_get_indexdef(i.indexrelid)
        FROM pg_class c
        JOIN pg_index i ON i.indexrelid = c.oid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = %s AND n.nspname = 'public' AND c.relkind = 'i'
        """,
        (INDEX_NAME,),
    )
    return rows  # [] or [(indisvalid, indisready, def)]


def wait_health(base, timeout=240):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            code, _ = http("GET", base, "/health", timeout=5)
            if code == 200:
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


def wait_state(bases, want, timeout=240):
    """Poll /indexes/status (round-robin) until state == want."""
    deadline = time.time() + timeout
    i = 0
    last = None
    while time.time() < deadline:
        code, st = http("GET", bases[i % len(bases)], "/indexes/status", timeout=30)
        i += 1
        if code == 200:
            last = st.get("state")
            if last == want:
                return st
        time.sleep(0.5)
    raise SystemExit(f"aborting: state never became {want!r} (last={last!r})")


# ---------------------------------------------------------------------------
# Instance control
# ---------------------------------------------------------------------------

class LocalInstance:
    def __init__(self, name, port, dsn, workdir, logdir):
        self.name, self.port, self.dsn, self.workdir = name, port, dsn, workdir
        self.logpath = os.path.join(logdir, f"{name}.log")
        self.proc = None

    def start(self):
        env = dict(os.environ)
        env["DATABASE_URL"] = self.dsn
        env["ORPHAN_GRACE_SECONDS"] = str(GRACE)
        self._log = open(self.logpath, "ab", buffering=0)
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(self.port)],
            cwd=self.workdir, env=env,
            stdout=self._log, stderr=subprocess.STDOUT)
        info(f"{self.name} started pid={self.proc.pid} (log: {self.logpath})")

    def kill(self):
        if self.proc and self.proc.poll() is None:
            self.proc.kill()  # SIGKILL: simulate a hard crash
            self.proc.wait()
        info(f"{self.name} killed")


class DockerInstance:
    def __init__(self, service, workdir):
        self.service, self.workdir = service, workdir

    def _compose(self, *args):
        subprocess.run(["docker", "compose", *args], cwd=self.workdir, check=True)

    def start(self):
        self._compose("start", self.service)

    def kill(self):
        self._compose("kill", self.service)


# ---------------------------------------------------------------------------
# Phases
# ---------------------------------------------------------------------------

def operator_reset(db):
    """Simulate the DBA wiping managed state so a fresh build is needed."""
    db.q(f'DROP INDEX CONCURRENTLY IF EXISTS "{INDEX_NAME}"')
    db.q("DELETE FROM index_build_jobs")


def parallel_builds(bases, n_per_instance):
    futs, results = [], []
    with ThreadPoolExecutor(max_workers=n_per_instance * len(bases)) as ex:
        for _ in range(n_per_instance):
            for base in bases:
                futs.append(ex.submit(http, "POST", base, "/indexes/build"))
        for f in futs:
            results.append(f.result())
    return results


def phase_p0(ctx):
    print("\n== P0: setup ==")
    if ctx["mode"] == "docker":
        subprocess.run(["docker", "compose", "up", "-d", "--build"],
                       cwd=ctx["workdir"], check=True)
    else:  # local
        subprocess.run(["pkill", "-f", "uvicorn app.main:app"],
                       capture_output=True)  # leftovers from previous runs
        time.sleep(1)
        for inst in ctx["instances"].values():
            inst.start()

    # schema gate
    deadline = time.time() + 300
    while True:
        try:
            ctx["db"].q("SELECT 1 FROM index_build_jobs LIMIT 1")
            break
        except Exception:
            if time.time() > deadline:
                raise SystemExit("schema never appeared")
            time.sleep(2)
    info("schema present")

    operator_reset(ctx["db"])
    info("operator reset done (index dropped if existed, jobs cleared)")

    for name, base in ctx["bases"].items():
        check(f"P0 {name} healthy", wait_health(base), base)
        code, body = http_raw(base, "/docs")
        check(f"P0 {name} Swagger UI", code == 200 and "swagger" in body.lower())

    for name, base in ctx["bases"].items():
        code, st = http("GET", base, "/indexes/status")
        check(f"P0 {name} initial state not_started",
              code == 200 and st["state"] == "not_started", str(st.get("state")))


def phase_p1(ctx):
    print("\n== P1: write via api1, read via api2 ==")
    b1, b2 = ctx["bases"]["api1"], ctx["bases"]["api2"]
    code, r = http("POST", b1, "/logs",
                   {"device_id": MARKER, "message": "p1 sanity", "severity": "error"})
    check("P1 write log", code == 201 and r.get("id"), f"id={r.get('id')}")
    code, r = http("GET", b2,
                   f"/logs?device_id={MARKER}&start=2000-01-01T00:00:00Z"
                   f"&end=2100-01-01T00:00:00Z")
    check("P1 read log back via other instance",
          code == 200 and r["count"] >= 1
          and any(it["message"] == "p1 sanity" for it in r["items"]))


def phase_p2(ctx):
    print("\n== P2: concurrent build requests -> single build; writes flow ==")
    bases = list(ctx["bases"].values())
    responses = parallel_builds(bases, 6)

    codes = [c for c, _ in responses]
    states = [r.get("state") for _, r in responses]
    job_ids = {r.get("job_id") for _, r in responses}
    check("P2 all requests HTTP 200", all(c == 200 for c in codes), str(codes))
    check("P2 states subset of started/building",
          set(states) <= {"started", "building"}, str(states))
    check("P2 exactly one 'started'", states.count("started") == 1, str(states))
    check("P2 exactly one distinct job id", len(job_ids) == 1, str(job_ids))
    job_id = next(iter(job_ids))

    saw = {"max_active": 0, "invalid_seen": False, "gone_after_invalid": False,
           "writes_ok": 0, "writes_fail": 0}
    deadline = time.time() + 240
    i = 0
    final = None
    while time.time() < deadline:
        code, st = http("GET", bases[i % 2], "/indexes/status", timeout=30)
        i += 1
        cat = st.get("catalog", {})
        saw["max_active"] = max(saw["max_active"], st.get("active_build_backends", 0))
        if cat.get("exists") and not cat.get("valid"):
            saw["invalid_seen"] = True
        if saw["invalid_seen"] and not cat.get("exists"):
            saw["gone_after_invalid"] = True
        if st.get("state") == "building":
            c, _ = http("POST", bases[i % 2], "/logs",
                        {"device_id": MARKER, "message": "written during build"})
            if c == 201:
                saw["writes_ok"] += 1
            else:
                saw["writes_fail"] += 1
        if st.get("state") == "ready":
            final = st
            break
        time.sleep(0.25)
    check("P2 build reached ready", final is not None)
    check("P2 never more than one active build backend",
          saw["max_active"] == 1, f"max_active={saw['max_active']}")
    check("P2 in-progress invalid catalog entry observed", saw["invalid_seen"])
    check("P2 in-progress entry never deleted mid-build",
          not saw["gone_after_invalid"])
    check("P2 writes flowed during build",
          saw["writes_ok"] > 0 and saw["writes_fail"] == 0,
          f"ok={saw['writes_ok']} fail={saw['writes_fail']}")
    check("P2 ready job matches claimed job",
          final and final.get("job_id") == job_id,
          f"{final and final.get('job_id')} vs {job_id}")

    cat = index_catalog(ctx["db"])
    check("P2 catalog: exactly one index, valid+ready, correct definition",
          len(cat) == 1 and cat[0][0] and cat[0][1]
          and "(device_id, occurred_at)" in cat[0][2],
          str(cat))

    code, r = http("GET", bases[0],
                   f"/logs?device_id={MARKER}&start=2000-01-01T00:00:00Z"
                   f"&end=2100-01-01T00:00:00Z&limit=1000")
    check("P2 logs written during build are queryable",
          code == 200 and r["count"] >= saw["writes_ok"],
          f"count={r.get('count')} writes={saw['writes_ok']}")
    return job_id


def phase_p3(ctx, job_id):
    print("\n== P3: idempotent ready ==")
    bases = list(ctx["bases"].values())
    responses = parallel_builds(bases, 2)
    states = [r.get("state") for _, r in responses]
    ids = {r.get("job_id") for _, r in responses}
    check("P3 all report ready", set(states) == {"ready"}, str(states))
    check("P3 same job id, no rebuild", ids == {job_id}, str(ids))
    cat = index_catalog(ctx["db"])
    check("P3 still exactly one valid index", len(cat) == 1 and cat[0][0])


def phase_p4(ctx):
    print("\n== P4a: crash, server-side build completes -> converge ready ==")
    db = ctx["db"]
    b1, b2 = ctx["bases"]["api1"], ctx["bases"]["api2"]
    operator_reset(db)

    code, r = http("POST", b1, "/indexes/build")
    check("P4a build started on api1", r.get("state") == "started", str(r))
    job_a = r["job_id"]
    deadline = time.time() + 60
    cat = []
    while time.time() < deadline:
        cat = index_catalog(db)
        if cat and not cat[0][0]:
            break
        time.sleep(0.2)
    check("P4a invalid in-progress index present before kill",
          len(cat) == 1 and not cat[0][0], str(cat))

    ctx["instances"]["api1"].kill()
    # The backend does not notice the dead client (client_connection_check_
    # interval=0) and finishes the CONCURRENTLY build server-side.
    deadline = time.time() + 90
    while time.time() < deadline:
        cat = index_catalog(db)
        if cat and cat[0][0]:
            break
        time.sleep(0.5)
    check("P4a server-side build completed despite client crash",
          len(cat) == 1 and cat[0][0], str(cat))

    info(f"waiting {GRACE + 3:.0f}s for orphan grace to expire")
    time.sleep(GRACE + 3)
    code, st = http("GET", b2, "/indexes/status")
    check("P4a orphan claim converged to failed",
          st.get("state") == "failed", str(st.get("state")))
    check("P4a catalog shows the (valid) finished index",
          st["catalog"]["exists"] and st["catalog"]["valid"])

    code, r = http("POST", b2, "/indexes/build")
    check("P4a next build request converges to ready without rebuild",
          r.get("state") == "ready", str(r.get("state")))
    cat = index_catalog(db)
    check("P4a catalog still exactly one valid index",
          len(cat) == 1 and cat[0][0], str(cat))

    print("\n== P4b: crash mid-build (interrupted) -> orphan -> cleanup -> retry ==")
    ctx["instances"]["api1"].start()  # api1 was killed in P4a
    check("P4b api1 healthy again", wait_health(b1))
    operator_reset(db)
    code, st = http("GET", b2, "/indexes/status")
    check("P4b reset -> not_started", st.get("state") == "not_started")

    code, r = http("POST", b1, "/indexes/build")
    check("P4b build started on api1", code == 200 and r.get("state") == "started",
          str(r))
    job_b = r["job_id"]

    # wait until the in-progress (invalid) catalog entry exists
    deadline = time.time() + 60
    cat = []
    while time.time() < deadline:
        cat = index_catalog(db)
        if cat and not cat[0][0]:
            break
        time.sleep(0.2)
    check("P4b invalid in-progress index present before kill",
          len(cat) == 1 and not cat[0][0], str(cat))

    ctx["instances"]["api1"].kill()
    # Make the interruption deterministic: terminate the build backend itself
    # (simulates e.g. db restart / OOM), aborting CREATE INDEX CONCURRENTLY.
    rows = db.q(
        "SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity "
        "WHERE pid <> pg_backend_pid() "
        "AND query ~* '^\\s*create\\s+index' AND query LIKE %s",
        (f"%{INDEX_NAME}%",))
    check("P4b terminated in-flight build backend",
          len(rows) >= 1 and all(r[1] for r in rows), str(rows))
    time.sleep(1)
    cat = index_catalog(db)
    check("P4b interrupted build leaves invalid leftover index",
          len(cat) == 1 and not cat[0][0], str(cat))
    rows = db.q("SELECT status FROM index_build_jobs WHERE id = %s",
                (uuid.UUID(job_b),))
    check("P4b job row still 'building' right after crash",
          rows and rows[0][0] == "building", str(rows))

    info(f"waiting {GRACE + 3:.0f}s for orphan grace to expire")
    time.sleep(GRACE + 3)
    code, st = http("GET", b2, "/indexes/status")
    check("P4b orphan detected -> failed", st.get("state") == "failed",
          str(st.get("state")))
    check("P4b failed state keeps invalid leftover visible",
          st["catalog"]["exists"] and not st["catalog"]["valid"])
    check("P4b no active build backend", st.get("active_build_backends") == 0)

    n = db.rowcount(
        "UPDATE index_build_jobs SET status = 'ready', error = 'late write' "
        "WHERE id = %s AND status = 'building'", (uuid.UUID(job_b),))
    check("P4b fencing: late write to old job touches 0 rows", n == 0)

    code, r = http("POST", b2, "/indexes/cleanup")
    check("P4b explicit cleanup cleaned+dropped invalid leftover",
          code == 200 and r.get("cleaned") and r.get("dropped_invalid_index"),
          str(r))
    cat = index_catalog(db)
    check("P4b invalid leftover gone after cleanup", len(cat) == 0, str(cat))
    code, st = http("GET", b2, "/indexes/status")
    check("P4b state not_started after cleanup",
          st.get("state") == "not_started", str(st.get("state")))

    responses = parallel_builds([b2], 8)
    states = [r.get("state") for _, r in responses]
    ids = {r.get("job_id") for _, r in responses}
    check("P4b retry: exactly one new job", len(ids) == 1, str(ids))
    job_c = next(iter(ids))
    check("P4b retry: new job differs from crashed one", job_c != job_b)
    check("P4b retry: one started, rest building",
          states.count("started") == 1
          and set(states) <= {"started", "building"}, str(states))

    final = wait_state([b2], "ready", timeout=240)
    check("P4b retry reached ready", final.get("job_id") == job_c)
    cat = index_catalog(db)
    check("P4b catalog: single valid index after cleanup+rebuild",
          len(cat) == 1 and cat[0][0] and cat[0][1], str(cat))

    n = db.rowcount(
        "UPDATE index_build_jobs SET status = 'failed' "
        "WHERE id = %s AND status = 'building'", (uuid.UUID(job_b),))
    check("P4b fencing: crashed job cannot overwrite new result", n == 0)

    ctx["instances"]["api1"].start()
    check("P4b api1 back healthy", wait_health(b1))
    code, st = http("GET", b1, "/indexes/status")
    check("P4b restarted api1 reports ready (consistent with catalog)",
          st.get("state") == "ready", str(st.get("state")))


def phase_p5(ctx):
    print("\n== P5: startup reconcile + restart consistency ==")
    db = ctx["db"]
    b1, b2 = ctx["bases"]["api1"], ctx["bases"]["api2"]
    operator_reset(db)

    code, r = http("POST", b2, "/indexes/build")
    check("P5 build started on api2", r.get("state") == "started", str(r))
    job_c = r["job_id"]
    deadline = time.time() + 60
    while time.time() < deadline:
        cat = index_catalog(db)
        if cat and not cat[0][0]:
            break
        time.sleep(0.2)
    check("P5 invalid in-progress index present", bool(cat) and not cat[0][0])

    ctx["instances"]["api2"].kill()
    # Deterministic interruption (as in P4b): the build must not finish
    # server-side, so terminate the backend running CREATE INDEX.
    rows = db.q(
        "SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity "
        "WHERE pid <> pg_backend_pid() "
        "AND query ~* '^\\s*create\\s+index' AND query LIKE %s",
        (f"%{INDEX_NAME}%",))
    check("P5 terminated in-flight build backend",
          len(rows) >= 1 and all(r[1] for r in rows), str(rows))
    info(f"waiting {GRACE + 3:.0f}s for orphan grace to expire")
    time.sleep(GRACE + 3)
    ctx["instances"]["api2"].start()  # startup reconcile should mark job C failed
    check("P5 api2 back healthy", wait_health(b2))
    st = wait_state([b1, b2], "failed", timeout=60)
    check("P5 reconcile marked orphaned job failed",
          st.get("job_id") == job_c, str(st.get("job_id")))

    code, r = http("POST", b1, "/indexes/build")
    check("P5 rebuild started after reconcile", r.get("state") == "started", str(r))
    wait_state([b1], "ready", timeout=240)
    cat = index_catalog(db)
    check("P5 catalog valid after rebuild", len(cat) == 1 and cat[0][0])

    for name in ("api1", "api2"):
        ctx["instances"][name].kill()
        ctx["instances"][name].start()
        check(f"P5 {name} healthy after restart", wait_health(ctx["bases"][name]))
        code, st = http("GET", ctx["bases"][name], "/indexes/status")
        check(f"P5 {name} state consistent with actual index after restart",
              st.get("state") == "ready", str(st.get("state")))


def phase_p6(ctx):
    print("\n== P6: query plan uses the index ==")
    b1 = ctx["bases"]["api1"]
    code, r = http("GET", b1,
                   "/logs/plan?device_id=dev-00001&start=2000-01-01T00:00:00Z"
                   "&end=2100-01-01T00:00:00Z")
    plan = " | ".join(r.get("plan", []))
    check("P6 plan uses idx_fault_logs_device_occurred",
          code == 200 and INDEX_NAME in plan, plan[:200])


# ---------------------------------------------------------------------------
# P7: fault events + acknowledgment
# ---------------------------------------------------------------------------

EVENT_WINDOW = "start=2026-01-01T00:00:00Z&end=2026-01-01T12:00:00Z"


def device(name):
    return f"{name}-{uuid.uuid4().hex[:8]}"


def event_time(minutes):
    return (datetime(2026, 1, 1, 6, 0, tzinfo=timezone.utc)
            + timedelta(minutes=minutes)).isoformat()


def write_critical(base, dev, minutes, severity="critical"):
    return http("POST", base, "/logs", {
        "device_id": dev, "message": f"at {minutes}",
        "severity": severity, "occurred_at": event_time(minutes),
    })


def get_events(base, dev):
    code, r = http("GET", base, f"/events?device_id={dev}&{EVENT_WINDOW}")
    return code, r


def phase_p7(ctx):
    print("\n== P7: fault events + acknowledgment ==")
    b1, b2 = ctx["bases"]["api1"], ctx["bases"]["api2"]

    # -- grouping threshold -------------------------------------------------
    dev = device("evt-threshold")
    for m in (0, 1):
        code, _ = write_critical(b1, dev, m)
        check(f"P7 write critical #{m}", code == 201)
    code, r = get_events(b2, dev)
    check("P7 two critical logs do not form an event",
          code == 200 and r["count"] == 0, str(r))
    write_critical(b1, dev, 2)
    code, r = get_events(b1, dev)
    check("P7 three critical logs within 5 min form one event",
          code == 200 and r["count"] == 1 and r["events"][0]["log_count"] == 3,
          str(r))

    # -- gap rule / severity filter ----------------------------------------
    write_critical(b1, dev, 20)
    write_critical(b1, dev, 21, severity="error")
    write_critical(b1, dev, 22, severity="warn")
    code, r = get_events(b1, dev)
    check("P7 a 18-min gap separates runs; single-log run is not an event",
          code == 200 and r["count"] == 1 and r["events"][0]["log_count"] == 3,
          str(r))

    # -- acknowledgment: exact set, idempotent ------------------------------
    ev = r["events"][0]
    code, ack1 = http("POST", b2, "/events/ack", {
        "device_id": dev, "event_id": ev["event_id"],
        "log_ids": ev["log_ids"], "acknowledged_by": "op-1",
    })
    check("P7 ack accepts the exact seen log set",
          code == 200 and ack1["acknowledged"]
          and ack1["already_acknowledged"] is False, str(ack1))
    code, ack2 = http("POST", b1, "/events/ack", {
        "device_id": dev, "event_id": ev["event_id"],
        "log_ids": list(reversed(ev["log_ids"])), "acknowledged_by": "op-2",
    })
    check("P7 repeat ack (other instance) is idempotent",
          code == 200 and ack2["already_acknowledged"] is True
          and ack2["acknowledged_at"] == ack1["acknowledged_at"],
          str(ack2))
    code, r = get_events(b2, dev)
    shown = r["events"][0]
    check("P7 event is reported acknowledged with who/when",
          shown["acknowledged"] and shown["acknowledged_by"] == "op-1"
          and shown["acknowledged_at"], str(shown))
    check("P7 ack response carries start/end/log ids",
          ack1["start"] == ev["start"] and ack1["end"] == ev["end"]
          and ack1["log_ids"] == ev["log_ids"], str(ack1))

    # -- backfill that extends the event invalidates the old ack ------------
    code, _ = write_critical(b2, dev, 3)   # inside what is now a 0..3 run
    code, r = get_events(b1, dev)
    ext = r["events"][0]
    check("P7 backfilled log extends the event",
          ext["log_count"] == 4 and ext["event_id"] != ev["event_id"], str(ext))
    check("P7 old ack no longer applies to the extended event",
          ext["acknowledged"] is False)
    code, err = http("POST", b1, "/events/ack", {
        "device_id": dev, "event_id": ev["event_id"],
        "log_ids": ev["log_ids"],
    })
    check("P7 old event id can no longer acknowledge the changed event",
          code == 409, f"{code} {err}")
    code, err = http("POST", b1, "/events/ack", {
        "device_id": dev, "event_id": ev["event_id"],
        "log_ids": ext["log_ids"],
    })
    check("P7 old event id cannot be replayed with the new log set",
          code == 409, f"{code} {err}")
    code, err = http("POST", b1, "/events/ack", {
        "device_id": dev, "event_id": ext["event_id"],
        "log_ids": ev["log_ids"],
    })
    check("P7 new event id requires the complete new log set",
          code == 409, f"{code} {err}")
    code, ack = http("POST", b1, "/events/ack", {
        "device_id": dev, "event_id": ext["event_id"],
        "log_ids": ext["log_ids"],
    })
    check("P7 the new event can be acknowledged", code == 200, str(ack))

    # -- backfill that merges two events invalidates both acks --------------
    dev = device("evt-merge")
    for m in (0, 2, 4):
        write_critical(b1, dev, m)
    for m in (30, 32, 34):
        write_critical(b2, dev, m)
    code, r = get_events(b1, dev)
    check("P7 two separated runs form two events", r["count"] == 2, str(r))
    acks = []
    for ev in r["events"]:
        code, a = http("POST", b2, "/events/ack", {
            "device_id": dev, "event_id": ev["event_id"],
            "log_ids": ev["log_ids"]})
        check("P7 ack event before merge", code == 200, str(a))
        acks.append(a["event_id"])
    write_critical(b1, dev, 5)   # extends the first run (0..5); still 25 to 30
    code, r = get_events(b1, dev)
    check("P7 a backfill that only extends leaves two events",
          r["count"] == 2 and sorted(e["log_count"] for e in r["events"]) == [3, 4],
          str(r))
    write_critical(b2, dev, 20)
    code, r = get_events(b2, dev)
    check("P7 a lone backfill 15 min from both sides still does not merge",
          r["count"] == 2, str(r))
    write_critical(b1, dev, 8)
    write_critical(b2, dev, 12)
    write_critical(b1, dev, 16)
    code, r = get_events(b2, dev)
    check("P7 partial bridging still keeps the runs separate",
          r["count"] == 2, str(r))
    write_critical(b1, dev, 24)
    write_critical(b2, dev, 28)  # 4 -> 8 -> 12 -> 16 -> 20 -> 24 -> 28 -> 30
    code, r = get_events(b2, dev)
    merged = r["events"][0] if r["count"] == 1 else None
    check("P7 bridging logs merge the runs into one event",
          merged is not None and merged["log_count"] == 13, str(r))
    check("P7 merged event inherits neither old acknowledgment",
          merged["acknowledged"] is False)
    for old in acks:
        code, err = http("POST", b1, "/events/ack", {
            "device_id": dev, "event_id": old,
            "log_ids": merged["log_ids"]})
        check("P7 pre-merge event id rejected for merged event",
              code == 409, f"{code} {err}")

    # -- range / tenant validation -----------------------------------------
    code, err = http("GET", b1, f"/events?device_id={dev}"
                     "&start=2026-01-01T00:00:00Z&end=2026-01-02T00:00:01Z")
    check("P7 >24 h range rejected", code == 422, f"{code} {err}")
    code, err = http("GET", b1, f"/events?device_id={dev}"
                     "&start=2026-01-02T00:00:00Z&end=2026-01-01T00:00:00Z")
    check("P7 inverted range rejected", code == 422, f"{code} {err}")
    code, err = http("POST", b2, "/events/ack", {
        "device_id": device("other"), "event_id": merged["event_id"],
        "log_ids": merged["log_ids"]})
    check("P7 event id / log ids are scoped to their device", code == 409,
          f"{code} {err}")

    # -- window straddle returns the whole event ---------------------------
    code, r = http("GET", b1, f"/events?device_id={dev}"
                   "&start=2026-01-01T06:10:00Z&end=2026-01-01T06:20:00Z")
    check("P7 window touching the middle still returns the whole event",
          r["count"] == 1 and r["events"][0]["log_count"] == 13, str(r))
    check("P7 event id and start/end do not depend on the query window",
          r["events"][0]["event_id"] == merged["event_id"]
          and r["events"][0]["start"] == merged["start"]
          and r["events"][0]["end"] == merged["end"], str(r))

    # -- concurrency: two instances, critical writes vs. ack ---------------
    print("  .. concurrent critical writes vs. ack across both instances")
    dev = device("evt-race")
    base_ids = []
    for m in (0, 2, 4):
        code, _ = write_critical(b1, dev, m)
        code, r = get_events(b1, dev)
    ev = r["events"][0]
    original = ev["event_id"]
    original_ids = list(ev["log_ids"])

    def ack_task(base):
        return http("POST", base, "/events/ack", {
            "device_id": dev, "event_id": original,
            "log_ids": original_ids})

    with ThreadPoolExecutor(max_workers=12) as ex:
        futures = [ex.submit(ack_task, b1 if i % 2 == 0 else b2)
                   for i in range(8)]
        # Backfill a log in the middle of the run concurrently.
        futures.append(ex.submit(write_critical, b2, dev, 3))
        results = [f.result() for f in futures]

    ack_codes = [c for c, _ in results[:8]]
    write_code = results[8][0]
    check("P7 concurrent backfill write succeeded", write_code == 201,
          str(results[8]))
    check("P7 concurrent acks: at least one succeeded and repeated acks are "
          "idempotent, stale-id acks are 409",
          all(c in (200, 409) for c in ack_codes) and 200 in ack_codes,
          str(ack_codes))
    code, r = get_events(b2, dev)
    final = r["events"][0]
    check("P7 concurrent result is internally consistent",
          final["log_count"] == 4
          and ((final["event_id"] == original and final["acknowledged"])
               or (final["event_id"] != original
                   and not final["acknowledged"])),
          str(final))
    tally = {"ok": sum(1 for c in ack_codes if c == 200),
             "conflict": sum(1 for c in ack_codes if c == 409)}
    info(f"concurrent acks: {tally}")

    # -- failed write must not affect the event ---------------------------
    dev = device("evt-failwrite")
    for m in (0, 2, 4):
        write_critical(b1, dev, m)
    code, r = get_events(b1, dev)
    before = r["events"][0]
    code, err = http("POST", b1, "/logs", {
        "device_id": dev, "message": "", "severity": "critical",
        "occurred_at": event_time(3),
    })
    check("P7 malformed critical write is rejected", code == 422,
          f"{code} {err}")
    code, r = get_events(b2, dev)
    after = r["events"][0]
    check("P7 failed write leaves the event unchanged",
          after["event_id"] == before["event_id"]
          and after["log_ids"] == before["log_ids"], str(after))
    code, ack = http("POST", b1, "/events/ack", {
        "device_id": dev, "event_id": before["event_id"],
        "log_ids": before["log_ids"]})
    check("P7 event remains acknowledgeable after the failed write",
          code == 200, str(ack))

    legacy_dev = device("evt-legacy")
    write_critical(b1, legacy_dev, 0)

    # -- other devices keep writing while one device is locked ------------
    dev_a, dev_b = device("evt-lock-a"), device("evt-lock-b")
    write_critical(b1, dev_a, 0)
    write_critical(b1, dev_b, 0)

    def hold_lock_then_release(seconds):
        with psycopg.connect(ctx["dsn"], autocommit=False) as locker:
            locker.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, %s))",
                (dev_a, 861_227_003))
            time.sleep(seconds)
            locker.rollback()

    holder = ThreadPoolExecutor(max_workers=1)
    t0 = time.time()
    holder.submit(hold_lock_then_release, 6)
    time.sleep(1)  # let the holder acquire the lock
    code_b, _ = write_critical(b2, dev_b, 1)   # different device
    elapsed_b = time.time() - t0
    code_a, _ = write_critical(b2, dev_a, 1)   # same device: must wait
    elapsed_a = time.time() - t0
    holder.shutdown(wait=True)
    check("P7 write for another device is not blocked by a held device lock",
          code_b == 201 and elapsed_b < 4, f"{elapsed_b:.1f}s")
    check("P7 same-device write waits for the device lock",
          code_a == 201 and elapsed_a >= 5, f"{elapsed_a:.1f}s")

    # -- acknowledgment uses only committed logs ---------------------------
    # A critical write that has taken the device lock but not committed must
    # be invisible to an ack; if it rolls back, the event is unchanged.
    dev = device("evt-inflight")
    for m in (0, 2, 4):
        write_critical(b1, dev, m)
    code, r = get_events(b1, dev)
    ev = r["events"][0]

    inflight = psycopg.connect(ctx["dsn"], autocommit=False)
    inflight.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, %s))",
        (dev, 861_227_003))
    inflight.execute(
        "INSERT INTO device_fault_logs (device_id, occurred_at, severity, message)"
        " VALUES (%s, %s, 'critical', 'in flight')",
        (dev, event_time(3)))
    # The row exists but is uncommitted, and the writer holds the device lock.
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(http, "POST", b2, "/events/ack", {
            "device_id": dev, "event_id": ev["event_id"],
            "log_ids": ev["log_ids"],
        })
        time.sleep(1.0)
        check("P7 ack blocks while an uncommitted critical write holds the lock",
              not fut.done())
        inflight.rollback()  # writer fails -> its log disappears with the lock
        code, ack = fut.result()
    inflight.close()
    check("P7 after the in-flight write rolls back, the old event is acked",
          code == 200 and ack["event_id"] == ev["event_id"], f"{code} {ack}")
    code, r = get_events(b2, dev)
    check("P7 rolled-back write left no log behind",
          r["events"][0]["log_ids"] == ev["log_ids"], str(r))

    # A write that commits before the ack acquires the lock changes the event,
    # so the ack must be rejected with 409.
    dev = device("evt-commit-race")
    for m in (0, 2, 4):
        write_critical(b1, dev, m)
    code, r = get_events(b1, dev)
    ev = r["events"][0]
    writer = psycopg.connect(ctx["dsn"], autocommit=False)
    writer.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, %s))",
        (dev, 861_227_003))
    writer.execute(
        "INSERT INTO device_fault_logs (device_id, occurred_at, severity, message)"
        " VALUES (%s, %s, 'critical', 'committed while ack waits')",
        (dev, event_time(3)))
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(http, "POST", b2, "/events/ack", {
            "device_id": dev, "event_id": ev["event_id"],
            "log_ids": ev["log_ids"],
        })
        time.sleep(1.0)
        check("P7 ack waits for the committing writer's lock", not fut.done())
        writer.commit()   # the log becomes visible before the ack validates
        code, err = fut.result()
    writer.close()
    check("P7 a committed extending write makes the stale ack 409",
          code == 409, f"{code} {err}")
    code, r = get_events(b1, dev)
    check("P7 the extended event is unacknowledged",
          r["events"][0]["log_count"] == 4
          and not r["events"][0]["acknowledged"], str(r))

    # -- existing APIs still work after the event feature -----------------
    code, r = http("GET", b2,
                   f"/logs?device_id={legacy_dev}"
                   f"&start=2000-01-01T00:00:00Z&end=2100-01-01T00:00:00Z")
    check("P7 legacy /logs query still works", code == 200 and r["count"] == 1,
          str(r.get("count")))
    code, _ = http("GET", b1, f"/events/plan?device_id={legacy_dev}"
                   f"&{EVENT_WINDOW}")
    check("P7 events plan endpoint responds", code == 200)
    code, st = http("GET", b2, "/indexes/status")
    check("P7 online index still ready/valid", code == 200
          and st.get("state") == "ready", str(st.get("state")))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["local", "docker"], required=True)
    ap.add_argument("--dsn", required=True)
    ap.add_argument("--api1", default="http://127.0.0.1:8001")
    ap.add_argument("--api2", default="http://127.0.0.1:8002")
    ap.add_argument("--workdir", default="/workspace")
    ap.add_argument("--logdir", default="/tmp/ops-logs")
    args = ap.parse_args()

    os.makedirs(args.logdir, exist_ok=True)
    db = Db(args.dsn)
    if args.mode == "local":
        instances = {
            "api1": LocalInstance("api1", 8001, args.dsn, args.workdir, args.logdir),
            "api2": LocalInstance("api2", 8002, args.dsn, args.workdir, args.logdir),
        }
    else:
        instances = {"api1": DockerInstance("api1", args.workdir),
                     "api2": DockerInstance("api2", args.workdir)}
    ctx = {
        "mode": args.mode,
        "db": db,
        "dsn": args.dsn,
        "instances": instances,
        "bases": {"api1": args.api1, "api2": args.api2},
        "workdir": args.workdir,
    }

    try:
        phase_p0(ctx)
        phase_p1(ctx)
        job_id = phase_p2(ctx)
        phase_p3(ctx, job_id)
        phase_p4(ctx)
        phase_p5(ctx)
        phase_p6(ctx)
        phase_p7(ctx)
    finally:
        passed = sum(1 for _, ok in RESULTS if ok)
        print(f"\n==== summary: {passed}/{len(RESULTS)} checks passed ====")
        if args.mode == "local":
            for inst in ctx["instances"].values():
                inst.kill()
    if not all(ok for _, ok in RESULTS):
        sys.exit(1)
    print("ALL CHECKS PASSED")


if __name__ == "__main__":
    main()
