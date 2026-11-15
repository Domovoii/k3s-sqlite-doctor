#!/usr/bin/env python3
"""k3s-sqlite-doctor: READ-ONLY diagnostic of the SQLite datastore of k3s (kine).

Tells whether state.db is bloated (free pages), which keys (leases...) drive the most writes,
and how many "Slow SQL" queries k3s logged. Never modifies anything.
Standard library only.
"""
import argparse
import json
import re
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

__version__ = "0.1.0"
SCHEMA_VERSION = 1
MIB = 1024 * 1024
DEFAULT_DB = "/var/lib/rancher/k3s/server/db/state.db"
REQUIRED_COLUMNS = {"id", "name", "deleted", "prev_revision"}
SYSTEM_LEASE_NAMESPACES = {"kube-system", "kube-node-lease"}
QUERY_TIMEOUT_S = 2

# Initial thresholds, to be tuned: starting values, not measurements.
BLOAT_FREE_RATIO = 0.5
BLOAT_MIN_BYTES = 100 * MIB
NOISY_LEASE_SHARE = 0.30
NOISY_LEASE_INTERVAL_S = 5
WAL_WARN_BYTES = 100 * MIB
COMPACTION_LAG_REVISIONS = 100_000
SLOW_SQL_WARN_S = 5.0
LOW_FREE_RATIO = 0.20
DISK_FREE_MIN_RATIO = 0.05

# shortcut: table read from the go.mod files of the k3s tags, not rechecked; review on each k3s release.
# First patch release that runs VACUUM at startup (kine >= 0.14.15), per minor branch.
STARTUP_VACUUM_FIRST_PATCH = {33: 11, 34: 7, 35: 4}
STARTUP_VACUUM_FIRST_MINOR = 36

SEVERITY_RANK = {"info": 0, "warn": 1, "crit": 2}
VERDICTS = {0: "healthy", 1: "watch", 2: "critical", 3: "unknown"}


class DoctorError(Exception):
    """Error that maps to exit code 3 (unknown)."""


class Parser(argparse.ArgumentParser):
    def error(self, message):  # argparse exits 2 by default: 2 is reserved for "critical"
        sys.stderr.write("usage error: %s\n" % message)
        sys.exit(3)


def safe(value, limit=120):
    """Printable-safe text (escape sequences, newlines...)."""
    out = []
    for ch in str(value)[:limit]:
        code = ord(ch)
        if " " <= ch <= "~":
            out.append(ch)
        elif code < 256:
            out.append("\\x%02x" % code)
        else:
            out.append("\\u%04x" % code)
    return "".join(out)


def parse_go_duration(text):
    """'850ms', '4.2s', '1m2.5s' -> seconds; None if unreadable."""
    units = {"h": 3600.0, "m": 60.0, "s": 1.0, "ms": 1e-3, "us": 1e-6, "µs": 1e-6, "ns": 1e-9}
    parts = re.findall(r"(\d+(?:\.\d+)?)(ms|us|µs|ns|h|m|s)", text)
    if not parts or "".join(n + u for n, u in parts) != text:
        return None
    return sum(float(n) * units[u] for n, u in parts)


def parse_k3s_version(text):
    m = re.search(r"v(\d+)\.(\d+)\.(\d+)", text or "")
    return tuple(int(x) for x in m.groups()) if m else None


def has_startup_vacuum(version):
    if version is None:
        return None
    _, minor, patch = version
    if minor >= STARTUP_VACUUM_FIRST_MINOR:
        return True
    first = STARTUP_VACUUM_FIRST_PATCH.get(minor)
    return first is not None and patch >= first


def classify(name):
    """(kind, namespace) of a kine key."""
    parts = str(name).split("/")
    if str(name).startswith("/registry/leases/") and len(parts) >= 5:
        ns = parts[3]
        return ("system-lease" if ns in SYSTEM_LEASE_NAMESPACES else "lease"), ns
    return "other", None


class Reader:
    """Read-only SQLite connection; every query is time-bounded."""

    def __init__(self, path):
        uri = Path(path).resolve().as_uri() + "?mode=ro"  # never immutable=1 nor nolock=1
        self.con = sqlite3.connect(uri, uri=True, timeout=QUERY_TIMEOUT_S, isolation_level=None)
        self.deadline = 0.0
        self.con.set_progress_handler(lambda: 1 if time.monotonic() > self.deadline else 0, 10000)
        self.skipped = []

    def q(self, sql, params=(), label=""):
        self.deadline = time.monotonic() + QUERY_TIMEOUT_S
        try:
            return self.con.execute(sql, params).fetchall()
        except sqlite3.Error:
            self.skipped.append(label or sql[:40])
            return None

    def close(self):
        self.con.close()


def scalar(rows):
    return rows[0][0] if rows else None


def read_journal(since):
    out = {"available": False, "since": since, "slow_ge_1s": None, "slow_ge_5s": None, "max_s": None}
    try:
        res = subprocess.run(
            ["journalctl", "-u", "k3s", "--since", "-" + since, "-o", "cat", "--no-pager"],
            capture_output=True, text=True, timeout=10, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return out
    if res.returncode != 0 or not res.stdout.strip() or "insufficient permissions" in res.stdout:
        return out
    durations = []
    for line in res.stdout.splitlines():
        if "Slow SQL" not in line:
            continue
        m = re.search(r"(?:total time: |duration=)([0-9a-zµ.]+)", line)
        d = parse_go_duration(m.group(1)) if m else None
        if d is not None:
            durations.append(d)
    out.update(available=True, slow_ge_1s=sum(d >= 1 for d in durations),
               slow_ge_5s=sum(d >= SLOW_SQL_WARN_S for d in durations),
               max_s=round(max(durations), 3) if durations else 0.0)
    return out


def read_k3s_version():
    try:
        res = subprocess.run(["k3s", "--version"], capture_output=True, text=True, timeout=5, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return None
    m = re.search(r"v\d+\.\d+\.\d+\S*", res.stdout)
    return m.group(0) if m else None


def blank(args):
    return {
        "schema_version": SCHEMA_VERSION, "tool_version": __version__, "db_path": args.db,
        "mode": "live" if args.sample > 0 else "offline",
        "k3s_version": None, "startup_vacuum": None,
        "verdict": None, "exit_code": None, "error": None,
        "file": {"db_bytes": None, "wal_bytes": None, "disk_free_bytes": None, "disk_total_bytes": None},
        "pages": {"size": None, "count": None, "free": None, "free_ratio": None},
        "rows": {"total": None, "live": None, "current_rev": None, "compact_rev": None},
        "writes": {"window": args.window, "rows_in_window": None, "rate_per_s": None,
                   "nonsystem_lease_share": None, "approximate": None, "top": []},
        "journal": {"available": False, "since": args.journal_since, "slow_ge_1s": None,
                    "slow_ge_5s": None, "max_s": None},
        "skipped_queries": [], "findings": [],
    }


def collect(args, s):
    path = Path(args.db)
    if not path.is_file():
        raise DoctorError("database not found: %s" % safe(args.db))
    s["file"]["db_bytes"] = path.stat().st_size
    wal = Path(str(path) + "-wal")
    s["file"]["wal_bytes"] = wal.stat().st_size if wal.exists() else 0
    du = shutil.disk_usage(str(path.resolve().parent))
    s["file"]["disk_free_bytes"], s["file"]["disk_total_bytes"] = du.free, du.total

    try:
        rd = Reader(path)
    except sqlite3.Error as e:
        raise DoctorError("cannot open: %s" % safe(e))
    try:
        cols = rd.q("PRAGMA table_info(kine)", label="schema")
        if cols is None:
            raise DoctorError("cannot read (not a SQLite database, or locked)")
        names = {c[1] for c in cols}  # column names only: types vary across versions
        if not names:
            raise DoctorError("kine table missing")
        if not REQUIRED_COLUMNS <= names:
            raise DoctorError("unknown kine schema, missing columns: %s"
                              % ", ".join(sorted(REQUIRED_COLUMNS - names)))

        size = scalar(rd.q("PRAGMA page_size", label="page_size"))
        count = scalar(rd.q("PRAGMA page_count", label="page_count"))
        free = scalar(rd.q("PRAGMA freelist_count", label="freelist"))
        s["pages"].update(size=size, count=count, free=free,
                          free_ratio=round(free / count, 4) if count else None)

        t0 = time.monotonic()
        r0 = scalar(rd.q("SELECT COALESCE(MAX(id),0) FROM kine", label="max_id")) or 0
        total = scalar(rd.q("SELECT COUNT(*) FROM kine", label="total"))
        live = scalar(rd.q(
            "SELECT COUNT(*) FROM kine k JOIN (SELECT MAX(id) AS id FROM kine GROUP BY name) m "
            "USING(id) WHERE k.deleted = 0 AND k.name <> 'compact_rev_key'", label="live"))
        compact = scalar(rd.q("SELECT MAX(prev_revision) FROM kine WHERE name = 'compact_rev_key'",
                              label="compact_rev"))
        s["rows"].update(total=total, live=live, current_rev=r0, compact_rev=compact)

        lo = r0 - args.window
        in_window = scalar(rd.q("SELECT COUNT(*) FROM kine WHERE id > ? AND name <> 'compact_rev_key'",
                                (lo,), "window_count")) or 0
        top = rd.q("SELECT name, COUNT(*) AS n FROM kine WHERE id > ? AND name <> 'compact_rev_key' "
                   "GROUP BY name ORDER BY n DESC LIMIT ?", (lo, args.top), "window_top") or []
        lease_n = scalar(rd.q(
            "SELECT COUNT(*) FROM kine WHERE id > ? AND name LIKE '/registry/leases/%' "
            "AND name NOT LIKE '/registry/leases/kube-system/%' "
            "AND name NOT LIKE '/registry/leases/kube-node-lease/%'", (lo,), "window_leases"))

        rate = None
        if args.sample > 0:
            time.sleep(max(0.0, args.sample - (time.monotonic() - t0)))
            r1 = scalar(rd.q("SELECT COALESCE(MAX(id),0) FROM kine", label="max_id_2"))
            dt = time.monotonic() - t0
            if r1 is not None and dt > 0:
                rate = round((r1 - r0) / dt, 3)

        w = s["writes"]
        w["rows_in_window"], w["rate_per_s"] = in_window, rate
        # Compaction has eaten into the window: shares become approximate.
        w["approximate"] = bool(r0 >= args.window and in_window < 0.9 * args.window)
        if in_window and lease_n is not None:
            w["nonsystem_lease_share"] = round(lease_n / in_window, 4)
        for name, n in top:
            kind, ns = classify(name)
            share = n / in_window if in_window else None
            krate = share * rate if (share is not None and rate) else None
            w["top"].append({
                "name": safe(name), "count": n,
                "share": round(share, 4) if share is not None else None,
                "rate_per_s": round(krate, 3) if krate else None,
                "interval_s": round(1 / krate, 2) if krate else None,
                "kind": kind, "namespace": safe(ns) if ns else None})
        s["skipped_queries"] = rd.skipped
    finally:
        rd.close()


def analyze(s):
    """Pure function: stats -> list of findings."""
    found = []

    def add(fid, sev, conf, verified, msg, advice=None):
        found.append({"id": fid, "severity": sev, "confidence": conf, "verified": verified,
                      "message": msg, "advice": advice})

    p, f, r, w, j = s["pages"], s["file"], s["rows"], s["writes"], s["journal"]
    noisy = False

    if p["free_ratio"] is not None and p["free_ratio"] >= BLOAT_FREE_RATIO \
            and (f["db_bytes"] or 0) >= BLOAT_MIN_BYTES:
        sv = s["startup_vacuum"]
        if sv is True:
            adv = "Restart k3s: this version runs VACUUM at startup (check free disk space first)."
        elif sv is False:
            adv = ("Upgrade k3s (>= v1.33.11, v1.34.7, v1.35.4 or v1.36): every start will run a VACUUM. "
                   "Otherwise run an offline VACUUM after taking a backup.")
        else:
            adv = "k3s version unknown: if it runs VACUUM at startup, a restart is enough."
        add("bloat", "warn", "high on the measure, medium on the impact", True,
            "%.0f %% free pages (%d of %d)" % (p["free_ratio"] * 100, p["free"], p["count"]), adv)

    leases = [t for t in w["top"] if t["kind"] == "lease"]
    share = w["nonsystem_lease_share"]
    if share is not None and share >= NOISY_LEASE_SHARE and leases:
        noisy = True
        intervals = [t["interval_s"] for t in leases if t["interval_s"]]
        fast = bool(intervals) and min(intervals) <= NOISY_LEASE_INTERVAL_S
        add("noisy-leases", "warn" if fast else "info", "medium", False,
            "%d non-system lease(s) in the top = %.0f %% of writes" % (len(leases), share * 100),
            "If the operator has a single replica, disable its leader election "
            "(check the replica count with kubectl: this tool does not).")

    # Leader election through a ConfigMap (Flink HA...) or any key rewritten in a loop: same cost as leases.
    hot = [t for t in w["top"] if t["kind"] == "other" and t["interval_s"]
           and t["interval_s"] <= NOISY_LEASE_INTERVAL_S and not t["name"].startswith("/registry/masterleases/")]
    if hot:
        add("hot-keys", "warn", "low", False,
            "%d non-lease key(s) rewritten every <= %gs, e.g. %s (~%gs)"
            % (len(hot), NOISY_LEASE_INTERVAL_S, hot[0]["name"], hot[0]["interval_s"]),
            "If this is leader election or a heartbeat (e.g. a Flink HA ConfigMap), "
            "increase its renewal interval if the application allows it.")

    if j["slow_ge_5s"]:
        add("slow-sql", "warn", "high", True,
            "slow SQL queries >= %gs over %s: %d (max %.1f s)" % (SLOW_SQL_WARN_S, j["since"], j["slow_ge_5s"], j["max_s"]))

    if (f["wal_bytes"] or 0) >= WAL_WARN_BYTES:
        add("wal", "warn", "medium", True, "WAL of %.0f MiB" % (f["wal_bytes"] / MIB),
            "The checkpoint may not be progressing (long reader, compaction lagging).")

    if r["current_rev"] is not None and r["compact_rev"] is not None \
            and r["current_rev"] - r["compact_rev"] > COMPACTION_LAG_REVISIONS:
        add("compaction-lag", "warn", "medium", True,
            "compaction lagging by %d revisions" % (r["current_rev"] - r["compact_rev"]))

    if p["size"] and p["count"] is not None and f["disk_free_bytes"] is not None:
        useful = (p["count"] - (p["free"] or 0)) * p["size"]
        if f["disk_free_bytes"] < 2 * useful or f["disk_free_bytes"] < DISK_FREE_MIN_RATIO * f["disk_total_bytes"]:
            add("disk-space", "crit", "high", True,
                "insufficient free disk space (%.1f GiB)" % (f["disk_free_bytes"] / 1024 ** 3),
                "A VACUUM (including the one at k3s startup) needs free space.")

    if j["slow_ge_5s"] and p["free_ratio"] is not None and p["free_ratio"] < LOW_FREE_RATIO and not noisy:
        add("disk-limit", "warn", "low", False,
            "slow queries without a bloated database or noisy leases: the disk is a suspect",
            "A faster disk helps; embedded etcd does more fsyncs and is not necessarily better "
            "on a DRAM-less SSD.")

    if w["approximate"]:
        add("approximate", "info", "high", True,
            "compaction has eaten into the window: write shares are approximate")
    if s["skipped_queries"]:
        add("partial", "info", "high", True,
            "queries interrupted (%d s timeout): %s" % (QUERY_TIMEOUT_S, ", ".join(s["skipped_queries"])))
    return found


def verdict_code(findings):
    ranks = [SEVERITY_RANK[x["severity"]] for x in findings]
    return 2 if 2 in ranks else (1 if 1 in ranks else 0)


def fmt_bytes(n):
    return "n/a" if n is None else ("%.1f MiB" % (n / MIB) if n < 1024 ** 3 else "%.1f GiB" % (n / 1024 ** 3))


def render_text(s):
    L = ["k3s-sqlite-doctor %s - %s (%s)" % (s["tool_version"], safe(s["db_path"]),
                                           "read-only" if s["mode"] == "live" else "copy / offline")]
    if s["k3s_version"]:
        sv = s["startup_vacuum"]
        L.append("k3s %s: %s" % (s["k3s_version"], {True: "startup VACUUM present",
                                                    False: "NO startup VACUUM",
                                                    None: "startup VACUUM: unknown"}[sv]))
    if s["error"]:
        L += ["", "ERROR: %s" % s["error"], "VERDICT: UNKNOWN (code 3)"]
        return "\n".join(L)
    f, p, r, w, j = s["file"], s["pages"], s["rows"], s["writes"], s["journal"]
    L += ["",
          "FILE      db %s   WAL %s   free disk %s" % (fmt_bytes(f["db_bytes"]), fmt_bytes(f["wal_bytes"]),
                                                      fmt_bytes(f["disk_free_bytes"])),
          "PAGES     %s x %s B  free %s (%s)" % (
              p["count"], p["size"], p["free"],
              "n/a" if p["free_ratio"] is None else "%.1f %%" % (p["free_ratio"] * 100)),
          "ROWS      %s total, %s live   revision %s, compacted %s" % (
              r["total"], r["live"], r["current_rev"], r["compact_rev"]),
          "RATE      %s" % ("n/a (offline)" if w["rate_per_s"] is None else "%.2f writes/s" % w["rate_per_s"]),
          "JOURNAL   %s" % (
              "unavailable" if not j["available"] else
              "%s: %d \"Slow SQL\" >= 1 s, of which %d >= 5 s, max %.1f s" % (
                  j["since"], j["slow_ge_1s"], j["slow_ge_5s"], j["max_s"]))]
    if w["top"]:
        L += ["", "WRITES (last %d revisions)%s" % (w["window"], "  [approximate]" if w["approximate"] else "")]
        for t in w["top"]:
            tag = {"lease": "LEASE", "system-lease": "LEASE (system)", "other": ""}[t["kind"]]
            L.append("  %-46s %5.1f %%  %s  %s" % (
                t["name"][:46], (t["share"] or 0) * 100,
                "      n/a" if t["rate_per_s"] is None else "%5.2f/s ~%gs" % (t["rate_per_s"], t["interval_s"]), tag))
        if w["nonsystem_lease_share"] is not None:
            L.append("  Non-system leases: %.1f %% of writes" % (w["nonsystem_lease_share"] * 100))
    L += ["", "FINDINGS"]
    if not s["findings"]:
        L.append("  nothing to report")
    for x in s["findings"]:
        L.append("[%s] %s (confidence: %s%s)" % (x["severity"].upper(), x["message"], x["confidence"],
                                                 "" if x["verified"] else ", UNVERIFIED"))
        if x["advice"]:
            L.append("       -> %s" % x["advice"])
    L += ["", "VERDICT: %s (code %d)" % (s["verdict"].upper(), s["exit_code"])]
    return "\n".join(L)


def run(args):
    s = blank(args)
    ver = read_k3s_version()
    s["k3s_version"], s["startup_vacuum"] = ver, has_startup_vacuum(parse_k3s_version(ver))
    try:
        collect(args, s)
        if args.journal == "auto":
            s["journal"] = read_journal(args.journal_since)
        s["findings"] = analyze(s)
        s["exit_code"] = verdict_code(s["findings"])
    except DoctorError as e:
        s["error"], s["exit_code"] = str(e), 3
    s["verdict"] = VERDICTS[s["exit_code"]]
    return s


def duration_arg(value):
    if not re.fullmatch(r"\d+[smhd]", value):
        raise argparse.ArgumentTypeError("expected: number + s/m/h/d (e.g. 24h)")
    return value


def build_parser():
    ap = Parser(prog="kine_doctor.py", description=__doc__.splitlines()[0])
    ap.add_argument("--version", action="version", version=__version__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("diagnose", help="read-only diagnostic")
    d.add_argument("--db", default=DEFAULT_DB)
    d.add_argument("--format", choices=["text", "json"], default="text")
    d.add_argument("--top", type=int, default=10)
    d.add_argument("--window", type=int, default=1000, help="last revisions analysed")
    d.add_argument("--sample", type=float, default=10,
                   help="seconds between two MAX(id) reads; 0 = no write rate")
    d.add_argument("--journal", choices=["auto", "off"], default="auto")
    d.add_argument("--journal-since", type=duration_arg, default="24h")
    return ap


def main(argv=None):
    ap = build_parser()
    args = ap.parse_args(argv)
    if args.top < 1 or args.window < 1 or args.sample < 0:
        ap.error("--top and --window must be >= 1, --sample >= 0")
    s = run(args)
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except AttributeError:
        pass
    print(json.dumps(s, indent=2, ensure_ascii=True) if args.format == "json" else render_text(s))
    return s["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
