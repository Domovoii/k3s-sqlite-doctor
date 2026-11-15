"""Tests for k3s-sqlite-doctor. SYNTHETIC databases only (demo-* names), no real data."""
import hashlib
import io
import json
import shutil
import sqlite3
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import kine_doctor as kd  # noqa: E402

COLS = ("id INTEGER PRIMARY KEY AUTOINCREMENT, name {nt}, created INTEGER, deleted INTEGER, "
        "create_revision INTEGER, prev_revision INTEGER, lease INTEGER, value BLOB, old_value BLOB")
LEASES = ["/registry/leases/demo-operators/alpha-ctrl",
          "/registry/leases/demo-monitoring/beta-op",
          "/registry/leases/demo-streaming/gamma-op"]


def insert(con, name, value=b"x", deleted=0, prev=0):
    con.execute("INSERT INTO kine(name,created,deleted,create_revision,prev_revision,lease,value,old_value) "
                "VALUES (?,1,?,0,?,0,?,NULL)", (name, deleted, prev, value))


def make_db(path, kind):
    """Create a synthetic kine database. kind: see the cases below."""
    path = str(path)
    if kind == "zero":
        Path(path).write_bytes(b"")
        return
    if kind == "text":
        Path(path).write_text("this is not a SQLite database " * 50)
        return
    con = sqlite3.connect(path, isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("BEGIN")
    if kind == "no_table":
        con.execute("CREATE TABLE autre(x)")
    elif kind == "unknown_schema":
        con.execute("CREATE TABLE kine(id INTEGER PRIMARY KEY, name TEXT, deleted INTEGER)")
    else:
        con.execute("CREATE TABLE kine(" + COLS.format(nt="INTEGER" if kind == "old_schema" else "TEXT") + ")")
        insert(con, "compact_rev_key", prev=1)
        if kind in ("healthy", "old_schema", "truncated"):
            for i in range(3000):
                insert(con, "/registry/pods/demo-apps/pod-%d" % i)
        elif kind == "bloated":
            for i in range(3000):
                insert(con, "/registry/pods/demo-apps/pod-%d" % i, value=b"v" * 8192)
            con.execute("DELETE FROM kine WHERE id % 14 <> 0")
        elif kind == "noisy_leases":
            insert(con, "/registry/leases/kube-node-lease/demo-node")
            for i in range(3000):
                insert(con, "/registry/pods/demo-apps/pod-%d" % i)
            for i in range(1000):  # window: 60 % non-system leases
                if i % 5 < 3:
                    insert(con, LEASES[i % 5])
                else:
                    insert(con, "/registry/events/demo-apps/ev-%d" % i)
        elif kind == "empty":
            con.execute("DELETE FROM kine")
        else:
            raise ValueError(kind)
    con.execute("COMMIT")
    con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    con.close()
    if kind == "truncated":
        data = Path(path).read_bytes()
        Path(path).write_bytes(data[:5000])


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="kd-test-")
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def diagnose(self, kind, *extra):
        db = str(Path(self.tmp) / (kind + ".db"))
        make_db(db, kind)
        return self.run_cli(db, *extra)

    def run_cli(self, db, *extra):
        out = io.StringIO()
        with redirect_stdout(out):
            code = kd.main(["diagnose", "--db", db, "--sample", "0", "--journal", "off", "--format", "json", *extra])
        data = json.loads(out.getvalue())
        self.assertEqual(code, data["exit_code"])
        return data


class TestDiagnose(Base):
    def test_healthy(self):
        d = self.diagnose("healthy")
        self.assertEqual(d["exit_code"], 0)
        self.assertEqual(d["rows"]["live"], 3000)
        self.assertLess(d["pages"]["free_ratio"], 0.5)
        self.assertEqual(d["verdict"], "healthy")

    def test_bloated(self):
        with mock.patch.object(kd, "BLOAT_MIN_BYTES", 1):
            d = self.diagnose("bloated")
        self.assertGreaterEqual(d["pages"]["free_ratio"], 0.9)
        self.assertEqual(d["exit_code"], 1)
        self.assertIn("bloat", [x["id"] for x in d["findings"]])

    def test_noisy_leases(self):
        d = self.diagnose("noisy_leases")
        top = d["writes"]["top"][:3]
        self.assertEqual({t["kind"] for t in top}, {"lease"})
        self.assertAlmostEqual(d["writes"]["nonsystem_lease_share"], 0.6, delta=0.02)
        self.assertEqual(d["writes"]["rows_in_window"], 1000)
        self.assertIsNone(d["writes"]["rate_per_s"])  # offline: no rate
        noisy = [x for x in d["findings"] if x["id"] == "noisy-leases"][0]
        self.assertEqual(noisy["severity"], "info")  # unknown interval: no warn
        self.assertFalse(noisy["verified"])

    def test_old_schema_name_integer(self):
        self.assertEqual(self.diagnose("old_schema")["exit_code"], 0)

    def test_unknown_schema_gives_no_verdict(self):
        d = self.diagnose("unknown_schema")
        self.assertEqual(d["exit_code"], 3)
        self.assertIn("prev_revision", d["error"])
        self.assertEqual(d["findings"], [])

    def test_no_table(self):
        self.assertEqual(self.diagnose("no_table")["exit_code"], 3)

    def test_empty_table(self):
        d = self.diagnose("empty")
        self.assertEqual(d["exit_code"], 0)
        self.assertEqual(d["writes"]["top"], [])

    def test_corrupt_files(self):
        for kind in ("zero", "text", "truncated"):
            with self.subTest(kind=kind):
                d = self.diagnose(kind)
                self.assertEqual(d["exit_code"], 3)
                self.assertTrue(d["error"])

    def test_missing_db(self):
        self.assertEqual(self.run_cli(str(Path(self.tmp) / "absent.db"))["exit_code"], 3)

    def test_database_file_unchanged(self):
        db = Path(self.tmp) / "ro.db"
        make_db(db, "noisy_leases")
        before = hashlib.sha256(db.read_bytes()).hexdigest()
        self.run_cli(str(db))
        self.assertEqual(hashlib.sha256(db.read_bytes()).hexdigest(), before)

    def test_rate_from_two_samples(self):
        db = Path(self.tmp) / "rate.db"
        make_db(db, "noisy_leases")
        clock = [100.0]

        def fake_sleep(seconds):  # 30 revisions arrive during the sample
            w = sqlite3.connect(str(db), isolation_level=None)
            for _ in range(30):
                insert(w, LEASES[0])
            w.close()
            clock[0] += seconds

        out = io.StringIO()
        with mock.patch.object(kd.time, "monotonic", lambda: clock[0]), \
                mock.patch.object(kd.time, "sleep", fake_sleep), redirect_stdout(out):
            kd.main(["diagnose", "--db", str(db), "--sample", "10", "--journal", "off", "--format", "json"])
        d = json.loads(out.getvalue())
        self.assertEqual(d["writes"]["rate_per_s"], 3.0)
        self.assertEqual(d["mode"], "live")

    def test_json_keys_are_stable(self):
        d = self.diagnose("healthy")
        self.assertEqual(d["schema_version"], 1)
        self.assertEqual(set(d), {"schema_version", "tool_version", "db_path", "mode", "k3s_version",
                                  "startup_vacuum", "verdict", "exit_code", "error", "file", "pages", "rows",
                                  "writes", "journal", "skipped_queries", "findings"})
        self.assertEqual(set(d["writes"]), {"window", "rows_in_window", "rate_per_s", "nonsystem_lease_share",
                                            "approximate", "top"})
        self.assertEqual(set(d["journal"]), {"available", "since", "slow_ge_1s", "slow_ge_5s", "max_s"})

    def test_text_output_escapes_hostile_names(self):
        db = Path(self.tmp) / "hostile.db"
        make_db(db, "healthy")
        con = sqlite3.connect(str(db), isolation_level=None)
        for _ in range(50):
            insert(con, "/registry/x/\x1b[31mRED\nend")
        con.close()
        out = io.StringIO()
        with redirect_stdout(out):
            kd.main(["diagnose", "--db", str(db), "--sample", "0", "--journal", "off"])
        self.assertNotIn("\x1b", out.getvalue())
        self.assertIn("\\x1b[31mRED\\x0aend", out.getvalue())


def stats(**over):
    s = {"startup_vacuum": None, "skipped_queries": [],
         "file": {"db_bytes": 10 * kd.MIB, "wal_bytes": 0, "disk_free_bytes": 50 * 1024 ** 3,
                  "disk_total_bytes": 100 * 1024 ** 3},
         "pages": {"size": 4096, "count": 1000, "free": 0, "free_ratio": 0.0},
         "rows": {"total": 10, "live": 10, "current_rev": 100, "compact_rev": 100},
         "writes": {"window": 1000, "rows_in_window": 1000, "rate_per_s": None,
                    "nonsystem_lease_share": None, "approximate": False, "top": []},
         "journal": {"available": True, "since": "24h", "slow_ge_1s": 0, "slow_ge_5s": 0, "max_s": 0.0}}
    for key, val in over.items():
        if isinstance(val, dict):
            s[key].update(val)
        else:
            s[key] = val
    return s


def ids(findings):
    return {x["id"]: x["severity"] for x in findings}


class TestAnalyze(unittest.TestCase):
    def test_healthy_has_no_findings(self):
        self.assertEqual(kd.analyze(stats()), [])

    def test_fast_noisy_leases_warn(self):
        lease = {"kind": "lease", "interval_s": 2.0, "name": "x", "count": 1, "share": 0.6,
                 "rate_per_s": 0.5, "namespace": "demo"}
        f = kd.analyze(stats(writes={"nonsystem_lease_share": 0.6, "top": [lease]}))
        self.assertEqual(ids(f)["noisy-leases"], "warn")

    def test_hot_non_lease_key_is_flagged_but_masterleases_is_not(self):
        cm = {"kind": "other", "interval_s": 4.3, "name": "/registry/configmaps/demo/app-cluster-config-map"}
        master = {"kind": "other", "interval_s": 3.0, "name": "/registry/masterleases/10.0.0.1"}
        slow = {"kind": "other", "interval_s": 120.0, "name": "/registry/configmaps/demo/calm"}
        self.assertEqual(ids(kd.analyze(stats(writes={"top": [cm]}))), {"hot-keys": "warn"})
        self.assertEqual(kd.analyze(stats(writes={"top": [master, slow]})), [])

    def test_slow_leases_only_info(self):
        lease = {"kind": "lease", "interval_s": 30.0}
        f = kd.analyze(stats(writes={"nonsystem_lease_share": 0.6, "top": [lease]}))
        self.assertEqual(ids(f)["noisy-leases"], "info")
        self.assertEqual(kd.verdict_code(f), 0)

    def test_bloat_advice_depends_on_k3s_version(self):
        for sv, word in ((True, "Restart k3s"), (False, "Upgrade k3s"), (None, "unknown")):
            f = kd.analyze(stats(startup_vacuum=sv,
                                 file={"db_bytes": 500 * kd.MIB}, pages={"free": 900, "free_ratio": 0.9}))
            self.assertIn(word, [x for x in f if x["id"] == "bloat"][0]["advice"])

    def test_slow_sql_and_disk_limit(self):
        j = {"slow_ge_5s": 3, "max_s": 18.0}
        self.assertEqual(ids(kd.analyze(stats(journal=j)))["slow-sql"], "warn")
        self.assertEqual(ids(kd.analyze(stats(journal=j)))["disk-limit"], "warn")

    def test_disk_space_is_critical(self):
        f = kd.analyze(stats(file={"disk_free_bytes": 1024}))
        self.assertEqual(ids(f)["disk-space"], "crit")
        self.assertEqual(kd.verdict_code(f), 2)

    def test_wal_and_compaction_lag(self):
        f = kd.analyze(stats(file={"wal_bytes": 200 * kd.MIB}, rows={"current_rev": 500_000, "compact_rev": 1}))
        self.assertEqual({"wal", "compaction-lag"}, set(ids(f)))


class TestHelpers(unittest.TestCase):
    def test_go_durations(self):
        self.assertAlmostEqual(kd.parse_go_duration("5.287467372s"), 5.287467372)
        self.assertAlmostEqual(kd.parse_go_duration("850ms"), 0.85)
        self.assertAlmostEqual(kd.parse_go_duration("1m2.5s"), 62.5)
        self.assertIsNone(kd.parse_go_duration("abc"))
        self.assertIsNone(kd.parse_go_duration("5.2x"))

    def test_startup_vacuum_versions(self):
        cases = {(1, 34, 6): False, (1, 34, 7): True, (1, 33, 10): False, (1, 33, 11): True,
                 (1, 35, 3): False, (1, 35, 4): True, (1, 36, 0): True, (1, 32, 13): False}
        for ver, expected in cases.items():
            self.assertEqual(kd.has_startup_vacuum(ver), expected, ver)
        self.assertIsNone(kd.has_startup_vacuum(None))
        self.assertEqual(kd.parse_k3s_version("k3s version v1.34.6+k3s1 (abc)"), (1, 34, 6))

    def test_classify(self):
        self.assertEqual(kd.classify("/registry/leases/kube-system/kcm"), ("system-lease", "kube-system"))
        self.assertEqual(kd.classify("/registry/leases/demo/op"), ("lease", "demo"))
        self.assertEqual(kd.classify("/registry/pods/demo/p"), ("other", None))

    def test_safe_escapes_control_characters(self):
        self.assertEqual(kd.safe("a\x1b[0m\né"), "a\\x1b[0m\\x0a\\xe9")
        self.assertEqual(len(kd.safe("x" * 500)), 120)

    def test_usage_errors_exit_3(self):
        for argv in (["diagnose", "--bogus"], [], ["diagnose", "--journal-since", "bad"], ["diagnose", "--top", "0"]):
            with self.subTest(argv=argv), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as cm:
                    kd.main(argv)
                self.assertEqual(cm.exception.code, 3)


if __name__ == "__main__":
    unittest.main()
