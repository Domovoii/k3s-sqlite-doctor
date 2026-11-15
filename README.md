# k3s-sqlite-doctor

**Read-only** diagnostic of the SQLite datastore of k3s (`state.db`, through [kine](https://github.com/k3s-io/kine)) for single-node k3s homelabs.

A single Python file, standard library only (Python 3.9+). Status: **v0.1, experimental**.

## The problem

On a single-node k3s, the whole cluster state lives in one SQLite file. Several things can go wrong without any warning:

- the file bloats (free pages are never returned: we saw 93 % free pages on a 637 MiB file holding ~3,000 useful rows);
- operators rewrite their leader-election lease every ~2 s and saturate the database with writes;
- writes become slow (multi-second `Slow SQL` lines in the k3s journal), a lease renewal exceeds its deadline and an operator restarts (`leader election lost`).

This is widely reported ([k3s#11901](https://github.com/k3s-io/k3s/issues/11901), [k3s#3660](https://github.com/k3s-io/k3s/issues/3660), [k3s#7786](https://github.com/k3s-io/k3s/discussions/7786), [a postmortem](https://dev.to/tomaszwostal/when-your-homelab-grows-up-how-sqlite-took-down-my-k3s-control-plane-1kdp)). Everyone currently writes their own ad-hoc `sqlite3` diagnosis. This tool does it for you and tells you **which keys** write the most.

## What it does (v0.1)

`diagnose` only:

- database and WAL sizes, free pages, total and live rows, current and compacted revision;
- writes per key over the latest revisions, with leases highlighted and an estimated write rate;
- number of `Slow SQL` lines in the k3s journal (counters only: queries are never printed);
- findings with a confidence level, a verdict and an exit code.

**It never modifies anything**: the database is opened with `mode=ro`, queries are bounded to 2 s, and there is no write, no checkpoint and no `VACUUM`. It does not talk to the Kubernetes API: recommendations that depend on it are flagged `UNVERIFIED`.

Out of scope: repairing the database, decoding values, etcd/MySQL/PostgreSQL backends.

## Usage

```sh
curl -fsSLO https://github.com/Domovoii/k3s-sqlite-doctor/releases/download/v0.1.0/kine_doctor.py
sha256sum kine_doctor.py   # compare with the value published in the release
sudo python3 kine_doctor.py diagnose            # live database, write rate sampled over 10 s
sudo python3 kine_doctor.py diagnose --format json
python3 kine_doctor.py diagnose --db copy.db --sample 0   # on a copy, no write rate
```

Reading `state.db` requires root. The script is short: read it before running it as root.

Example output (synthetic data):

```
k3s-sqlite-doctor 0.1.0 - /var/lib/rancher/k3s/server/db/state.db (read-only)
k3s v1.34.6+k3s1: NO startup VACUUM

FILE      db 468.8 MiB   WAL 4.1 MiB   free disk 61.2 GiB
PAGES     120000 x 4096 B  free 108000 (90.0 %)
RATE      2.60 writes/s

WRITES (last 1000 revisions)
  /registry/leases/demo-operators/alpha-ctrl      20.5 %   0.53/s ~1.9s  LEASE
  /registry/leases/demo-monitoring/beta-op        19.8 %   0.51/s ~2s    LEASE
  Non-system leases: 59.3 % of writes

FINDINGS
[WARN] 90 % free pages (confidence: high on the measure, medium on the impact)
       -> Upgrade k3s (>= v1.33.11, v1.34.7, v1.35.4 or v1.36): every start will run a VACUUM...
[WARN] 2 non-system lease(s) in the top = 59 % of writes (confidence: medium, UNVERIFIED)
       -> If the operator has a single replica, disable its leader election ...

VERDICT: WATCH (code 1)
```

Exit codes (monitoring convention): `0` healthy, `1` watch, `2` critical, `3` unknown (missing or unreadable database, unknown schema, usage error).

## Startup VACUUM in k3s

Recent k3s versions (kine >= 0.14.15) run a `VACUUM` at every start: on those versions a plain `systemctl restart k3s` reclaims the free pages. According to the `go.mod` files of the tags (not rechecked, please verify): v1.33.11, v1.34.7, v1.35.4, v1.36.0 and later. The tool detects the local `k3s` version and adapts its advice.

## Caveats

- The **thresholds** behind the findings are starting values, not calibrated.
- Lease recommendations assume a **single-replica** operator: check before disabling a leader election.
- The write rate is estimated from two reads of `MAX(id)` (the table has no timestamp): it is an estimate.
- The output contains namespace and key names: review it before pasting it in a public issue.

## Development

```sh
python -m unittest
```

Tests use synthetic databases only. See `CONTRIBUTING.md`. License: Apache-2.0.
