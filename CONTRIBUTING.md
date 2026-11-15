# Contributing

- **No real data** in the repository or in issues: no real `state.db`, no host names, domains or IP addresses. Tests build synthetic databases (`make_db` in `tests/test_kine_doctor.py`, `demo-*` names).
- **Standard library only**, a single `kine_doctor.py` file. No dependencies.
- **Read-only**: any write or checkpoint on the live database is rejected.
- New behaviour comes with a test: `python -m unittest` must pass on Python 3.9 and 3.13.
- Thresholds are constants at the top of the file; changing one must rest on a measurement or a real case described in the PR.
