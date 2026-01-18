Closes: #60687

This PR removes redundant `try/except` blocks in `BigtableDeleteInstanceOperator` in `airflow/providers/google/cloud/operators/bigtable.py`.

The underlying `BigtableHook.delete_instance` method already handles the case where the instance does not exist by logging a warning. Therefore, catching `NotFound` in the operator was unnecessary and duplicated logic.

I have also:
- Updated the unit tests in `providers/google/tests/unit/google/cloud/operators/test_bigtable.py` to remove tests that specifically enforced this redundant exception handling.
- Corrected the docstring in `airflow/providers/google/cloud/hooks/bigtable.py` which incorrectly stated that `delete_instance` raises `NotFound`.

Generated-by: Antigravity (Google DeepMind)
