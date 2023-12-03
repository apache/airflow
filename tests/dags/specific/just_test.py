from __future__ import annotations

# needed to work against airflow "safe mode" parsing
from airflow.models import DAG  # noqa

raise Exception("This dag file should have been ignored!")
