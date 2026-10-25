from __future__ import annotations

import logging
from datetime import datetime


from airflow.sdk import dag, task, emit_dag_warning

log = logging.getLogger(__name__)

FAILING_DAG_IDS = {"B", "E"}

for dag_id in "ABCDE":
    try:
        if dag_id in FAILING_DAG_IDS:
            raise RuntimeError(f"Failed to create dynamic DAG {dag_id}")

        @dag(dag_id=f"repro_29275_{dag_id}", schedule=None, start_date=datetime(2023, 1, 31), catchup=False)
        def dynamic_dag():
            @task
            def t():
                print(f"inside task {dag_id}")

            t()

        dynamic_dag()
    except Exception:
        emit_dag_warning(f"repro_{dag_id}", f"Failed to create dynamic DAG {dag_id}", stacktrace=None)
        log.exception("Failed to create dynamic DAG %s", dag_id)
