from dataclasses import dataclass
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from itertools import product


@dataclass
class PerformanceDagDescriptor:
    dag_id: str
    max_active_tasks: int = None
    max_active_tis_per_dag: int = None
    max_active_tis_per_dagrun: int = None
    pool: str = None
    priority_weight: int = None
    n_sec: int = 1


def create_performance_dag_with_limits(
    dag_id: str,
    max_active_tasks: int = None,
    max_active_tis_per_dag: int = None,
    max_active_tis_per_dagrun: int = None,
    pool: str = None,
) -> DAG:
    dag_args = {
        "dag_id": dag_id,
        "start_date": datetime(2025, 1, 1),
        "schedule": "@daily",
        "catchup": True,
    }
    if max_active_tasks is not None:
        dag_args["max_active_tasks"] = max_active_tasks

    dag = DAG(**dag_args)

    task_args = {}
    if max_active_tis_per_dag is not None:
        task_args["max_active_tis_per_dag"] = max_active_tis_per_dag
    if max_active_tis_per_dagrun is not None:
        task_args["max_active_tis_per_dagrun"] = max_active_tis_per_dagrun
    if pool is not None:
        task_args["pool"] = pool

    with dag:
        BashOperator(task_id="sleep_task", bash_command="sleep 5", **task_args)

    return dag


# Global specification of limits
global_limits = {
    "max_active_tasks": 16,
    "max_active_tis_per_dag": 24,
    "max_active_tis_per_dagrun": 200,
    "pool": "default_pool",
}

limits_keys = ["max_active_tasks", "max_active_tis_per_dag", "max_active_tis_per_dagrun", "pool"]

from itertools import product

all_combinations = product([False, True], repeat=4)

for idx, combo in enumerate(all_combinations, 1):
    apply = dict(zip(limits_keys, combo))
    dag_id = f"performance_dag_{idx}"
    kwargs = {k: global_limits[k] if apply[k] else None for k in limits_keys}
    globals()[dag_id] = create_performance_dag_with_limits(dag_id, **kwargs)


from airflow.operators.trigger_dagrun import TriggerDagRunOperator

N = 5  # Replace with your desired number of runs per DAG

default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="trigger_performance_dags",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["trigger"],
) as dag:
    previous_task = None

    for i in range(1, 17):  # For 16 DAGs
        for run_num in range(1, N + 1):  # Trigger each DAG N times
            task = TriggerDagRunOperator(
                task_id=f"trigger_performance_dag_{i}_run_{run_num}",
                trigger_dag_id=f"performance_dag_{i}",
                poke_interval=60,
            )
            if previous_task:
                previous_task >> task  # Chain tasks sequentially
            previous_task = task
