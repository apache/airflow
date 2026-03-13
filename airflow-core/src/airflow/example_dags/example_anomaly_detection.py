"""Example DAG allowing you to interactively choose the times for each TaskInstance run.

The TaskInstances simply sleep for the specified amount of time
"""

from __future__ import annotations

import datetime
import time
from pathlib import Path

from airflow.sdk import DAG, Param, TriggerRule, task
from airflow.models.anomalydetector import AnomalyDetector, ThresholdAnomaly

# [START params_trigger]
with DAG(
    dag_id=Path(__file__).stem,
    dag_display_name="Custom Task Runtime",
    description=__doc__.partition(".")[0],
    doc_md=__doc__,
    schedule=None,
    start_date=datetime.datetime(2022, 3, 4),
    catchup=False,
    tags=["example", "params"],
    params={
        "times": Param(
            [1, 2, 3],
            type="array",
            items = {
                "type": "number"
            },
            description="Define the list of times to run the task for, in seconds. Note that real runtimes may be higher due to task overhead.",
            title="Runtimes",
        ),
    },
) as dag:

    @task(task_id="get_times", task_display_name="Retrieve times from params")
    def get_times(**kwargs) -> list[float]:
        params = kwargs["params"]
        if "times" not in params:
            print("No times given, was no UI used to trigger?")
            return []
        return params["times"]

    
    anomaly_detector = AnomalyDetector(
        min_runs = 2,
        max_runs = 4,
        algorithm = ThresholdAnomaly(max_runtime=3)
    )

    @task(task_id="run_for_time", task_display_name="Run for time", on_success_callback = anomaly_detector)
    def run_for_time(n_seconds: float) -> float:
        time.sleep(n_seconds)
        print(f"Paused for {n_seconds}")
        return n_seconds

    @task(task_id="print_runtimess", task_display_name="Print greetings", trigger_rule=TriggerRule.ALL_DONE)
    def print_runtimes(runtimes) -> None:
        for r in runtimes:
            print(r)

    times_to_run = get_times()
    times_run = run_for_time.expand(n_seconds = times_to_run)
    results_print = print_runtimes(times_run)
# [END]
