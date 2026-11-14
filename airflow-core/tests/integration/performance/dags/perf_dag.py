# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
The fan-out Dag of the performance harness: a first task produces a list, nine mapped stages
expand over it, each behind the previous one. Another shape is another file in this folder.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from airflow.sdk import DAG, task

STAGE_COUNT = 9


@task
def produce_parameters(**context: Any) -> list[int]:
    """Return the list every mapped stage expands over."""
    # expand() needs a sequence that exists at run time, one element per instance; this turns the
    # count the run carries into that sequence. The values themselves are never used.
    return list(range(int(context["params"]["mapped_task_count"])))


@task
def run_stage(parameter: int) -> None:
    """Do nothing."""
    # Any real work here would turn the run into a worker throughput test; with an empty body the
    # time a task instance takes is framework cost, and the scheduler is what a run measures.


with DAG(
    dag_id="perf_dag",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 0},
    # The structure is fixed; a run picks its size through its conf, which overrides this.
    # A run has STAGE_COUNT times mapped_task_count mapped instances.
    params={"mapped_task_count": 20},
    tags=["perf"],
):
    parameters = produce_parameters()
    previous = None
    for stage in range(1, STAGE_COUNT + 1):
        # Expanding over ``parameters`` also makes the first task an upstream of every stage.
        current = run_stage.override(task_id=f"stage_{stage}").expand(parameter=parameters)
        # Every stage depends on the whole previous one, so each boundary is a barrier: the
        # scheduler resolves the dependencies of a complete fan-out before the next stage runs.
        if previous is not None:
            previous >> current
        previous = current
