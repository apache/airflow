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
Python stub Dags mirroring the Go SDK example bundle (``go-sdk/example/bundle``).

Three Dags, all backed by the same Go bundle: ``simple_dag`` (extract/transform/
load, below), ``concurrent_xcom_dag`` (one ``pull_xcoms_concurrently`` task
timing sequential vs goroutine XCom pulls), and ``taskflow_binding_dag`` (one
task per shape of the TaskFlow argument-binding surface; see its Dag function
below).

``simple_dag`` sandwiches the Go tasks between two native Python tasks so the
run exercises XCom across the language boundary, the same way
``java-sdk/dags/java_examples.py`` does for the Java SDK::

    python_task_1 >> extract >> transform >> [load, python_task_2]

* ``python_task_1`` (Python) pushes an XCom.
* ``extract`` / ``transform`` / ``load`` are ``@task.stub(queue="golang")`` tasks
  whose implementations live in the compiled Go bundle. The ``golang`` queue is
  routed to the ``ExecutableCoordinator``, which locates the bundle by dag_id and
  runs the binary in coordinator mode. ``extract`` returns a map (pushed as its
  ``return_value`` XCom); ``transform`` reads the ``my_variable`` variable.
* ``transform`` is called TaskFlow-style -- ``transform("uk", extract())`` -- so
  the stub captures a positional-argument spec (a literal plus an XCom
  reference) that the Go runtime binds onto the Go function's ``country`` and
  ``extracted`` parameters, pulling ``extract``'s XCom on demand.
* ``load`` (``retries=1``) returns an error on its first attempt and succeeds
  on the retry, exercising the UP_FOR_RETRY path through the Go coordinator. It
  is a leaf (not upstream of ``python_task_2``) so its retry is observable
  while leaving the Go -> Python XCom hop intact.
* ``python_task_2`` (Python) pulls the Go ``extract`` task's XCom and re-emits
  it, demonstrating the Go -> Python direction end-to-end.

The dag_id and the Go task ids MUST match the identities the Go bundle exposes
via ``--airflow-metadata`` so the coordinator's bundle scanner can locate the
binary by dag_id and look up each task by id. The Python task ids run on the
default Python executor and are independent of the bundle.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from uuid import UUID

from airflow.sdk import dag, task


@task()
def python_task_1():
    print("python_task_1")
    print("Push Python Task 'python_task_1' XCom:")
    return "value_from_python_task_1"


@task.stub(queue="golang")
def extract(): ...


@task.stub(queue="golang")
def transform(country: str, extracted: dict): ...


# ``load`` fails on its first attempt and succeeds on the retry, exercising the
# UP_FOR_RETRY path through the Go coordinator. The short ``retry_delay`` keeps
# the end-to-end run fast.
@task.stub(queue="golang", retries=1, retry_delay=timedelta(seconds=5))
def load(): ...


@task()
def python_task_2(extracted):
    print("python_task_2")
    print("Pull Go Task 'extract' XCom:")
    print(extracted)
    return extracted


@dag(dag_id="simple_dag")
def simple_dag():
    extracted = extract()
    transformed = transform("uk", extracted)
    python_task_1() >> extracted >> transformed
    # ``load`` fails once then succeeds on retry; keep it a leaf (not upstream
    # of python_task_2) so its retry is observable without affecting the Python
    # task that pulls the Go XCom.
    transformed >> [load(), python_task_2(extracted)]


simple_dag()


@task.stub(queue="golang")
def pull_xcoms_concurrently(): ...


@dag(dag_id="concurrent_xcom_dag")
def concurrent_xcom_dag():
    pull_xcoms_concurrently()


concurrent_xcom_dag()


@task.stub(queue="golang")
def make_config(): ...


@task.stub(queue="golang")
def make_numbers(): ...


@task.stub(queue="golang")
def make_region(): ...


@task.stub(queue="golang")
def via_flat_args(
    name: str,
    count: int,
    ratio: float,
    enabled: bool,
    tags: list,
    config: dict,
    numbers: list,
    note: str | None = None,
): ...


# Go fields match these snake_case names without ``arg:`` tags.
@task.stub(queue="golang")
def via_struct_no_tags(region_code: str, threshold: float): ...


@task.stub(queue="golang")
def via_struct_arg_tag(region_code: str, threshold: float): ...


@task.stub(queue="golang")
def via_struct_unmatched_arg(region_code: str, sample_rate: float = 0.1): ...


@task.stub(queue="golang")
def via_flat_map(config: dict): ...


@task.stub(queue="golang")
def via_struct_map(payload: dict): ...


@task.stub(queue="golang")
def via_plain_map(labels: dict): ...


# Annotations select native Go types.
@task.stub(queue="golang")
def via_temporal_args(when: datetime, window: timedelta, trace_id: UUID): ...


@dag(dag_id="taskflow_binding_dag")
def taskflow_binding_dag():
    """
    Exercise the Go SDK's TaskFlow argument-binding surface, one shape per task.

    A Go task declares either flat data parameters, which bind *positionally*
    (order matters, every one must be filled), or a single struct, whose fields
    bind by *name* like keyword arguments -- an unmatched field stays at its Go
    zero value instead of failing the task.

    * ``via_flat_args``: every scalar literal, an array literal, keyword args,
      an unpassed ``None`` default, and XComs fanned in from two upstream tasks.
    * ``via_struct_no_tags``: fields fall back to their own Go names, matched
      case- and underscore-insensitively.
    * ``via_struct_arg_tag``: fields bind via explicit ``arg:`` tags.
    * ``via_struct_unmatched_arg``: a Go field no argument names, and a stub
      default no Go field claims.
    * ``via_flat_map`` / ``via_struct_map`` / ``via_plain_map``: one dict bound
      whole into a struct, onto a struct's map field, and into a plain Go map.
    * ``via_temporal_args``: formatted strings bound to native temporal and UUID types.

    Each ``via_struct_*`` call mixes a ``threshold`` literal with ``make_region``'s
    XCom, so struct fields are proven against both argument sources. The Go
    tasks verify every bound value and fail on any mismatch.
    """
    via_flat_args(
        "summary",
        3,
        2.5,
        True,
        ["metrics", "hourly"],
        config=make_config(),
        numbers=make_numbers(),
    )
    region = make_region()
    via_struct_no_tags(region_code=region, threshold=0.75)
    via_struct_arg_tag(region_code=region, threshold=0.75)
    via_struct_unmatched_arg(region_code=region)
    via_flat_map(config={"region": "eu-west-1", "count": 3})
    via_struct_map(payload={"region": "eu-west-1", "count": 3})
    via_plain_map(labels={"team": "data", "tier": "gold"})
    # Cross-runtime arguments use JSON strings.
    via_temporal_args(
        when="2024-01-02T03:04:05Z",
        window="PT5M",
        trace_id="6ba7b810-9dad-11d1-80b4-00c04fd430c8",
    )


taskflow_binding_dag()
