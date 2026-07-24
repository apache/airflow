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
timing sequential vs goroutine XCom pulls), and ``taskflow_binding_dag``
(stressing the TaskFlow argument-binding surface -- the flat, positional
parameter list ``via_flat_args`` binds onto, plus three name-based
(keyword-style) struct examples, ``via_struct_no_tags``/``via_struct_arg_tag``/
``via_struct_unmatched_arg``, each isolating one field-binding mode; see its
Dag function below).

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

from datetime import timedelta

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
    # TaskFlow-style call: "uk" is captured as a literal argument and
    # ``extracted`` as an XCom reference; both bind onto the Go function's
    # data parameters at execution time (this also wires extract >> transform).
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


# Capitalized parameters on purpose: with no ``arg:`` tags on the Go side, each
# struct field binds the argument spelled exactly like its Go field name.
@task.stub(queue="golang")
def via_struct_no_tags(RegionCode: str, Threshold: float): ...


@task.stub(queue="golang")
def via_struct_arg_tag(region_code: str, threshold: float): ...


@task.stub(queue="golang")
def via_struct_unmatched_arg(region_code: str, sample_rate: float = 0.1): ...


@task.stub(queue="golang")
def via_flat_map(config: dict): ...


@task.stub(queue="golang")
def via_struct_map(payload: dict): ...


@dag(dag_id="taskflow_binding_dag")
def taskflow_binding_dag():
    """
    Stress the TaskFlow argument-binding surface beyond ``simple_dag``'s transform.

    Conceptually, the flat parameter list is *positional-argument* binding: order
    matters, and every parameter must be filled or the task fails before it runs.
    A task whose sole data parameter is a struct is closer to *keyword-argument*
    binding: fields match by name, and (see ``via_struct_unmatched_arg`` below) a
    field whose name has no corresponding TaskFlow call argument simply stays at
    its zero value instead of failing the task -- the same way an unpassed keyword
    argument falls back to a default in kwargs-style calls.

    ``via_flat_args``'s one mixed positional/keyword call carries literals of every
    scalar type plus an array literal, and fans in XComs from *two* upstream Go
    tasks: ``make_config`` returns an object that binds onto a strictly-decoded Go
    struct, ``make_numbers`` an array that binds onto ``[]int``. ``note`` is not
    passed, so its ``None`` default is captured and arrives in Go as a nil
    ``*string``. The Go ``via_flat_args`` (``go-sdk/example/bundle/taskflowbinding``)
    verifies every bound value and fails the task on any mismatch.

    Three further tasks demonstrate the Go SDK's name-based struct binding, used
    when a struct is the task's sole data parameter. Each call mixes a literal
    (``threshold``) with an XCom reference: the
    ``region_code`` argument is ``make_region``'s output, so every struct example
    also proves an XCom-sourced value binds onto a struct field. One field-binding
    mode at a time:

    * ``via_struct_no_tags``: no ``arg:`` tags at all -- each struct field binds
      the argument spelled exactly like its Go field name, hence this stub's
      capitalized ``RegionCode``/``Threshold`` parameters.
    * ``via_struct_arg_tag``: every field names its argument via an explicit
      ``arg:`` tag -- ``Region`` is genuinely renamed to ``region_code``, and
      ``Threshold`` is tagged ``threshold`` to pull the snake_case argument its
      verbatim field name would miss.
    * ``via_struct_unmatched_arg``: the mismatch tolerance in both directions.
      The Go struct declares a field with no corresponding argument in this
      TaskFlow call at all -- it stays at its Go zero value rather than failing
      the task. And the stub's defaulted ``sample_rate`` is never passed, so its
      captured-from-default entry needs no matching struct field (an explicitly
      passed argument no field claims would fail the task instead).

    ``via_flat_map`` and ``via_struct_map`` each pass a single dict literal to
    show the two ways one map binds on the Go side: ``via_flat_map``'s ``config``
    argument matches no struct field, so the whole dict is decoded into a Go
    struct (flat), while ``via_struct_map``'s ``payload`` argument binds by name
    onto a Go struct's ``map`` field (struct-based).
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
    via_struct_no_tags(RegionCode=region, Threshold=0.75)
    via_struct_arg_tag(region_code=region, threshold=0.75)
    via_struct_unmatched_arg(region_code=region)
    via_flat_map(config={"region": "eu-west-1", "count": 3})
    via_struct_map(payload={"region": "eu-west-1", "count": 3})


taskflow_binding_dag()
