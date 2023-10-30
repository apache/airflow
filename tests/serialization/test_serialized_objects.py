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

from __future__ import annotations

import json
from datetime import datetime, timedelta

import pytest
from dateutil import relativedelta
from kubernetes.client import models as k8s
from pendulum.tz.timezone import Timezone

from airflow.datasets import Dataset
from airflow.exceptions import SerializationError
from airflow.jobs.job import Job
from airflow.models.connection import Connection
from airflow.models.dag import DAG, DagModel
from airflow.models.dagrun import DagRun
from airflow.models.param import Param
from airflow.models.taskinstance import SimpleTaskInstance, TaskInstance
from airflow.models.xcom_arg import XComArg
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.serialization.enums import DagAttributeTypes as DAT
from airflow.serialization.pydantic.dag import DagModelPydantic
from airflow.serialization.pydantic.dag_run import DagRunPydantic
from airflow.serialization.pydantic.job import JobPydantic
from airflow.serialization.pydantic.taskinstance import TaskInstancePydantic
from airflow.settings import _ENABLE_AIP_44
from airflow.utils.operator_resources import Resources
from airflow.utils.state import DagRunState, State
from airflow.utils.task_group import TaskGroup
from airflow.utils.types import DagRunType
from tests import REPO_ROOT


def test_recursive_serialize_calls_must_forward_kwargs():
    """Any time we recurse cls.serialize, we must forward all kwargs."""
    import ast

    valid_recursive_call_count = 0
    file = REPO_ROOT / "airflow/serialization/serialized_objects.py"
    content = file.read_text()
    tree = ast.parse(content)

    class_def = None
    for stmt in ast.walk(tree):
        if isinstance(stmt, ast.ClassDef) and stmt.name == "BaseSerialization":
            class_def = stmt

    method_def = None
    for elem in ast.walk(class_def):
        if isinstance(elem, ast.FunctionDef) and elem.name == "serialize":
            method_def = elem
            break
    kwonly_args = [x.arg for x in method_def.args.kwonlyargs]

    for elem in ast.walk(method_def):
        if isinstance(elem, ast.Call) and getattr(elem.func, "attr", "") == "serialize":
            kwargs = {y.arg: y.value for y in elem.keywords}
            for name in kwonly_args:
                if name not in kwargs or getattr(kwargs[name], "id", "") != name:
                    ref = f"{file}:{elem.lineno}"
                    message = (
                        f"Error at {ref}; recursive calls to `cls.serialize` "
                        f"must forward the `{name}` argument"
                    )
                    raise Exception(message)
                valid_recursive_call_count += 1
    print(f"validated calls: {valid_recursive_call_count}")
    assert valid_recursive_call_count > 0


def test_strict_mode():
    """If strict=True, serialization should fail when object is not JSON serializable."""

    class Test:
        a = 1

    from airflow.serialization.serialized_objects import BaseSerialization

    obj = [[[Test()]]]  # nested to verify recursive behavior
    BaseSerialization.serialize(obj)  # does not raise
    with pytest.raises(SerializationError, match="Encountered unexpected type"):
        BaseSerialization.serialize(obj, strict=True)  # now raises


TI = TaskInstance(
    task=EmptyOperator(task_id="test-task"),
    run_id="fake_run",
    state=State.RUNNING,
)

TI_WITH_START_DAY = TaskInstance(
    task=EmptyOperator(task_id="test-task"),
    run_id="fake_run",
    state=State.RUNNING,
)
TI_WITH_START_DAY.start_date = datetime.utcnow()

DAG_RUN = DagRun(
    dag_id="test_dag_id",
    run_id="test_dag_run_id",
    run_type=DagRunType.MANUAL,
    execution_date=datetime.utcnow(),
    start_date=datetime.utcnow(),
    external_trigger=True,
    state=DagRunState.SUCCESS,
)
DAG_RUN.id = 1


def equals(a, b) -> bool:
    return a == b


def equal_time(a: datetime, b: datetime) -> bool:
    return a.strftime("%s") == b.strftime("%s")


@pytest.mark.parametrize(
    "input, encoded_type, cmp_func",
    [
        ("test_str", None, equals),
        (1, None, equals),
        (datetime.utcnow(), DAT.DATETIME, equal_time),
        (timedelta(minutes=2), DAT.TIMEDELTA, equals),
        (Timezone("UTC"), DAT.TIMEZONE, lambda a, b: a.name == b.name),
        (relativedelta.relativedelta(hours=+1), DAT.RELATIVEDELTA, lambda a, b: a.hours == b.hours),
        ({"test": "dict", "test-1": 1}, None, equals),
        (["array_item", 2], None, equals),
        (("tuple_item", 3), DAT.TUPLE, equals),
        (set(["set_item", 3]), DAT.SET, equals),
        (
            k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(
                    name="test", annotations={"test": "annotation"}, creation_timestamp=datetime.utcnow()
                )
            ),
            DAT.POD,
            equals,
        ),
        (
            DAG(
                "fake-dag",
                schedule="*/10 * * * *",
                default_args={"depends_on_past": True},
                start_date=datetime.utcnow(),
                catchup=False,
            ),
            DAT.DAG,
            lambda a, b: a.dag_id == b.dag_id and equal_time(a.start_date, b.start_date),
        ),
        (Resources(cpus=0.1, ram=2048), None, None),
        (EmptyOperator(task_id="test-task"), None, None),
        (TaskGroup(group_id="test-group", dag=DAG(dag_id="test_dag", start_date=datetime.now())), None, None),
        (
            Param("test", "desc"),
            DAT.PARAM,
            lambda a, b: a.value == b.value and a.description == b.description,
        ),
        (
            XComArg(
                operator=PythonOperator(
                    python_callable=int,
                    task_id="test_xcom_op",
                    do_xcom_push=True,
                )
            ),
            DAT.XCOM_REF,
            None,
        ),
        (Dataset(uri="test"), DAT.DATASET, equals),
        (SimpleTaskInstance.from_ti(ti=TI), DAT.SIMPLE_TASK_INSTANCE, equals),
        (
            Connection(conn_id="TEST_ID", uri="mysql://"),
            DAT.CONNECTION,
            lambda a, b: a.get_uri() == b.get_uri(),
        ),
    ],
)
def test_serialize_deserialize(input, encoded_type, cmp_func):
    from airflow.serialization.serialized_objects import BaseSerialization

    serialized = BaseSerialization.serialize(input)  # does not raise
    json.dumps(serialized)  # does not raise
    if encoded_type is not None:
        assert serialized["__type"] == encoded_type
        assert serialized["__var"] is not None
    if cmp_func is not None:
        deserialized = BaseSerialization.deserialize(serialized)
        assert cmp_func(input, deserialized)

    # Verify recursive behavior
    obj = [[input]]
    serialized = BaseSerialization.serialize(obj)  # does not raise
    # Verify the result is JSON-serializable
    json.dumps(serialized)  # does not raise


@pytest.mark.skipif(not _ENABLE_AIP_44, reason="AIP-44 is disabled")
@pytest.mark.parametrize(
    "input, pydantic_class, encoded_type, cmp_func",
    [
        (
            Job(state=State.RUNNING, latest_heartbeat=datetime.utcnow()),
            JobPydantic,
            DAT.BASE_JOB,
            lambda a, b: equal_time(a.latest_heartbeat, b.latest_heartbeat),
        ),
        (
            TI_WITH_START_DAY,
            TaskInstancePydantic,
            DAT.TASK_INSTANCE,
            lambda a, b: equal_time(a.start_date, b.start_date),
        ),
        (
            DAG_RUN,
            DagRunPydantic,
            DAT.DAG_RUN,
            lambda a, b: equal_time(a.execution_date, b.execution_date)
            and equal_time(a.start_date, b.start_date),
        ),
        # DataSet is already serialized by non-Pydantic serialization. Is DatasetPydantic needed then?
        # (
        #     Dataset(
        #         uri="foo://bar",
        #         extra={"foo": "bar"},
        #     ),
        #     DatasetPydantic,
        #     DAT.DATA_SET,
        #     lambda a, b: a.uri == b.uri and a.extra == b.extra,
        # ),
        (
            DagModel(
                dag_id="TEST_DAG_1",
                fileloc="/tmp/dag_1.py",
                schedule_interval="2 2 * * *",
                is_paused=True,
            ),
            DagModelPydantic,
            DAT.DAG_MODEL,
            lambda a, b: a.fileloc == b.fileloc and a.schedule_interval == b.schedule_interval,
        ),
    ],
)
def test_serialize_deserialize_pydantic(input, pydantic_class, encoded_type, cmp_func):
    """If use_pydantic_models=True the objects should be serialized to Pydantic objects."""

    from airflow.serialization.serialized_objects import BaseSerialization

    serialized = BaseSerialization.serialize(input, use_pydantic_models=True)  # does not raise
    # Verify the result is JSON-serializable
    json.dumps(serialized)  # does not raise
    assert serialized["__type"] == encoded_type
    assert serialized["__var"] is not None
    deserialized = BaseSerialization.deserialize(serialized, use_pydantic_models=True)
    assert isinstance(deserialized, pydantic_class)
    assert cmp_func(input, deserialized)

    # Verify recursive behavior
    obj = [[input]]
    BaseSerialization.serialize(obj, use_pydantic_models=True)  # does not raise


@pytest.mark.db_test
def test_serialized_mapped_operator_unmap(dag_maker):
    from airflow.serialization.serialized_objects import SerializedDAG
    from tests.test_utils.mock_operators import MockOperator

    with dag_maker(dag_id="dag") as dag:
        MockOperator(task_id="task1", arg1="x")
        MockOperator.partial(task_id="task2").expand(arg1=["a", "b"])

    serialized_dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))
    assert serialized_dag.dag_id == "dag"

    serialized_task1 = serialized_dag.get_task("task1")
    assert serialized_task1.dag is serialized_dag

    serialized_task2 = serialized_dag.get_task("task2")
    assert serialized_task2.dag is serialized_dag

    serialized_unmapped_task = serialized_task2.unmap(None)
    assert serialized_unmapped_task.dag is serialized_dag
