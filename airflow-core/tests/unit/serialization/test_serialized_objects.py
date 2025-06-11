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
from collections.abc import Iterator
from datetime import datetime, timedelta

import pendulum
import pytest
from dateutil import relativedelta
from kubernetes.client import models as k8s
from pendulum.tz.timezone import Timezone

from airflow.exceptions import (
    AirflowException,
    AirflowFailException,
    AirflowRescheduleException,
    SerializationError,
    TaskDeferred,
)
from airflow.models.connection import Connection
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import SimpleTaskInstance, TaskInstance
from airflow.models.xcom_arg import XComArg
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.triggers.file import FileDeleteTrigger
from airflow.sdk.definitions.asset import Asset, AssetAlias, AssetAliasEvent, AssetUniqueKey, AssetWatcher
from airflow.sdk.definitions.deadline import DeadlineAlert, DeadlineAlertFields, DeadlineReference
from airflow.sdk.definitions.decorators import task
from airflow.sdk.definitions.param import Param
from airflow.sdk.execution_time.context import OutletEventAccessor, OutletEventAccessors
from airflow.serialization.enums import DagAttributeTypes as DAT, Encoding
from airflow.serialization.serialized_objects import BaseSerialization, LazyDeserializedDAG, SerializedDAG
from airflow.triggers.base import BaseTrigger
from airflow.utils import timezone
from airflow.utils.db import LazySelectSequence
from airflow.utils.operator_resources import Resources
from airflow.utils.state import DagRunState, State
from airflow.utils.task_group import TaskGroup
from airflow.utils.types import DagRunType

from unit.models import DEFAULT_DATE

DAG_ID = "dag_id_1"

TEST_CALLBACK_PATH = f"{__name__}.test_callback_for_deadline"
TEST_CALLBACK_KWARGS = {"arg1": "value1"}

REFERENCE_TYPES = [
    pytest.param(DeadlineReference.DAGRUN_LOGICAL_DATE, id="logical_date"),
    pytest.param(DeadlineReference.DAGRUN_QUEUED_AT, id="queued_at"),
    pytest.param(DeadlineReference.FIXED_DATETIME(DEFAULT_DATE), id="fixed_deadline"),
]


def test_callback_for_deadline():
    """Used in a number of tests to confirm that Deadlines and DeadlineAlerts function correctly."""
    pass


def test_recursive_serialize_calls_must_forward_kwargs():
    """Any time we recurse cls.serialize, we must forward all kwargs."""
    import ast
    from pathlib import Path

    import airflow.serialization

    valid_recursive_call_count = 0
    skipped_recursive_calls = 0  # when another serialize method called
    file = Path(airflow.serialization.__path__[0]) / "serialized_objects.py"
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
            if not elem.func.value.id == "cls":
                skipped_recursive_calls += 1
                break
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
    assert skipped_recursive_calls == 1


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
TI_WITH_START_DAY.start_date = timezone.utcnow()

DAG_RUN = DagRun(
    dag_id="test_dag_id",
    run_id="test_dag_run_id",
    run_type=DagRunType.MANUAL,
    logical_date=timezone.utcnow(),
    start_date=timezone.utcnow(),
    state=DagRunState.SUCCESS,
)
DAG_RUN.id = 1


# we add the tasks out of order, to ensure they are deserialized in the correct order
DAG_WITH_TASKS = DAG(dag_id="test_dag", start_date=datetime.now())
EmptyOperator(task_id="task2", dag=DAG_WITH_TASKS)
EmptyOperator(task_id="task1", dag=DAG_WITH_TASKS)


def create_outlet_event_accessors(
    key: Asset | AssetAlias, extra: dict, asset_alias_events: list[AssetAliasEvent]
) -> OutletEventAccessors:
    o = OutletEventAccessors()
    o[key].extra = extra
    o[key].asset_alias_events = asset_alias_events

    return o


def equals(a, b) -> bool:
    return a == b


def equal_time(a: datetime, b: datetime) -> bool:
    return a.strftime("%s") == b.strftime("%s")


def equal_exception(a: AirflowException, b: AirflowException) -> bool:
    return a.__class__ == b.__class__ and str(a) == str(b)


def equal_outlet_event_accessors(a: OutletEventAccessors, b: OutletEventAccessors) -> bool:
    return a._dict.keys() == b._dict.keys() and all(  # type: ignore[attr-defined]
        equal_outlet_event_accessor(a._dict[key], b._dict[key])  # type: ignore[attr-defined]
        for key in a._dict  # type: ignore[attr-defined]
    )


def equal_outlet_event_accessor(a: OutletEventAccessor, b: OutletEventAccessor) -> bool:
    return a.key == b.key and a.extra == b.extra and a.asset_alias_events == b.asset_alias_events


class MockLazySelectSequence(LazySelectSequence):
    _data = ["a", "b", "c"]

    def __init__(self):
        super().__init__(None, None, session="MockSession")

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)


@pytest.mark.parametrize(
    "input, encoded_type, cmp_func",
    [
        ("test_str", None, equals),
        (1, None, equals),
        (timezone.utcnow(), DAT.DATETIME, equal_time),
        (timedelta(minutes=2), DAT.TIMEDELTA, equals),
        (Timezone("UTC"), DAT.TIMEZONE, lambda a, b: a.name == b.name),
        (
            relativedelta.relativedelta(hours=+1),
            DAT.RELATIVEDELTA,
            lambda a, b: a.hours == b.hours,
        ),
        ({"test": "dict", "test-1": 1}, None, equals),
        (["array_item", 2], None, equals),
        (("tuple_item", 3), DAT.TUPLE, equals),
        (set(["set_item", 3]), DAT.SET, equals),
        (
            k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(
                    name="test",
                    annotations={"test": "annotation"},
                    creation_timestamp=timezone.utcnow(),
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
                start_date=timezone.utcnow(),
                catchup=False,
            ),
            DAT.DAG,
            lambda a, b: a.dag_id == b.dag_id and equal_time(a.start_date, b.start_date),
        ),
        (Resources(cpus=0.1, ram=2048), None, None),
        (EmptyOperator(task_id="test-task"), None, None),
        (
            TaskGroup(
                group_id="test-group",
                dag=DAG(dag_id="test_dag", start_date=datetime.now()),
            ),
            None,
            None,
        ),
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
        (
            MockLazySelectSequence(),
            None,
            lambda a, b: len(a) == len(b) and isinstance(b, list),
        ),
        (Asset(uri="test://asset1", name="test"), DAT.ASSET, equals),
        (
            Asset(
                uri="test://asset1",
                name="test",
                watchers=[AssetWatcher(name="test", trigger=FileDeleteTrigger(filepath="/tmp"))],
            ),
            DAT.ASSET,
            equals,
        ),
        (SimpleTaskInstance.from_ti(ti=TI), DAT.SIMPLE_TASK_INSTANCE, equals),
        (
            Connection(conn_id="TEST_ID", uri="mysql://"),
            DAT.CONNECTION,
            lambda a, b: a.get_uri() == b.get_uri(),
        ),
        (
            create_outlet_event_accessors(
                Asset(uri="test", name="test", group="test-group"), {"key": "value"}, []
            ),
            DAT.ASSET_EVENT_ACCESSORS,
            equal_outlet_event_accessors,
        ),
        (
            create_outlet_event_accessors(
                AssetAlias(name="test_alias", group="test-alias-group"),
                {"key": "value"},
                [
                    AssetAliasEvent(
                        source_alias_name="test_alias",
                        dest_asset_key=AssetUniqueKey(name="test_name", uri="test://asset-uri"),
                        extra={},
                    )
                ],
            ),
            DAT.ASSET_EVENT_ACCESSORS,
            equal_outlet_event_accessors,
        ),
        (
            AirflowException("test123 wohoo!"),
            DAT.AIRFLOW_EXC_SER,
            equal_exception,
        ),
        (
            AirflowFailException("uuups, failed :-("),
            DAT.AIRFLOW_EXC_SER,
            equal_exception,
        ),
        (
            DAG_WITH_TASKS,
            DAT.DAG,
            lambda _, b: list(b.task_group.children.keys()) == sorted(b.task_group.children.keys()),
        ),
        (
            DeadlineAlert(
                reference=DeadlineReference.DAGRUN_QUEUED_AT,
                interval=timedelta(hours=1),
                callback="valid.callback.path",
                callback_kwargs={"arg1": "value1"},
            ),
            DAT.DEADLINE_ALERT,
            equals,
        ),
    ],
)
def test_serialize_deserialize(input, encoded_type, cmp_func):
    from airflow.serialization.serialized_objects import BaseSerialization

    serialized = BaseSerialization.serialize(input)  # does not raise
    json.dumps(serialized)  # does not raise
    if encoded_type is not None:
        assert serialized[Encoding.TYPE] == encoded_type
        assert serialized[Encoding.VAR] is not None
    if cmp_func is not None:
        deserialized = BaseSerialization.deserialize(serialized)
        assert cmp_func(input, deserialized)

    # Verify recursive behavior
    obj = [[input]]
    serialized = BaseSerialization.serialize(obj)  # does not raise
    # Verify the result is JSON-serializable
    json.dumps(serialized)  # does not raise


@pytest.mark.parametrize("reference", REFERENCE_TYPES)
def test_serialize_deserialize_deadline_alert(reference):
    public_deadline_alert_fields = {
        field.lower() for field in vars(DeadlineAlertFields) if not field.startswith("_")
    }
    original = DeadlineAlert(
        reference=reference,
        interval=timedelta(hours=1),
        callback=test_callback_for_deadline,
        callback_kwargs=TEST_CALLBACK_KWARGS,
    )

    serialized = original.serialize_deadline_alert()
    assert serialized[Encoding.TYPE] == DAT.DEADLINE_ALERT
    assert set(serialized[Encoding.VAR].keys()) == public_deadline_alert_fields

    deserialized = DeadlineAlert.deserialize_deadline_alert(serialized)
    assert deserialized.reference.serialize_reference() == reference.serialize_reference()
    assert deserialized.interval == original.interval
    assert deserialized.callback_kwargs == original.callback_kwargs
    assert isinstance(deserialized.callback, str)
    assert deserialized.callback == TEST_CALLBACK_PATH


@pytest.mark.parametrize(
    "conn_uri",
    [
        pytest.param("aws://", id="only-conn-type"),
        pytest.param(
            "postgres://username:password@ec2.compute.com:5432/the_database",
            id="all-non-extra",
        ),
        pytest.param(
            "///?__extra__=%7B%22foo%22%3A+%22bar%22%2C+%22answer%22%3A+42%2C+%22"
            "nullable%22%3A+null%2C+%22empty%22%3A+%22%22%2C+%22zero%22%3A+0%7D",
            id="extra",
        ),
    ],
)
def test_backcompat_deserialize_connection(conn_uri):
    """Test deserialize connection which serialised by previous serializer implementation."""
    from airflow.serialization.serialized_objects import BaseSerialization

    conn_obj = {
        Encoding.TYPE: DAT.CONNECTION,
        Encoding.VAR: {"conn_id": "TEST_ID", "uri": conn_uri},
    }
    deserialized = BaseSerialization.deserialize(conn_obj)
    assert deserialized.get_uri() == conn_uri


@pytest.mark.db_test
def test_serialized_mapped_operator_unmap(dag_maker):
    from airflow.serialization.serialized_objects import SerializedDAG

    from tests_common.test_utils.mock_operators import MockOperator

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


def test_ser_of_asset_event_accessor():
    # todo: (Airflow 3.0) we should force reserialization on upgrade
    d = OutletEventAccessors()
    d[
        Asset("hi")
    ].extra = "blah1"  # todo: this should maybe be forbidden?  i.e. can extra be any json or just dict?
    d[Asset(name="yo", uri="test://yo")].extra = {"this": "that", "the": "other"}
    ser = BaseSerialization.serialize(var=d)
    deser = BaseSerialization.deserialize(ser)
    assert deser[Asset(uri="hi", name="hi")].extra == "blah1"
    assert d[Asset(name="yo", uri="test://yo")].extra == {"this": "that", "the": "other"}


class MyTrigger(BaseTrigger):
    def __init__(self, hi):
        self.hi = hi

    def serialize(self):
        return "unit.serialization.test_serialized_objects.MyTrigger", {"hi": self.hi}

    async def run(self):
        yield


def test_roundtrip_exceptions():
    """
    This is for AIP-44 when we need to send certain non-error exceptions
    as part of an RPC call e.g. TaskDeferred or AirflowRescheduleException.
    """
    some_date = pendulum.now()
    resched_exc = AirflowRescheduleException(reschedule_date=some_date)
    ser = BaseSerialization.serialize(resched_exc)
    deser = BaseSerialization.deserialize(ser)
    assert isinstance(deser, AirflowRescheduleException)
    assert deser.reschedule_date == some_date
    del ser
    del deser
    exc = TaskDeferred(
        trigger=MyTrigger(hi="yo"),
        method_name="meth_name",
        kwargs={"have": "pie"},
        timeout=timedelta(seconds=30),
    )
    ser = BaseSerialization.serialize(exc)
    deser = BaseSerialization.deserialize(ser)
    assert deser.trigger.hi == "yo"
    assert deser.method_name == "meth_name"
    assert deser.kwargs == {"have": "pie"}
    assert deser.timeout == timedelta(seconds=30)


@pytest.mark.db_test
def test_serialized_dag_to_dict_and_from_dict_gives_same_result_in_tasks(dag_maker):
    with dag_maker() as dag:
        BashOperator(task_id="task1", bash_command="echo 1")

    dag1 = SerializedDAG.to_dict(dag)
    from_dict = SerializedDAG.from_dict(dag1)
    dag2 = SerializedDAG.to_dict(from_dict)

    assert dag2["dag"]["tasks"][0]["__var"].keys() == dag1["dag"]["tasks"][0]["__var"].keys()


@pytest.mark.parametrize(
    "concurrency_parameter",
    [
        "max_active_tis_per_dag",
        "max_active_tis_per_dagrun",
    ],
)
@pytest.mark.db_test
def test_serialized_dag_has_task_concurrency_limits(dag_maker, concurrency_parameter):
    with dag_maker() as dag:
        BashOperator(task_id="task1", bash_command="echo 1", **{concurrency_parameter: 1})

    ser_dict = SerializedDAG.to_dict(dag)
    lazy_serialized_dag = LazyDeserializedDAG(data=ser_dict)

    assert lazy_serialized_dag.has_task_concurrency_limits


@pytest.mark.parametrize(
    "concurrency_parameter",
    [
        "max_active_tis_per_dag",
        "max_active_tis_per_dagrun",
    ],
)
@pytest.mark.db_test
def test_serialized_dag_mapped_task_has_task_concurrency_limits(dag_maker, concurrency_parameter):
    with dag_maker() as dag:

        @task
        def my_task():
            return [1, 2, 3, 4, 5, 6, 7]

        @task(**{concurrency_parameter: 1})
        def map_me_but_slowly(a):
            pass

        map_me_but_slowly.expand(a=my_task())

    ser_dict = SerializedDAG.to_dict(dag)
    lazy_serialized_dag = LazyDeserializedDAG(data=ser_dict)

    assert lazy_serialized_dag.has_task_concurrency_limits


def test_get_task_assets():
    asset1 = Asset("1")
    with DAG("testdag") as source_dag:
        a = BashOperator(task_id="a", outlets=[asset1], bash_command="echo u")
        b = BashOperator(task_id="b", inlets=[asset1], bash_command="echo v")
        c = BashOperator.partial(task_id="c", inlets=[asset1]).expand(bash_command=["echo w", "echo x"])
        d = BashOperator.partial(task_id="d", outlets=[asset1]).expand(bash_command=["echo y", "echo z"])
        a >> b >> c >> d

    deser_dag = LazyDeserializedDAG(data=SerializedDAG.to_dict(source_dag))
    assert sorted(deser_dag.get_task_assets()) == [
        ("a", asset1),
        ("b", asset1),
        ("c", asset1),
        ("d", asset1),
    ]
