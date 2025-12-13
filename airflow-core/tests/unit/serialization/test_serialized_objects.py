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
import math
import sys
from collections.abc import Iterator
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import pendulum
import pytest
from dateutil import relativedelta
from kubernetes.client import models as k8s
from pendulum.tz.timezone import FixedTimezone, Timezone
from uuid6 import uuid7

from airflow._shared.timezones import timezone
from airflow.callbacks.callback_requests import DagCallbackRequest, TaskCallbackRequest
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
from airflow.models.taskinstance import TaskInstance
from airflow.models.xcom_arg import XComArg
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.triggers.file import FileDeleteTrigger
from airflow.sdk import BaseOperator
from airflow.sdk.definitions.asset import (
    Asset,
    AssetAlias,
    AssetAliasEvent,
    AssetAll,
    AssetAny,
    AssetRef,
    AssetUniqueKey,
    AssetWatcher,
)
from airflow.sdk.definitions.deadline import AsyncCallback, DeadlineAlert, DeadlineReference
from airflow.sdk.definitions.decorators import task
from airflow.sdk.definitions.operator_resources import Resources
from airflow.sdk.definitions.param import Param
from airflow.sdk.definitions.taskgroup import TaskGroup
from airflow.sdk.execution_time.context import OutletEventAccessor, OutletEventAccessors
from airflow.serialization.decoders import decode_deadline_alert
from airflow.serialization.encoders import encode_deadline_alert
from airflow.serialization.enums import DagAttributeTypes as DAT, Encoding
from airflow.serialization.serialized_objects import (
    BaseSerialization,
    LazyDeserializedDAG,
    SerializedDAG,
    _has_kubernetes,
    create_scheduler_operator,
)
from airflow.triggers.base import BaseTrigger
from airflow.utils.db import LazySelectSequence
from airflow.utils.state import DagRunState, State
from airflow.utils.types import DagRunType

from unit.models import DEFAULT_DATE

if TYPE_CHECKING:
    from pydantic.types import JsonValue

DAG_ID = "dag_id_1"

TEST_CALLBACK_PATH = f"{__name__}.empty_callback_for_deadline"
TEST_CALLBACK_KWARGS = {"arg1": "value1"}

REFERENCE_TYPES = [
    pytest.param(DeadlineReference.DAGRUN_LOGICAL_DATE, id="logical_date"),
    pytest.param(DeadlineReference.DAGRUN_QUEUED_AT, id="queued_at"),
    pytest.param(DeadlineReference.FIXED_DATETIME(DEFAULT_DATE), id="fixed_deadline"),
]


async def empty_callback_for_deadline():
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


def test_prevent_re_serialization_of_serialized_operators():
    """SerializedBaseOperator should not be re-serializable."""
    from airflow.serialization.serialized_objects import BaseSerialization, SerializedBaseOperator

    serialized_op = SerializedBaseOperator(task_id="test_task")

    with pytest.raises(SerializationError, match="Encountered unexpected type"):
        BaseSerialization.serialize(serialized_op, strict=True)


def test_validate_schema():
    from airflow.serialization.serialized_objects import BaseSerialization

    with pytest.raises(AirflowException, match="BaseSerialization is not set"):
        BaseSerialization.validate_schema({"any": "thing"})

    BaseSerialization._json_schema = object()
    with pytest.raises(TypeError, match="Invalid type: Only dict and str are supported"):
        BaseSerialization.validate_schema(123)


def test_serde_validate_schema_valid_json():
    from airflow.serialization.serialized_objects import BaseSerialization

    class Test:
        def validate(self, obj):
            self.obj = obj

    t = Test()
    BaseSerialization._json_schema = t
    BaseSerialization.validate_schema('{"foo": "bar"}')
    assert t.obj == {"foo": "bar"}


TI = TaskInstance(
    task=create_scheduler_operator(EmptyOperator(task_id="test-task")),
    run_id="fake_run",
    state=State.RUNNING,
    dag_version_id=uuid7(),
)

TI_WITH_START_DAY = TaskInstance(
    task=create_scheduler_operator(EmptyOperator(task_id="test-task")),
    run_id="fake_run",
    state=State.RUNNING,
    dag_version_id=uuid7(),
)
TI_WITH_START_DAY.start_date = timezone.datetime(2020, 1, 1, 0, 0, 0)

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
    key: Asset | AssetAlias, extra: dict[str, JsonValue], asset_alias_events: list[AssetAliasEvent]
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
    return a._dict.keys() == b._dict.keys() and all(
        equal_outlet_event_accessor(a._dict[key], b._dict[key]) for key in a._dict
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
    ("input", "encoded_type", "cmp_func"),
    [
        ("test_str", None, equals),
        (1, None, equals),
        (math.nan, None, lambda a, b: b == "nan"),
        (math.inf, None, lambda a, b: b == "inf"),
        (-math.inf, None, lambda a, b: b == "-inf"),
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
        (
            Connection(conn_id="TEST_ID", uri="mysql://"),
            DAT.CONNECTION,
            lambda a, b: a.get_uri() == b.get_uri(),
        ),
        (
            TaskCallbackRequest(
                filepath="filepath",
                ti=TI,
                bundle_name="testing",
                bundle_version=None,
            ),
            DAT.TASK_CALLBACK_REQUEST,
            lambda a, b: a.ti == b.ti,
        ),
        (
            DagCallbackRequest(
                filepath="filepath",
                dag_id="fake_dag",
                run_id="fake_run",
                bundle_name="testing",
                bundle_version=None,
            ),
            DAT.DAG_CALLBACK_REQUEST,
            lambda a, b: a.dag_id == b.dag_id,
        ),
        (Asset.ref(name="test"), DAT.ASSET_REF, lambda a, b: a.name == b.name),
        (
            DeadlineAlert(
                reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
                interval=timedelta(),
                callback=AsyncCallback("fake_callable"),
            ),
            None,
            None,
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
                        dest_asset_extra={"extra": "from asset itself"},
                        extra={"extra": "from event"},
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
                callback=AsyncCallback("valid.callback.path", kwargs={"arg1": "value1"}),
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
    original = DeadlineAlert(
        reference=reference,
        interval=timedelta(hours=1),
        callback=AsyncCallback(empty_callback_for_deadline, kwargs=TEST_CALLBACK_KWARGS),
    )

    serialized = encode_deadline_alert(original)
    assert serialized == {
        "reference": None,
        "interval": None,
        "callback": None,
    }

    deserialized = decode_deadline_alert(serialized)
    assert deserialized.reference.serialize_reference() == reference.serialize_reference()
    assert deserialized.interval == original.interval
    assert deserialized.callback == original.callback


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


def test_hash_property():
    from airflow.models.serialized_dag import SerializedDagModel

    data = {"dag": {"dag_id": "dag1"}}
    lazy_serialized_dag = LazyDeserializedDAG(data=data)
    assert lazy_serialized_dag.hash == SerializedDagModel.hash(data)


@pytest.mark.parametrize(
    ("payload", "expected_cls"),
    [
        pytest.param(
            {
                "__type": DAT.ASSET,
                "name": "test_asset",
                "uri": "test://asset-uri",
                "group": "test-group",
                "extra": {},
            },
            Asset,
            id="asset",
        ),
        pytest.param(
            {
                "__type": DAT.ASSET_ALL,
                "objects": [
                    {
                        "__type": DAT.ASSET,
                        "name": "x",
                        "uri": "test://x",
                        "group": "g",
                        "extra": {},
                    },
                    {
                        "__type": DAT.ASSET,
                        "name": "x",
                        "uri": "test://x",
                        "group": "g",
                        "extra": {},
                    },
                ],
            },
            AssetAll,
            id="asset_all",
        ),
        pytest.param(
            {
                "__type": DAT.ASSET_ANY,
                "objects": [
                    {
                        "__type": DAT.ASSET,
                        "name": "y",
                        "uri": "test://y",
                        "group": "g",
                        "extra": {},
                    }
                ],
            },
            AssetAny,
            id="asset_any",
        ),
        pytest.param(
            {"__type": DAT.ASSET_ALIAS, "name": "alias", "group": "g"},
            AssetAlias,
            id="asset_alias",
        ),
        pytest.param(
            {"__type": DAT.ASSET_REF, "name": "ref"},
            AssetRef,
            id="asset_ref",
        ),
    ],
)
def test_serde_decode_asset_condition_success(payload, expected_cls):
    from airflow.serialization.serialized_objects import decode_asset_condition

    assert isinstance(decode_asset_condition(payload), expected_cls)


def test_serde_decode_asset_condition_unknown_type():
    from airflow.serialization.serialized_objects import decode_asset_condition

    with pytest.raises(
        ValueError,
        match="deserialization not implemented for DAT 'UNKNOWN_TYPE'",
    ):
        decode_asset_condition({"__type": "UNKNOWN_TYPE"})


def test_encode_timezone():
    from airflow.serialization.serialized_objects import encode_timezone

    assert encode_timezone(FixedTimezone(0)) == "UTC"
    with pytest.raises(ValueError, match="DAG timezone should be a pendulum.tz.Timezone"):
        encode_timezone(object())


class TestSerializedBaseOperator:
    # ensure the default logging config is used for this test, no matter what ran before
    @pytest.mark.usefixtures("reset_logging_config")
    def test_logging_propogated_by_default(self, caplog):
        """Test that when set_context hasn't been called that log records are emitted"""
        BaseOperator(task_id="test").log.warning("test")
        # This looks like "how could it fail" but this actually checks that the handler called `emit`. Testing
        # the other case (that when we have set_context it goes to the file is harder to achieve without
        # leaking a lot of state)
        assert caplog.messages == ["test"]

    def test_resume_execution(self):
        from airflow.models.trigger import TriggerFailureReason
        from airflow.sdk.exceptions import TaskDeferralTimeout

        op = BaseOperator(task_id="hi")
        with pytest.raises(TaskDeferralTimeout):
            op.resume_execution(
                next_method="__fail__",
                next_kwargs={"error": TriggerFailureReason.TRIGGER_TIMEOUT},
                context={},
            )


class TestKubernetesImportAvoidance:
    """Test that serialization doesn't import kubernetes unnecessarily."""

    def test_has_kubernetes_no_import_when_not_needed(self):
        """Ensure _has_kubernetes() doesn't import k8s when not already loaded."""
        # Remove kubernetes from sys.modules if present
        k8s_modules = [m for m in list(sys.modules.keys()) if m.startswith("kubernetes")]
        if k8s_modules:
            pytest.skip("Kubernetes already imported, cannot test import avoidance")

        # Call _has_kubernetes() - should check sys.modules and return False without importing
        _has_kubernetes.cache_clear()
        result = _has_kubernetes()

        assert result is False
        assert "kubernetes.client" not in sys.modules

    def test_has_kubernetes_uses_existing_import(self):
        """Ensure _has_kubernetes() uses k8s when it's already imported."""
        pytest.importorskip("kubernetes")

        # Now k8s is imported, should return True
        _has_kubernetes.cache_clear()
        result = _has_kubernetes()

        assert result is True
