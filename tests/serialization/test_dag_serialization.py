#
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
"""Unit tests for stringified DAGs."""
from __future__ import annotations

import copy
import importlib
import importlib.util
import json
import multiprocessing
import os
import pickle
from datetime import datetime, timedelta
from glob import glob
from unittest import mock

import pendulum
import pytest
from dateutil.relativedelta import FR, relativedelta
from kubernetes.client import models as k8s

from airflow.datasets import Dataset
from airflow.exceptions import SerializationError
from airflow.hooks.base import BaseHook
from airflow.kubernetes.pod_generator import PodGenerator
from airflow.models import DAG, Connection, DagBag, Operator
from airflow.models.baseoperator import BaseOperator, BaseOperatorLink
from airflow.models.mappedoperator import MappedOperator
from airflow.models.param import Param, ParamsDict
from airflow.models.xcom import XCOM_RETURN_KEY, XCom
from airflow.operators.bash import BashOperator
from airflow.security import permissions
from airflow.sensors.bash import BashSensor
from airflow.serialization.json_schema import load_dag_schema_dict
from airflow.serialization.serialized_objects import (
    DagDependency,
    DependencyDetector,
    SerializedBaseOperator,
    SerializedDAG,
)
from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.timetables.simple import NullTimetable, OnceTimetable
from airflow.utils import timezone
from airflow.utils.context import Context
from airflow.utils.operator_resources import Resources
from airflow.utils.task_group import TaskGroup
from tests.test_utils.config import conf_vars
from tests.test_utils.mock_operators import CustomOperator, GoogleLink, MockOperator
from tests.test_utils.timetables import CustomSerializationTimetable, cron_timetable, delta_timetable


class CustomDepOperator(BashOperator):
    """
    Used for testing custom dependency detector.

    TODO: remove in Airflow 3.0
    """


class CustomDependencyDetector(DependencyDetector):
    """
    Prior to deprecation of custom dependency detector, the return type as Optional[DagDependency].
    This class verifies that custom dependency detector classes which assume that return type will still
    work until support for them is removed in 3.0.

    TODO: remove in Airflow 3.0
    """

    @staticmethod
    def detect_task_dependencies(task: Operator) -> DagDependency | None:  # type: ignore
        if isinstance(task, CustomDepOperator):
            return DagDependency(
                source=task.dag_id,
                target='nothing',
                dependency_type='abc',
                dependency_id=task.task_id,
            )
        else:
            return DependencyDetector().detect_task_dependencies(task)  # type: ignore


executor_config_pod = k8s.V1Pod(
    metadata=k8s.V1ObjectMeta(name="my-name"),
    spec=k8s.V1PodSpec(
        containers=[
            k8s.V1Container(name="base", volume_mounts=[k8s.V1VolumeMount(name="my-vol", mount_path="/vol/")])
        ]
    ),
)

serialized_simple_dag_ground_truth = {
    "__version": 1,
    "dag": {
        "default_args": {
            "__type": "dict",
            "__var": {
                "depends_on_past": False,
                "retries": 1,
                "retry_delay": {"__type": "timedelta", "__var": 300.0},
                "max_retry_delay": {"__type": "timedelta", "__var": 600.0},
                "sla": {"__type": "timedelta", "__var": 100.0},
            },
        },
        "start_date": 1564617600.0,
        '_task_group': {
            '_group_id': None,
            'prefix_group_id': True,
            'children': {'bash_task': ('operator', 'bash_task'), 'custom_task': ('operator', 'custom_task')},
            'tooltip': '',
            'ui_color': 'CornflowerBlue',
            'ui_fgcolor': '#000',
            'upstream_group_ids': [],
            'downstream_group_ids': [],
            'upstream_task_ids': [],
            'downstream_task_ids': [],
        },
        "is_paused_upon_creation": False,
        "_dag_id": "simple_dag",
        "doc_md": "### DAG Tutorial Documentation",
        "fileloc": None,
        "tasks": [
            {
                "task_id": "bash_task",
                "owner": "airflow",
                "retries": 1,
                "retry_delay": 300.0,
                "max_retry_delay": 600.0,
                "sla": 100.0,
                "downstream_task_ids": [],
                "_is_empty": False,
                "ui_color": "#f0ede4",
                "ui_fgcolor": "#000",
                "template_ext": ['.sh', '.bash'],
                "template_fields": ['bash_command', 'env'],
                "template_fields_renderers": {'bash_command': 'bash', 'env': 'json'},
                "bash_command": "echo {{ task.task_id }}",
                "_task_type": "BashOperator",
                "_task_module": "airflow.operators.bash",
                "pool": "default_pool",
                "executor_config": {
                    '__type': 'dict',
                    '__var': {
                        "pod_override": {
                            '__type': 'k8s.V1Pod',
                            '__var': PodGenerator.serialize_pod(executor_config_pod),
                        }
                    },
                },
                "doc_md": "### Task Tutorial Documentation",
            },
            {
                "task_id": "custom_task",
                "retries": 1,
                "retry_delay": 300.0,
                "max_retry_delay": 600.0,
                "sla": 100.0,
                "downstream_task_ids": [],
                "_is_empty": False,
                "_operator_extra_links": [{"tests.test_utils.mock_operators.CustomOpLink": {}}],
                "ui_color": "#fff",
                "ui_fgcolor": "#000",
                "template_ext": [],
                "template_fields": ['bash_command'],
                "template_fields_renderers": {},
                "_task_type": "CustomOperator",
                "_operator_name": "@custom",
                "_task_module": "tests.test_utils.mock_operators",
                "pool": "default_pool",
            },
        ],
        "schedule_interval": {"__type": "timedelta", "__var": 86400.0},
        "dataset_triggers": [],
        "timezone": "UTC",
        "_access_control": {
            "__type": "dict",
            "__var": {
                "test_role": {
                    "__type": "set",
                    "__var": [permissions.ACTION_CAN_READ, permissions.ACTION_CAN_EDIT],
                }
            },
        },
        "edge_info": {},
        "dag_dependencies": [],
        "params": {},
    },
}

ROOT_FOLDER = os.path.realpath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir)
)

CUSTOM_TIMETABLE_SERIALIZED = {
    "__type": "tests.test_utils.timetables.CustomSerializationTimetable",
    "__var": {"value": "foo"},
}


def make_example_dags(module_path):
    """Loads DAGs from a module for test."""
    dagbag = DagBag(module_path)
    return dagbag.dags


def make_simple_dag():
    """Make very simple DAG to verify serialization result."""
    with DAG(
        dag_id='simple_dag',
        default_args={
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "max_retry_delay": timedelta(minutes=10),
            "depends_on_past": False,
            "sla": timedelta(seconds=100),
        },
        start_date=datetime(2019, 8, 1),
        is_paused_upon_creation=False,
        access_control={"test_role": {permissions.ACTION_CAN_READ, permissions.ACTION_CAN_EDIT}},
        doc_md="### DAG Tutorial Documentation",
    ) as dag:
        CustomOperator(task_id='custom_task')
        BashOperator(
            task_id='bash_task',
            bash_command='echo {{ task.task_id }}',
            owner='airflow',
            executor_config={"pod_override": executor_config_pod},
            doc_md="### Task Tutorial Documentation",
        )
        return {'simple_dag': dag}


def make_user_defined_macro_filter_dag():
    """Make DAGs with user defined macros and filters using locally defined methods.

    For Webserver, we do not include ``user_defined_macros`` & ``user_defined_filters``.

    The examples here test:
        (1) functions can be successfully displayed on UI;
        (2) templates with function macros have been rendered before serialization.
    """

    def compute_next_execution_date(dag, execution_date):
        return dag.following_schedule(execution_date)

    default_args = {'start_date': datetime(2019, 7, 10)}
    dag = DAG(
        'user_defined_macro_filter_dag',
        default_args=default_args,
        user_defined_macros={
            'next_execution_date': compute_next_execution_date,
        },
        user_defined_filters={'hello': lambda name: f'Hello {name}'},
        catchup=False,
    )
    BashOperator(
        task_id='echo',
        bash_command='echo "{{ next_execution_date(dag, execution_date) }}"',
        dag=dag,
    )
    return {dag.dag_id: dag}


def collect_dags(dag_folder=None):
    """Collects DAGs to test."""
    dags = {}
    dags.update(make_simple_dag())
    dags.update(make_user_defined_macro_filter_dag())

    if dag_folder:
        if isinstance(dag_folder, (list, tuple)):
            patterns = dag_folder
        else:
            patterns = [dag_folder]
    else:
        patterns = [
            "airflow/example_dags",
            "airflow/providers/*/example_dags",  # TODO: Remove once AIP-47 is completed
            "airflow/providers/*/*/example_dags",  # TODO: Remove once AIP-47 is completed
            "tests/system/providers/*/",
            "tests/system/providers/*/*/",
        ]
    for pattern in patterns:
        for directory in glob(f"{ROOT_FOLDER}/{pattern}"):
            dags.update(make_example_dags(directory))

    # Filter subdags as they are stored in same row in Serialized Dag table
    dags = {dag_id: dag for dag_id, dag in dags.items() if not dag.is_subdag}
    return dags


def get_timetable_based_simple_dag(timetable):
    """Create a simple_dag variant that uses timetable instead of schedule_interval."""
    dag = collect_dags(["airflow/example_dags"])["simple_dag"]
    dag.timetable = timetable
    dag.schedule_interval = timetable.summary
    return dag


def serialize_subprocess(queue, dag_folder):
    """Validate pickle in a subprocess."""
    dags = collect_dags(dag_folder)
    for dag in dags.values():
        queue.put(SerializedDAG.to_json(dag))
    queue.put(None)


@pytest.fixture()
def timetable_plugin(monkeypatch):
    """Patch plugins manager to always and only return our custom timetable."""
    from airflow import plugins_manager

    monkeypatch.setattr(plugins_manager, "initialize_timetables_plugins", lambda: None)
    monkeypatch.setattr(
        plugins_manager,
        "timetable_classes",
        {"tests.test_utils.timetables.CustomSerializationTimetable": CustomSerializationTimetable},
    )


class TestStringifiedDAGs:
    """Unit tests for stringified DAGs."""

    def setup_method(self):
        self.backup_base_hook_get_connection = BaseHook.get_connection
        BaseHook.get_connection = mock.Mock(
            return_value=Connection(
                extra=(
                    '{'
                    '"project_id": "mock", '
                    '"location": "mock", '
                    '"instance": "mock", '
                    '"database_type": "postgres", '
                    '"use_proxy": "False", '
                    '"use_ssl": "False"'
                    '}'
                )
            )
        )
        self.maxDiff = None

    def teardown_method(self):
        BaseHook.get_connection = self.backup_base_hook_get_connection

    def test_serialization(self):
        """Serialization and deserialization should work for every DAG and Operator."""
        dags = collect_dags()
        serialized_dags = {}
        for _, v in dags.items():
            dag = SerializedDAG.to_dict(v)
            SerializedDAG.validate_schema(dag)
            serialized_dags[v.dag_id] = dag

        # Compares with the ground truth of JSON string.
        self.validate_serialized_dag(serialized_dags['simple_dag'], serialized_simple_dag_ground_truth)

    @pytest.mark.parametrize(
        "timetable, serialized_timetable",
        [
            (
                cron_timetable("0 0 * * *"),
                {
                    "__type": "airflow.timetables.interval.CronDataIntervalTimetable",
                    "__var": {"expression": "0 0 * * *", "timezone": "UTC"},
                },
            ),
            (
                CustomSerializationTimetable("foo"),
                CUSTOM_TIMETABLE_SERIALIZED,
            ),
        ],
    )
    @pytest.mark.usefixtures("timetable_plugin")
    def test_dag_serialization_to_timetable(self, timetable, serialized_timetable):
        """Verify a timetable-backed schedule_interval is excluded in serialization."""
        dag = get_timetable_based_simple_dag(timetable)
        serialized_dag = SerializedDAG.to_dict(dag)
        SerializedDAG.validate_schema(serialized_dag)

        expected = copy.deepcopy(serialized_simple_dag_ground_truth)
        del expected["dag"]["schedule_interval"]
        expected["dag"]["timetable"] = serialized_timetable

        self.validate_serialized_dag(serialized_dag, expected)

    def test_dag_serialization_unregistered_custom_timetable(self):
        """Verify serialization fails without timetable registration."""
        dag = get_timetable_based_simple_dag(CustomSerializationTimetable("bar"))
        with pytest.raises(SerializationError) as ctx:
            SerializedDAG.to_dict(dag)

        message = (
            "Failed to serialize DAG 'simple_dag': Timetable class "
            "'tests.test_utils.timetables.CustomSerializationTimetable' "
            "is not registered"
        )
        assert str(ctx.value) == message

    def validate_serialized_dag(self, json_dag, ground_truth_dag):
        """Verify serialized DAGs match the ground truth."""
        assert json_dag['dag']['fileloc'].split('/')[-1] == 'test_dag_serialization.py'
        json_dag['dag']['fileloc'] = None

        def sorted_serialized_dag(dag_dict: dict):
            """
            Sorts the "tasks" list and "access_control" permissions in the
            serialised dag python dictionary. This is needed as the order of
            items should not matter but assertEqual would fail if the order of
            items changes in the dag dictionary
            """
            dag_dict["dag"]["tasks"] = sorted(dag_dict["dag"]["tasks"], key=lambda x: sorted(x.keys()))
            dag_dict["dag"]["_access_control"]["__var"]["test_role"]["__var"] = sorted(
                dag_dict["dag"]["_access_control"]["__var"]["test_role"]["__var"]
            )
            return dag_dict

        assert sorted_serialized_dag(ground_truth_dag) == sorted_serialized_dag(json_dag)

    def test_deserialization_across_process(self):
        """A serialized DAG can be deserialized in another process."""

        # Since we need to parse the dags twice here (once in the subprocess,
        # and once here to get a DAG to compare to) we don't want to load all
        # dags.
        queue = multiprocessing.Queue()
        proc = multiprocessing.Process(target=serialize_subprocess, args=(queue, "airflow/example_dags"))
        proc.daemon = True
        proc.start()

        stringified_dags = {}
        while True:
            v = queue.get()
            if v is None:
                break
            dag = SerializedDAG.from_json(v)
            assert isinstance(dag, DAG)
            stringified_dags[dag.dag_id] = dag

        dags = collect_dags("airflow/example_dags")
        assert set(stringified_dags.keys()) == set(dags.keys())

        # Verify deserialized DAGs.
        for dag_id in stringified_dags:
            self.validate_deserialized_dag(stringified_dags[dag_id], dags[dag_id])

    def test_roundtrip_provider_example_dags(self):
        dags = collect_dags(
            [
                "airflow/providers/*/example_dags",
                "airflow/providers/*/*/example_dags",
            ]
        )

        # Verify deserialized DAGs.
        for dag in dags.values():
            serialized_dag = SerializedDAG.from_json(SerializedDAG.to_json(dag))
            self.validate_deserialized_dag(serialized_dag, dag)

    @pytest.mark.parametrize(
        "timetable",
        [cron_timetable("0 0 * * *"), CustomSerializationTimetable("foo")],
    )
    @pytest.mark.usefixtures("timetable_plugin")
    def test_dag_roundtrip_from_timetable(self, timetable):
        """Verify a timetable-backed serialization can be deserialized."""
        dag = get_timetable_based_simple_dag(timetable)
        roundtripped = SerializedDAG.from_json(SerializedDAG.to_json(dag))
        self.validate_deserialized_dag(roundtripped, dag)

    def validate_deserialized_dag(self, serialized_dag, dag):
        """
        Verify that all example DAGs work with DAG Serialization by
        checking fields between Serialized Dags & non-Serialized Dags
        """
        exclusion_list = {
            # Doesn't implement __eq__ properly. Check manually.
            'timetable',
            'timezone',
            # Need to check fields in it, to exclude functions.
            'default_args',
            "_task_group",
            'params',
        }
        fields_to_check = dag.get_serialized_fields() - exclusion_list
        for field in fields_to_check:
            assert getattr(serialized_dag, field) == getattr(
                dag, field
            ), f'{dag.dag_id}.{field} does not match'

        if dag.default_args:
            for k, v in dag.default_args.items():
                if callable(v):
                    # Check we stored _something_.
                    assert k in serialized_dag.default_args
                else:
                    assert (
                        v == serialized_dag.default_args[k]
                    ), f'{dag.dag_id}.default_args[{k}] does not match'

        assert serialized_dag.timetable.summary == dag.timetable.summary
        assert serialized_dag.timetable.serialize() == dag.timetable.serialize()
        assert serialized_dag.timezone.name == dag.timezone.name

        for task_id in dag.task_ids:
            self.validate_deserialized_task(serialized_dag.get_task(task_id), dag.get_task(task_id))

    def validate_deserialized_task(
        self,
        serialized_task,
        task,
    ):
        """Verify non-airflow operators are casted to BaseOperator."""
        assert isinstance(serialized_task, SerializedBaseOperator)
        assert not isinstance(task, SerializedBaseOperator)
        assert isinstance(task, BaseOperator)

        # Every task should have a task_group property -- even if it's the DAG's root task group
        assert serialized_task.task_group

        fields_to_check = task.get_serialized_fields() - {
            # Checked separately
            '_task_type',
            '_operator_name',
            'subdag',
            # Type is excluded, so don't check it
            '_log',
            # List vs tuple. Check separately
            'template_ext',
            'template_fields',
            # We store the string, real dag has the actual code
            'on_failure_callback',
            'on_success_callback',
            'on_retry_callback',
            # Checked separately
            'resources',
            'params',
        }

        assert serialized_task.task_type == task.task_type

        assert set(serialized_task.template_ext) == set(task.template_ext)
        assert set(serialized_task.template_fields) == set(task.template_fields)

        assert serialized_task.upstream_task_ids == task.upstream_task_ids
        assert serialized_task.downstream_task_ids == task.downstream_task_ids

        for field in fields_to_check:
            assert getattr(serialized_task, field) == getattr(
                task, field
            ), f'{task.dag.dag_id}.{task.task_id}.{field} does not match'

        if serialized_task.resources is None:
            assert task.resources is None or task.resources == []
        else:
            assert serialized_task.resources == task.resources

        # Ugly hack as some operators override params var in their init
        if isinstance(task.params, ParamsDict):
            assert serialized_task.params.dump() == task.params.dump()

        # Check that for Deserialized task, task.subdag is None for all other Operators
        # except for the SubDagOperator where task.subdag is an instance of DAG object
        if task.task_type == "SubDagOperator":
            assert serialized_task.subdag is not None
            assert isinstance(serialized_task.subdag, DAG)
        else:
            assert serialized_task.subdag is None

    @pytest.mark.parametrize(
        "dag_start_date, task_start_date, expected_task_start_date",
        [
            (datetime(2019, 8, 1, tzinfo=timezone.utc), None, datetime(2019, 8, 1, tzinfo=timezone.utc)),
            (
                datetime(2019, 8, 1, tzinfo=timezone.utc),
                datetime(2019, 8, 2, tzinfo=timezone.utc),
                datetime(2019, 8, 2, tzinfo=timezone.utc),
            ),
            (
                datetime(2019, 8, 1, tzinfo=timezone.utc),
                datetime(2019, 7, 30, tzinfo=timezone.utc),
                datetime(2019, 8, 1, tzinfo=timezone.utc),
            ),
            (pendulum.datetime(2019, 8, 1, tz='UTC'), None, pendulum.datetime(2019, 8, 1, tz='UTC')),
        ],
    )
    def test_deserialization_start_date(self, dag_start_date, task_start_date, expected_task_start_date):
        dag = DAG(dag_id='simple_dag', start_date=dag_start_date)
        BaseOperator(task_id='simple_task', dag=dag, start_date=task_start_date)

        serialized_dag = SerializedDAG.to_dict(dag)
        if not task_start_date or dag_start_date >= task_start_date:
            # If dag.start_date > task.start_date -> task.start_date=dag.start_date
            # because of the logic in dag.add_task()
            assert "start_date" not in serialized_dag["dag"]["tasks"][0]
        else:
            assert "start_date" in serialized_dag["dag"]["tasks"][0]

        dag = SerializedDAG.from_dict(serialized_dag)
        simple_task = dag.task_dict["simple_task"]
        assert simple_task.start_date == expected_task_start_date

    def test_deserialization_with_dag_context(self):
        with DAG(dag_id='simple_dag', start_date=datetime(2019, 8, 1, tzinfo=timezone.utc)) as dag:
            BaseOperator(task_id='simple_task')
            # should not raise RuntimeError: dictionary changed size during iteration
            SerializedDAG.to_dict(dag)

    @pytest.mark.parametrize(
        "dag_end_date, task_end_date, expected_task_end_date",
        [
            (datetime(2019, 8, 1, tzinfo=timezone.utc), None, datetime(2019, 8, 1, tzinfo=timezone.utc)),
            (
                datetime(2019, 8, 1, tzinfo=timezone.utc),
                datetime(2019, 8, 2, tzinfo=timezone.utc),
                datetime(2019, 8, 1, tzinfo=timezone.utc),
            ),
            (
                datetime(2019, 8, 1, tzinfo=timezone.utc),
                datetime(2019, 7, 30, tzinfo=timezone.utc),
                datetime(2019, 7, 30, tzinfo=timezone.utc),
            ),
        ],
    )
    def test_deserialization_end_date(self, dag_end_date, task_end_date, expected_task_end_date):
        dag = DAG(dag_id='simple_dag', start_date=datetime(2019, 8, 1), end_date=dag_end_date)
        BaseOperator(task_id='simple_task', dag=dag, end_date=task_end_date)

        serialized_dag = SerializedDAG.to_dict(dag)
        if not task_end_date or dag_end_date <= task_end_date:
            # If dag.end_date < task.end_date -> task.end_date=dag.end_date
            # because of the logic in dag.add_task()
            assert "end_date" not in serialized_dag["dag"]["tasks"][0]
        else:
            assert "end_date" in serialized_dag["dag"]["tasks"][0]

        dag = SerializedDAG.from_dict(serialized_dag)
        simple_task = dag.task_dict["simple_task"]
        assert simple_task.end_date == expected_task_end_date

    @pytest.mark.parametrize(
        "serialized_timetable, expected_timetable",
        [
            ({"__type": "airflow.timetables.simple.NullTimetable", "__var": {}}, NullTimetable()),
            (
                {
                    "__type": "airflow.timetables.interval.CronDataIntervalTimetable",
                    "__var": {"expression": "@weekly", "timezone": "UTC"},
                },
                cron_timetable("0 0 * * 0"),
            ),
            ({"__type": "airflow.timetables.simple.OnceTimetable", "__var": {}}, OnceTimetable()),
            (
                {
                    "__type": "airflow.timetables.interval.DeltaDataIntervalTimetable",
                    "__var": {"delta": 86400.0},
                },
                delta_timetable(timedelta(days=1)),
            ),
            (CUSTOM_TIMETABLE_SERIALIZED, CustomSerializationTimetable("foo")),
        ],
    )
    @pytest.mark.usefixtures("timetable_plugin")
    def test_deserialization_timetable(
        self,
        serialized_timetable,
        expected_timetable,
    ):
        serialized = {
            "__version": 1,
            "dag": {
                "default_args": {"__type": "dict", "__var": {}},
                "_dag_id": "simple_dag",
                "fileloc": __file__,
                "tasks": [],
                "timezone": "UTC",
                "timetable": serialized_timetable,
            },
        }
        SerializedDAG.validate_schema(serialized)
        dag = SerializedDAG.from_dict(serialized)
        assert dag.timetable == expected_timetable

    def test_deserialization_timetable_unregistered(self):
        serialized = {
            "__version": 1,
            "dag": {
                "default_args": {"__type": "dict", "__var": {}},
                "_dag_id": "simple_dag",
                "fileloc": __file__,
                "tasks": [],
                "timezone": "UTC",
                "timetable": CUSTOM_TIMETABLE_SERIALIZED,
            },
        }
        SerializedDAG.validate_schema(serialized)
        with pytest.raises(ValueError) as ctx:
            SerializedDAG.from_dict(serialized)
        message = (
            "Timetable class "
            "'tests.test_utils.timetables.CustomSerializationTimetable' "
            "is not registered"
        )
        assert str(ctx.value) == message

    @pytest.mark.parametrize(
        "serialized_schedule_interval, expected_timetable",
        [
            (None, NullTimetable()),
            ("@weekly", cron_timetable("0 0 * * 0")),
            ("@once", OnceTimetable()),
            (
                {"__type": "timedelta", "__var": 86400.0},
                delta_timetable(timedelta(days=1)),
            ),
        ],
    )
    def test_deserialization_schedule_interval(
        self,
        serialized_schedule_interval,
        expected_timetable,
    ):
        """Test DAGs serialized before 2.2 can be correctly deserialized."""
        serialized = {
            "__version": 1,
            "dag": {
                "default_args": {"__type": "dict", "__var": {}},
                "_dag_id": "simple_dag",
                "fileloc": __file__,
                "tasks": [],
                "timezone": "UTC",
                "schedule_interval": serialized_schedule_interval,
            },
        }

        SerializedDAG.validate_schema(serialized)
        dag = SerializedDAG.from_dict(serialized)
        assert dag.timetable == expected_timetable

    @pytest.mark.parametrize(
        "val, expected",
        [
            (relativedelta(days=-1), {"__type": "relativedelta", "__var": {"days": -1}}),
            (relativedelta(month=1, days=-1), {"__type": "relativedelta", "__var": {"month": 1, "days": -1}}),
            # Every friday
            (relativedelta(weekday=FR), {"__type": "relativedelta", "__var": {"weekday": [4]}}),
            # Every second friday
            (relativedelta(weekday=FR(2)), {"__type": "relativedelta", "__var": {"weekday": [4, 2]}}),
        ],
    )
    def test_roundtrip_relativedelta(self, val, expected):
        serialized = SerializedDAG.serialize(val)
        assert serialized == expected

        round_tripped = SerializedDAG.deserialize(serialized)
        assert val == round_tripped

    @pytest.mark.parametrize(
        "val, expected_val",
        [
            (None, {}),
            ({"param_1": "value_1"}, {"param_1": "value_1"}),
            ({"param_1": {1, 2, 3}}, {"param_1": {1, 2, 3}}),
        ],
    )
    def test_dag_params_roundtrip(self, val, expected_val):
        """
        Test that params work both on Serialized DAGs & Tasks
        """
        dag = DAG(dag_id='simple_dag', params=val)
        BaseOperator(task_id='simple_task', dag=dag, start_date=datetime(2019, 8, 1))

        serialized_dag_json = SerializedDAG.to_json(dag)

        serialized_dag = json.loads(serialized_dag_json)

        assert "params" in serialized_dag["dag"]

        deserialized_dag = SerializedDAG.from_dict(serialized_dag)
        deserialized_simple_task = deserialized_dag.task_dict["simple_task"]
        assert expected_val == deserialized_dag.params.dump()
        assert expected_val == deserialized_simple_task.params.dump()

    def test_invalid_params(self):
        """
        Test to make sure that only native Param objects are being passed as dag or task params
        """

        class S3Param(Param):
            def __init__(self, path: str):
                schema = {"type": "string", "pattern": r"s3:\/\/(.+?)\/(.+)"}
                super().__init__(default=path, schema=schema)

        dag = DAG(dag_id='simple_dag', params={'path': S3Param('s3://my_bucket/my_path')})

        with pytest.raises(SerializationError):
            SerializedDAG.to_dict(dag)

        dag = DAG(dag_id='simple_dag')
        BaseOperator(
            task_id='simple_task',
            dag=dag,
            start_date=datetime(2019, 8, 1),
            params={'path': S3Param('s3://my_bucket/my_path')},
        )

    @pytest.mark.parametrize(
        'param',
        [
            Param('my value', description='hello', schema={'type': 'string'}),
            Param('my value', description='hello'),
            Param(None, description=None),
        ],
    )
    def test_full_param_roundtrip(self, param):
        """
        Test to make sure that only native Param objects are being passed as dag or task params
        """

        dag = DAG(dag_id='simple_dag', params={'my_param': param})
        serialized_json = SerializedDAG.to_json(dag)
        serialized = json.loads(serialized_json)
        SerializedDAG.validate_schema(serialized)
        dag = SerializedDAG.from_dict(serialized)

        assert dag.params["my_param"] == param.value
        observed_param = dag.params.get_param('my_param')
        assert isinstance(observed_param, Param)
        assert observed_param.description == param.description
        assert observed_param.schema == param.schema

    @pytest.mark.parametrize(
        "val, expected_val",
        [
            (None, {}),
            ({"param_1": "value_1"}, {"param_1": "value_1"}),
            ({"param_1": {1, 2, 3}}, {"param_1": {1, 2, 3}}),
        ],
    )
    def test_task_params_roundtrip(self, val, expected_val):
        """
        Test that params work both on Serialized DAGs & Tasks
        """
        dag = DAG(dag_id='simple_dag')
        BaseOperator(task_id='simple_task', dag=dag, params=val, start_date=datetime(2019, 8, 1))

        serialized_dag = SerializedDAG.to_dict(dag)
        if val:
            assert "params" in serialized_dag["dag"]["tasks"][0]
        else:
            assert "params" not in serialized_dag["dag"]["tasks"][0]

        deserialized_dag = SerializedDAG.from_dict(serialized_dag)
        deserialized_simple_task = deserialized_dag.task_dict["simple_task"]
        assert expected_val == deserialized_simple_task.params.dump()

    @pytest.mark.parametrize(
        ("bash_command", "serialized_links", "links"),
        [
            pytest.param(
                "true",
                [{'tests.test_utils.mock_operators.CustomOpLink': {}}],
                {"Google Custom": "http://google.com/custom_base_link?search=true"},
                id="non-indexed-link",
            ),
            pytest.param(
                ["echo", "true"],
                [
                    {'tests.test_utils.mock_operators.CustomBaseIndexOpLink': {'index': 0}},
                    {'tests.test_utils.mock_operators.CustomBaseIndexOpLink': {'index': 1}},
                ],
                {
                    "BigQuery Console #1": "https://console.cloud.google.com/bigquery?j=echo",
                    "BigQuery Console #2": "https://console.cloud.google.com/bigquery?j=true",
                },
                id="multiple-indexed-links",
            ),
        ],
    )
    def test_extra_serialized_field_and_operator_links(
        self, bash_command, serialized_links, links, dag_maker
    ):
        """
        Assert extra field exists & OperatorLinks defined in Plugins and inbuilt Operator Links.

        This tests also depends on GoogleLink() registered as a plugin
        in tests/plugins/test_plugin.py

        The function tests that if extra operator links are registered in plugin
        in ``operator_extra_links`` and the same is also defined in
        the Operator in ``BaseOperator.operator_extra_links``, it has the correct
        extra link.

        If CustomOperator is called with a string argument for bash_command it
        has a single link, if called with an array it has one link per element.
        We use this to test the serialization of link data.
        """
        test_date = timezone.DateTime(2019, 8, 1, tzinfo=timezone.utc)

        with dag_maker(dag_id='simple_dag', start_date=test_date) as dag:
            CustomOperator(task_id='simple_task', bash_command=bash_command)

        serialized_dag = SerializedDAG.to_dict(dag)
        assert "bash_command" in serialized_dag["dag"]["tasks"][0]

        dag = SerializedDAG.from_dict(serialized_dag)
        simple_task = dag.task_dict["simple_task"]
        assert getattr(simple_task, "bash_command") == bash_command

        #########################################################
        # Verify Operator Links work with Serialized Operator
        #########################################################
        # Check Serialized version of operator link only contains the inbuilt Op Link
        assert serialized_dag["dag"]["tasks"][0]["_operator_extra_links"] == serialized_links

        # Test all the extra_links are set
        assert set(simple_task.extra_links) == {*links, 'airflow', 'github', 'google'}

        dr = dag_maker.create_dagrun(execution_date=test_date)
        (ti,) = dr.task_instances
        XCom.set(
            key='search_query',
            value=bash_command,
            task_id=simple_task.task_id,
            dag_id=simple_task.dag_id,
            run_id=dr.run_id,
        )

        # Test Deserialized inbuilt link
        for name, expected in links.items():
            link = simple_task.get_extra_links(ti, name)
            assert link == expected

        # Test Deserialized link registered via Airflow Plugin
        link = simple_task.get_extra_links(ti, GoogleLink.name)
        assert "https://www.google.com" == link

    def test_extra_operator_links_logs_error_for_non_registered_extra_links(self, caplog):
        """
        Assert OperatorLinks not registered via Plugins and if it is not an inbuilt Operator Link,
        it can still deserialize the DAG (does not error) but just logs an error
        """

        class TaskStateLink(BaseOperatorLink):
            """OperatorLink not registered via Plugins nor a built-in OperatorLink"""

            name = 'My Link'

            def get_link(self, operator, *, ti_key):
                return 'https://www.google.com'

        class MyOperator(BaseOperator):
            """Just a EmptyOperator using above defined Extra Operator Link"""

            operator_extra_links = [TaskStateLink()]

            def execute(self, context: Context):
                pass

        with DAG(dag_id='simple_dag', start_date=datetime(2019, 8, 1)) as dag:
            MyOperator(task_id='blah')

        serialized_dag = SerializedDAG.to_dict(dag)

        with caplog.at_level("ERROR", logger="airflow.serialization.serialized_objects"):
            SerializedDAG.from_dict(serialized_dag)

        expected_err_msg = (
            "Operator Link class 'tests.serialization.test_dag_serialization.TaskStateLink' not registered"
        )
        assert expected_err_msg in caplog.text

    class ClassWithCustomAttributes:
        """
        Class for testing purpose: allows to create objects with custom attributes in one single statement.
        """

        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)

        def __str__(self):
            return f"{self.__class__.__name__}({str(self.__dict__)})"

        def __repr__(self):
            return self.__str__()

        def __eq__(self, other):
            return self.__dict__ == other.__dict__

        def __ne__(self, other):
            return not self.__eq__(other)

    @pytest.mark.parametrize(
        "templated_field, expected_field",
        [
            (None, None),
            ([], []),
            ({}, {}),
            ("{{ task.task_id }}", "{{ task.task_id }}"),
            (["{{ task.task_id }}", "{{ task.task_id }}"]),
            ({"foo": "{{ task.task_id }}"}, {"foo": "{{ task.task_id }}"}),
            ({"foo": {"bar": "{{ task.task_id }}"}}, {"foo": {"bar": "{{ task.task_id }}"}}),
            (
                [{"foo1": {"bar": "{{ task.task_id }}"}}, {"foo2": {"bar": "{{ task.task_id }}"}}],
                [{"foo1": {"bar": "{{ task.task_id }}"}}, {"foo2": {"bar": "{{ task.task_id }}"}}],
            ),
            (
                {"foo": {"bar": {"{{ task.task_id }}": ["sar"]}}},
                {"foo": {"bar": {"{{ task.task_id }}": ["sar"]}}},
            ),
            (
                ClassWithCustomAttributes(
                    att1="{{ task.task_id }}", att2="{{ task.task_id }}", template_fields=["att1"]
                ),
                "ClassWithCustomAttributes("
                "{'att1': '{{ task.task_id }}', 'att2': '{{ task.task_id }}', 'template_fields': ['att1']})",
            ),
            (
                ClassWithCustomAttributes(
                    nested1=ClassWithCustomAttributes(
                        att1="{{ task.task_id }}", att2="{{ task.task_id }}", template_fields=["att1"]
                    ),
                    nested2=ClassWithCustomAttributes(
                        att3="{{ task.task_id }}", att4="{{ task.task_id }}", template_fields=["att3"]
                    ),
                    template_fields=["nested1"],
                ),
                "ClassWithCustomAttributes("
                "{'nested1': ClassWithCustomAttributes({'att1': '{{ task.task_id }}', "
                "'att2': '{{ task.task_id }}', 'template_fields': ['att1']}), "
                "'nested2': ClassWithCustomAttributes({'att3': '{{ task.task_id }}', 'att4': "
                "'{{ task.task_id }}', 'template_fields': ['att3']}), 'template_fields': ['nested1']})",
            ),
        ],
    )
    def test_templated_fields_exist_in_serialized_dag(self, templated_field, expected_field):
        """
        Test that templated_fields exists for all Operators in Serialized DAG

        Since we don't want to inflate arbitrary python objects (it poses a RCE/security risk etc.)
        we want check that non-"basic" objects are turned in to strings after deserializing.
        """

        dag = DAG("test_serialized_template_fields", start_date=datetime(2019, 8, 1))
        with dag:
            BashOperator(task_id="test", bash_command=templated_field)

        serialized_dag = SerializedDAG.to_dict(dag)
        deserialized_dag = SerializedDAG.from_dict(serialized_dag)
        deserialized_test_task = deserialized_dag.task_dict["test"]
        assert expected_field == getattr(deserialized_test_task, "bash_command")

    def test_dag_serialized_fields_with_schema(self):
        """
        Additional Properties are disabled on DAGs. This test verifies that all the
        keys in DAG.get_serialized_fields are listed in Schema definition.
        """
        dag_schema: dict = load_dag_schema_dict()["definitions"]["dag"]["properties"]

        # The parameters we add manually in Serialization need to be ignored
        ignored_keys: set = {
            "is_subdag",
            "tasks",
            "has_on_success_callback",
            "has_on_failure_callback",
            "dag_dependencies",
            "params",
        }

        keys_for_backwards_compat: set = {
            "_concurrency",
        }
        dag_params: set = set(dag_schema.keys()) - ignored_keys - keys_for_backwards_compat
        assert set(DAG.get_serialized_fields()) == dag_params

    def test_operator_subclass_changing_base_defaults(self):
        assert (
            BaseOperator(task_id='dummy').do_xcom_push is True
        ), "Precondition check! If this fails the test won't make sense"

        class MyOperator(BaseOperator):
            def __init__(self, do_xcom_push=False, **kwargs):
                super().__init__(**kwargs)
                self.do_xcom_push = do_xcom_push

        op = MyOperator(task_id='dummy')
        assert op.do_xcom_push is False

        blob = SerializedBaseOperator.serialize_operator(op)
        serialized_op = SerializedBaseOperator.deserialize_operator(blob)

        assert serialized_op.do_xcom_push is False

    def test_no_new_fields_added_to_base_operator(self):
        """
        This test verifies that there are no new fields added to BaseOperator. And reminds that
        tests should be added for it.
        """
        base_operator = BaseOperator(task_id="10")
        fields = {k: v for (k, v) in vars(base_operator).items() if k in BaseOperator.get_serialized_fields()}
        assert fields == {
            '_log': base_operator.log,
            '_post_execute_hook': None,
            '_pre_execute_hook': None,
            'depends_on_past': False,
            'do_xcom_push': True,
            'doc': None,
            'doc_json': None,
            'doc_md': None,
            'doc_rst': None,
            'doc_yaml': None,
            'downstream_task_ids': set(),
            'email': None,
            'email_on_failure': True,
            'email_on_retry': True,
            'execution_timeout': None,
            'executor_config': {},
            'ignore_first_depends_on_past': True,
            'inlets': [],
            'max_active_tis_per_dag': None,
            'max_retry_delay': None,
            'on_execute_callback': None,
            'on_failure_callback': None,
            'on_retry_callback': None,
            'on_success_callback': None,
            'outlets': [],
            'owner': 'airflow',
            'params': {},
            'pool': 'default_pool',
            'pool_slots': 1,
            'priority_weight': 1,
            'queue': 'default',
            'resources': None,
            'retries': 0,
            'retry_delay': timedelta(0, 300),
            'retry_exponential_backoff': False,
            'run_as_user': None,
            'sla': None,
            'task_id': '10',
            'trigger_rule': 'all_success',
            'wait_for_downstream': False,
            'weight_rule': 'downstream',
        }, """
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

     ACTION NEEDED! PLEASE READ THIS CAREFULLY AND CORRECT TESTS CAREFULLY

 Some fields were added to the BaseOperator! Please add them to the list above and make sure that
 you add support for DAG serialization - you should add the field to
 `airflow/serialization/schema.json` - they should have correct type defined there.

 Note that we do not support versioning yet so you should only add optional fields to BaseOperator.

!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                         """

    def test_operator_deserialize_old_names(self):
        blob = {
            "task_id": "custom_task",
            "_downstream_task_ids": ['foo'],
            "template_ext": [],
            "template_fields": ['bash_command'],
            "template_fields_renderers": {},
            "_task_type": "CustomOperator",
            "_task_module": "tests.test_utils.mock_operators",
            "pool": "default_pool",
            "ui_color": "#fff",
            "ui_fgcolor": "#000",
        }

        SerializedDAG._json_schema.validate(blob, _schema=load_dag_schema_dict()['definitions']['operator'])
        serialized_op = SerializedBaseOperator.deserialize_operator(blob)
        assert serialized_op.downstream_task_ids == {'foo'}

    def test_task_resources(self):
        """
        Test task resources serialization/deserialization.
        """
        from airflow.operators.empty import EmptyOperator

        execution_date = datetime(2020, 1, 1)
        task_id = 'task1'
        with DAG("test_task_resources", start_date=execution_date) as dag:
            task = EmptyOperator(task_id=task_id, resources={"cpus": 0.1, "ram": 2048})

        SerializedDAG.validate_schema(SerializedDAG.to_dict(dag))

        json_dag = SerializedDAG.from_json(SerializedDAG.to_json(dag))
        deserialized_task = json_dag.get_task(task_id)
        assert deserialized_task.resources == task.resources
        assert isinstance(deserialized_task.resources, Resources)

    def test_task_group_serialization(self):
        """
        Test TaskGroup serialization/deserialization.
        """
        from airflow.operators.empty import EmptyOperator

        execution_date = datetime(2020, 1, 1)
        with DAG("test_task_group_serialization", start_date=execution_date) as dag:
            task1 = EmptyOperator(task_id="task1")
            with TaskGroup("group234") as group234:
                _ = EmptyOperator(task_id="task2")

                with TaskGroup("group34") as group34:
                    _ = EmptyOperator(task_id="task3")
                    _ = EmptyOperator(task_id="task4")

            task5 = EmptyOperator(task_id="task5")
            task1 >> group234
            group34 >> task5

        dag_dict = SerializedDAG.to_dict(dag)
        SerializedDAG.validate_schema(dag_dict)
        json_dag = SerializedDAG.from_json(SerializedDAG.to_json(dag))
        self.validate_deserialized_dag(json_dag, dag)

        serialized_dag = SerializedDAG.deserialize_dag(SerializedDAG.serialize_dag(dag))

        assert serialized_dag.task_group.children
        assert serialized_dag.task_group.children.keys() == dag.task_group.children.keys()

        def check_task_group(node):
            assert node.dag is serialized_dag
            try:
                children = node.children.values()
            except AttributeError:
                # Round-trip serialization and check the result
                expected_serialized = SerializedBaseOperator.serialize_operator(dag.get_task(node.task_id))
                expected_deserialized = SerializedBaseOperator.deserialize_operator(expected_serialized)
                expected_dict = SerializedBaseOperator.serialize_operator(expected_deserialized)
                assert node
                assert SerializedBaseOperator.serialize_operator(node) == expected_dict
                return

            for child in children:
                check_task_group(child)

        check_task_group(serialized_dag.task_group)

    def test_deps_sorted(self):
        """
        Tests serialize_operator, make sure the deps is in order
        """
        from airflow.operators.empty import EmptyOperator
        from airflow.sensors.external_task import ExternalTaskSensor

        execution_date = datetime(2020, 1, 1)
        with DAG(dag_id="test_deps_sorted", start_date=execution_date) as dag:
            task1 = ExternalTaskSensor(
                task_id="task1",
                external_dag_id="external_dag_id",
                mode="reschedule",
            )
            task2 = EmptyOperator(task_id="task2")
            task1 >> task2

        serialize_op = SerializedBaseOperator.serialize_operator(dag.task_dict["task1"])

        deps = serialize_op["deps"]
        assert deps == [
            'airflow.ti_deps.deps.not_in_retry_period_dep.NotInRetryPeriodDep',
            'airflow.ti_deps.deps.not_previously_skipped_dep.NotPreviouslySkippedDep',
            'airflow.ti_deps.deps.prev_dagrun_dep.PrevDagrunDep',
            'airflow.ti_deps.deps.ready_to_reschedule.ReadyToRescheduleDep',
            'airflow.ti_deps.deps.trigger_rule_dep.TriggerRuleDep',
        ]

    def test_error_on_unregistered_ti_dep_serialization(self):
        # trigger rule not registered through the plugin system will not be serialized
        class DummyTriggerRule(BaseTIDep):
            pass

        class DummyTask(BaseOperator):
            deps = frozenset(list(BaseOperator.deps) + [DummyTriggerRule()])

        execution_date = datetime(2020, 1, 1)
        with DAG(dag_id="test_error_on_unregistered_ti_dep_serialization", start_date=execution_date) as dag:
            DummyTask(task_id="task1")

        with pytest.raises(SerializationError):
            SerializedBaseOperator.serialize_operator(dag.task_dict["task1"])

    def test_error_on_unregistered_ti_dep_deserialization(self):
        from airflow.operators.empty import EmptyOperator

        with DAG("test_error_on_unregistered_ti_dep_deserialization", start_date=datetime(2019, 8, 1)) as dag:
            EmptyOperator(task_id="task1")
        serialize_op = SerializedBaseOperator.serialize_operator(dag.task_dict["task1"])
        serialize_op['deps'] = [
            'airflow.ti_deps.deps.not_in_retry_period_dep.NotInRetryPeriodDep',
            # manually injected noncore ti dep should be ignored
            'test_plugin.NotATriggerRule',
        ]
        with pytest.raises(SerializationError):
            SerializedBaseOperator.deserialize_operator(serialize_op)

    def test_serialize_and_deserialize_custom_ti_deps(self):
        from test_plugin import CustomTestTriggerRule

        class DummyTask(BaseOperator):
            deps = frozenset(list(BaseOperator.deps) + [CustomTestTriggerRule()])

        execution_date = datetime(2020, 1, 1)
        with DAG(dag_id="test_serialize_custom_ti_deps", start_date=execution_date) as dag:
            DummyTask(task_id="task1")

        serialize_op = SerializedBaseOperator.serialize_operator(dag.task_dict["task1"])

        assert serialize_op["deps"] == [
            'airflow.ti_deps.deps.not_in_retry_period_dep.NotInRetryPeriodDep',
            'airflow.ti_deps.deps.not_previously_skipped_dep.NotPreviouslySkippedDep',
            'airflow.ti_deps.deps.prev_dagrun_dep.PrevDagrunDep',
            'airflow.ti_deps.deps.trigger_rule_dep.TriggerRuleDep',
            'test_plugin.CustomTestTriggerRule',
        ]

        op = SerializedBaseOperator.deserialize_operator(serialize_op)
        assert sorted(str(dep) for dep in op.deps) == [
            '<TIDep(CustomTestTriggerRule)>',
            '<TIDep(Not In Retry Period)>',
            '<TIDep(Not Previously Skipped)>',
            '<TIDep(Previous Dagrun State)>',
            '<TIDep(Trigger Rule)>',
        ]

    def test_serialize_mapped_outlets(self):
        with DAG(dag_id="d", start_date=datetime.now()):
            op = MockOperator.partial(task_id="x").expand(arg1=[1, 2])

        assert op.inlets == []
        assert op.outlets == []

        serialized = SerializedBaseOperator.serialize_mapped_operator(op)
        assert "inlets" not in serialized
        assert "outlets" not in serialized

        round_tripped = SerializedBaseOperator.deserialize_operator(serialized)
        assert isinstance(round_tripped, MappedOperator)
        assert round_tripped.inlets == []
        assert round_tripped.outlets == []

    def test_derived_dag_deps_sensor(self):
        """
        Tests DAG dependency detection for sensors, including derived classes
        """
        from airflow.operators.empty import EmptyOperator
        from airflow.sensors.external_task import ExternalTaskSensor

        class DerivedSensor(ExternalTaskSensor):
            pass

        execution_date = datetime(2020, 1, 1)
        for class_ in [ExternalTaskSensor, DerivedSensor]:
            with DAG(dag_id="test_derived_dag_deps_sensor", start_date=execution_date) as dag:
                task1 = class_(
                    task_id="task1",
                    external_dag_id="external_dag_id",
                    mode="reschedule",
                )
                task2 = EmptyOperator(task_id="task2")
                task1 >> task2

            dag = SerializedDAG.to_dict(dag)
            assert dag['dag']['dag_dependencies'] == [
                {
                    'source': 'external_dag_id',
                    'target': 'test_derived_dag_deps_sensor',
                    'dependency_type': 'sensor',
                    'dependency_id': 'task1',
                }
            ]

    @conf_vars(
        {
            (
                'scheduler',
                'dependency_detector',
            ): 'tests.serialization.test_dag_serialization.CustomDependencyDetector'
        }
    )
    def test_custom_dep_detector(self):
        """
        Prior to deprecation of custom dependency detector, the return type was Optional[DagDependency].
        This class verifies that custom dependency detector classes which assume that return type will still
        work until support for them is removed in 3.0.

        TODO: remove in Airflow 3.0
        """
        from airflow.sensors.external_task import ExternalTaskSensor

        execution_date = datetime(2020, 1, 1)
        with DAG(dag_id="test", start_date=execution_date) as dag:
            ExternalTaskSensor(
                task_id="task1",
                external_dag_id="external_dag_id",
                mode="reschedule",
            )
            CustomDepOperator(task_id='hello', bash_command='hi')
            dag = SerializedDAG.to_dict(dag)
            assert sorted(dag['dag']['dag_dependencies'], key=lambda x: tuple(x.values())) == sorted(
                [
                    {
                        'source': 'external_dag_id',
                        'target': 'test',
                        'dependency_type': 'sensor',
                        'dependency_id': 'task1',
                    },
                    {
                        'source': 'test',
                        'target': 'nothing',
                        'dependency_type': 'abc',
                        'dependency_id': 'hello',
                    },
                ],
                key=lambda x: tuple(x.values()),
            )

    def test_dag_deps_datasets(self):
        """
        Check that dag_dependencies node is populated correctly for a DAG with datasets.
        """
        from airflow.sensors.external_task import ExternalTaskSensor

        d1 = Dataset('d1')
        d2 = Dataset('d2')
        d3 = Dataset('d3')
        d4 = Dataset('d4')
        execution_date = datetime(2020, 1, 1)
        with DAG(dag_id="test", start_date=execution_date, schedule=[d1]) as dag:
            ExternalTaskSensor(
                task_id="task1",
                external_dag_id="external_dag_id",
                mode="reschedule",
            )
            BashOperator(task_id='dataset_writer', bash_command="echo hello", outlets=[d2, d3])

            @dag.task(outlets=[d4])
            def other_dataset_writer(x):
                pass

            other_dataset_writer.expand(x=[1, 2])

        dag = SerializedDAG.to_dict(dag)
        actual = sorted(dag['dag']['dag_dependencies'], key=lambda x: tuple(x.values()))
        expected = sorted(
            [
                {
                    'source': 'test',
                    'target': 'dataset',
                    'dependency_type': 'dataset',
                    'dependency_id': 'd4',
                },
                {
                    'source': 'external_dag_id',
                    'target': 'test',
                    'dependency_type': 'sensor',
                    'dependency_id': 'task1',
                },
                {
                    'source': 'test',
                    'target': 'dataset',
                    'dependency_type': 'dataset',
                    'dependency_id': 'd3',
                },
                {
                    'source': 'test',
                    'target': 'dataset',
                    'dependency_type': 'dataset',
                    'dependency_id': 'd2',
                },
                {
                    'source': 'dataset',
                    'target': 'test',
                    'dependency_type': 'dataset',
                    'dependency_id': 'd1',
                },
            ],
            key=lambda x: tuple(x.values()),
        )
        assert actual == expected

    def test_derived_dag_deps_operator(self):
        """
        Tests DAG dependency detection for operators, including derived classes
        """
        from airflow.operators.empty import EmptyOperator
        from airflow.operators.trigger_dagrun import TriggerDagRunOperator

        class DerivedOperator(TriggerDagRunOperator):
            pass

        execution_date = datetime(2020, 1, 1)
        for class_ in [TriggerDagRunOperator, DerivedOperator]:
            with DAG(dag_id="test_derived_dag_deps_trigger", start_date=execution_date) as dag:
                task1 = EmptyOperator(task_id="task1")
                task2 = class_(
                    task_id="task2",
                    trigger_dag_id="trigger_dag_id",
                )
                task1 >> task2

            dag = SerializedDAG.to_dict(dag)
            assert dag['dag']['dag_dependencies'] == [
                {
                    'source': 'test_derived_dag_deps_trigger',
                    'target': 'trigger_dag_id',
                    'dependency_type': 'trigger',
                    'dependency_id': 'task2',
                }
            ]

    def test_task_group_sorted(self):
        """
        Tests serialize_task_group, make sure the list is in order
        """
        from airflow.operators.empty import EmptyOperator
        from airflow.serialization.serialized_objects import SerializedTaskGroup

        """
                    start
                    ╱  ╲
                  ╱      ╲
        task_group_up1  task_group_up2
            (task_up1)  (task_up2)
                 ╲       ╱
              task_group_middle
                (task_middle)
                  ╱      ╲
        task_group_down1 task_group_down2
           (task_down1) (task_down2)
                 ╲        ╱
                   ╲    ╱
                    end
        """
        execution_date = datetime(2020, 1, 1)
        with DAG(dag_id="test_task_group_sorted", start_date=execution_date) as dag:
            start = EmptyOperator(task_id="start")

            with TaskGroup("task_group_up1") as task_group_up1:
                _ = EmptyOperator(task_id="task_up1")

            with TaskGroup("task_group_up2") as task_group_up2:
                _ = EmptyOperator(task_id="task_up2")

            with TaskGroup("task_group_middle") as task_group_middle:
                _ = EmptyOperator(task_id="task_middle")

            with TaskGroup("task_group_down1") as task_group_down1:
                _ = EmptyOperator(task_id="task_down1")

            with TaskGroup("task_group_down2") as task_group_down2:
                _ = EmptyOperator(task_id="task_down2")

            end = EmptyOperator(task_id='end')

            start >> task_group_up1
            start >> task_group_up2
            task_group_up1 >> task_group_middle
            task_group_up2 >> task_group_middle
            task_group_middle >> task_group_down1
            task_group_middle >> task_group_down2
            task_group_down1 >> end
            task_group_down2 >> end

        task_group_middle_dict = SerializedTaskGroup.serialize_task_group(
            dag.task_group.children["task_group_middle"]
        )
        upstream_group_ids = task_group_middle_dict["upstream_group_ids"]
        assert upstream_group_ids == ['task_group_up1', 'task_group_up2']

        upstream_task_ids = task_group_middle_dict["upstream_task_ids"]
        assert upstream_task_ids == ['task_group_up1.task_up1', 'task_group_up2.task_up2']

        downstream_group_ids = task_group_middle_dict["downstream_group_ids"]
        assert downstream_group_ids == ['task_group_down1', 'task_group_down2']

        task_group_down1_dict = SerializedTaskGroup.serialize_task_group(
            dag.task_group.children["task_group_down1"]
        )
        downstream_task_ids = task_group_down1_dict["downstream_task_ids"]
        assert downstream_task_ids == ['end']

    def test_edge_info_serialization(self):
        """
        Tests edge_info serialization/deserialization.
        """
        from airflow.operators.empty import EmptyOperator
        from airflow.utils.edgemodifier import Label

        with DAG("test_edge_info_serialization", start_date=datetime(2020, 1, 1)) as dag:
            task1 = EmptyOperator(task_id="task1")
            task2 = EmptyOperator(task_id="task2")
            task1 >> Label("test label") >> task2

        dag_dict = SerializedDAG.to_dict(dag)
        SerializedDAG.validate_schema(dag_dict)
        json_dag = SerializedDAG.from_json(SerializedDAG.to_json(dag))
        self.validate_deserialized_dag(json_dag, dag)

        serialized_dag = SerializedDAG.deserialize_dag(SerializedDAG.serialize_dag(dag))

        assert serialized_dag.edge_info == dag.edge_info

    @pytest.mark.parametrize("mode", ["poke", "reschedule"])
    def test_serialize_sensor(self, mode):
        from airflow.sensors.base import BaseSensorOperator

        class DummySensor(BaseSensorOperator):
            def poke(self, context: Context):
                return False

        op = DummySensor(task_id='dummy', mode=mode, poke_interval=23)

        blob = SerializedBaseOperator.serialize_operator(op)
        assert "deps" in blob

        serialized_op = SerializedBaseOperator.deserialize_operator(blob)
        assert serialized_op.reschedule == (mode == "reschedule")
        assert op.deps == serialized_op.deps

    @pytest.mark.parametrize("mode", ["poke", "reschedule"])
    def test_serialize_mapped_sensor_has_reschedule_dep(self, mode):
        from airflow.sensors.base import BaseSensorOperator

        class DummySensor(BaseSensorOperator):
            def poke(self, context: Context):
                return False

        op = DummySensor.partial(task_id='dummy', mode=mode).expand(poke_interval=[23])

        blob = SerializedBaseOperator.serialize_mapped_operator(op)
        assert "deps" in blob

        assert 'airflow.ti_deps.deps.ready_to_reschedule.ReadyToRescheduleDep' in blob['deps']

    @pytest.mark.parametrize(
        "passed_success_callback, expected_value",
        [
            ({"on_success_callback": lambda x: print("hi")}, True),
            ({}, False),
        ],
    )
    def test_dag_on_success_callback_roundtrip(self, passed_success_callback, expected_value):
        """
        Test that when on_success_callback is passed to the DAG, has_on_success_callback is stored
        in Serialized JSON blob. And when it is de-serialized dag.has_on_success_callback is set to True.

        When the callback is not set, has_on_success_callback should not be stored in Serialized blob
        and so default to False on de-serialization
        """
        dag = DAG(dag_id='test_dag_on_success_callback_roundtrip', **passed_success_callback)
        BaseOperator(task_id='simple_task', dag=dag, start_date=datetime(2019, 8, 1))

        serialized_dag = SerializedDAG.to_dict(dag)
        if expected_value:
            assert "has_on_success_callback" in serialized_dag["dag"]
        else:
            assert "has_on_success_callback" not in serialized_dag["dag"]

        deserialized_dag = SerializedDAG.from_dict(serialized_dag)

        assert deserialized_dag.has_on_success_callback is expected_value

    @pytest.mark.parametrize(
        "passed_failure_callback, expected_value",
        [
            ({"on_failure_callback": lambda x: print("hi")}, True),
            ({}, False),
        ],
    )
    def test_dag_on_failure_callback_roundtrip(self, passed_failure_callback, expected_value):
        """
        Test that when on_failure_callback is passed to the DAG, has_on_failure_callback is stored
        in Serialized JSON blob. And when it is de-serialized dag.has_on_failure_callback is set to True.

        When the callback is not set, has_on_failure_callback should not be stored in Serialized blob
        and so default to False on de-serialization
        """
        dag = DAG(dag_id='test_dag_on_failure_callback_roundtrip', **passed_failure_callback)
        BaseOperator(task_id='simple_task', dag=dag, start_date=datetime(2019, 8, 1))

        serialized_dag = SerializedDAG.to_dict(dag)
        if expected_value:
            assert "has_on_failure_callback" in serialized_dag["dag"]
        else:
            assert "has_on_failure_callback" not in serialized_dag["dag"]

        deserialized_dag = SerializedDAG.from_dict(serialized_dag)

        assert deserialized_dag.has_on_failure_callback is expected_value

    @pytest.mark.parametrize(
        "object_to_serialized, expected_output",
        [
            (
                ['task_1', 'task_5', 'task_2', 'task_4'],
                ['task_1', 'task_5', 'task_2', 'task_4'],
            ),
            (
                {'task_1', 'task_5', 'task_2', 'task_4'},
                ['task_1', 'task_2', 'task_4', 'task_5'],
            ),
            (
                ('task_1', 'task_5', 'task_2', 'task_4'),
                ['task_1', 'task_5', 'task_2', 'task_4'],
            ),
            (
                {
                    "staging_schema": [
                        {"key:": "foo", "value": "bar"},
                        {"key:": "this", "value": "that"},
                        "test_conf",
                    ]
                },
                {
                    "staging_schema": [
                        {"__type": "dict", "__var": {"key:": "foo", "value": "bar"}},
                        {
                            "__type": "dict",
                            "__var": {"key:": "this", "value": "that"},
                        },
                        "test_conf",
                    ]
                },
            ),
            (
                {"task3": "test3", "task2": "test2", "task1": "test1"},
                {"task1": "test1", "task2": "test2", "task3": "test3"},
            ),
            (
                ('task_1', 'task_5', 'task_2', 3, ["x", "y"]),
                ['task_1', 'task_5', 'task_2', 3, ["x", "y"]],
            ),
        ],
    )
    def test_serialized_objects_are_sorted(self, object_to_serialized, expected_output):
        """Test Serialized Sets are sorted while list and tuple preserve order"""
        serialized_obj = SerializedDAG.serialize(object_to_serialized)
        if isinstance(serialized_obj, dict) and "__type" in serialized_obj:
            serialized_obj = serialized_obj["__var"]
        assert serialized_obj == expected_output

    def test_params_upgrade(self):
        """when pre-2.2.0 param (i.e. primitive) is deserialized we convert to Param"""
        serialized = {
            "__version": 1,
            "dag": {
                "_dag_id": "simple_dag",
                "fileloc": '/path/to/file.py',
                "tasks": [],
                "timezone": "UTC",
                "params": {"none": None, "str": "str", "dict": {"a": "b"}},
            },
        }
        dag = SerializedDAG.from_dict(serialized)

        assert dag.params["none"] is None
        assert isinstance(dag.params.get_param("none"), Param)
        assert dag.params["str"] == "str"

    def test_params_serialize_default_2_2_0(self):
        """In 2.0.0, param ``default`` was assumed to be json-serializable objects and were not run though
        the standard serializer function.  In 2.2.2 we serialize param ``default``.  We keep this
        test only to ensure that params stored in 2.2.0 can still be parsed correctly."""
        serialized = {
            "__version": 1,
            "dag": {
                "_dag_id": "simple_dag",
                "fileloc": '/path/to/file.py',
                "tasks": [],
                "timezone": "UTC",
                "params": {"str": {"__class": "airflow.models.param.Param", "default": "str"}},
            },
        }
        SerializedDAG.validate_schema(serialized)
        dag = SerializedDAG.from_dict(serialized)

        assert isinstance(dag.params.get_param("str"), Param)
        assert dag.params["str"] == "str"

    def test_params_serialize_default(self):
        serialized = {
            "__version": 1,
            "dag": {
                "_dag_id": "simple_dag",
                "fileloc": '/path/to/file.py',
                "tasks": [],
                "timezone": "UTC",
                "params": {
                    "my_param": {
                        "default": "a string value",
                        "description": "hello",
                        "schema": {"__var": {"type": "string"}, "__type": "dict"},
                        "__class": "airflow.models.param.Param",
                    }
                },
            },
        }
        SerializedDAG.validate_schema(serialized)
        dag = SerializedDAG.from_dict(serialized)

        assert dag.params["my_param"] == "a string value"
        param = dag.params.get_param('my_param')
        assert isinstance(param, Param)
        assert param.description == 'hello'
        assert param.schema == {'type': 'string'}


def test_kubernetes_optional():
    """Serialisation / deserialisation continues to work without kubernetes installed"""

    def mock__import__(name, globals_=None, locals_=None, fromlist=(), level=0):
        if level == 0 and name.partition('.')[0] == 'kubernetes':
            raise ImportError("No module named 'kubernetes'")
        return importlib.__import__(name, globals=globals_, locals=locals_, fromlist=fromlist, level=level)

    with mock.patch('builtins.__import__', side_effect=mock__import__) as import_mock:
        # load module from scratch, this does not replace any already imported
        # airflow.serialization.serialized_objects module in sys.modules
        spec = importlib.util.find_spec("airflow.serialization.serialized_objects")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # if we got this far, the module did not try to load kubernetes, but
        # did it try to access airflow.kubernetes.*?
        imported_airflow = {
            c.args[0].split('.', 2)[1] for c in import_mock.call_args_list if c.args[0].startswith("airflow.")
        }
        assert "kubernetes" not in imported_airflow

        # pod loading is not supported when kubernetes is not available
        pod_override = {
            '__type': 'k8s.V1Pod',
            '__var': PodGenerator.serialize_pod(executor_config_pod),
        }

        with pytest.raises(RuntimeError):
            module.BaseSerialization.from_dict(pod_override)

        # basic serialization should succeed
        module.SerializedDAG.to_dict(make_simple_dag()["simple_dag"])


def test_operator_expand_serde():
    literal = [1, 2, {'a': 'b'}]
    real_op = BashOperator.partial(task_id='a', executor_config={'dict': {'sub': 'value'}}).expand(
        bash_command=literal
    )

    serialized = SerializedBaseOperator.serialize(real_op)

    assert serialized == {
        '_is_empty': False,
        '_is_mapped': True,
        '_task_module': 'airflow.operators.bash',
        '_task_type': 'BashOperator',
        'downstream_task_ids': [],
        'expand_input': {
            "type": "dict-of-lists",
            "value": {
                "__type": "dict",
                "__var": {'bash_command': [1, 2, {"__type": "dict", "__var": {'a': 'b'}}]},
            },
        },
        'partial_kwargs': {
            'executor_config': {
                '__type': 'dict',
                '__var': {'dict': {"__type": "dict", "__var": {'sub': 'value'}}},
            },
        },
        'task_id': 'a',
        'operator_extra_links': [],
        'template_fields': ['bash_command', 'env'],
        'template_ext': ['.sh', '.bash'],
        'template_fields_renderers': {'bash_command': 'bash', 'env': 'json'},
        'ui_color': '#f0ede4',
        'ui_fgcolor': '#000',
        "_disallow_kwargs_override": False,
        '_expand_input_attr': 'expand_input',
    }

    op = SerializedBaseOperator.deserialize_operator(serialized)
    assert isinstance(op, MappedOperator)
    assert op.deps is MappedOperator.deps_for(BaseOperator)

    assert op.operator_class == {
        '_task_type': 'BashOperator',
        'downstream_task_ids': [],
        'task_id': 'a',
        'template_ext': ['.sh', '.bash'],
        'template_fields': ['bash_command', 'env'],
        'template_fields_renderers': {'bash_command': 'bash', 'env': 'json'},
        'ui_color': '#f0ede4',
        'ui_fgcolor': '#000',
    }
    assert op.expand_input.value['bash_command'] == literal
    assert op.partial_kwargs['executor_config'] == {'dict': {'sub': 'value'}}


def test_operator_expand_xcomarg_serde():
    from airflow.models.xcom_arg import PlainXComArg, XComArg
    from airflow.serialization.serialized_objects import _XComRef

    with DAG("test-dag", start_date=datetime(2020, 1, 1)) as dag:
        task1 = BaseOperator(task_id="op1")
        mapped = MockOperator.partial(task_id='task_2').expand(arg2=XComArg(task1))

    serialized = SerializedBaseOperator.serialize(mapped)
    assert serialized == {
        '_is_empty': False,
        '_is_mapped': True,
        '_task_module': 'tests.test_utils.mock_operators',
        '_task_type': 'MockOperator',
        'downstream_task_ids': [],
        'expand_input': {
            "type": "dict-of-lists",
            "value": {
                "__type": "dict",
                "__var": {'arg2': {'__type': 'xcomref', '__var': {'task_id': 'op1', 'key': 'return_value'}}},
            },
        },
        'partial_kwargs': {},
        'task_id': 'task_2',
        'template_fields': ['arg1', 'arg2'],
        'template_ext': [],
        'template_fields_renderers': {},
        'operator_extra_links': [],
        'ui_color': '#fff',
        'ui_fgcolor': '#000',
        "_disallow_kwargs_override": False,
        '_expand_input_attr': 'expand_input',
    }

    op = SerializedBaseOperator.deserialize_operator(serialized)
    assert op.deps is MappedOperator.deps_for(BaseOperator)

    # The XComArg can't be deserialized before the DAG is.
    xcom_ref = op.expand_input.value['arg2']
    assert xcom_ref == _XComRef({"task_id": "op1", "key": XCOM_RETURN_KEY})

    serialized_dag: DAG = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

    xcom_arg = serialized_dag.task_dict['task_2'].expand_input.value['arg2']
    assert isinstance(xcom_arg, PlainXComArg)
    assert xcom_arg.operator is serialized_dag.task_dict['op1']


@pytest.mark.parametrize("strict", [True, False])
def test_operator_expand_kwargs_literal_serde(strict):
    from airflow.models.xcom_arg import PlainXComArg, XComArg
    from airflow.serialization.serialized_objects import _XComRef

    with DAG("test-dag", start_date=datetime(2020, 1, 1)) as dag:
        task1 = BaseOperator(task_id="op1")
        mapped = MockOperator.partial(task_id='task_2').expand_kwargs(
            [{"a": "x"}, {"a": XComArg(task1)}],
            strict=strict,
        )

    serialized = SerializedBaseOperator.serialize(mapped)
    assert serialized == {
        '_is_empty': False,
        '_is_mapped': True,
        '_task_module': 'tests.test_utils.mock_operators',
        '_task_type': 'MockOperator',
        'downstream_task_ids': [],
        'expand_input': {
            "type": "list-of-dicts",
            "value": [
                {"__type": "dict", "__var": {"a": "x"}},
                {
                    "__type": "dict",
                    "__var": {"a": {'__type': 'xcomref', '__var': {'task_id': 'op1', 'key': 'return_value'}}},
                },
            ],
        },
        'partial_kwargs': {},
        'task_id': 'task_2',
        'template_fields': ['arg1', 'arg2'],
        'template_ext': [],
        'template_fields_renderers': {},
        'operator_extra_links': [],
        'ui_color': '#fff',
        'ui_fgcolor': '#000',
        "_disallow_kwargs_override": strict,
        '_expand_input_attr': 'expand_input',
    }

    op = SerializedBaseOperator.deserialize_operator(serialized)
    assert op.deps is MappedOperator.deps_for(BaseOperator)
    assert op._disallow_kwargs_override == strict

    # The XComArg can't be deserialized before the DAG is.
    expand_value = op.expand_input.value
    assert expand_value == [{"a": "x"}, {"a": _XComRef({"task_id": "op1", "key": XCOM_RETURN_KEY})}]

    serialized_dag: DAG = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

    resolved_expand_value = serialized_dag.task_dict['task_2'].expand_input.value
    resolved_expand_value == [{"a": "x"}, {"a": PlainXComArg(serialized_dag.task_dict['op1'])}]


@pytest.mark.parametrize("strict", [True, False])
def test_operator_expand_kwargs_xcomarg_serde(strict):
    from airflow.models.xcom_arg import PlainXComArg, XComArg
    from airflow.serialization.serialized_objects import _XComRef

    with DAG("test-dag", start_date=datetime(2020, 1, 1)) as dag:
        task1 = BaseOperator(task_id="op1")
        mapped = MockOperator.partial(task_id='task_2').expand_kwargs(XComArg(task1), strict=strict)

    serialized = SerializedBaseOperator.serialize(mapped)
    assert serialized == {
        '_is_empty': False,
        '_is_mapped': True,
        '_task_module': 'tests.test_utils.mock_operators',
        '_task_type': 'MockOperator',
        'downstream_task_ids': [],
        'expand_input': {
            "type": "list-of-dicts",
            "value": {'__type': 'xcomref', '__var': {'task_id': 'op1', 'key': 'return_value'}},
        },
        'partial_kwargs': {},
        'task_id': 'task_2',
        'template_fields': ['arg1', 'arg2'],
        'template_ext': [],
        'template_fields_renderers': {},
        'operator_extra_links': [],
        'ui_color': '#fff',
        'ui_fgcolor': '#000',
        "_disallow_kwargs_override": strict,
        '_expand_input_attr': 'expand_input',
    }

    op = SerializedBaseOperator.deserialize_operator(serialized)
    assert op.deps is MappedOperator.deps_for(BaseOperator)
    assert op._disallow_kwargs_override == strict

    # The XComArg can't be deserialized before the DAG is.
    xcom_ref = op.expand_input.value
    assert xcom_ref == _XComRef({"task_id": "op1", "key": XCOM_RETURN_KEY})

    serialized_dag: DAG = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

    xcom_arg = serialized_dag.task_dict['task_2'].expand_input.value
    assert isinstance(xcom_arg, PlainXComArg)
    assert xcom_arg.operator is serialized_dag.task_dict['op1']


def test_operator_expand_deserialized_unmap():
    """Unmap a deserialized mapped operator should be similar to deserializing an non-mapped operator."""
    normal = BashOperator(task_id='a', bash_command=[1, 2], executor_config={"a": "b"})
    mapped = BashOperator.partial(task_id='a', executor_config={"a": "b"}).expand(bash_command=[1, 2])

    serialize = SerializedBaseOperator.serialize
    deserialize = SerializedBaseOperator.deserialize_operator
    assert deserialize(serialize(mapped)).unmap(None) == deserialize(serialize(normal))


def test_sensor_expand_deserialized_unmap():
    """Unmap a deserialized mapped sensor should be similar to deserializing a non-mapped sensor"""
    normal = BashSensor(task_id='a', bash_command=[1, 2], mode='reschedule')
    mapped = BashSensor.partial(task_id='a', mode='reschedule').expand(bash_command=[1, 2])

    serialize = SerializedBaseOperator.serialize

    deserialize = SerializedBaseOperator.deserialize_operator
    assert deserialize(serialize(mapped)).unmap(None) == deserialize(serialize(normal))


def test_task_resources_serde():
    """
    Test task resources serialization/deserialization.
    """
    from airflow.operators.empty import EmptyOperator

    execution_date = datetime(2020, 1, 1)
    task_id = 'task1'
    with DAG("test_task_resources", start_date=execution_date) as _:
        task = EmptyOperator(task_id=task_id, resources={"cpus": 0.1, "ram": 2048})

    serialized = SerializedBaseOperator.serialize(task)
    assert serialized['resources'] == {
        "cpus": {"name": "CPU", "qty": 0.1, "units_str": "core(s)"},
        "disk": {"name": "Disk", "qty": 512, "units_str": "MB"},
        "gpus": {"name": "GPU", "qty": 0, "units_str": "gpu(s)"},
        "ram": {"name": "RAM", "qty": 2048, "units_str": "MB"},
    }


def test_taskflow_expand_serde():
    from airflow.decorators import task
    from airflow.models.xcom_arg import XComArg
    from airflow.serialization.serialized_objects import _ExpandInputRef, _XComRef

    with DAG("test-dag", start_date=datetime(2020, 1, 1)) as dag:
        op1 = BaseOperator(task_id="op1")

        @task(retry_delay=30)
        def x(arg1, arg2, arg3):
            print(arg1, arg2, arg3)

        print('**', type(x), type(x.partial), type(x.expand))
        x.partial(arg1=[1, 2, {"a": "b"}]).expand(arg2={"a": 1, "b": 2}, arg3=XComArg(op1))

    original = dag.get_task("x")

    serialized = SerializedBaseOperator.serialize(original)
    assert serialized == {
        '_is_empty': False,
        '_is_mapped': True,
        '_task_module': 'airflow.decorators.python',
        '_task_type': '_PythonDecoratedOperator',
        '_operator_name': '@task',
        'downstream_task_ids': [],
        'partial_kwargs': {
            'op_args': [],
            'op_kwargs': {
                '__type': 'dict',
                '__var': {'arg1': [1, 2, {"__type": "dict", "__var": {'a': 'b'}}]},
            },
            'retry_delay': {'__type': 'timedelta', '__var': 30.0},
        },
        'op_kwargs_expand_input': {
            "type": "dict-of-lists",
            "value": {
                "__type": "dict",
                "__var": {
                    'arg2': {"__type": "dict", "__var": {'a': 1, 'b': 2}},
                    'arg3': {'__type': 'xcomref', '__var': {'task_id': 'op1', 'key': 'return_value'}},
                },
            },
        },
        'operator_extra_links': [],
        'ui_color': '#ffefeb',
        'ui_fgcolor': '#000',
        'task_id': 'x',
        'template_ext': [],
        'template_fields': ['op_args', 'op_kwargs'],
        'template_fields_renderers': {"op_args": "py", "op_kwargs": "py"},
        "_disallow_kwargs_override": False,
        '_expand_input_attr': 'op_kwargs_expand_input',
    }

    deserialized = SerializedBaseOperator.deserialize_operator(serialized)
    assert isinstance(deserialized, MappedOperator)
    assert deserialized.deps is MappedOperator.deps_for(BaseOperator)
    assert deserialized.upstream_task_ids == set()
    assert deserialized.downstream_task_ids == set()

    assert deserialized.op_kwargs_expand_input == _ExpandInputRef(
        key="dict-of-lists",
        value={"arg2": {"a": 1, "b": 2}, "arg3": _XComRef({"task_id": "op1", "key": XCOM_RETURN_KEY})},
    )
    assert deserialized.partial_kwargs == {
        "op_args": [],
        "op_kwargs": {"arg1": [1, 2, {"a": "b"}]},
        "retry_delay": timedelta(seconds=30),
    }

    # Ensure the serialized operator can also be correctly pickled, to ensure
    # correct interaction between DAG pickling and serialization. This is done
    # here so we don't need to duplicate tests between pickled and non-pickled
    # DAGs everywhere else.
    pickled = pickle.loads(pickle.dumps(deserialized))
    assert pickled.op_kwargs_expand_input == _ExpandInputRef(
        key="dict-of-lists",
        value={"arg2": {"a": 1, "b": 2}, "arg3": _XComRef({"task_id": "op1", "key": XCOM_RETURN_KEY})},
    )
    assert pickled.partial_kwargs == {
        "op_args": [],
        "op_kwargs": {"arg1": [1, 2, {"a": "b"}]},
        "retry_delay": timedelta(seconds=30),
    }


@pytest.mark.parametrize("strict", [True, False])
def test_taskflow_expand_kwargs_serde(strict):
    from airflow.decorators import task
    from airflow.models.xcom_arg import XComArg
    from airflow.serialization.serialized_objects import _ExpandInputRef, _XComRef

    with DAG("test-dag", start_date=datetime(2020, 1, 1)) as dag:
        op1 = BaseOperator(task_id="op1")

        @task(retry_delay=30)
        def x(arg1, arg2, arg3):
            print(arg1, arg2, arg3)

        x.partial(arg1=[1, 2, {"a": "b"}]).expand_kwargs(XComArg(op1), strict=strict)

    original = dag.get_task("x")

    serialized = SerializedBaseOperator.serialize(original)
    assert serialized == {
        '_is_empty': False,
        '_is_mapped': True,
        '_task_module': 'airflow.decorators.python',
        '_task_type': '_PythonDecoratedOperator',
        '_operator_name': '@task',
        'downstream_task_ids': [],
        'partial_kwargs': {
            'op_args': [],
            'op_kwargs': {
                '__type': 'dict',
                '__var': {'arg1': [1, 2, {"__type": "dict", "__var": {'a': 'b'}}]},
            },
            'retry_delay': {'__type': 'timedelta', '__var': 30.0},
        },
        'op_kwargs_expand_input': {
            "type": "list-of-dicts",
            "value": {
                "__type": "xcomref",
                "__var": {'task_id': 'op1', 'key': 'return_value'},
            },
        },
        'operator_extra_links': [],
        'ui_color': '#ffefeb',
        'ui_fgcolor': '#000',
        'task_id': 'x',
        'template_ext': [],
        'template_fields': ['op_args', 'op_kwargs'],
        'template_fields_renderers': {"op_args": "py", "op_kwargs": "py"},
        "_disallow_kwargs_override": strict,
        '_expand_input_attr': 'op_kwargs_expand_input',
    }

    deserialized = SerializedBaseOperator.deserialize_operator(serialized)
    assert isinstance(deserialized, MappedOperator)
    assert deserialized.deps is MappedOperator.deps_for(BaseOperator)
    assert deserialized._disallow_kwargs_override == strict
    assert deserialized.upstream_task_ids == set()
    assert deserialized.downstream_task_ids == set()

    assert deserialized.op_kwargs_expand_input == _ExpandInputRef(
        key="list-of-dicts",
        value=_XComRef({"task_id": "op1", "key": XCOM_RETURN_KEY}),
    )
    assert deserialized.partial_kwargs == {
        "op_args": [],
        "op_kwargs": {"arg1": [1, 2, {"a": "b"}]},
        "retry_delay": timedelta(seconds=30),
    }

    # Ensure the serialized operator can also be correctly pickled, to ensure
    # correct interaction between DAG pickling and serialization. This is done
    # here so we don't need to duplicate tests between pickled and non-pickled
    # DAGs everywhere else.
    pickled = pickle.loads(pickle.dumps(deserialized))
    assert pickled.op_kwargs_expand_input == _ExpandInputRef(
        "list-of-dicts",
        _XComRef({"task_id": "op1", "key": XCOM_RETURN_KEY}),
    )
    assert pickled.partial_kwargs == {
        "op_args": [],
        "op_kwargs": {"arg1": [1, 2, {"a": "b"}]},
        "retry_delay": timedelta(seconds=30),
    }
