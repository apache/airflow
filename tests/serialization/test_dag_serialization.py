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

import multiprocessing
import os
import unittest
from datetime import datetime, timedelta
from glob import glob
from unittest import mock

from dateutil.relativedelta import FR, relativedelta
from parameterized import parameterized

from airflow.hooks.base_hook import BaseHook
from airflow.models import DAG, Connection, DagBag, TaskInstance
from airflow.models.baseoperator import BaseOperator
from airflow.operators.bash import BashOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.serialization.json_schema import load_dag_schema_dict
from airflow.serialization.serialized_objects import SerializedBaseOperator, SerializedDAG
from tests.test_utils.mock_operators import CustomOperator, CustomOpLink, GoogleLink

serialized_simple_dag_ground_truth = {
    "__version": 1,
    "dag": {
        "default_args": {
            "__type": "dict",
            "__var": {
                "depends_on_past": False,
                "retries": 1,
                "retry_delay": {
                    "__type": "timedelta",
                    "__var": 300.0
                }
            }
        },
        "start_date": 1564617600.0,
        "is_paused_upon_creation": False,
        "_dag_id": "simple_dag",
        "fileloc": None,
        "tasks": [
            {
                "task_id": "bash_task",
                "owner": "airflow",
                "retries": 1,
                "retry_delay": 300.0,
                "_downstream_task_ids": [],
                "_inlets": [],
                "_outlets": [],
                "ui_color": "#f0ede4",
                "ui_fgcolor": "#000",
                "template_fields": ['bash_command', 'env'],
                "bash_command": "echo {{ task.task_id }}",
                "_task_type": "BashOperator",
                "_task_module": "airflow.operators.bash",
            },
            {
                "task_id": "custom_task",
                "retries": 1,
                "retry_delay": 300.0,
                "_downstream_task_ids": [],
                "_inlets": [],
                "_outlets": [],
                "_operator_extra_links": [{"tests.test_utils.mock_operators.CustomOpLink": {}}],
                "ui_color": "#fff",
                "ui_fgcolor": "#000",
                "template_fields": [],
                "_task_type": "CustomOperator",
                "_task_module": "tests.test_utils.mock_operators",
            },
        ],
        "timezone": "UTC",
    },
}

ROOT_FOLDER = os.path.realpath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir)
)


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
            "depends_on_past": False,
        },
        start_date=datetime(2019, 8, 1),
        is_paused_upon_creation=False,
    ) as dag:
        CustomOperator(task_id='custom_task')
        BashOperator(task_id='bash_task', bash_command='echo {{ task.task_id }}', owner='airflow')
    return {'simple_dag': dag}


def make_user_defined_macro_filter_dag():
    """ Make DAGs with user defined macros and filters using locally defined methods.

    For Webserver, we do not include ``user_defined_macros`` & ``user_defined_filters``.

    The examples here test:
        (1) functions can be successfully displayed on UI;
        (2) templates with function macros have been rendered before serialization.
    """

    def compute_next_execution_date(dag, execution_date):
        return dag.following_schedule(execution_date)

    default_args = {
        'start_date': datetime(2019, 7, 10)
    }
    dag = DAG(
        'user_defined_macro_filter_dag',
        default_args=default_args,
        user_defined_macros={
            'next_execution_date': compute_next_execution_date,
        },
        user_defined_filters={
            'hello': lambda name: 'Hello %s' % name
        },
        catchup=False
    )
    BashOperator(
        task_id='echo',
        bash_command='echo "{{ next_execution_date(dag, execution_date) }}"',
        dag=dag,
    )
    return {dag.dag_id: dag}


def collect_dags():
    """Collects DAGs to test."""
    dags = {}
    dags.update(make_simple_dag())
    dags.update(make_user_defined_macro_filter_dag())
    patterns = [
        "airflow/example_dags",
        "airflow/providers/*/example_dags",
        "airflow/providers/*/*/example_dags",
    ]
    for pattern in patterns:
        for directory in glob(f"{ROOT_FOLDER}/{pattern}"):
            dags.update(make_example_dags(directory))

    # Filter subdags as they are stored in same row in Serialized Dag table
    dags = {dag_id: dag for dag_id, dag in dags.items() if not dag.is_subdag}
    return dags


def serialize_subprocess(queue):
    """Validate pickle in a subprocess."""
    dags = collect_dags()
    for dag in dags.values():
        queue.put(SerializedDAG.to_json(dag))
    queue.put(None)


class TestStringifiedDAGs(unittest.TestCase):
    """Unit tests for stringified DAGs."""

    def setUp(self):
        super().setUp()
        BaseHook.get_connection = mock.Mock(
            return_value=Connection(
                extra=('{'
                       '"project_id": "mock", '
                       '"location": "mock", '
                       '"instance": "mock", '
                       '"database_type": "postgres", '
                       '"use_proxy": "False", '
                       '"use_ssl": "False"'
                       '}')))
        self.maxDiff = None  # pylint: disable=invalid-name

    def test_serialization(self):
        """Serialization and deserialization should work for every DAG and Operator."""
        dags = collect_dags()
        serialized_dags = {}
        for _, v in dags.items():
            dag = SerializedDAG.to_dict(v)
            SerializedDAG.validate_schema(dag)
            serialized_dags[v.dag_id] = dag

        # Compares with the ground truth of JSON string.
        self.validate_serialized_dag(
            serialized_dags['simple_dag'],
            serialized_simple_dag_ground_truth)

    def validate_serialized_dag(self, json_dag, ground_truth_dag):
        """Verify serialized DAGs match the ground truth."""
        self.assertTrue(
            json_dag['dag']['fileloc'].split('/')[-1] == 'test_dag_serialization.py')
        json_dag['dag']['fileloc'] = None

        def sorted_serialized_dag(dag_dict: dict):
            """
            Sorts the "tasks" list in the serialised dag python dictionary
            This is needed as the order of tasks should not matter but assertEqual
            would fail if the order of tasks list changes in dag dictionary
            """
            dag_dict["dag"]["tasks"] = sorted(dag_dict["dag"]["tasks"],
                                              key=lambda x: sorted(x.keys()))
            return dag_dict

        self.assertEqual(sorted_serialized_dag(ground_truth_dag),
                         sorted_serialized_dag(json_dag))

    def test_deserialization(self):
        """A serialized DAG can be deserialized in another process."""
        queue = multiprocessing.Queue()
        proc = multiprocessing.Process(
            target=serialize_subprocess, args=(queue,))
        proc.daemon = True
        proc.start()

        stringified_dags = {}
        while True:
            v = queue.get()
            if v is None:
                break
            dag = SerializedDAG.from_json(v)
            self.assertTrue(isinstance(dag, DAG))
            stringified_dags[dag.dag_id] = dag

        dags = collect_dags()
        self.assertTrue(set(stringified_dags.keys()) == set(dags.keys()))

        # Verify deserialized DAGs.
        for dag_id in stringified_dags:
            self.validate_deserialized_dag(stringified_dags[dag_id], dags[dag_id])

        example_skip_dag = stringified_dags['example_skip_dag']
        skip_operator_1_task = example_skip_dag.task_dict['skip_operator_1']
        self.validate_deserialized_task(
            skip_operator_1_task, 'DummySkipOperator', '#e8b7e4', '#000')

        # Verify that the DAG object has 'full_filepath' attribute
        # and is equal to fileloc
        self.assertTrue(hasattr(example_skip_dag, 'full_filepath'))
        self.assertEqual(example_skip_dag.full_filepath, example_skip_dag.fileloc)

        example_subdag_operator = stringified_dags['example_subdag_operator']
        section_1_task = example_subdag_operator.task_dict['section-1']
        self.validate_deserialized_task(
            section_1_task,
            SubDagOperator.__name__,
            SubDagOperator.ui_color,
            SubDagOperator.ui_fgcolor
        )

    def validate_deserialized_dag(self, serialized_dag, dag):
        """
        Verify that all example DAGs work with DAG Serialization by
        checking fields between Serialized Dags & non-Serialized Dags
        """
        fields_to_check = [
            "task_ids", "params", "fileloc", "max_active_runs", "concurrency",
            "is_paused_upon_creation", "doc_md", "safe_dag_id", "is_subdag",
            "catchup", "description", "start_date", "end_date", "parent_dag",
            "template_searchpath"
        ]

        # fields_to_check = dag.get_serialized_fields()
        for field in fields_to_check:
            self.assertEqual(getattr(serialized_dag, field), getattr(dag, field))

    def validate_deserialized_task(self, task, task_type, ui_color, ui_fgcolor):
        """Verify non-airflow operators are casted to BaseOperator."""
        self.assertTrue(isinstance(task, SerializedBaseOperator))
        # Verify the original operator class is recorded for UI.
        self.assertTrue(task.task_type == task_type)
        self.assertTrue(task.ui_color == ui_color)
        self.assertTrue(task.ui_fgcolor == ui_fgcolor)

        # Check that for Deserialised task, task.subdag is None for all other Operators
        # except for the SubDagOperator where task.subdag is an instance of DAG object
        if task.task_type == "SubDagOperator":
            self.assertIsNotNone(task.subdag)
            self.assertTrue(isinstance(task.subdag, DAG))
        else:
            self.assertIsNone(task.subdag)
        self.assertEqual({}, task.params)
        self.assertEqual({}, task.executor_config)

    @parameterized.expand([
        (datetime(2019, 8, 1), None, datetime(2019, 8, 1)),
        (datetime(2019, 8, 1), datetime(2019, 8, 2), datetime(2019, 8, 2)),
        (datetime(2019, 8, 1), datetime(2019, 7, 30), datetime(2019, 8, 1)),
    ])
    def test_deserialization_start_date(self,
                                        dag_start_date,
                                        task_start_date,
                                        expected_task_start_date):
        dag = DAG(dag_id='simple_dag', start_date=dag_start_date)
        BaseOperator(task_id='simple_task', dag=dag, start_date=task_start_date)

        serialized_dag = SerializedDAG.to_dict(dag)
        if not task_start_date or dag_start_date >= task_start_date:
            # If dag.start_date > task.start_date -> task.start_date=dag.start_date
            # because of the logic in dag.add_task()
            self.assertNotIn("start_date", serialized_dag["dag"]["tasks"][0])
        else:
            self.assertIn("start_date", serialized_dag["dag"]["tasks"][0])

        dag = SerializedDAG.from_dict(serialized_dag)
        simple_task = dag.task_dict["simple_task"]
        self.assertEqual(simple_task.start_date, expected_task_start_date)

    @parameterized.expand([
        (datetime(2019, 8, 1), None, datetime(2019, 8, 1)),
        (datetime(2019, 8, 1), datetime(2019, 8, 2), datetime(2019, 8, 1)),
        (datetime(2019, 8, 1), datetime(2019, 7, 30), datetime(2019, 7, 30)),
    ])
    def test_deserialization_end_date(self,
                                      dag_end_date,
                                      task_end_date,
                                      expected_task_end_date):
        dag = DAG(dag_id='simple_dag', start_date=datetime(2019, 8, 1),
                  end_date=dag_end_date)
        BaseOperator(task_id='simple_task', dag=dag, end_date=task_end_date)

        serialized_dag = SerializedDAG.to_dict(dag)
        if not task_end_date or dag_end_date <= task_end_date:
            # If dag.end_date < task.end_date -> task.end_date=dag.end_date
            # because of the logic in dag.add_task()
            self.assertNotIn("end_date", serialized_dag["dag"]["tasks"][0])
        else:
            self.assertIn("end_date", serialized_dag["dag"]["tasks"][0])

        dag = SerializedDAG.from_dict(serialized_dag)
        simple_task = dag.task_dict["simple_task"]
        self.assertEqual(simple_task.end_date, expected_task_end_date)

    @parameterized.expand([
        (None, None),
        ("@weekly", "@weekly"),
        ({"__type": "timedelta", "__var": 86400.0}, timedelta(days=1)),
    ])
    def test_deserialization_schedule_interval(self, serialized_schedule_interval, expected):
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

        self.assertEqual(dag.schedule_interval, expected)

    @parameterized.expand([
        (relativedelta(days=-1), {"__type": "relativedelta", "__var": {"days": -1}}),
        (relativedelta(month=1, days=-1), {"__type": "relativedelta", "__var": {"month": 1, "days": -1}}),
        # Every friday
        (relativedelta(weekday=FR), {"__type": "relativedelta", "__var": {"weekday": [4]}}),
        # Every second friday
        (relativedelta(weekday=FR(2)), {"__type": "relativedelta", "__var": {"weekday": [4, 2]}})
    ])
    def test_roundtrip_relativedelta(self, val, expected):
        serialized = SerializedDAG._serialize(val)
        self.assertDictEqual(serialized, expected)

        round_tripped = SerializedDAG._deserialize(serialized)
        self.assertEqual(val, round_tripped)

    @parameterized.expand([
        (None, {}),
        ({"param_1": "value_1"}, {"param_1": "value_1"}),
    ])
    def test_dag_params_roundtrip(self, val, expected_val):
        """
        Test that params work both on Serialized DAGs & Tasks
        """
        dag = DAG(dag_id='simple_dag', params=val)
        BaseOperator(task_id='simple_task', dag=dag, start_date=datetime(2019, 8, 1))

        serialized_dag = SerializedDAG.to_dict(dag)
        if val:
            self.assertIn("params", serialized_dag["dag"])
        else:
            self.assertNotIn("params", serialized_dag["dag"])

        deserialized_dag = SerializedDAG.from_dict(serialized_dag)
        deserialized_simple_task = deserialized_dag.task_dict["simple_task"]
        self.assertEqual(expected_val, deserialized_dag.params)
        self.assertEqual(expected_val, deserialized_simple_task.params)

    @parameterized.expand([
        (None, {}),
        ({"param_1": "value_1"}, {"param_1": "value_1"}),
    ])
    def test_task_params_roundtrip(self, val, expected_val):
        """
        Test that params work both on Serialized DAGs & Tasks
        """
        dag = DAG(dag_id='simple_dag')
        BaseOperator(task_id='simple_task', dag=dag, params=val,
                     start_date=datetime(2019, 8, 1))

        serialized_dag = SerializedDAG.to_dict(dag)
        if val:
            self.assertIn("params", serialized_dag["dag"]["tasks"][0])
        else:
            self.assertNotIn("params", serialized_dag["dag"]["tasks"][0])

        deserialized_dag = SerializedDAG.from_dict(serialized_dag)
        deserialized_simple_task = deserialized_dag.task_dict["simple_task"]
        self.assertEqual(expected_val, deserialized_simple_task.params)

    def test_extra_serialized_field_and_operator_links(self):
        """
        Assert extra field exists & OperatorLinks defined in Plugins and inbuilt Operator Links.

        This tests also depends on GoogleLink() registered as a plugin
        in tests/plugins/test_plugin.py

        The function tests that if extra operator links are registered in plugin
        in ``operator_extra_links`` and the same is also defined in
        the Operator in ``BaseOperator.operator_extra_links``, it has the correct
        extra link.
        """
        test_date = datetime(2019, 8, 1)
        dag = DAG(dag_id='simple_dag', start_date=test_date)
        CustomOperator(task_id='simple_task', dag=dag, bash_command="true")

        serialized_dag = SerializedDAG.to_dict(dag)
        self.assertIn("bash_command", serialized_dag["dag"]["tasks"][0])

        dag = SerializedDAG.from_dict(serialized_dag)
        simple_task = dag.task_dict["simple_task"]
        self.assertEqual(getattr(simple_task, "bash_command"), "true")

        #########################################################
        # Verify Operator Links work with Serialized Operator
        #########################################################
        # Check Serialized version of operator link only contains the inbuilt Op Link
        self.assertEqual(
            serialized_dag["dag"]["tasks"][0]["_operator_extra_links"],
            [{'tests.test_utils.mock_operators.CustomOpLink': {}}]
        )

        # Test all the extra_links are set
        self.assertCountEqual(simple_task.extra_links, ['Google Custom', 'airflow', 'github', 'google'])

        ti = TaskInstance(task=simple_task, execution_date=test_date)
        ti.xcom_push('search_query', "dummy_value_1")

        # Test Deserialized inbuilt link
        custom_inbuilt_link = simple_task.get_extra_links(test_date, CustomOpLink.name)
        self.assertEqual('http://google.com/custom_base_link?search=dummy_value_1', custom_inbuilt_link)

        # Test Deserialized link registered via Airflow Plugin
        google_link_from_plugin = simple_task.get_extra_links(test_date, GoogleLink.name)
        self.assertEqual("https://www.google.com", google_link_from_plugin)

    def test_extra_serialized_field_and_multiple_operator_links(self):
        """
        Assert extra field exists & OperatorLinks defined in Plugins and inbuilt Operator Links.

        This tests also depends on GoogleLink() registered as a plugin
        in tests/plugins/test_plugin.py

        The function tests that if extra operator links are registered in plugin
        in ``operator_extra_links`` and the same is also defined in
        the Operator in ``BaseOperator.operator_extra_links``, it has the correct
        extra link.
        """
        test_date = datetime(2019, 8, 1)
        dag = DAG(dag_id='simple_dag', start_date=test_date)
        CustomOperator(task_id='simple_task', dag=dag, bash_command=["echo", "true"])

        serialized_dag = SerializedDAG.to_dict(dag)
        self.assertIn("bash_command", serialized_dag["dag"]["tasks"][0])

        dag = SerializedDAG.from_dict(serialized_dag)
        simple_task = dag.task_dict["simple_task"]
        self.assertEqual(getattr(simple_task, "bash_command"), ["echo", "true"])

        #########################################################
        # Verify Operator Links work with Serialized Operator
        #########################################################
        # Check Serialized version of operator link only contains the inbuilt Op Link
        self.assertEqual(
            serialized_dag["dag"]["tasks"][0]["_operator_extra_links"],
            [
                {'tests.test_utils.mock_operators.CustomBaseIndexOpLink': {'index': 0}},
                {'tests.test_utils.mock_operators.CustomBaseIndexOpLink': {'index': 1}},
            ]
        )

        # Test all the extra_links are set
        self.assertCountEqual(simple_task.extra_links, [
            'BigQuery Console #1', 'BigQuery Console #2', 'airflow', 'github', 'google'])

        ti = TaskInstance(task=simple_task, execution_date=test_date)
        ti.xcom_push('search_query', ["dummy_value_1", "dummy_value_2"])

        # Test Deserialized inbuilt link #1
        custom_inbuilt_link = simple_task.get_extra_links(test_date, "BigQuery Console #1")
        self.assertEqual('https://console.cloud.google.com/bigquery?j=dummy_value_1', custom_inbuilt_link)

        # Test Deserialized inbuilt link #2
        custom_inbuilt_link = simple_task.get_extra_links(test_date, "BigQuery Console #2")
        self.assertEqual('https://console.cloud.google.com/bigquery?j=dummy_value_2', custom_inbuilt_link)

        # Test Deserialized link registered via Airflow Plugin
        google_link_from_plugin = simple_task.get_extra_links(test_date, GoogleLink.name)
        self.assertEqual("https://www.google.com", google_link_from_plugin)

    class ClassWithCustomAttributes:
        """
        Class for testing purpose: allows to create objects with custom attributes in one single statement.
        """

        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)

        def __str__(self):
            return "{}({})".format(self.__class__.__name__, str(self.__dict__))

        def __repr__(self):
            return self.__str__()

        def __eq__(self, other):
            return self.__dict__ == other.__dict__

        def __ne__(self, other):
            return not self.__eq__(other)

    @parameterized.expand([
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
            {"foo": {"bar": {"{{ task.task_id }}": ["sar"]}}}),
        (
            ClassWithCustomAttributes(
                att1="{{ task.task_id }}", att2="{{ task.task_id }}", template_fields=["att1"]),
            "ClassWithCustomAttributes("
            "{'att1': '{{ task.task_id }}', 'att2': '{{ task.task_id }}', 'template_fields': ['att1']})",
        ),
        (
            ClassWithCustomAttributes(nested1=ClassWithCustomAttributes(att1="{{ task.task_id }}",
                                                                        att2="{{ task.task_id }}",
                                                                        template_fields=["att1"]),
                                      nested2=ClassWithCustomAttributes(att3="{{ task.task_id }}",
                                                                        att4="{{ task.task_id }}",
                                                                        template_fields=["att3"]),
                                      template_fields=["nested1"]),
            "ClassWithCustomAttributes("
            "{'nested1': ClassWithCustomAttributes({'att1': '{{ task.task_id }}', "
            "'att2': '{{ task.task_id }}', 'template_fields': ['att1']}), "
            "'nested2': ClassWithCustomAttributes({'att3': '{{ task.task_id }}', "
            "'att4': '{{ task.task_id }}', 'template_fields': ['att3']}), 'template_fields': ['nested1']})",
        ),
    ])
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
        self.assertEqual(expected_field, getattr(deserialized_test_task, "bash_command"))

    def test_dag_serialized_fields_with_schema(self):
        """
        Additional Properties are disabled on DAGs. This test verifies that all the
        keys in DAG.get_serialized_fields are listed in Schema definition.
        """
        dag_schema: dict = load_dag_schema_dict()["definitions"]["dag"]["properties"]

        # The parameters we add manually in Serialization needs to be ignored
        ignored_keys: set = {"is_subdag", "tasks"}
        dag_params: set = set(dag_schema.keys()) - ignored_keys
        self.assertEqual(set(DAG.get_serialized_fields()), dag_params)

    def test_no_new_fields_added_to_base_operator(self):
        """
        This test verifies that there are no new fields added to BaseOperator. And reminds that
        tests should be added for it.
        """
        base_operator = BaseOperator(task_id="10")
        fields = base_operator.__dict__
        self.assertEqual({'_dag': None,
                          '_downstream_task_ids': set(),
                          '_inlets': [],
                          '_log': base_operator.log,
                          '_outlets': [],
                          '_upstream_task_ids': set(),
                          'depends_on_past': False,
                          'do_xcom_push': True,
                          'email': None,
                          'email_on_failure': True,
                          'email_on_retry': True,
                          'end_date': None,
                          'execution_timeout': None,
                          'executor_config': {},
                          'inlets': [],
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
                          'start_date': None,
                          'subdag': None,
                          'task_concurrency': None,
                          'task_id': '10',
                          'trigger_rule': 'all_success',
                          'wait_for_downstream': False,
                          'weight_rule': 'downstream'}, fields,
                         """
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

     ACTION NEEDED! PLEASE READ THIS CAREFULLY AND CORRECT TESTS CAREFULLY

 Some fields were added to the BaseOperator! Please add them to the list above and make sure that
 you add support for DAG serialization - you should add the field to
 `airflow/serialization/schema.json` - they should have correct type defined there.

 Note that we do not support versioning yet so you should only add optional fields to BaseOperator.

!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                         """
                         )


if __name__ == '__main__':
    unittest.main()
