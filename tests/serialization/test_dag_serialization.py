# -*- coding: utf-8 -*-
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
import unittest
from datetime import datetime, timedelta
from unittest import mock

from dateutil.relativedelta import FR, relativedelta
from parameterized import parameterized

from airflow import example_dags
from airflow.contrib import example_dags as contrib_example_dags
from airflow.gcp import example_dags as gcp_example_dags
from airflow.hooks.base_hook import BaseHook
from airflow.models import DAG, Connection, DagBag
from airflow.models.baseoperator import BaseOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.serialization.serialized_objects import SerializedBaseOperator, SerializedDAG
from airflow.utils.tests import CustomBaseOperator, GoogleLink

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
        "params": {},
        "_dag_id": "simple_dag",
        "fileloc": None,
        "tasks": [
            {
                "task_id": "simple_task",
                "owner": "airflow",
                "retries": 1,
                "retry_delay": 300.0,
                "_downstream_task_ids": [],
                "_inlets": [],
                "_outlets": [],
                "ui_color": "#fff",
                "ui_fgcolor": "#000",
                "template_fields": [],
                "_task_type": "BaseOperator",
                "_task_module": "airflow.models.baseoperator",
            },
            {
                "task_id": "custom_task",
                "retries": 1,
                "retry_delay": 300.0,
                "_downstream_task_ids": [],
                "_inlets": [],
                "_outlets": [],
                "ui_color": "#fff",
                "ui_fgcolor": "#000",
                "template_fields": [],
                "_task_type": "CustomBaseOperator",
                "_task_module": "airflow.utils.tests",
            },
        ],
        "timezone": "UTC",
    },
}


def make_example_dags(module):
    """Loads DAGs from a module for test."""
    dagbag = DagBag(module.__path__[0])
    return dagbag.dags


def make_simple_dag():
    """Make very simple DAG to verify serialization result."""
    dag = DAG(
        dag_id='simple_dag',
        default_args={
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "depends_on_past": False,
        },
        start_date=datetime(2019, 8, 1),
    )
    BaseOperator(task_id='simple_task', dag=dag, owner='airflow')
    CustomBaseOperator(task_id='custom_task', dag=dag)
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
    dags.update(make_example_dags(example_dags))
    dags.update(make_example_dags(contrib_example_dags))
    dags.update(make_example_dags(gcp_example_dags))
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

        simple_dag = stringified_dags['simple_dag']
        custom_task = simple_dag.task_dict['custom_task']
        self.validate_operator_extra_links(custom_task)

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

    def validate_operator_extra_links(self, task):
        """
        This tests also depends on GoogleLink() registered as a plugin
        in tests/plugins/test_plugin.py

        The function tests that if extra operator links are registered in plugin
        in ``operator_extra_links`` and the same is also defined in
        the Operator in ``BaseOperator.operator_extra_links``, it has the correct
        extra link.
        """
        self.assertEqual(
            task.operator_extra_link_dict[GoogleLink.name].get_link(
                task, datetime(2019, 8, 1)),
            "https://www.google.com"
        )

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
                "params": {},
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


if __name__ == '__main__':
    unittest.main()
