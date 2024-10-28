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
# Note: Any AirflowException raised is expected to cause the TaskInstance
#       to be marked in an ERROR state

from __future__ import annotations

import json
import os

import pytest
import re2 as re
from airflow.configuration import conf
from airflow.models import DagBag
from airflow.utils.trigger_rule import TriggerRule

DAGS_DIR = os.path.join(
    os.path.dirname(__file__), "../src/performance_dags/performance_dag"
)


def setup_dag(
    dag_count="1",
    task_count="10",
    start_date="",
    start_ago="1h",
    schedule_interval_env="@once",
    dag_shape="no_structure",
    sleep_time="0",
    operator_type="bash",
    start_paused="1",
    task_trigger_rule=TriggerRule.ALL_SUCCESS,
    **extra_args,
):
    os.environ["PERF_DAGS_COUNT"] = dag_count
    os.environ["PERF_TASKS_COUNT"] = task_count
    os.environ["PERF_START_DATE"] = start_date
    os.environ["PERF_START_AGO"] = start_ago
    os.environ["PERF_SCHEDULE_INTERVAL"] = schedule_interval_env
    os.environ["PERF_SHAPE"] = dag_shape
    os.environ["PERF_SLEEP_TIME"] = sleep_time
    os.environ["PERF_OPERATOR_TYPE"] = operator_type
    os.environ["PERF_START_PAUSED"] = start_paused
    os.environ["PERF_TASKS_TRIGGER_RULE"] = task_trigger_rule
    os.environ["PERF_OPERATOR_EXTRA_KWARGS"] = json.dumps(extra_args)


def get_top_level_tasks(dag):
    result = []
    for task in dag.tasks:
        if not task.upstream_list:
            result.append(task)
    return result


def get_leaf_tasks(dag):
    result = []
    for task in dag.tasks:
        if not task.downstream_list:
            result.append(task)
    return result


# Test fixture
@pytest.fixture(scope="session", autouse=True)
def airflow_config():
    """
    Update airflow config for the test.

    It sets the following configuration values:
    - core.unit_test_mode: True
    - lineage.backend: ""

    Returns:
        AirflowConfigParser: The Airflow configuration object.
    """
    conf.set("lineage", "backend", "")
    return conf


def get_dags(dag_count=1, task_count=10, operator_type="bash", dag_shape="no_structure"):
    """Generate a tuple of dag_id, <DAG objects> in the DagBag."""
    setup_dag(
        task_count=str(task_count),
        dag_count=str(dag_count),
        operator_type=operator_type,
        dag_shape=dag_shape,
    )
    dag_bag = DagBag(DAGS_DIR, include_examples=False)

    def strip_path_prefix(path):
        return os.path.relpath(path, DAGS_DIR)

    return [(k, v, strip_path_prefix(v.fileloc)) for k, v in dag_bag.dags.items()]


def get_import_errors():
    """Generate a tuple for import errors in the dag bag."""
    dag_bag = DagBag(DAGS_DIR, include_examples=False)

    def strip_path_prefix(path):
        return os.path.relpath(path, DAGS_DIR)

    # prepend "(None,None)" to ensure that a test object is always created even if it's a no op.
    return [(None, None)] + [
        (strip_path_prefix(k), v.strip()) for k, v in dag_bag.import_errors.items()
    ]


@pytest.mark.parametrize(
    "rel_path,rv", get_import_errors(), ids=[x[0] for x in get_import_errors()]
)
def test_file_imports(rel_path, rv):
    """Test for import errors on a file."""
    if rel_path and rv:
        pytest.fail(f"{rel_path} failed to import with message \n {rv}")


@pytest.mark.parametrize("dag_count,task_count", [(1, 1), (1, 10), (10, 10), (10, 100)])
def test_performance_dag(dag_count, task_count):
    dags = get_dags(dag_count=dag_count, task_count=task_count)
    assert len(dags) == dag_count
    ids = [x[0] for x in dags]
    pattern = f"performance_dag__SHAPE_no_structure__DAGS_COUNT_\\d+_of_{dag_count}__TASKS_COUNT_{task_count}__START_DATE_1h__SCHEDULE_INTERVAL_once"
    for id in ids:
        assert re.search(pattern, id)
    for dag in dags:
        performance_dag = dag[1]
        assert len(performance_dag.tasks) == task_count, f"DAG has no {task_count} tasks"
        for task in performance_dag.tasks:
            t_rule = task.trigger_rule
            assert t_rule == "all_success", f"{task} in DAG has the trigger rule {t_rule}"
            assert (
                task.operator_name == "BashOperator"
            ), f"{task} should be based on bash operator"


def test_performance_dag_shape_binary_tree():
    def assert_two_downstream(task):
        assert len(task.downstream_list) <= 2
        for downstream_task in task.downstream_list:
            assert_two_downstream(downstream_task)

    dags = get_dags(task_count=100, dag_shape="binary_tree")
    id, dag, _ = dags[0]
    assert (
        id
        == "performance_dag__SHAPE_binary_tree__DAGS_COUNT_1_of_1__TASKS_COUNT_100__START_DATE_1h__SCHEDULE_INTERVAL_once"
    )
    assert len(dag.tasks) == 100
    top_level_tasks = get_top_level_tasks(dag)
    assert len(top_level_tasks) == 1
    for task in top_level_tasks:
        assert_two_downstream(task)
