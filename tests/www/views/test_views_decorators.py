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
from __future__ import annotations

import urllib.parse
from unittest import mock

import pytest

from airflow.models import DagBag, DagRun, TaskInstance, Variable
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from airflow.www import app
from airflow.www.views import action_has_dag_edit_access
from tests.test_utils.db import clear_db_runs, clear_db_variables
from tests.test_utils.www import _check_last_log, _check_last_log_masked_variable, check_content_in_response

EXAMPLE_DAG_DEFAULT_DATE = timezone.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)


@pytest.fixture(scope="module")
def dagbag():
    DagBag(include_examples=True, read_dags_from_db=False).sync_to_db()
    return DagBag(include_examples=True, read_dags_from_db=True)


@pytest.fixture(scope="module")
def bash_dag(dagbag):
    return dagbag.get_dag("example_bash_operator")


@pytest.fixture(scope="module")
def sub_dag(dagbag):
    return dagbag.get_dag("example_subdag_operator")


@pytest.fixture(scope="module")
def xcom_dag(dagbag):
    return dagbag.get_dag("example_xcom")


@pytest.fixture(autouse=True)
def dagruns(bash_dag, sub_dag, xcom_dag):
    bash_dagrun = bash_dag.create_dagrun(
        run_type=DagRunType.SCHEDULED,
        execution_date=EXAMPLE_DAG_DEFAULT_DATE,
        data_interval=(EXAMPLE_DAG_DEFAULT_DATE, EXAMPLE_DAG_DEFAULT_DATE),
        start_date=timezone.utcnow(),
        state=State.RUNNING,
    )

    sub_dagrun = sub_dag.create_dagrun(
        run_type=DagRunType.SCHEDULED,
        execution_date=EXAMPLE_DAG_DEFAULT_DATE,
        data_interval=(EXAMPLE_DAG_DEFAULT_DATE, EXAMPLE_DAG_DEFAULT_DATE),
        start_date=timezone.utcnow(),
        state=State.RUNNING,
    )

    xcom_dagrun = xcom_dag.create_dagrun(
        run_type=DagRunType.SCHEDULED,
        execution_date=EXAMPLE_DAG_DEFAULT_DATE,
        data_interval=(EXAMPLE_DAG_DEFAULT_DATE, EXAMPLE_DAG_DEFAULT_DATE),
        start_date=timezone.utcnow(),
        state=State.RUNNING,
    )

    yield bash_dagrun, sub_dagrun, xcom_dagrun

    clear_db_runs()


@pytest.fixture(autouse=True)
def clean_db():
    clear_db_variables()
    yield
    clear_db_variables()


@action_has_dag_edit_access
def some_view_action_which_requires_dag_edit_access(*args) -> bool:
    return True


def test_action_logging_get(session, admin_client):
    url = (
        f"dags/example_bash_operator/graph?"
        f"execution_date={urllib.parse.quote_plus(str(EXAMPLE_DAG_DEFAULT_DATE))}"
    )
    resp = admin_client.get(url, follow_redirects=True)
    check_content_in_response("runme_1", resp)

    # In mysql backend, this commit() is needed to write down the logs
    session.commit()
    _check_last_log(
        session,
        dag_id="example_bash_operator",
        event="graph",
        execution_date=EXAMPLE_DAG_DEFAULT_DATE,
    )


def test_action_logging_get_legacy_view(session, admin_client):
    url = (
        f"graph?dag_id=example_bash_operator&"
        f"execution_date={urllib.parse.quote_plus(str(EXAMPLE_DAG_DEFAULT_DATE))}"
    )
    resp = admin_client.get(url, follow_redirects=True)
    check_content_in_response("runme_1", resp)

    # In mysql backend, this commit() is needed to write down the logs
    session.commit()
    _check_last_log(
        session,
        dag_id="example_bash_operator",
        event="legacy_graph",
        execution_date=EXAMPLE_DAG_DEFAULT_DATE,
    )


def test_action_logging_post(session, admin_client):
    form = dict(
        task_id="runme_1",
        dag_id="example_bash_operator",
        execution_date=EXAMPLE_DAG_DEFAULT_DATE,
        upstream="false",
        downstream="false",
        future="false",
        past="false",
        only_failed="false",
    )
    resp = admin_client.post("clear", data=form)
    check_content_in_response(["example_bash_operator", "Wait a minute"], resp)
    # In mysql backend, this commit() is needed to write down the logs
    session.commit()
    _check_last_log(
        session,
        dag_id="example_bash_operator",
        event="clear",
        execution_date=EXAMPLE_DAG_DEFAULT_DATE,
    )


def delete_variable(session, key):
    session.query(Variable).filter(Variable.key == key).delete()
    session.commit()


def test_action_logging_variables_post(session, admin_client):
    form = dict(key="random", val="random")
    admin_client.post("/variable/add", data=form)
    session.commit()
    _check_last_log(session, dag_id=None, event="variable.create", execution_date=None)


def test_action_logging_variables_masked_secrets(session, admin_client):
    form = dict(key="x_secret", val="randomval")
    admin_client.post("/variable/add", data=form)
    session.commit()
    _check_last_log_masked_variable(session, dag_id=None, event="variable.create", execution_date=None)


def test_calendar(admin_client, dagruns):
    url = "calendar?dag_id=example_bash_operator"
    resp = admin_client.get(url, follow_redirects=True)

    bash_dagrun, _, _ = dagruns

    datestr = bash_dagrun.execution_date.date().isoformat()
    expected = rf"{{\"date\":\"{datestr}\",\"state\":\"running\",\"count\":1}}"
    check_content_in_response(expected, resp)


@pytest.mark.parametrize(
    "class_type, no_instances, no_unique_dags",
    [
        (None, 0, 0),
        (TaskInstance, 0, 0),
        (TaskInstance, 1, 1),
        (TaskInstance, 10, 1),
        (TaskInstance, 10, 5),
        (DagRun, 0, 0),
        (DagRun, 1, 1),
        (DagRun, 10, 1),
        (DagRun, 10, 9),
    ],
)
def test_action_has_dag_edit_access(create_task_instance, class_type, no_instances, no_unique_dags):
    unique_dag_ids = [f"test_dag_id_{nr}" for nr in range(no_unique_dags)]
    tis: list[TaskInstance] = [
        create_task_instance(
            task_id=f"test_task_instance_{nr}",
            execution_date=timezone.datetime(2021, 1, 1 + nr),
            dag_id=unique_dag_ids[nr % len(unique_dag_ids)],
            run_id=f"test_run_id_{nr}",
        )
        for nr in range(no_instances)
    ]
    if class_type is None:
        test_items = None
    else:
        test_items = tis if class_type == TaskInstance else [ti.get_dagrun() for ti in tis]
        test_items = test_items[0] if len(test_items) == 1 else test_items
    application = app.create_app(testing=True)
    with application.app_context():
        with mock.patch.object(application.appbuilder.sm, "can_edit_dag") as mocked_can_edit:
            mocked_can_edit.return_value = True
            assert not isinstance(test_items, list) or len(test_items) == no_instances
            assert some_view_action_which_requires_dag_edit_access(None, test_items) is True
            assert mocked_can_edit.call_count == no_unique_dags
    clear_db_runs()


def test_action_has_dag_edit_access_exception():
    with pytest.raises(ValueError):
        some_view_action_which_requires_dag_edit_access(None, "some_incorrect_value")
