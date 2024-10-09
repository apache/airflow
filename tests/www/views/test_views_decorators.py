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

import pytest

from airflow.models import DagBag, Variable
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.types import DagRunType

from dev.tests_common.test_utils.compat import AIRFLOW_V_3_0_PLUS
from dev.tests_common.test_utils.db import clear_db_runs, clear_db_variables
from dev.tests_common.test_utils.www import (
    _check_last_log,
    _check_last_log_masked_variable,
    check_content_in_response,
)

if AIRFLOW_V_3_0_PLUS:
    from airflow.utils.types import DagRunTriggeredByType

pytestmark = pytest.mark.db_test

EXAMPLE_DAG_DEFAULT_DATE = timezone.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)


@pytest.fixture(scope="module")
def dagbag():
    DagBag(include_examples=True, read_dags_from_db=False).sync_to_db()
    return DagBag(include_examples=True, read_dags_from_db=True)


@pytest.fixture(scope="module")
def bash_dag(dagbag):
    return dagbag.get_dag("example_bash_operator")


@pytest.fixture(scope="module")
def xcom_dag(dagbag):
    return dagbag.get_dag("example_xcom")


@pytest.fixture(autouse=True)
def dagruns(bash_dag, xcom_dag):
    triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
    bash_dagrun = bash_dag.create_dagrun(
        run_type=DagRunType.SCHEDULED,
        execution_date=EXAMPLE_DAG_DEFAULT_DATE,
        data_interval=(EXAMPLE_DAG_DEFAULT_DATE, EXAMPLE_DAG_DEFAULT_DATE),
        start_date=timezone.utcnow(),
        state=State.RUNNING,
        **triggered_by_kwargs,
    )

    xcom_dagrun = xcom_dag.create_dagrun(
        run_type=DagRunType.SCHEDULED,
        execution_date=EXAMPLE_DAG_DEFAULT_DATE,
        data_interval=(EXAMPLE_DAG_DEFAULT_DATE, EXAMPLE_DAG_DEFAULT_DATE),
        start_date=timezone.utcnow(),
        state=State.RUNNING,
        **triggered_by_kwargs,
    )

    yield bash_dagrun, xcom_dagrun

    clear_db_runs()


@pytest.fixture(autouse=True)
def _clean_db():
    clear_db_variables()
    yield
    clear_db_variables()


def test_action_logging_robots(session, admin_client):
    url = "/robots.txt"
    admin_client.get(url, follow_redirects=True)

    # In mysql backend, this commit() is needed to write down the logs
    session.commit()
    _check_last_log(
        session,
        event="robots",
        dag_id=None,
        execution_date=None,
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
    check_content_in_response(["example_bash_operator", "Please confirm"], resp)
    # In mysql backend, this commit() is needed to write down the logs
    session.commit()
    _check_last_log(
        session,
        dag_id="example_bash_operator",
        event="clear",
        execution_date=EXAMPLE_DAG_DEFAULT_DATE,
        expected_extra={
            "upstream": "false",
            "downstream": "false",
            "future": "false",
            "past": "false",
            "only_failed": "false",
        },
    )


def delete_variable(session, key):
    session.query(Variable).filter(Variable.key == key).delete()
    session.commit()


def test_action_logging_variables_post(session, admin_client):
    form = dict(key="random", val="random")
    admin_client.post("/variable/add", data=form)
    session.commit()
    _check_last_log(session, dag_id=None, event="variable.create", execution_date=None)


@pytest.mark.enable_redact
def test_action_logging_variables_masked_secrets(session, admin_client):
    form = dict(key="x_secret", val="randomval")
    admin_client.post("/variable/add", data=form)
    session.commit()
    _check_last_log_masked_variable(session, dag_id=None, event="variable.create", execution_date=None)
