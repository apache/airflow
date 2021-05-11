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
import urllib.parse

import pytest

from airflow.models import DagBag, Log
from airflow.utils import dates, timezone
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from tests.test_utils.db import clear_db_runs
from tests.test_utils.www import check_content_in_response

EXAMPLE_DAG_DEFAULT_DATE = dates.days_ago(2)


@pytest.fixture(scope="module")
def dagbag():
    DagBag(include_examples=True, read_dags_from_db=False).sync_to_db()
    return DagBag(include_examples=True, read_dags_from_db=True)


@pytest.fixture(scope="module")
def bash_dag(dagbag):
    return dagbag.get_dag('example_bash_operator')


@pytest.fixture(scope="module")
def sub_dag(dagbag):
    return dagbag.get_dag('example_subdag_operator')


@pytest.fixture(scope="module")
def xcom_dag(dagbag):
    return dagbag.get_dag('example_xcom')


@pytest.fixture(autouse=True)
def dagruns(bash_dag, sub_dag, xcom_dag):
    bash_dagrun = bash_dag.create_dagrun(
        run_type=DagRunType.SCHEDULED,
        execution_date=EXAMPLE_DAG_DEFAULT_DATE,
        start_date=timezone.utcnow(),
        state=State.RUNNING,
    )

    sub_dagrun = sub_dag.create_dagrun(
        run_type=DagRunType.SCHEDULED,
        execution_date=EXAMPLE_DAG_DEFAULT_DATE,
        start_date=timezone.utcnow(),
        state=State.RUNNING,
    )

    xcom_dagrun = xcom_dag.create_dagrun(
        run_type=DagRunType.SCHEDULED,
        execution_date=EXAMPLE_DAG_DEFAULT_DATE,
        start_date=timezone.utcnow(),
        state=State.RUNNING,
    )

    yield bash_dagrun, sub_dagrun, xcom_dagrun

    clear_db_runs()


def _check_last_log(session, dag_id, event, execution_date):
    logs = (
        session.query(
            Log.dag_id,
            Log.task_id,
            Log.event,
            Log.execution_date,
            Log.owner,
            Log.extra,
        )
        .filter(
            Log.dag_id == dag_id,
            Log.event == event,
            Log.execution_date == execution_date,
        )
        .order_by(Log.dttm.desc())
        .limit(5)
        .all()
    )
    assert len(logs) >= 1
    assert logs[0].extra


def test_action_logging_get(session, admin_client):
    url = 'graph?dag_id=example_bash_operator&execution_date={}'.format(
        urllib.parse.quote_plus(str(EXAMPLE_DAG_DEFAULT_DATE))
    )
    resp = admin_client.get(url, follow_redirects=True)
    check_content_in_response('runme_1', resp)

    # In mysql backend, this commit() is needed to write down the logs
    session.commit()
    _check_last_log(
        session,
        dag_id="example_bash_operator",
        event="graph",
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
    check_content_in_response(['example_bash_operator', 'Wait a minute'], resp)
    # In mysql backend, this commit() is needed to write down the logs
    session.commit()
    _check_last_log(
        session,
        dag_id="example_bash_operator",
        event="clear",
        execution_date=EXAMPLE_DAG_DEFAULT_DATE,
    )


def test_calendar(admin_client, dagruns):
    url = 'calendar?dag_id=example_bash_operator'
    resp = admin_client.get(url, follow_redirects=True)

    bash_dagrun, _, _ = dagruns

    datestr = bash_dagrun.execution_date.date().isoformat()
    expected = rf'{{\"date\":\"{datestr}\",\"state\":\"running\",\"count\":1}}'
    check_content_in_response(expected, resp)
