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
import json

import pytest

from airflow.models import DagBag, DagRun, TaskInstance
from airflow.utils import timezone
from airflow.utils.session import create_session
from tests.test_utils.config import conf_vars
from tests.test_utils.www import check_content_in_response


@pytest.fixture(scope="module", autouse=True)
def init_blank_dagrun():
    """Make sure there are no runs before we test anything.

    This really shouldn't be needed, but tests elsewhere leave the db dirty.
    """
    with create_session() as session:
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()


@pytest.fixture(autouse=True)
def reset_dagrun():
    yield
    with create_session() as session:
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()


@pytest.mark.parametrize(
    "post_date, expected",
    [
        ("2018-07-06 05:04:03Z", timezone.datetime(2018, 7, 6, 5, 4, 3)),
        ("2018-07-06 05:04:03-04:00", timezone.datetime(2018, 7, 6, 9, 4, 3)),
        ("2018-07-06 05:04:03-08:00", timezone.datetime(2018, 7, 6, 13, 4, 3)),
        ("2018-07-06 05:04:03", timezone.datetime(2018, 7, 6, 5, 4, 3)),
    ],
    ids=["UTC", "EDT", "PST", "naive"],
)
def test_create_dagrun(session, admin_client, post_date, expected):
    data = {
        "state": "running",
        "dag_id": "example_bash_operator",
        "execution_date": post_date,
        "run_id": "test_create_dagrun",
    }
    resp = admin_client.post('/dagrun/add', data=data, follow_redirects=True)
    check_content_in_response('Added Row', resp)

    dr = session.query(DagRun).one()

    assert dr.execution_date == expected


@conf_vars({("core", "default_timezone"): "America/Toronto"})
def test_create_dagrun_without_timezone_default(session, admin_client):
    data = {
        "state": "running",
        "dag_id": "example_bash_operator",
        "execution_date": "2018-07-06 05:04:03",
        "run_id": "test_create_dagrun",
    }
    resp = admin_client.post('/dagrun/add', data=data, follow_redirects=True)
    check_content_in_response('Added Row', resp)

    dr = session.query(DagRun).one()

    assert dr.execution_date == timezone.datetime(2018, 7, 6, 9, 4, 3)


def test_create_dagrun_valid_conf(session, admin_client):
    conf_value = dict(Valid=True)
    data = {
        "state": "running",
        "dag_id": "example_bash_operator",
        "execution_date": "2018-07-06 05:05:03-02:00",
        "run_id": "test_create_dagrun_valid_conf",
        "conf": json.dumps(conf_value),
    }

    resp = admin_client.post('/dagrun/add', data=data, follow_redirects=True)
    check_content_in_response('Added Row', resp)
    dr = session.query(DagRun).one()
    assert dr.conf == conf_value


def test_create_dagrun_invalid_conf(session, admin_client):
    data = {
        "state": "running",
        "dag_id": "example_bash_operator",
        "execution_date": "2018-07-06 05:06:03",
        "run_id": "test_create_dagrun_invalid_conf",
        "conf": "INVALID: [JSON",
    }

    resp = admin_client.post('/dagrun/add', data=data, follow_redirects=True)
    check_content_in_response('JSON Validation Error:', resp)
    dr = session.query(DagRun).all()
    assert not dr


def test_list_dagrun_includes_conf(session, admin_client):
    data = {
        "state": "running",
        "dag_id": "example_bash_operator",
        "execution_date": "2018-07-06 05:06:03",
        "run_id": "test_list_dagrun_includes_conf",
        "conf": '{"include": "me"}',
    }
    admin_client.post('/dagrun/add', data=data, follow_redirects=True)
    dr = session.query(DagRun).one()

    expect_date = timezone.convert_to_utc(timezone.datetime(2018, 7, 6, 5, 6, 3))
    assert dr.execution_date == expect_date
    assert dr.conf == {"include": "me"}

    resp = admin_client.get('/dagrun/list', follow_redirects=True)
    check_content_in_response("{&#34;include&#34;: &#34;me&#34;}", resp)


def test_clear_dag_runs_action(session, admin_client):
    dag = DagBag().get_dag("example_bash_operator")
    task0 = dag.get_task("runme_0")
    task1 = dag.get_task("runme_1")
    execution_date = timezone.datetime(2016, 1, 9)
    tis = [
        TaskInstance(task0, execution_date, state="success"),
        TaskInstance(task1, execution_date, state="failed"),
    ]
    session.bulk_save_objects(tis)
    dr = dag.create_dagrun(
        state="running",
        execution_date=execution_date,
        run_id="test_clear_dag_runs_action",
        session=session,
    )

    data = {"action": "clear", "rowid": [dr.id]}
    resp = admin_client.post("/dagrun/action_post", data=data, follow_redirects=True)
    check_content_in_response("1 dag runs and 2 task instances were cleared", resp)
    assert [ti.state for ti in session.query(TaskInstance).all()] == [None, None]


def test_clear_dag_runs_action_fails(admin_client):
    data = {"action": "clear", "rowid": ["0"]}
    resp = admin_client.post("/dagrun/action_post", data=data, follow_redirects=True)
    check_content_in_response("Failed to clear state", resp)
