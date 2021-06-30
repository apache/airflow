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


@pytest.fixture()
def running_dag_run(session):
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
        run_id="test_dag_runs_action",
        session=session,
    )
    return dr


@pytest.mark.parametrize(
    "action, expected_ti_states, expected_message",
    [
        (
            "clear",
            [None, None],
            "1 dag runs and 2 task instances were cleared",
        ),
        (
            "set_success",
            ["success", "success"],
            "1 dag runs and 1 task instances were set to success",
        ),
        (
            "set_failed",
            ["success", "failed"],  # The success ti is not set to failed.
            "1 dag runs and 0 task instances were set to failed",
        ),
        (
            "set_running",
            ["success", "failed"],  # Unchanged.
            "1 dag runs were set to running",
        ),
    ],
    ids=["clear", "success", "failed", "running"],
)
def test_set_dag_runs_action(
    session,
    admin_client,
    running_dag_run,
    action,
    expected_ti_states,
    expected_message,
):
    resp = admin_client.post(
        "/dagrun/action_post",
        data={"action": action, "rowid": [running_dag_run.id]},
        follow_redirects=True,
    )
    check_content_in_response(expected_message, resp)
    assert [ti.state for ti in session.query(TaskInstance).all()] == expected_ti_states


@pytest.mark.parametrize(
    "action, expected_message",
    [
        ("clear", "Failed to clear state"),
        ("set_success", "Failed to set state"),
        ("set_failed", "Failed to set state"),
        ("set_running", "Failed to set state"),
    ],
    ids=["clear", "success", "failed", "running"],
)
def test_set_dag_runs_action_fails(admin_client, action, expected_message):
    resp = admin_client.post(
        "/dagrun/action_post",
        data={"action": action, "rowid": ["0"]},
        follow_redirects=True,
    )
    check_content_in_response(expected_message, resp)


def test_muldelete_dag_runs_action(session, admin_client, running_dag_run):
    dag_run_id = running_dag_run.id
    resp = admin_client.post(
        "/dagrun/action_post",
        data={"action": "muldelete", "rowid": [dag_run_id]},
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert session.query(TaskInstance).count() == 2  # Does not delete TIs.
    assert session.query(DagRun).filter(DagRun.id == dag_run_id).count() == 0
