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
import datetime
import json

import pytest

from airflow.models import DagBag, DagRun
from airflow.security import permissions
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.types import DagRunType
from tests.test_utils.api_connexion_utils import create_test_client
from tests.test_utils.www import check_content_in_response


@pytest.fixture(scope="function", autouse=True)
def initialize_one_dag():
    with create_session() as session:
        DagBag().get_dag("example_bash_operator").sync_to_db(session=session)
    yield
    with create_session() as session:
        session.query(DagRun).delete()


def test_trigger_dag_button_normal_exist(admin_client):
    resp = admin_client.get('/', follow_redirects=True)
    assert '/trigger?dag_id=example_bash_operator' in resp.data.decode('utf-8')
    assert "return confirmDeleteDag(this, 'example_bash_operator')" in resp.data.decode('utf-8')


# test trigger button with and without run_id
@pytest.mark.parametrize(
    "req , expected_run_id", [('', DagRunType.MANUAL), ('&run_id=test_run_id', 'test_run_id')]
)
def test_trigger_dag_button(admin_client, req, expected_run_id):
    test_dag_id = 'example_bash_operator'
    admin_client.post(f'trigger?dag_id={test_dag_id}{req}')
    with create_session() as session:
        run = session.query(DagRun).filter(DagRun.dag_id == test_dag_id).first()
    assert run is not None
    assert run.run_type == DagRunType.MANUAL
    assert expected_run_id in run.run_id


def test_duplicate_run_id(admin_client):
    test_dag_id = 'example_bash_operator'
    run_id = 'test_run'
    admin_client.post(f'trigger?dag_id={test_dag_id}&run_id={run_id}', follow_redirects=True)
    response = admin_client.post(f'trigger?dag_id={test_dag_id}&run_id={run_id}', follow_redirects=True)
    check_content_in_response(f'The run ID {run_id} already exists', response)


def test_trigger_dag_conf(admin_client):
    test_dag_id = "example_bash_operator"
    conf_dict = {'string': 'Hello, World!'}

    admin_client.post(f'trigger?dag_id={test_dag_id}', data={'conf': json.dumps(conf_dict)})

    with create_session() as session:
        run = session.query(DagRun).filter(DagRun.dag_id == test_dag_id).first()
    assert run is not None
    assert DagRunType.MANUAL in run.run_id
    assert run.run_type == DagRunType.MANUAL
    assert run.conf == conf_dict


def test_trigger_dag_conf_malformed(admin_client):
    test_dag_id = "example_bash_operator"

    response = admin_client.post(f'trigger?dag_id={test_dag_id}', data={'conf': '{"a": "b"'})
    check_content_in_response('Invalid JSON configuration', response)

    with create_session() as session:
        run = session.query(DagRun).filter(DagRun.dag_id == test_dag_id).first()
    assert run is None


def test_trigger_dag_conf_not_dict(admin_client):
    test_dag_id = "example_bash_operator"

    response = admin_client.post(f'trigger?dag_id={test_dag_id}', data={'conf': 'string and not a dict'})
    check_content_in_response('must be a dict', response)

    with create_session() as session:
        run = session.query(DagRun).filter(DagRun.dag_id == test_dag_id).first()
    assert run is None


def test_trigger_dag_wrong_execution_date(admin_client):
    test_dag_id = "example_bash_operator"

    response = admin_client.post(f'trigger?dag_id={test_dag_id}', data={'execution_date': "not_a_date"})
    check_content_in_response("Invalid execution date", response)

    with create_session() as session:
        run = session.query(DagRun).filter(DagRun.dag_id == test_dag_id).first()
    assert run is None


def test_trigger_dag_execution_date_data_interval(admin_client):
    test_dag_id = "example_bash_operator"
    exec_date = timezone.utcnow()

    admin_client.post(f'trigger?dag_id={test_dag_id}', data={'execution_date': exec_date.isoformat()})

    with create_session() as session:
        run = session.query(DagRun).filter(DagRun.dag_id == test_dag_id).first()
    assert run is not None
    assert DagRunType.MANUAL in run.run_id
    assert run.run_type == DagRunType.MANUAL
    assert run.execution_date == exec_date

    # Since example_bash_operator runs once per day, the data interval should be
    # between midnight yesterday and midnight today.
    today_midnight = exec_date.replace(hour=0, minute=0, second=0, microsecond=0)
    assert run.data_interval_start == (today_midnight - datetime.timedelta(days=1))
    assert run.data_interval_end == today_midnight


def test_trigger_dag_form(admin_client):
    test_dag_id = "example_bash_operator"
    resp = admin_client.get(f'trigger?dag_id={test_dag_id}')
    check_content_in_response(f'Trigger DAG: {test_dag_id}', resp)


@pytest.mark.parametrize(
    "test_origin, expected_origin",
    [
        ("javascript:alert(1)", "/home"),
        ("http://google.com", "/home"),
        ("36539'%3balert(1)%2f%2f166", "/home"),
        (
            '"><script>alert(99)</script><a href="',
            "&#34;&gt;&lt;script&gt;alert(99)&lt;/script&gt;&lt;a href=&#34;",
        ),
        (
            "%2Ftree%3Fdag_id%3Dexample_bash_operator';alert(33)//",
            "/home",
        ),
        ("%2Ftree%3Fdag_id%3Dexample_bash_operator", "/tree?dag_id=example_bash_operator"),
        ("%2Fgraph%3Fdag_id%3Dexample_bash_operator", "/graph?dag_id=example_bash_operator"),
    ],
)
def test_trigger_dag_form_origin_url(admin_client, test_origin, expected_origin):
    test_dag_id = "example_bash_operator"

    resp = admin_client.get(f'trigger?dag_id={test_dag_id}&origin={test_origin}')
    check_content_in_response(f'<a class="btn" href="{expected_origin}">Cancel</a>', resp)


@pytest.mark.parametrize(
    "request_conf, expected_conf",
    [
        (None, {"example_key": "example_value"}),
        ({"other": "test_data", "key": 12}, {"other": "test_data", "key": 12}),
    ],
)
def test_trigger_dag_params_conf(admin_client, request_conf, expected_conf):
    """
    Test that textarea in Trigger DAG UI is pre-populated
    with json config when the conf URL parameter is passed,
    or if a params dict is passed in the DAG

        1. Conf is not included in URL parameters -> DAG.conf is in textarea
        2. Conf is passed as a URL parameter -> passed conf json is in textarea
    """
    test_dag_id = "example_bash_operator"
    doc_md = "Example Bash Operator"

    if not request_conf:
        resp = admin_client.get(f'trigger?dag_id={test_dag_id}')
    else:
        test_request_conf = json.dumps(request_conf, indent=4)
        resp = admin_client.get(f'trigger?dag_id={test_dag_id}&conf={test_request_conf}&doc_md={doc_md}')

    expected_dag_conf = json.dumps(expected_conf, indent=4).replace("\"", "&#34;")

    check_content_in_response(
        f'<textarea class="form-control" name="conf" id="json">{expected_dag_conf}</textarea>',
        resp,
    )


def test_trigger_endpoint_uses_existing_dagbag(admin_client):
    """
    Test that Trigger Endpoint uses the DagBag already created in views.py
    instead of creating a new one.
    """
    url = 'trigger?dag_id=example_bash_operator'
    resp = admin_client.post(url, data={}, follow_redirects=True)
    check_content_in_response('example_bash_operator', resp)


def test_viewer_cant_trigger_dag(app):
    """
    Test that the test_viewer user can't trigger DAGs.
    """
    with create_test_client(
        app,
        user_name="test_user",
        role_name="test_role",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_DAG_RUN),
        ],
    ) as client:
        url = 'trigger?dag_id=example_bash_operator'
        resp = client.get(url, follow_redirects=True)
        response_data = resp.data.decode()
        assert "Access is Denied" in response_data
