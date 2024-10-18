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

import datetime
import json
from decimal import Decimal
from urllib.parse import quote

import pytest

from airflow.models import DagBag, DagRun
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.security import permissions
from airflow.utils import timezone
from airflow.utils.json import WebEncoder
from airflow.utils.session import create_session
from airflow.utils.types import DagRunType

from providers.tests.fab.auth_manager.api_endpoints.api_connexion_utils import create_test_client
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.www import check_content_in_response

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def _initialize_one_dag():
    with create_session() as session:
        DagBag().get_dag("example_bash_operator").sync_to_db(session=session)
    yield
    with create_session() as session:
        session.query(DagRun).delete()


def test_trigger_dag_button_normal_exist(admin_client):
    resp = admin_client.get("/", follow_redirects=True)
    assert "/dags/example_bash_operator/trigger" in resp.data.decode("utf-8")
    assert "return confirmDeleteDag(this, 'example_bash_operator')" in resp.data.decode("utf-8")


# test trigger button with and without run_id
@pytest.mark.parametrize(
    "req , expected_run_id", [("", DagRunType.MANUAL), ("&run_id=test_run_id", "test_run_id")]
)
def test_trigger_dag_button(admin_client, req, expected_run_id):
    test_dag_id = "example_bash_operator"
    admin_client.post(f"dags/{test_dag_id}/trigger?{req}", data={"conf": "{}"})
    with create_session() as session:
        run = session.query(DagRun).filter(DagRun.dag_id == test_dag_id).first()
    assert run is not None
    assert run.run_type == DagRunType.MANUAL
    assert expected_run_id in run.run_id


def test_duplicate_run_id(admin_client):
    test_dag_id = "example_bash_operator"
    run_id = "test_run"
    admin_client.post(
        f"dags/{test_dag_id}/trigger?run_id={run_id}", data={"conf": "{}"}, follow_redirects=True
    )
    response = admin_client.post(
        f"dags/{test_dag_id}/trigger?run_id={run_id}", data={"conf": "{}"}, follow_redirects=True
    )
    check_content_in_response(f"The run ID {run_id} already exists", response)


def test_trigger_dag_conf(admin_client):
    test_dag_id = "example_bash_operator"
    conf_dict = {"string": "Hello, World!"}

    admin_client.post(f"dags/{test_dag_id}/trigger", data={"conf": json.dumps(conf_dict)})

    with create_session() as session:
        run = session.query(DagRun).filter(DagRun.dag_id == test_dag_id).first()
    assert run is not None
    assert DagRunType.MANUAL in run.run_id
    assert run.run_type == DagRunType.MANUAL
    assert run.conf == conf_dict


def test_trigger_dag_conf_serializable_fields(admin_client):
    test_dag_id = "example_bash_operator"
    time_now = timezone.utcnow()
    conf_dict = {
        "string": "Hello, World!",
        "date_str": "2024-08-08T09:57:35.300858",
        "datetime": time_now,
        "decimal": Decimal(10.465),
    }
    expected_conf = {
        "string": "Hello, World!",
        "date_str": "2024-08-08T09:57:35.300858",
        "datetime": time_now.isoformat(),
        "decimal": 10.465,
    }

    admin_client.post(f"dags/{test_dag_id}/trigger", data={"conf": json.dumps(conf_dict, cls=WebEncoder)})

    with create_session() as session:
        run = session.query(DagRun).filter(DagRun.dag_id == test_dag_id).first()
    assert run is not None
    assert DagRunType.MANUAL in run.run_id
    assert run.run_type == DagRunType.MANUAL
    assert run.conf == expected_conf


def test_trigger_dag_conf_malformed(admin_client):
    test_dag_id = "example_bash_operator"

    response = admin_client.post(f"dags/{test_dag_id}/trigger", data={"conf": '{"a": "b"'})
    check_content_in_response("Invalid JSON configuration", response)

    with create_session() as session:
        run = session.query(DagRun).filter(DagRun.dag_id == test_dag_id).first()
    assert run is None


def test_trigger_dag_conf_not_dict(admin_client):
    test_dag_id = "example_bash_operator"

    response = admin_client.post(f"dags/{test_dag_id}/trigger", data={"conf": "string and not a dict"})
    check_content_in_response("Invalid JSON configuration", response)

    with create_session() as session:
        run = session.query(DagRun).filter(DagRun.dag_id == test_dag_id).first()
    assert run is None


def test_trigger_dag_wrong_execution_date(admin_client):
    test_dag_id = "example_bash_operator"

    response = admin_client.post(
        f"dags/{test_dag_id}/trigger", data={"conf": "{}", "execution_date": "not_a_date"}
    )
    check_content_in_response("Invalid execution date", response)

    with create_session() as session:
        run = session.query(DagRun).filter(DagRun.dag_id == test_dag_id).first()
    assert run is None


def test_trigger_dag_execution_date_data_interval(admin_client):
    test_dag_id = "example_bash_operator"
    exec_date = timezone.utcnow()

    admin_client.post(
        f"dags/{test_dag_id}/trigger", data={"conf": "{}", "execution_date": exec_date.isoformat()}
    )

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
    resp = admin_client.get(f"dags/{test_dag_id}/trigger")
    check_content_in_response("Trigger DAG: <a href", resp)
    check_content_in_response(f">{test_dag_id}</a>", resp)


@pytest.mark.parametrize(
    "test_origin, expected_origin",
    [
        ("javascript:alert(1)", "/home"),
        ("http://google.com", "/home"),
        ("36539'%3balert(1)%2f%2f166", "/home"),
        (
            '"><script>alert(99)</script><a href="',
            "http://localhost/&#34;&gt;&lt;script&gt;alert(99)&lt;/script&gt;&lt;a href=&#34;",
        ),
        (
            "%2Ftree%3Fdag_id%3Dexample_bash_operator';alert(33)//",
            "/home",
        ),
        ("%2Ftree%3Fdag_id%3Dexample_bash_operator", "http://localhost/tree?dag_id=example_bash_operator"),
        ("%2Fgraph%3Fdag_id%3Dexample_bash_operator", "http://localhost/graph?dag_id=example_bash_operator"),
    ],
)
def test_trigger_dag_form_origin_url(admin_client, test_origin, expected_origin):
    test_dag_id = "example_bash_operator"

    resp = admin_client.get(f"dags/{test_dag_id}/trigger?origin={test_origin}")
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
        resp = admin_client.get(f"dags/{test_dag_id}/trigger")
    else:
        test_request_conf = json.dumps(request_conf, indent=4)
        resp = admin_client.get(f"dags/{test_dag_id}/trigger?conf={test_request_conf}&doc_md={doc_md}")
    for key in expected_conf.keys():
        check_content_in_response(key, resp)
        check_content_in_response(str(expected_conf[key]), resp)


def test_trigger_dag_params_render(admin_client, dag_maker, session, app, monkeypatch):
    """
    Test that textarea in Trigger DAG UI is pre-populated
    with param value set in DAG.
    """
    account = {"name": "account_name_1", "country": "usa"}
    expected_conf = {"accounts": [account]}
    expected_dag_conf = json.dumps(expected_conf, indent=4).replace('"', "&#34;")
    DAG_ID = "params_dag"
    param = Param(
        [account],
        schema={
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "default": account,
                "properties": {"name": {"type": "string"}, "country": {"type": "string"}},
                "required": ["name", "country"],
            },
        },
    )
    with monkeypatch.context() as m:
        with dag_maker(dag_id=DAG_ID, serialized=True, session=session, params={"accounts": param}):
            EmptyOperator(task_id="task1")

        m.setattr(app, "dag_bag", dag_maker.dagbag)
        resp = admin_client.get(f"dags/{DAG_ID}/trigger")

    check_content_in_response(
        f'<textarea style="display: none;" id="json_start" name="json_start">{expected_dag_conf}</textarea>',
        resp,
    )


def test_trigger_endpoint_uses_existing_dagbag(admin_client):
    """
    Test that Trigger Endpoint uses the DagBag already created in views.py
    instead of creating a new one.
    """
    url = "dags/example_bash_operator/trigger"
    resp = admin_client.post(url, data={}, follow_redirects=True)
    check_content_in_response("example_bash_operator", resp)


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
        url = "dags/example_bash_operator/trigger"
        resp = client.get(url, follow_redirects=True)
        response_data = resp.data.decode()
        assert "Access is Denied" in response_data


def test_trigger_dag_params_array_value_none_render(admin_client, dag_maker, session, app, monkeypatch):
    """
    Test that textarea in Trigger DAG UI is pre-populated
    with param value None and type ["null", "array"] set in DAG.
    """
    expected_conf = {"dag_param": None}
    expected_dag_conf = json.dumps(expected_conf, indent=4).replace('"', "&#34;")
    DAG_ID = "params_dag"
    param = Param(
        None,
        type=["null", "array"],
        minItems=0,
    )
    with monkeypatch.context() as m:
        with dag_maker(dag_id=DAG_ID, serialized=True, session=session, params={"dag_param": param}):
            EmptyOperator(task_id="task1")

        m.setattr(app, "dag_bag", dag_maker.dagbag)
        resp = admin_client.get(f"dags/{DAG_ID}/trigger")

    check_content_in_response(
        f'<textarea style="display: none;" id="json_start" name="json_start">{expected_dag_conf}</textarea>',
        resp,
    )


@pytest.mark.parametrize(
    "pattern, run_id, result",
    [
        ["^[A-Z]", "ABC", True],
        ["^[A-Z]", "abc", False],
        ["^[0-9]", "123", True],
        # The below params tests that user configuration does not affect internally generated
        # run_ids. We use manual__ as a prefix for manually triggered DAGs due to a restriction
        # in manually triggered DAGs that the run_id must not start with scheduled__.
        ["", "manual__2023-01-01T00:00:00+00:00", True],
        ["", "scheduled_2023-01-01T00", False],
        ["", "manual_2023-01-01T00", False],
        ["", "dataset_triggered_2023-01-01T00", False],
        ["^[0-9]", "manual__2023-01-01T00:00:00+00:00", True],
        ["^[a-z]", "manual__2023-01-01T00:00:00+00:00", True],
    ],
)
def test_dag_run_id_pattern(session, admin_client, pattern, run_id, result):
    with conf_vars({("scheduler", "allowed_run_id_pattern"): pattern}):
        test_dag_id = "example_bash_operator"
        run_id = quote(run_id)
        admin_client.post(f"dags/{test_dag_id}/trigger?run_id={run_id}", data={"conf": "{}"})
        run = session.query(DagRun).filter(DagRun.dag_id == test_dag_id).first()
        if result:
            assert run is not None
            assert run.run_type == DagRunType.MANUAL
        else:
            assert run is None
