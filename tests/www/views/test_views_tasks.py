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

import html
import json
import unittest.mock
import urllib.parse
from getpass import getuser

import pendulum
import pytest
import time_machine

from airflow import settings
from airflow.models.dag import DAG, DagModel
from airflow.models.dagbag import DagBag
from airflow.models.dagcode import DagCode
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskreschedule import TaskReschedule
from airflow.models.xcom import XCom
from airflow.operators.empty import EmptyOperator
from airflow.providers.celery.executors.celery_executor import CeleryExecutor
from airflow.security import permissions
from airflow.utils import timezone
from airflow.utils.log.logging_mixin import ExternalLoggingMixin
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, State
from airflow.utils.types import DagRunType
from airflow.www.views import TaskInstanceModelView, _safe_parse_datetime
from tests_common.test_utils.compat import AIRFLOW_V_3_0_PLUS, BashOperator
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_runs, clear_db_xcom
from tests_common.test_utils.www import (
    check_content_in_response,
    check_content_not_in_response,
    client_with_login,
)

from providers.tests.fab.auth_manager.api_endpoints.api_connexion_utils import (
    create_user,
    delete_roles,
    delete_user,
)

if AIRFLOW_V_3_0_PLUS:
    from airflow.utils.types import DagRunTriggeredByType

pytestmark = pytest.mark.db_test

DEFAULT_DATE = timezone.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
STR_DEFAULT_DATE = urllib.parse.quote(DEFAULT_DATE.strftime("%Y-%m-%dT%H:%M:%S.%f%z"))

DEFAULT_VAL = urllib.parse.quote_plus(str(DEFAULT_DATE))

DEFAULT_DAGRUN = "TEST_DAGRUN"


@pytest.fixture(scope="module", autouse=True)
def _reset_dagruns():
    """Clean up stray garbage from other tests."""
    clear_db_runs()


@pytest.fixture(autouse=True)
def _init_dagruns(app):
    with time_machine.travel(DEFAULT_DATE, tick=False):
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        app.dag_bag.get_dag("example_bash_operator").create_dagrun(
            run_id=DEFAULT_DAGRUN,
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            start_date=timezone.utcnow(),
            state=State.RUNNING,
            **triggered_by_kwargs,
        )
        app.dag_bag.get_dag("example_python_operator").create_dagrun(
            run_id=DEFAULT_DAGRUN,
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            start_date=timezone.utcnow(),
            state=State.RUNNING,
            **triggered_by_kwargs,
        )
        XCom.set(
            key="return_value",
            value="{'x':1}",
            task_id="runme_0",
            dag_id="example_bash_operator",
            run_id=DEFAULT_DAGRUN,
        )
        app.dag_bag.get_dag("example_xcom").create_dagrun(
            run_id=DEFAULT_DAGRUN,
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            start_date=timezone.utcnow(),
            state=State.RUNNING,
            **triggered_by_kwargs,
        )
        app.dag_bag.get_dag("latest_only").create_dagrun(
            run_id=DEFAULT_DAGRUN,
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            start_date=timezone.utcnow(),
            state=State.RUNNING,
            **triggered_by_kwargs,
        )
        app.dag_bag.get_dag("example_task_group").create_dagrun(
            run_id=DEFAULT_DAGRUN,
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            start_date=timezone.utcnow(),
            state=State.RUNNING,
            **triggered_by_kwargs,
        )
    yield
    clear_db_runs()
    clear_db_xcom()


@pytest.fixture(scope="module")
def client_ti_without_dag_edit(app):
    create_user(
        app,
        username="all_ti_permissions_except_dag_edit",
        role_name="all_ti_permissions_except_dag_edit",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_DAG_RUN),
        ],
    )

    yield client_with_login(
        app,
        username="all_ti_permissions_except_dag_edit",
        password="all_ti_permissions_except_dag_edit",
    )

    delete_user(app, username="all_ti_permissions_except_dag_edit")  # type: ignore
    delete_roles(app)


@pytest.mark.parametrize(
    "url, contents",
    [
        pytest.param(
            "/",
            [
                "/delete?dag_id=example_bash_operator",
                "return confirmDeleteDag(this, 'example_bash_operator')",
            ],
            id="delete-dag-button-normal",
        ),
        pytest.param(
            f"task?task_id=runme_0&dag_id=example_bash_operator&execution_date={DEFAULT_VAL}",
            ["Grid"],
            id="task-grid-button",
        ),
        pytest.param(
            f"task?task_id=runme_0&dag_id=example_bash_operator&execution_date={DEFAULT_VAL}",
            ["Task Instance Details"],
            id="task",
        ),
        pytest.param(
            f"log?task_id=runme_0&dag_id=example_bash_operator&execution_date={DEFAULT_VAL}",
            ["Grid"],
            id="log-grid-button",
        ),
        pytest.param(
            f"xcom?task_id=runme_0&dag_id=example_bash_operator&execution_date={DEFAULT_VAL}",
            ["XCom"],
            id="xcom",
        ),
        pytest.param(
            f"xcom?task_id=runme_0&dag_id=example_bash_operator&execution_date={DEFAULT_VAL}",
            ["Grid"],
            id="xcom-grid-button",
        ),
        pytest.param("xcom/list", ["List XComs"], id="xcom-list"),
        pytest.param(
            f"rendered-templates?task_id=runme_0&dag_id=example_bash_operator&execution_date={DEFAULT_VAL}",
            ["Rendered Template"],
            id="rendered-templates",
        ),
        pytest.param(
            f"rendered-templates?task_id=runme_0&dag_id=example_bash_operator&execution_date={DEFAULT_VAL}",
            ["Grid"],
            id="rendered-templates-grid-button",
        ),
        pytest.param(
            "object/graph_data?dag_id=example_bash_operator",
            ["runme_1"],
            id="graph-data",
        ),
        pytest.param(
            "object/grid_data?dag_id=example_bash_operator",
            ["runme_1"],
            id="grid-data",
        ),
        pytest.param(
            "duration?days=30&dag_id=example_bash_operator",
            ["example_bash_operator"],
            id="duration-url-param",
        ),
        pytest.param(
            "dags/example_bash_operator/duration?days=30",
            ["example_bash_operator"],
            id="duration",
        ),
        pytest.param(
            "duration?days=30&dag_id=missing_dag",
            ["seems to be missing"],
            id="duration-missing-url-param",
        ),
        pytest.param(
            "dags/missing_dag/duration?days=30",
            ["seems to be missing"],
            id="duration-missing",
        ),
        pytest.param(
            "tries?days=30&dag_id=example_bash_operator",
            ["example_bash_operator"],
            id="tries-url-param",
        ),
        pytest.param(
            "dags/example_bash_operator/tries?days=30",
            ["example_bash_operator"],
            id="tries",
        ),
        pytest.param(
            "landing_times?days=30&dag_id=example_bash_operator",
            ["example_bash_operator"],
            id="landing-times-url-param",
        ),
        pytest.param(
            "dags/example_bash_operator/landing-times?days=30",
            ["example_bash_operator"],
            id="landing-times",
        ),
        pytest.param(
            "gantt?dag_id=example_bash_operator",
            ["example_bash_operator"],
            id="gantt-url-param",
        ),
        pytest.param(
            "dags/example_bash_operator/gantt",
            ["example_bash_operator"],
            id="gantt",
        ),
        pytest.param(
            "dag-dependencies",
            ["child_task1", "test_trigger_dagrun"],
            id="dag-dependencies",
        ),
        # Test that Graph, Tree, Calendar & Dag Details View uses the DagBag
        # already created in views.py
        pytest.param(
            "graph?dag_id=example_bash_operator",
            ["example_bash_operator"],
            id="existing-dagbag-graph-url-param",
        ),
        pytest.param(
            "dags/example_bash_operator/graph",
            ["example_bash_operator"],
            id="existing-dagbag-graph",
        ),
        pytest.param(
            "tree?dag_id=example_bash_operator",
            ["example_bash_operator"],
            id="existing-dagbag-tree-url-param",
        ),
        pytest.param(
            "dags/example_bash_operator/grid",
            ["example_bash_operator"],
            id="existing-dagbag-grid",
        ),
        pytest.param(
            "calendar?dag_id=example_bash_operator",
            ["example_bash_operator"],
            id="existing-dagbag-calendar-url-param",
        ),
        pytest.param(
            "dags/example_bash_operator/calendar",
            ["example_bash_operator"],
            id="existing-dagbag-calendar",
        ),
        pytest.param(
            "dags/latest_only/calendar",
            ["latest_only"],
            id="existing-dagbag-non-cron-schedule-calendar",
        ),
        pytest.param(
            "dag_details?dag_id=example_bash_operator",
            ["example_bash_operator"],
            id="existing-dagbag-dag-details-url-param",
        ),
        pytest.param(
            "dags/example_bash_operator/details",
            ["example_bash_operator"],
            id="existing-dagbag-dag-details",
        ),
        pytest.param(
            f"confirm?task_id=runme_0&dag_id=example_bash_operator&state=success"
            f"&dag_run_id={DEFAULT_DAGRUN}",
            ["Please confirm"],
            id="confirm-success",
        ),
        pytest.param(
            f"confirm?task_id=runme_0&dag_id=example_bash_operator&state=failed&dag_run_id={DEFAULT_DAGRUN}",
            ["Please confirm"],
            id="confirm-failed",
        ),
        pytest.param(
            f"confirm?task_id=runme_0&dag_id=invalid_dag&state=failed&dag_run_id={DEFAULT_DAGRUN}",
            ["DAG invalid_dag not found"],
            id="confirm-failed",
        ),
        pytest.param(
            f"confirm?task_id=invalid_task&dag_id=example_bash_operator&state=failed"
            f"&dag_run_id={DEFAULT_DAGRUN}",
            ["Task invalid_task not found"],
            id="confirm-failed",
        ),
        pytest.param(
            f"confirm?task_id=runme_0&dag_id=example_bash_operator&state=invalid"
            f"&dag_run_id={DEFAULT_DAGRUN}",
            ["Invalid state invalid, must be either &#39;success&#39; or &#39;failed&#39;"],
            id="confirm-invalid",
        ),
    ],
)
def test_views_get(admin_client, url, contents):
    resp = admin_client.get(url, follow_redirects=True)
    for content in contents:
        check_content_in_response(content, resp)


def test_xcom_return_value_is_not_bytes(admin_client):
    url = f"xcom?dag_id=example_bash_operator&task_id=runme_0&execution_date={DEFAULT_VAL}&map_index=-1"
    resp = admin_client.get(url, follow_redirects=True)
    # check that {"x":1} is in the response
    content = "{&#39;x&#39;:1}"
    check_content_in_response(content, resp)
    # check that b'{"x":1}' is not in the response
    content = "b&#39;&#34;{\\&#39;x\\&#39;:1}&#34;&#39;"
    check_content_not_in_response(content, resp)


def test_rendered_task_view(admin_client):
    url = f"task?task_id=runme_0&dag_id=example_bash_operator&execution_date={DEFAULT_VAL}"
    resp = admin_client.get(url, follow_redirects=True)
    resp_html = resp.data.decode("utf-8")
    assert resp.status_code == 200
    assert "<td>_try_number</td>" not in resp_html
    assert "<td>try_number</td>" in resp_html


def test_rendered_k8s(admin_client):
    url = f"rendered-k8s?task_id=runme_0&dag_id=example_bash_operator&execution_date={DEFAULT_VAL}"
    with unittest.mock.patch.object(settings, "IS_K8S_OR_K8SCELERY_EXECUTOR", True):
        resp = admin_client.get(url, follow_redirects=True)
        check_content_in_response("K8s Pod Spec", resp)


@conf_vars({("core", "executor"): "LocalExecutor"})
def test_rendered_k8s_without_k8s(admin_client):
    url = f"rendered-k8s?task_id=runme_0&dag_id=example_bash_operator&execution_date={DEFAULT_VAL}"
    resp = admin_client.get(url, follow_redirects=True)
    assert 404 == resp.status_code


def test_tree_trigger_origin_tree_view(app, admin_client):
    triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
    app.dag_bag.get_dag("test_tree_view").create_dagrun(
        run_type=DagRunType.SCHEDULED,
        execution_date=DEFAULT_DATE,
        data_interval=(DEFAULT_DATE, DEFAULT_DATE),
        start_date=timezone.utcnow(),
        state=State.RUNNING,
        **triggered_by_kwargs,
    )

    url = "tree?dag_id=test_tree_view"
    resp = admin_client.get(url, follow_redirects=True)
    params = {"origin": "/dags/test_tree_view/grid"}
    href = f"/dags/test_tree_view/trigger?{html.escape(urllib.parse.urlencode(params))}"
    check_content_in_response(href, resp)


def test_graph_trigger_origin_grid_view(app, admin_client):
    triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
    app.dag_bag.get_dag("test_tree_view").create_dagrun(
        run_type=DagRunType.SCHEDULED,
        execution_date=DEFAULT_DATE,
        data_interval=(DEFAULT_DATE, DEFAULT_DATE),
        start_date=timezone.utcnow(),
        state=State.RUNNING,
        **triggered_by_kwargs,
    )

    url = "/dags/test_tree_view/graph"
    resp = admin_client.get(url, follow_redirects=True)
    params = {"origin": "/dags/test_tree_view/grid?tab=graph"}
    href = f"/dags/test_tree_view/trigger?{html.escape(urllib.parse.urlencode(params))}"
    check_content_in_response(href, resp)


def test_gantt_trigger_origin_grid_view(app, admin_client):
    triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
    app.dag_bag.get_dag("test_tree_view").create_dagrun(
        run_type=DagRunType.SCHEDULED,
        execution_date=DEFAULT_DATE,
        data_interval=(DEFAULT_DATE, DEFAULT_DATE),
        start_date=timezone.utcnow(),
        state=State.RUNNING,
        **triggered_by_kwargs,
    )

    url = "/dags/test_tree_view/gantt"
    resp = admin_client.get(url, follow_redirects=True)
    params = {"origin": "/dags/test_tree_view/grid?tab=gantt"}
    href = f"/dags/test_tree_view/trigger?{html.escape(urllib.parse.urlencode(params))}"
    check_content_in_response(href, resp)


def test_graph_view_without_dag_permission(app, one_dag_perm_user_client):
    url = "/dags/example_bash_operator/graph"
    resp = one_dag_perm_user_client.get(url, follow_redirects=True)
    assert resp.status_code == 200
    assert (
        resp.request.url
        == "http://localhost/dags/example_bash_operator/grid?tab=graph&dag_run_id=TEST_DAGRUN"
    )
    check_content_in_response("example_bash_operator", resp)

    url = "/dags/example_xcom/graph"
    resp = one_dag_perm_user_client.get(url, follow_redirects=True)
    assert resp.status_code == 200
    assert resp.request.url == "http://localhost/home"
    check_content_in_response("Access is Denied", resp)


def test_last_dagruns(admin_client):
    resp = admin_client.post("last_dagruns", follow_redirects=True)
    check_content_in_response("example_bash_operator", resp)


def test_last_dagruns_success_when_selecting_dags(admin_client):
    resp = admin_client.post(
        "last_dagruns", data={"dag_ids": ["example_python_operator"]}, follow_redirects=True
    )
    assert resp.status_code == 200
    stats = json.loads(resp.data.decode("utf-8"))
    assert "example_bash_operator" not in stats
    assert "example_python_operator" in stats

    # Multiple
    resp = admin_client.post(
        "last_dagruns",
        data={"dag_ids": ["example_python_operator", "example_bash_operator"]},
        follow_redirects=True,
    )
    stats = json.loads(resp.data.decode("utf-8"))
    assert "example_bash_operator" in stats
    check_content_not_in_response("example_xcom", resp)


def test_code(admin_client):
    url = "code?dag_id=example_bash_operator"
    resp = admin_client.get(url, follow_redirects=True)
    check_content_not_in_response("Failed to load DAG file Code", resp)
    check_content_in_response("example_bash_operator", resp)


def test_code_from_db(admin_client):
    dag = DagBag(include_examples=True).get_dag("example_bash_operator")
    DagCode(dag.fileloc, DagCode._get_code_from_file(dag.fileloc)).sync_to_db()
    url = "code?dag_id=example_bash_operator"
    resp = admin_client.get(url, follow_redirects=True)
    check_content_not_in_response("Failed to load DAG file Code", resp)
    check_content_in_response("example_bash_operator", resp)


def test_code_from_db_all_example_dags(admin_client):
    dagbag = DagBag(include_examples=True)
    for dag in dagbag.dags.values():
        DagCode(dag.fileloc, DagCode._get_code_from_file(dag.fileloc)).sync_to_db()
    url = "code?dag_id=example_bash_operator"
    resp = admin_client.get(url, follow_redirects=True)
    check_content_not_in_response("Failed to load DAG file Code", resp)
    check_content_in_response("example_bash_operator", resp)


@pytest.mark.parametrize(
    "url, data, content",
    [
        ("paused?dag_id=example_bash_operator&is_paused=false", None, "OK"),
        (
            "failed",
            dict(
                task_id="run_this_last",
                dag_id="example_bash_operator",
                dag_run_id=DEFAULT_DAGRUN,
                upstream="false",
                downstream="false",
                future="false",
                past="false",
                origin="/graph?dag_id=example_bash_operator",
            ),
            "Marked failed on 1 task instances",
        ),
        (
            "success",
            dict(
                task_id="run_this_last",
                dag_id="example_bash_operator",
                dag_run_id=DEFAULT_DAGRUN,
                upstream="false",
                downstream="false",
                future="false",
                past="false",
                origin="/graph?dag_id=example_bash_operator",
            ),
            "Marked success on 1 task instances",
        ),
        (
            "clear",
            dict(
                task_id="runme_1",
                dag_id="example_bash_operator",
                execution_date=DEFAULT_DATE,
                upstream="false",
                downstream="false",
                future="false",
                past="false",
                only_failed="false",
            ),
            "example_bash_operator",
        ),
        (
            "clear",
            dict(
                group_id="section_1",
                dag_id="example_task_group",
                execution_date=DEFAULT_DATE,
                upstream="false",
                downstream="false",
                future="false",
                past="false",
                only_failed="false",
            ),
            "example_task_group",
        ),
    ],
    ids=[
        "paused",
        "failed-flash-hint",
        "success-flash-hint",
        "clear",
        "clear-task-group",
    ],
)
def test_views_post(admin_client, url, data, content):
    resp = admin_client.post(url, data=data, follow_redirects=True)
    check_content_in_response(content, resp)


@pytest.mark.parametrize("url", ["failed", "success"])
def test_dag_never_run(admin_client, url):
    dag_id = "example_bash_operator"
    form = dict(
        task_id="run_this_last",
        dag_id=dag_id,
        execution_date=DEFAULT_DATE,
        upstream="false",
        downstream="false",
        future="false",
        past="false",
        origin="/graph?dag_id=example_bash_operator",
    )
    clear_db_runs()
    resp = admin_client.post(url, data=form, follow_redirects=True)
    check_content_in_response(f"Cannot mark tasks as {url}, seem that DAG {dag_id} has never run", resp)


class _ForceHeartbeatCeleryExecutor(CeleryExecutor):
    def heartbeat(self):
        return True


@pytest.fixture
def new_id_example_bash_operator():
    dag_id = "example_bash_operator"
    test_dag_id = "non_existent_dag"
    with create_session() as session:
        dag_query = session.query(DagModel).filter(DagModel.dag_id == dag_id)
        dag_query.first().tags = []  # To avoid "FOREIGN KEY constraint" error)
    with create_session() as session:
        dag_query.update({"dag_id": test_dag_id})
    yield test_dag_id
    with create_session() as session:
        session.query(DagModel).filter(DagModel.dag_id == test_dag_id).update({"dag_id": dag_id})


def test_delete_dag_button_for_dag_on_scheduler_only(admin_client, new_id_example_bash_operator):
    # The delete-dag URL should be generated correctly
    test_dag_id = new_id_example_bash_operator
    resp = admin_client.get("/", follow_redirects=True)
    check_content_in_response(f"/delete?dag_id={test_dag_id}", resp)
    check_content_in_response(f"return confirmDeleteDag(this, '{test_dag_id}')", resp)


@pytest.fixture
def new_dag_to_delete():
    dag = DAG(
        "new_dag_to_delete", is_paused_upon_creation=True, schedule="0 * * * *", start_date=DEFAULT_DATE
    )
    session = settings.Session()
    dag.sync_to_db(session=session)
    return dag


@pytest.fixture
def per_dag_perm_user_client(app, new_dag_to_delete):
    sm = app.appbuilder.sm
    perm = f"{permissions.RESOURCE_DAG_PREFIX}{new_dag_to_delete.dag_id}"

    sm.create_permission(permissions.ACTION_CAN_DELETE, perm)

    create_user(
        app,
        username="test_user_per_dag_perms",
        role_name="User with some perms",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_DELETE, perm),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ],
    )

    sm.find_user(username="test_user_per_dag_perms")

    yield client_with_login(
        app,
        username="test_user_per_dag_perms",
        password="test_user_per_dag_perms",
    )

    delete_user(app, username="test_user_per_dag_perms")  # type: ignore
    delete_roles(app)


@pytest.fixture
def one_dag_perm_user_client(app):
    username = "test_user_one_dag_perm"
    dag_id = "example_bash_operator"
    sm = app.appbuilder.sm
    perm = f"{permissions.RESOURCE_DAG_PREFIX}{dag_id}"

    sm.create_permission(permissions.ACTION_CAN_READ, perm)

    create_user(
        app,
        username=username,
        role_name="User with permission to access only one dag",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_LOG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            (permissions.ACTION_CAN_READ, perm),
        ],
    )

    sm.find_user(username=username)

    yield client_with_login(
        app,
        username=username,
        password=username,
    )

    delete_user(app, username=username)  # type: ignore
    delete_roles(app)


def test_delete_just_dag_per_dag_permissions(new_dag_to_delete, per_dag_perm_user_client):
    resp = per_dag_perm_user_client.post(
        f"delete?dag_id={new_dag_to_delete.dag_id}&next=/home", follow_redirects=True
    )
    check_content_in_response(f"Deleting DAG with id {new_dag_to_delete.dag_id}.", resp)


def test_delete_just_dag_resource_permissions(new_dag_to_delete, user_client):
    resp = user_client.post(f"delete?dag_id={new_dag_to_delete.dag_id}&next=/home", follow_redirects=True)
    check_content_in_response(f"Deleting DAG with id {new_dag_to_delete.dag_id}.", resp)


@pytest.mark.parametrize("endpoint", ["graph", "tree"])
def test_show_external_log_redirect_link_with_local_log_handler(capture_templates, admin_client, endpoint):
    """Do not show external links if log handler is local."""
    url = f"{endpoint}?dag_id=example_bash_operator"
    with capture_templates() as templates:
        admin_client.get(url, follow_redirects=True)
        ctx = templates[0].local_context
        assert not ctx["show_external_log_redirect"]
        assert ctx["external_log_name"] is None


class _ExternalHandler(ExternalLoggingMixin):
    _supports_external_link = True
    LOG_NAME = "ExternalLog"

    @property
    def log_name(self) -> str:
        return self.LOG_NAME

    def get_external_log_url(self, *args, **kwargs) -> str:
        return "http://external-service.com"

    @property
    def supports_external_link(self) -> bool:
        return self._supports_external_link


@pytest.mark.parametrize("endpoint", ["graph", "tree"])
@unittest.mock.patch(
    "airflow.utils.log.log_reader.TaskLogReader.log_handler",
    new_callable=unittest.mock.PropertyMock,
    return_value=_ExternalHandler(),
)
def test_show_external_log_redirect_link_with_external_log_handler(
    _, capture_templates, admin_client, endpoint
):
    """Show external links if log handler is external."""
    url = f"{endpoint}?dag_id=example_bash_operator"
    with capture_templates() as templates:
        admin_client.get(url, follow_redirects=True)
        ctx = templates[0].local_context
        assert ctx["show_external_log_redirect"]
        assert ctx["external_log_name"] == _ExternalHandler.LOG_NAME


@pytest.mark.parametrize("endpoint", ["graph", "tree"])
@unittest.mock.patch(
    "airflow.utils.log.log_reader.TaskLogReader.log_handler",
    new_callable=unittest.mock.PropertyMock,
    return_value=_ExternalHandler(),
)
def test_external_log_redirect_link_with_external_log_handler_not_shown(
    _external_handler, capture_templates, admin_client, endpoint
):
    """Show external links if log handler is external."""
    _external_handler.return_value._supports_external_link = False
    url = f"{endpoint}?dag_id=example_bash_operator"
    with capture_templates() as templates:
        admin_client.get(url, follow_redirects=True)
        ctx = templates[0].local_context
        assert not ctx["show_external_log_redirect"]
        assert ctx["external_log_name"] is None


def _get_appbuilder_pk_string(model_view_cls, instance) -> str:
    """Utility to get Flask-Appbuilder's string format "pk" for an object.

    Used to generate requests to FAB action views without *too* much difficulty.
    The implementation relies on FAB internals, but unfortunately I don't see
    a better way around it.

    Example usage::

        from airflow.www.views import TaskInstanceModelView

        ti = session.Query(TaskInstance).filter(...).one()
        pk = _get_appbuilder_pk_string(TaskInstanceModelView, ti)
        client.post("...", data={"action": "...", "rowid": pk})
    """
    pk_value = model_view_cls.datamodel.get_pk_value(instance)
    return model_view_cls._serialize_pk_if_composite(model_view_cls, pk_value)


def test_task_instance_delete(session, admin_client, create_task_instance):
    task_instance_to_delete = create_task_instance(
        task_id="test_task_instance_delete",
        execution_date=timezone.utcnow(),
        state=State.DEFERRED,
    )
    composite_key = _get_appbuilder_pk_string(TaskInstanceModelView, task_instance_to_delete)
    task_id = task_instance_to_delete.task_id

    assert session.query(TaskInstance).filter(TaskInstance.task_id == task_id).count() == 1
    admin_client.post(f"/taskinstance/delete/{composite_key}", follow_redirects=True)
    assert session.query(TaskInstance).filter(TaskInstance.task_id == task_id).count() == 0


def test_task_instance_delete_permission_denied(session, client_ti_without_dag_edit, create_task_instance):
    task_instance_to_delete = create_task_instance(
        task_id="test_task_instance_delete_permission_denied",
        execution_date=timezone.utcnow(),
        state=State.DEFERRED,
        session=session,
    )
    session.commit()
    composite_key = _get_appbuilder_pk_string(TaskInstanceModelView, task_instance_to_delete)
    task_id = task_instance_to_delete.task_id

    assert session.query(TaskInstance).filter(TaskInstance.task_id == task_id).count() == 1
    resp = client_ti_without_dag_edit.post(f"/taskinstance/delete/{composite_key}", follow_redirects=True)
    check_content_in_response("Access is Denied", resp)
    assert session.query(TaskInstance).filter(TaskInstance.task_id == task_id).count() == 1


@pytest.mark.parametrize(
    "client_fixture, should_succeed",
    [
        ("admin_client", True),
        ("user_client", True),
        ("viewer_client", False),
        ("anonymous_client", False),
    ],
)
def test_task_instance_clear(session, request, client_fixture, should_succeed):
    client = request.getfixturevalue(client_fixture)
    task_id = "runme_0"
    initial_state = State.SUCCESS

    # Set the state to success for clearing.
    ti_q = session.query(TaskInstance).filter(TaskInstance.task_id == task_id)
    ti_q.update({"state": initial_state})
    session.commit()

    # Send a request to clear.
    rowid = _get_appbuilder_pk_string(TaskInstanceModelView, ti_q.one())
    resp = client.post(
        "/taskinstance/action_post",
        data={"action": "clear", "rowid": rowid},
        follow_redirects=True,
    )
    assert resp.status_code == 200
    if not should_succeed and client_fixture != "anonymous_client":
        check_content_in_response("Access is Denied", resp)

    # Now the state should be None.
    state = session.query(TaskInstance.state).filter(TaskInstance.task_id == task_id).scalar()
    assert state == (State.NONE if should_succeed else initial_state)


def test_task_instance_clear_downstream(session, admin_client, dag_maker):
    """Ensures clearing a task instance clears its downstream dependencies exclusively"""
    with dag_maker(
        dag_id="test_dag_id",
        serialized=True,
        session=session,
        start_date=pendulum.DateTime(2023, 1, 1, 0, 0, 0, tzinfo=pendulum.UTC),
    ):
        EmptyOperator(task_id="task_1") >> EmptyOperator(task_id="task_2")
        EmptyOperator(task_id="task_3")
    triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
    run1 = dag_maker.create_dagrun(
        run_id="run_1",
        state=DagRunState.SUCCESS,
        run_type=DagRunType.SCHEDULED,
        execution_date=dag_maker.dag.start_date,
        start_date=dag_maker.dag.start_date,
        session=session,
        **triggered_by_kwargs,
    )

    run2 = dag_maker.create_dagrun(
        run_id="run_2",
        state=DagRunState.SUCCESS,
        run_type=DagRunType.SCHEDULED,
        execution_date=dag_maker.dag.start_date.add(days=1),
        start_date=dag_maker.dag.start_date.add(days=1),
        session=session,
        **triggered_by_kwargs,
    )

    for run in (run1, run2):
        for ti in run.task_instances:
            ti.state = State.SUCCESS

    # Clear task_1 from dag run 1
    run1_ti1 = run1.get_task_instance(task_id="task_1")
    rowid = _get_appbuilder_pk_string(TaskInstanceModelView, run1_ti1)
    resp = admin_client.post(
        "/taskinstance/action_post",
        data={"action": "clear_downstream", "rowid": rowid},
        follow_redirects=True,
    )
    assert resp.status_code == 200

    # Assert that task_1 and task_2 of dag run 1 are cleared, but task_3 is left untouched
    run1_ti1.refresh_from_db(session=session)
    run1_ti2 = run1.get_task_instance(task_id="task_2")
    run1_ti3 = run1.get_task_instance(task_id="task_3")

    assert run1_ti1.state == State.NONE
    assert run1_ti2.state == State.NONE
    assert run1_ti3.state == State.SUCCESS

    # Assert that task_1 of dag run 2 is left untouched
    run2_ti1 = run2.get_task_instance(task_id="task_1")
    assert run2_ti1.state == State.SUCCESS


def test_task_instance_clear_failure(admin_client):
    rowid = '["12345"]'  # F.A.B. crashes if the rowid is *too* invalid.
    resp = admin_client.post(
        "/taskinstance/action_post",
        data={"action": "clear", "rowid": rowid},
        follow_redirects=True,
    )
    assert resp.status_code == 200
    check_content_in_response("Failed to clear task instances:", resp)


@pytest.mark.parametrize(
    "action, expected_state",
    [
        ("set_failed", State.FAILED),
        ("set_success", State.SUCCESS),
        ("set_retry", State.UP_FOR_RETRY),
        ("set_skipped", State.SKIPPED),
    ],
    ids=["failed", "success", "retry", "skipped"],
)
def test_task_instance_set_state(session, admin_client, action, expected_state):
    task_id = "runme_0"

    # Send a request to clear.
    ti_q = session.query(TaskInstance).filter(TaskInstance.task_id == task_id)
    rowid = _get_appbuilder_pk_string(TaskInstanceModelView, ti_q.one())
    resp = admin_client.post(
        "/taskinstance/action_post",
        data={"action": action, "rowid": rowid},
        follow_redirects=True,
    )
    assert resp.status_code == 200

    # Now the state should be modified.
    state = session.query(TaskInstance.state).filter(TaskInstance.task_id == task_id).scalar()
    assert state == expected_state


@pytest.mark.parametrize(
    "action",
    [
        "set_failed",
        "set_success",
        "set_retry",
        "set_skipped",
    ],
)
def test_task_instance_set_state_failure(admin_client, action):
    rowid = '["12345"]'  # F.A.B. crashes if the rowid is *too* invalid.
    resp = admin_client.post(
        "/taskinstance/action_post",
        data={"action": action, "rowid": rowid},
        follow_redirects=True,
    )
    assert resp.status_code == 200
    check_content_in_response("Failed to set state", resp)


def test_action_muldelete_task_instance(session, admin_client):
    task_search_tuples = [("example_xcom", "bash_push"), ("example_bash_operator", "run_this_last")]
    # get task instances to delete
    tasks_to_delete = []
    for task_search_tuple in task_search_tuples:
        dag_id, task_id = task_search_tuple
        tasks_to_delete.append(
            session.query(TaskInstance)
            .filter(TaskInstance.task_id == task_id, TaskInstance.dag_id == dag_id)
            .one()
        )

    # add task reschedules for those tasks to make sure that the delete cascades to the required tables
    trs = [
        TaskReschedule(
            task_id=task.task_id,
            dag_id=task.dag_id,
            run_id=task.run_id,
            try_number=1,
            start_date=timezone.datetime(2021, 1, 1),
            end_date=timezone.datetime(2021, 1, 2),
            reschedule_date=timezone.datetime(2021, 1, 3),
        )
        for task in tasks_to_delete
    ]
    session.bulk_save_objects(trs)
    session.flush()

    # run the function to test
    resp = admin_client.post(
        "/taskinstance/action_post",
        data={
            "action": "muldelete",
            "rowid": [_get_appbuilder_pk_string(TaskInstanceModelView, task) for task in tasks_to_delete],
        },
        follow_redirects=True,
    )

    # assert expected behavior for that function and its response
    assert resp.status_code == 200
    for task_search_tuple in task_search_tuples:
        dag_id, task_id = task_search_tuple
        assert (
            session.query(TaskInstance)
            .filter(TaskInstance.task_id == task_id, TaskInstance.dag_id == dag_id)
            .count()
            == 0
        )
    assert session.query(TaskReschedule).count() == 0


def test_graph_view_doesnt_fail_on_recursion_error(app, dag_maker, admin_client):
    """Test that the graph view doesn't fail on a recursion error."""
    from airflow.models.baseoperator import chain

    with dag_maker("test_fails_with_recursion") as dag:
        tasks = [
            BashOperator(
                task_id=f"task_{i}",
                bash_command="echo test",
            )
            for i in range(1, 1000 + 1)
        ]
        chain(*tasks)
    with unittest.mock.patch.object(app, "dag_bag") as mocked_dag_bag:
        mocked_dag_bag.get_dag.return_value = dag
        url = f"/dags/{dag.dag_id}/graph"
        resp = admin_client.get(url, follow_redirects=True)
        assert resp.status_code == 200


def test_get_date_time_num_runs_dag_runs_form_data_graph_view(app, dag_maker, admin_client):
    """Test the get_date_time_num_runs_dag_runs_form_data function."""
    from airflow.www.views import get_date_time_num_runs_dag_runs_form_data

    execution_date = pendulum.now(tz="UTC")
    with dag_maker(
        dag_id="test_get_date_time_num_runs_dag_runs_form_data",
        start_date=execution_date,
    ) as dag:
        BashOperator(task_id="task_1", bash_command="echo test")

    with unittest.mock.patch.object(app, "dag_bag") as mocked_dag_bag:
        mocked_dag_bag.get_dag.return_value = dag
        url = f"/dags/{dag.dag_id}/graph"
        resp = admin_client.get(url, follow_redirects=True)
        assert resp.status_code == 200

    with create_session() as session:
        data = get_date_time_num_runs_dag_runs_form_data(resp.request, session, dag)

        dttm = pendulum.parse(data["dttm"].isoformat())
        base_date = pendulum.parse(data["base_date"].isoformat())

        assert dttm.date() == execution_date.date()
        assert dttm.time().hour == _safe_parse_datetime(execution_date.time().isoformat()).time().hour
        assert dttm.time().minute == _safe_parse_datetime(execution_date.time().isoformat()).time().minute
        assert base_date.date() == execution_date.date()


def test_task_instances(admin_client):
    """Test task_instances view."""
    resp = admin_client.get(
        f"/object/task_instances?dag_id=example_bash_operator&execution_date={STR_DEFAULT_DATE}",
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert resp.json == {
        "also_run_this": {
            "custom_operator_name": None,
            "dag_id": "example_bash_operator",
            "duration": None,
            "end_date": None,
            "execution_date": DEFAULT_DATE.isoformat(),
            "executor": None,
            "executor_config": {},
            "external_executor_id": None,
            "hostname": "",
            "job_id": None,
            "map_index": -1,
            "max_tries": 0,
            "next_kwargs": None,
            "next_method": None,
            "operator": "BashOperator",
            "pid": None,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 2,
            "queue": "default",
            "queued_by_job_id": None,
            "queued_dttm": None,
            "rendered_map_index": None,
            "run_id": "TEST_DAGRUN",
            "start_date": None,
            "state": None,
            "task_display_name": "also_run_this",
            "task_id": "also_run_this",
            "trigger_id": None,
            "trigger_timeout": None,
            "try_number": 0,
            "unixname": getuser(),
            "updated_at": DEFAULT_DATE.isoformat(),
        },
        "run_after_loop": {
            "custom_operator_name": None,
            "dag_id": "example_bash_operator",
            "duration": None,
            "end_date": None,
            "execution_date": DEFAULT_DATE.isoformat(),
            "executor": None,
            "executor_config": {},
            "external_executor_id": None,
            "hostname": "",
            "job_id": None,
            "map_index": -1,
            "max_tries": 0,
            "next_kwargs": None,
            "next_method": None,
            "operator": "BashOperator",
            "pid": None,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 2,
            "queue": "default",
            "queued_by_job_id": None,
            "queued_dttm": None,
            "rendered_map_index": None,
            "run_id": "TEST_DAGRUN",
            "start_date": None,
            "state": None,
            "task_display_name": "run_after_loop",
            "task_id": "run_after_loop",
            "trigger_id": None,
            "trigger_timeout": None,
            "try_number": 0,
            "unixname": getuser(),
            "updated_at": DEFAULT_DATE.isoformat(),
        },
        "run_this_last": {
            "custom_operator_name": None,
            "dag_id": "example_bash_operator",
            "duration": None,
            "end_date": None,
            "execution_date": DEFAULT_DATE.isoformat(),
            "executor": None,
            "executor_config": {},
            "external_executor_id": None,
            "hostname": "",
            "job_id": None,
            "map_index": -1,
            "max_tries": 0,
            "next_kwargs": None,
            "next_method": None,
            "operator": "EmptyOperator",
            "pid": None,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 1,
            "queue": "default",
            "queued_by_job_id": None,
            "queued_dttm": None,
            "rendered_map_index": None,
            "run_id": "TEST_DAGRUN",
            "start_date": None,
            "state": None,
            "task_display_name": "run_this_last",
            "task_id": "run_this_last",
            "trigger_id": None,
            "trigger_timeout": None,
            "try_number": 0,
            "unixname": getuser(),
            "updated_at": DEFAULT_DATE.isoformat(),
        },
        "runme_0": {
            "custom_operator_name": None,
            "dag_id": "example_bash_operator",
            "duration": None,
            "end_date": None,
            "execution_date": DEFAULT_DATE.isoformat(),
            "executor": None,
            "executor_config": {},
            "external_executor_id": None,
            "hostname": "",
            "job_id": None,
            "map_index": -1,
            "max_tries": 0,
            "next_kwargs": None,
            "next_method": None,
            "operator": "BashOperator",
            "pid": None,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 3,
            "queue": "default",
            "queued_by_job_id": None,
            "queued_dttm": None,
            "rendered_map_index": None,
            "run_id": "TEST_DAGRUN",
            "start_date": None,
            "state": None,
            "task_display_name": "runme_0",
            "task_id": "runme_0",
            "trigger_id": None,
            "trigger_timeout": None,
            "try_number": 0,
            "unixname": getuser(),
            "updated_at": DEFAULT_DATE.isoformat(),
        },
        "runme_1": {
            "custom_operator_name": None,
            "dag_id": "example_bash_operator",
            "duration": None,
            "end_date": None,
            "execution_date": DEFAULT_DATE.isoformat(),
            "executor": None,
            "executor_config": {},
            "external_executor_id": None,
            "hostname": "",
            "job_id": None,
            "map_index": -1,
            "max_tries": 0,
            "next_kwargs": None,
            "next_method": None,
            "operator": "BashOperator",
            "pid": None,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 3,
            "queue": "default",
            "queued_by_job_id": None,
            "queued_dttm": None,
            "rendered_map_index": None,
            "run_id": "TEST_DAGRUN",
            "start_date": None,
            "state": None,
            "task_display_name": "runme_1",
            "task_id": "runme_1",
            "trigger_id": None,
            "trigger_timeout": None,
            "try_number": 0,
            "unixname": getuser(),
            "updated_at": DEFAULT_DATE.isoformat(),
        },
        "runme_2": {
            "custom_operator_name": None,
            "dag_id": "example_bash_operator",
            "duration": None,
            "end_date": None,
            "execution_date": DEFAULT_DATE.isoformat(),
            "executor": None,
            "executor_config": {},
            "external_executor_id": None,
            "hostname": "",
            "job_id": None,
            "map_index": -1,
            "max_tries": 0,
            "next_kwargs": None,
            "next_method": None,
            "operator": "BashOperator",
            "pid": None,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 3,
            "queue": "default",
            "queued_by_job_id": None,
            "queued_dttm": None,
            "rendered_map_index": None,
            "run_id": "TEST_DAGRUN",
            "start_date": None,
            "state": None,
            "task_display_name": "runme_2",
            "task_id": "runme_2",
            "trigger_id": None,
            "trigger_timeout": None,
            "try_number": 0,
            "unixname": getuser(),
            "updated_at": DEFAULT_DATE.isoformat(),
        },
        "this_will_skip": {
            "custom_operator_name": None,
            "dag_id": "example_bash_operator",
            "duration": None,
            "end_date": None,
            "execution_date": DEFAULT_DATE.isoformat(),
            "executor": None,
            "executor_config": {},
            "external_executor_id": None,
            "hostname": "",
            "job_id": None,
            "map_index": -1,
            "max_tries": 0,
            "next_kwargs": None,
            "next_method": None,
            "operator": "BashOperator",
            "pid": None,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 2,
            "queue": "default",
            "queued_by_job_id": None,
            "queued_dttm": None,
            "rendered_map_index": None,
            "run_id": "TEST_DAGRUN",
            "start_date": None,
            "state": None,
            "task_display_name": "this_will_skip",
            "task_id": "this_will_skip",
            "trigger_id": None,
            "trigger_timeout": None,
            "try_number": 0,
            "unixname": getuser(),
            "updated_at": DEFAULT_DATE.isoformat(),
        },
    }
