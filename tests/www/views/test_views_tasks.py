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
import re
import unittest.mock
import urllib.parse

import pytest
import time_machine

from airflow import settings
from airflow.exceptions import AirflowException
from airflow.executors.celery_executor import CeleryExecutor
from airflow.executors.local_executor import LocalExecutor
from airflow.models import DAG, DagBag, DagModel, TaskFail, TaskInstance, TaskReschedule
from airflow.models.dagcode import DagCode
from airflow.operators.bash import BashOperator
from airflow.security import permissions
from airflow.ti_deps.dependencies_states import QUEUEABLE_STATES, RUNNABLE_STATES
from airflow.utils import timezone
from airflow.utils.log.logging_mixin import ExternalLoggingMixin
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from airflow.www.views import TaskInstanceModelView
from tests.test_utils.api_connexion_utils import create_user, delete_roles, delete_user
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_runs
from tests.test_utils.www import check_content_in_response, check_content_not_in_response, client_with_login

DEFAULT_DATE = timezone.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

DEFAULT_VAL = urllib.parse.quote_plus(str(DEFAULT_DATE))

DEFAULT_DAGRUN = "TEST_DAGRUN"


@pytest.fixture(scope="module", autouse=True)
def reset_dagruns():
    """Clean up stray garbage from other tests."""
    clear_db_runs()


@pytest.fixture(autouse=True)
def init_dagruns(app, reset_dagruns):
    with time_machine.travel(DEFAULT_DATE, tick=False):
        app.dag_bag.get_dag("example_bash_operator").create_dagrun(
            run_id=DEFAULT_DAGRUN,
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            start_date=timezone.utcnow(),
            state=State.RUNNING,
        )
        app.dag_bag.get_dag("example_subdag_operator").create_dagrun(
            run_id=DEFAULT_DAGRUN,
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            start_date=timezone.utcnow(),
            state=State.RUNNING,
        )
        app.dag_bag.get_dag("example_xcom").create_dagrun(
            run_id=DEFAULT_DAGRUN,
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            start_date=timezone.utcnow(),
            state=State.RUNNING,
        )
    yield
    clear_db_runs()


@pytest.fixture(scope="module")
def client_ti_without_dag_edit(app):
    create_user(
        app,
        username="all_ti_permissions_except_dag_edit",
        role_name="all_ti_permissions_except_dag_edit",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_TASK_INSTANCE),
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
            ["Task Instance Details"],
            id="task",
        ),
        pytest.param(
            f"xcom?task_id=runme_0&dag_id=example_bash_operator&execution_date={DEFAULT_VAL}",
            ["XCom"],
            id="xcom",
        ),
        pytest.param("xcom/list", ["List XComs"], id="xcom-list"),
        pytest.param(
            f"rendered-templates?task_id=runme_0&dag_id=example_bash_operator&execution_date={DEFAULT_VAL}",
            ["Rendered Template"],
            id="rendered-templates",
        ),
        pytest.param(
            "dag_details?dag_id=example_bash_operator",
            ["DAG Details"],
            id="dag-details-url-param",
        ),
        pytest.param(
            "dag_details?dag_id=example_subdag_operator.section-1",
            ["DAG Details"],
            id="dag-details-subdag-url-param",
        ),
        pytest.param(
            "dags/example_subdag_operator.section-1/details",
            ["DAG Details"],
            id="dag-details-subdag",
        ),
        pytest.param(
            "graph?dag_id=example_bash_operator",
            ["runme_1"],
            id="graph-url-param",
        ),
        pytest.param(
            "dags/example_bash_operator/graph",
            ["runme_1"],
            id="graph",
        ),
        pytest.param(
            "object/grid_data?dag_id=example_bash_operator",
            ["runme_1"],
            id="grid-data",
        ),
        pytest.param(
            "object/grid_data?dag_id=example_subdag_operator.section-1",
            ["section-1-task-1"],
            id="grid-data-subdag",
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
            ["Wait a minute"],
            id="confirm-success",
        ),
        pytest.param(
            f"confirm?task_id=runme_0&dag_id=example_bash_operator&state=failed&dag_run_id={DEFAULT_DAGRUN}",
            ["Wait a minute"],
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
    app.dag_bag.get_dag("test_tree_view").create_dagrun(
        run_type=DagRunType.SCHEDULED,
        execution_date=DEFAULT_DATE,
        data_interval=(DEFAULT_DATE, DEFAULT_DATE),
        start_date=timezone.utcnow(),
        state=State.RUNNING,
    )

    url = "tree?dag_id=test_tree_view"
    resp = admin_client.get(url, follow_redirects=True)
    params = {"dag_id": "test_tree_view", "origin": "/dags/test_tree_view/grid"}
    href = f"/trigger?{html.escape(urllib.parse.urlencode(params))}"
    check_content_in_response(href, resp)


def test_graph_trigger_origin_graph_view(app, admin_client):
    app.dag_bag.get_dag("test_tree_view").create_dagrun(
        run_type=DagRunType.SCHEDULED,
        execution_date=DEFAULT_DATE,
        data_interval=(DEFAULT_DATE, DEFAULT_DATE),
        start_date=timezone.utcnow(),
        state=State.RUNNING,
    )

    url = "/dags/test_tree_view/graph"
    resp = admin_client.get(url, follow_redirects=True)
    params = {"dag_id": "test_tree_view", "origin": "/dags/test_tree_view/graph"}
    href = f"/trigger?{html.escape(urllib.parse.urlencode(params))}"
    check_content_in_response(href, resp)


def test_dag_details_trigger_origin_dag_details_view(app, admin_client):
    app.dag_bag.get_dag("test_graph_view").create_dagrun(
        run_type=DagRunType.SCHEDULED,
        execution_date=DEFAULT_DATE,
        data_interval=(DEFAULT_DATE, DEFAULT_DATE),
        start_date=timezone.utcnow(),
        state=State.RUNNING,
    )

    url = "/dags/test_graph_view/details"
    resp = admin_client.get(url, follow_redirects=True)
    params = {"dag_id": "test_graph_view", "origin": "/dags/test_graph_view/details"}
    href = f"/trigger?{html.escape(urllib.parse.urlencode(params))}"
    check_content_in_response(href, resp)


def test_last_dagruns(admin_client):
    resp = admin_client.post("last_dagruns", follow_redirects=True)
    check_content_in_response("example_bash_operator", resp)


def test_last_dagruns_success_when_selecting_dags(admin_client):
    resp = admin_client.post(
        "last_dagruns", data={"dag_ids": ["example_subdag_operator"]}, follow_redirects=True
    )
    assert resp.status_code == 200
    stats = json.loads(resp.data.decode("utf-8"))
    assert "example_bash_operator" not in stats
    assert "example_subdag_operator" in stats

    # Multiple
    resp = admin_client.post(
        "last_dagruns",
        data={"dag_ids": ["example_subdag_operator", "example_bash_operator"]},
        follow_redirects=True,
    )
    stats = json.loads(resp.data.decode("utf-8"))
    assert "example_bash_operator" in stats
    assert "example_subdag_operator" in stats
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
            "run",
            dict(
                task_id="runme_0",
                dag_id="example_bash_operator",
                ignore_all_deps="false",
                ignore_ti_state="true",
                execution_date=DEFAULT_DATE,
            ),
            "",
        ),
    ],
    ids=[
        "paused",
        "failed-flash-hint",
        "success-flash-hint",
        "clear",
        "run",
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


@pytest.mark.parametrize("state", RUNNABLE_STATES)
@unittest.mock.patch(
    "airflow.executors.executor_loader.ExecutorLoader.get_default_executor",
    return_value=_ForceHeartbeatCeleryExecutor(),
)
def test_run_with_runnable_states(_, admin_client, session, state):
    task_id = "runme_0"
    session.query(TaskInstance).filter(TaskInstance.task_id == task_id).update(
        {"state": state, "end_date": timezone.utcnow()}
    )
    session.commit()

    form = dict(
        task_id=task_id,
        dag_id="example_bash_operator",
        ignore_all_deps="false",
        ignore_ti_state="false",
        dag_run_id=DEFAULT_DAGRUN,
        origin="/home",
    )
    resp = admin_client.post("run", data=form, follow_redirects=True)
    check_content_in_response("", resp)

    msg = f"Task is in the &#39;{state}&#39 state."
    assert not re.search(msg, resp.get_data(as_text=True))


@unittest.mock.patch(
    "airflow.executors.executor_loader.ExecutorLoader.get_default_executor",
    return_value=_ForceHeartbeatCeleryExecutor(),
)
def test_run_ignoring_deps_sets_queued_dttm(_, admin_client, session, time_machine):
    task_id = "runme_0"
    session.query(TaskInstance).filter(TaskInstance.task_id == task_id).update(
        {"state": State.SCHEDULED, "queued_dttm": None}
    )
    session.commit()

    assert session.query(TaskInstance.queued_dttm).filter(TaskInstance.task_id == task_id).all() == [(None,)]

    form = dict(
        task_id=task_id,
        dag_id="example_bash_operator",
        ignore_all_deps="true",
        dag_run_id=DEFAULT_DAGRUN,
        origin="/home",
    )
    now = timezone.utcnow()

    time_machine.move_to(now, tick=False)
    resp = admin_client.post("run", data=form, follow_redirects=True)

    assert resp.status_code == 200
    assert session.query(TaskInstance.queued_dttm).filter(TaskInstance.task_id == task_id).scalar() == now


@pytest.mark.parametrize("state", QUEUEABLE_STATES)
@unittest.mock.patch(
    "airflow.executors.executor_loader.ExecutorLoader.get_default_executor",
    return_value=CeleryExecutor(),
)
def test_run_with_not_runnable_states(_, admin_client, session, state):
    assert state not in RUNNABLE_STATES

    task_id = "runme_0"
    session.query(TaskInstance).filter(TaskInstance.task_id == task_id).update(
        {"state": state, "end_date": timezone.utcnow()}
    )
    session.commit()

    form = dict(
        task_id=task_id,
        dag_id="example_bash_operator",
        ignore_all_deps="false",
        ignore_ti_state="false",
        dag_run_id=DEFAULT_DAGRUN,
        origin="/home",
    )
    resp = admin_client.post("run", data=form, follow_redirects=True)
    check_content_in_response("", resp)

    msg = f"Task is in the &#39;{state}&#39; state."
    assert re.search(msg, resp.get_data(as_text=True))


@pytest.mark.parametrize("state", QUEUEABLE_STATES)
@unittest.mock.patch(
    "airflow.executors.executor_loader.ExecutorLoader.get_default_executor",
    return_value=LocalExecutor(),
)
def test_run_with_the_unsupported_executor(_, admin_client, session, state):
    assert state not in RUNNABLE_STATES

    task_id = "runme_0"
    session.query(TaskInstance).filter(TaskInstance.task_id == task_id).update(
        {"state": state, "end_date": timezone.utcnow()}
    )
    session.commit()

    form = dict(
        task_id=task_id,
        dag_id="example_bash_operator",
        ignore_all_deps="false",
        ignore_ti_state="false",
        dag_run_id=DEFAULT_DAGRUN,
        origin="/home",
    )
    resp = admin_client.post("run", data=form, follow_redirects=True)
    check_content_in_response("", resp)

    msg = "LocalExecutor does not support ad hoc task runs"
    assert re.search(msg, resp.get_data(as_text=True))


@pytest.fixture()
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
    # Test for JIRA AIRFLOW-3233 (PR 4069):
    # The delete-dag URL should be generated correctly for DAGs
    # that exist on the scheduler (DB) but not the webserver DagBag
    test_dag_id = new_id_example_bash_operator
    resp = admin_client.get("/", follow_redirects=True)
    check_content_in_response(f"/delete?dag_id={test_dag_id}", resp)
    check_content_in_response(f"return confirmDeleteDag(this, '{test_dag_id}')", resp)


@pytest.fixture()
def new_dag_to_delete():
    dag = DAG("new_dag_to_delete", is_paused_upon_creation=True)
    session = settings.Session()
    dag.sync_to_db(session=session)
    return dag


@pytest.fixture()
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

        >>> from airflow.www.views import TaskInstanceModelView
        >>> ti = session.Query(TaskInstance).filter(...).one()
        >>> pk = _get_appbuilder_pk_string(TaskInstanceModelView, ti)
        >>> client.post("...", data={"action": "...", "rowid": pk})
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
    check_content_in_response(f"Access denied for dag_id {task_instance_to_delete.dag_id}", resp)
    assert session.query(TaskInstance).filter(TaskInstance.task_id == task_id).count() == 1


def test_task_instance_clear(session, admin_client):
    task_id = "runme_0"

    # Set the state to success for clearing.
    ti_q = session.query(TaskInstance).filter(TaskInstance.task_id == task_id)
    ti_q.update({"state": State.SUCCESS})
    session.commit()

    # Send a request to clear.
    rowid = _get_appbuilder_pk_string(TaskInstanceModelView, ti_q.one())
    resp = admin_client.post(
        "/taskinstance/action_post",
        data={"action": "clear", "rowid": rowid},
        follow_redirects=True,
    )
    assert resp.status_code == 200

    # Now the state should be None.
    state = session.query(TaskInstance.state).filter(TaskInstance.task_id == task_id).scalar()
    assert state == State.NONE


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
        ("set_running", State.RUNNING),
        ("set_failed", State.FAILED),
        ("set_success", State.SUCCESS),
        ("set_retry", State.UP_FOR_RETRY),
        ("set_skipped", State.SKIPPED),
    ],
    ids=["running", "failed", "success", "retry", "skipped"],
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
        "set_running",
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


@pytest.mark.parametrize(
    "task_search_tuples",
    [
        [("example_xcom", "bash_push"), ("example_bash_operator", "run_this_last")],
        [("example_subdag_operator", "some-other-task")],
    ],
    ids=["multiple_tasks", "one_task"],
)
def test_action_muldelete_task_instance(session, admin_client, task_search_tuples):
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
            task=task,
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


def test_task_fail_duration(app, admin_client, dag_maker, session):
    """Task duration page with a TaskFail entry should render without error."""
    with dag_maker() as dag:
        op1 = BashOperator(task_id="fail", bash_command="exit 1")
        op2 = BashOperator(task_id="success", bash_command="exit 0")

    with pytest.raises(AirflowException):
        op1.run()
    op2.run()

    op1_fails = (
        session.query(TaskFail)
        .filter(
            TaskFail.task_id == "fail",
            TaskFail.dag_id == dag.dag_id,
        )
        .all()
    )

    op2_fails = (
        session.query(TaskFail)
        .filter(
            TaskFail.task_id == "success",
            TaskFail.dag_id == dag.dag_id,
        )
        .all()
    )

    assert len(op1_fails) == 1
    assert len(op2_fails) == 0

    with unittest.mock.patch.object(app, "dag_bag") as mocked_dag_bag:
        mocked_dag_bag.get_dag.return_value = dag
        resp = admin_client.get(f"dags/{dag.dag_id}/duration", follow_redirects=True)
        html = resp.get_data().decode()
        cumulative_chart = json.loads(re.search("data_cumlinechart=(.*);", html).group(1))
        line_chart = json.loads(re.search("data_linechart=(.*);", html).group(1))

        assert resp.status_code == 200
        assert sorted(item["key"] for item in cumulative_chart) == ["fail", "success"]
        assert sorted(item["key"] for item in line_chart) == ["fail", "success"]


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


def test_task_instances(admin_client):
    """Test task_instances view."""
    resp = admin_client.get(
        f"/object/task_instances?dag_id=example_bash_operator&execution_date={DEFAULT_DATE}",
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert resp.json == {
        "also_run_this": {
            "dag_id": "example_bash_operator",
            "duration": None,
            "end_date": None,
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
            "run_id": "TEST_DAGRUN",
            "start_date": None,
            "state": None,
            "task_id": "also_run_this",
            "trigger_id": None,
            "trigger_timeout": None,
            "try_number": 1,
            "unixname": "root",
            "updated_at": DEFAULT_DATE.isoformat(),
        },
        "run_after_loop": {
            "dag_id": "example_bash_operator",
            "duration": None,
            "end_date": None,
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
            "run_id": "TEST_DAGRUN",
            "start_date": None,
            "state": None,
            "task_id": "run_after_loop",
            "trigger_id": None,
            "trigger_timeout": None,
            "try_number": 1,
            "unixname": "root",
            "updated_at": DEFAULT_DATE.isoformat(),
        },
        "run_this_last": {
            "dag_id": "example_bash_operator",
            "duration": None,
            "end_date": None,
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
            "run_id": "TEST_DAGRUN",
            "start_date": None,
            "state": None,
            "task_id": "run_this_last",
            "trigger_id": None,
            "trigger_timeout": None,
            "try_number": 1,
            "unixname": "root",
            "updated_at": DEFAULT_DATE.isoformat(),
        },
        "runme_0": {
            "dag_id": "example_bash_operator",
            "duration": None,
            "end_date": None,
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
            "run_id": "TEST_DAGRUN",
            "start_date": None,
            "state": None,
            "task_id": "runme_0",
            "trigger_id": None,
            "trigger_timeout": None,
            "try_number": 1,
            "unixname": "root",
            "updated_at": DEFAULT_DATE.isoformat(),
        },
        "runme_1": {
            "dag_id": "example_bash_operator",
            "duration": None,
            "end_date": None,
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
            "run_id": "TEST_DAGRUN",
            "start_date": None,
            "state": None,
            "task_id": "runme_1",
            "trigger_id": None,
            "trigger_timeout": None,
            "try_number": 1,
            "unixname": "root",
            "updated_at": DEFAULT_DATE.isoformat(),
        },
        "runme_2": {
            "dag_id": "example_bash_operator",
            "duration": None,
            "end_date": None,
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
            "run_id": "TEST_DAGRUN",
            "start_date": None,
            "state": None,
            "task_id": "runme_2",
            "trigger_id": None,
            "trigger_timeout": None,
            "try_number": 1,
            "unixname": "root",
            "updated_at": DEFAULT_DATE.isoformat(),
        },
        "this_will_skip": {
            "dag_id": "example_bash_operator",
            "duration": None,
            "end_date": None,
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
            "run_id": "TEST_DAGRUN",
            "start_date": None,
            "state": None,
            "task_id": "this_will_skip",
            "trigger_id": None,
            "trigger_timeout": None,
            "try_number": 1,
            "unixname": "root",
            "updated_at": DEFAULT_DATE.isoformat(),
        },
    }
