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
import html
import json
import re
import unittest.mock
import urllib.parse

import pytest

from airflow import settings
from airflow.executors.celery_executor import CeleryExecutor
from airflow.models import DagBag, DagModel, TaskInstance
from airflow.models.dagcode import DagCode
from airflow.ti_deps.dependencies_states import QUEUEABLE_STATES, RUNNABLE_STATES
from airflow.utils import dates, timezone
from airflow.utils.log.logging_mixin import ExternalLoggingMixin
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from airflow.www.views import TaskInstanceModelView
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_runs
from tests.test_utils.www import check_content_in_response, check_content_not_in_response

DEFAULT_DATE = dates.days_ago(2)

DEFAULT_VAL = urllib.parse.quote_plus(str(DEFAULT_DATE))


@pytest.fixture(scope="module", autouse=True)
def reset_dagruns():
    """Clean up stray garbage from other tests."""
    clear_db_runs()


@pytest.fixture(autouse=True)
def init_dagruns(app, reset_dagruns):
    app.dag_bag.get_dag("example_bash_operator").create_dagrun(
        run_type=DagRunType.SCHEDULED,
        execution_date=DEFAULT_DATE,
        start_date=timezone.utcnow(),
        state=State.RUNNING,
    )
    app.dag_bag.get_dag("example_subdag_operator").create_dagrun(
        run_type=DagRunType.SCHEDULED,
        execution_date=DEFAULT_DATE,
        start_date=timezone.utcnow(),
        state=State.RUNNING,
    )
    app.dag_bag.get_dag("example_xcom").create_dagrun(
        run_type=DagRunType.SCHEDULED,
        execution_date=DEFAULT_DATE,
        start_date=timezone.utcnow(),
        state=State.RUNNING,
    )
    yield
    clear_db_runs()


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
            f'task?task_id=runme_0&dag_id=example_bash_operator&execution_date={DEFAULT_VAL}',
            ['Task Instance Details'],
            id="task",
        ),
        pytest.param(
            f'xcom?task_id=runme_0&dag_id=example_bash_operator&execution_date={DEFAULT_VAL}',
            ['XCom'],
            id="xcom",
        ),
        pytest.param('xcom/list', ['List XComs'], id="xcom-list"),
        pytest.param(
            f'rendered-templates?task_id=runme_0&dag_id=example_bash_operator&execution_date={DEFAULT_VAL}',
            ['Rendered Template'],
            id="rendered-templates",
        ),
        pytest.param(
            'dag_details?dag_id=example_bash_operator',
            ['DAG Details'],
            id="dag-details",
        ),
        pytest.param(
            'dag_details?dag_id=example_subdag_operator.section-1',
            ['DAG Details'],
            id="dag-details-subdag",
        ),
        pytest.param(
            'graph?dag_id=example_bash_operator',
            ['runme_1'],
            id='graph',
        ),
        pytest.param(
            'tree?dag_id=example_bash_operator',
            ['runme_1'],
            id='tree',
        ),
        pytest.param(
            'tree?dag_id=example_subdag_operator.section-1',
            ['section-1-task-1'],
            id="tree-subdag",
        ),
        pytest.param(
            'duration?days=30&dag_id=example_bash_operator',
            ['example_bash_operator'],
            id='duration',
        ),
        pytest.param(
            'duration?days=30&dag_id=missing_dag',
            ['seems to be missing'],
            id='duration-missing',
        ),
        pytest.param(
            'tries?days=30&dag_id=example_bash_operator',
            ['example_bash_operator'],
            id='tries',
        ),
        pytest.param(
            'landing_times?days=30&dag_id=example_bash_operator',
            ['example_bash_operator'],
            id='landing-times',
        ),
        pytest.param(
            'gantt?dag_id=example_bash_operator',
            ['example_bash_operator'],
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
            id="existing-dagbag-graph",
        ),
        pytest.param(
            "tree?dag_id=example_bash_operator",
            ["example_bash_operator"],
            id="existing-dagbag-tree",
        ),
        pytest.param(
            "calendar?dag_id=example_bash_operator",
            ["example_bash_operator"],
            id="existing-dagbag-calendar",
        ),
        pytest.param(
            "dag_details?dag_id=example_bash_operator",
            ["example_bash_operator"],
            id="existing-dagbag-dag-details",
        ),
        pytest.param(
            f'confirm?task_id=runme_0&dag_id=example_bash_operator&state=success'
            f'&execution_date={DEFAULT_VAL}',
            ['Wait a minute.'],
            id="confirm-success",
        ),
        pytest.param(
            f'confirm?task_id=runme_0&dag_id=example_bash_operator&state=failed&execution_date={DEFAULT_VAL}',
            ['Wait a minute.'],
            id="confirm-failed",
        ),
        pytest.param(
            f'confirm?task_id=runme_0&dag_id=invalid_dag&state=failed&execution_date={DEFAULT_VAL}',
            ['DAG invalid_dag not found'],
            id="confirm-failed",
        ),
        pytest.param(
            f'confirm?task_id=invalid_task&dag_id=example_bash_operator&state=failed'
            f'&execution_date={DEFAULT_VAL}',
            ['Task invalid_task not found'],
            id="confirm-failed",
        ),
        pytest.param(
            f'confirm?task_id=runme_0&dag_id=example_bash_operator&state=invalid'
            f'&execution_date={DEFAULT_VAL}',
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
    url = f'rendered-k8s?task_id=runme_0&dag_id=example_bash_operator&execution_date={DEFAULT_VAL}'
    with unittest.mock.patch.object(settings, "IS_K8S_OR_K8SCELERY_EXECUTOR", True):
        resp = admin_client.get(url, follow_redirects=True)
        check_content_in_response('K8s Pod Spec', resp)


@conf_vars({('core', 'executor'): 'LocalExecutor'})
def test_rendered_k8s_without_k8s(admin_client):
    url = f'rendered-k8s?task_id=runme_0&dag_id=example_bash_operator&execution_date={DEFAULT_VAL}'
    resp = admin_client.get(url, follow_redirects=True)
    assert 404 == resp.status_code


@pytest.mark.parametrize(
    "test_str, expected_text",
    [
        ("hello\nworld", r'\"conf\":{\"abc\":\"hello\\nworld\"}'),
        ("hello'world", r'\"conf\":{\"abc\":\"hello\\u0027world\"}'),
        ("<script>", r'\"conf\":{\"abc\":\"\\u003cscript\\u003e\"}'),
        ("\"", r'\"conf\":{\"abc\":\"\\\"\"}'),
    ],
)
def test_escape_in_tree_view(app, admin_client, test_str, expected_text):
    app.dag_bag.get_dag('test_tree_view').create_dagrun(
        execution_date=DEFAULT_DATE,
        start_date=timezone.utcnow(),
        run_type=DagRunType.MANUAL,
        state=State.RUNNING,
        conf={"abc": test_str},
    )

    url = 'tree?dag_id=test_tree_view'
    resp = admin_client.get(url, follow_redirects=True)
    check_content_in_response(expected_text, resp)


def test_dag_details_trigger_origin_tree_view(app, admin_client):
    app.dag_bag.get_dag('test_tree_view').create_dagrun(
        run_type=DagRunType.SCHEDULED,
        execution_date=DEFAULT_DATE,
        start_date=timezone.utcnow(),
        state=State.RUNNING,
    )

    url = 'dag_details?dag_id=test_tree_view'
    resp = admin_client.get(url, follow_redirects=True)
    params = {'dag_id': 'test_tree_view', 'origin': '/tree?dag_id=test_tree_view'}
    href = f"/trigger?{html.escape(urllib.parse.urlencode(params))}"
    check_content_in_response(href, resp)


def test_dag_details_trigger_origin_graph_view(app, admin_client):
    app.dag_bag.get_dag('test_graph_view').create_dagrun(
        run_type=DagRunType.SCHEDULED,
        execution_date=DEFAULT_DATE,
        start_date=timezone.utcnow(),
        state=State.RUNNING,
    )

    url = 'dag_details?dag_id=test_graph_view'
    resp = admin_client.get(url, follow_redirects=True)
    params = {'dag_id': 'test_graph_view', 'origin': '/graph?dag_id=test_graph_view'}
    href = f"/trigger?{html.escape(urllib.parse.urlencode(params))}"
    check_content_in_response(href, resp)


def test_last_dagruns(admin_client):
    resp = admin_client.post('last_dagruns', follow_redirects=True)
    check_content_in_response('example_bash_operator', resp)


def test_last_dagruns_success_when_selecting_dags(admin_client):
    resp = admin_client.post(
        'last_dagruns', data={'dag_ids': ['example_subdag_operator']}, follow_redirects=True
    )
    assert resp.status_code == 200
    stats = json.loads(resp.data.decode('utf-8'))
    assert 'example_bash_operator' not in stats
    assert 'example_subdag_operator' in stats

    # Multiple
    resp = admin_client.post(
        'last_dagruns',
        data={'dag_ids': ['example_subdag_operator', 'example_bash_operator']},
        follow_redirects=True,
    )
    stats = json.loads(resp.data.decode('utf-8'))
    assert 'example_bash_operator' in stats
    assert 'example_subdag_operator' in stats
    check_content_not_in_response('example_xcom', resp)


def test_code(admin_client):
    url = 'code?dag_id=example_bash_operator'
    resp = admin_client.get(url, follow_redirects=True)
    check_content_not_in_response('Failed to load file', resp)
    check_content_in_response('example_bash_operator', resp)


def test_code_no_file(admin_client):
    url = 'code?dag_id=example_bash_operator'
    mock_open_patch = unittest.mock.mock_open(read_data='')
    mock_open_patch.side_effect = FileNotFoundError
    with unittest.mock.patch('builtins.open', mock_open_patch), unittest.mock.patch(
        "airflow.models.dagcode.STORE_DAG_CODE", False
    ):
        resp = admin_client.get(url, follow_redirects=True)
        check_content_in_response('Failed to load file', resp)
        check_content_in_response('example_bash_operator', resp)


@conf_vars({("core", "store_dag_code"): "True"})
def test_code_from_db(admin_client):
    dag = DagBag(include_examples=True).get_dag("example_bash_operator")
    DagCode(dag.fileloc, DagCode._get_code_from_file(dag.fileloc)).sync_to_db()
    url = 'code?dag_id=example_bash_operator'
    resp = admin_client.get(url)
    check_content_not_in_response('Failed to load file', resp)
    check_content_in_response('example_bash_operator', resp)


@conf_vars({("core", "store_dag_code"): "True"})
def test_code_from_db_all_example_dags(admin_client):
    dagbag = DagBag(include_examples=True)
    for dag in dagbag.dags.values():
        DagCode(dag.fileloc, DagCode._get_code_from_file(dag.fileloc)).sync_to_db()
    url = 'code?dag_id=example_bash_operator'
    resp = admin_client.get(url)
    check_content_not_in_response('Failed to load file', resp)
    check_content_in_response('example_bash_operator', resp)


@pytest.mark.parametrize(
    "url, data, content",
    [
        ('paused?dag_id=example_bash_operator&is_paused=false', None, 'OK'),
        (
            "failed",
            dict(
                task_id="run_this_last",
                dag_id="example_bash_operator",
                execution_date=DEFAULT_DATE,
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
                execution_date=DEFAULT_DATE,
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
    check_content_in_response(f"Cannot mark tasks as {url}, seem that dag {dag_id} has never run", resp)


class _ForceHeartbeatCeleryExecutor(CeleryExecutor):
    def heartbeat(self):
        return True


@pytest.mark.parametrize("state", RUNNABLE_STATES)
@unittest.mock.patch(
    'airflow.executors.executor_loader.ExecutorLoader.get_default_executor',
    return_value=_ForceHeartbeatCeleryExecutor(),
)
def test_run_with_runnable_states(_, admin_client, session, state):
    task_id = 'runme_0'
    session.query(TaskInstance).filter(TaskInstance.task_id == task_id).update(
        {'state': state, 'end_date': timezone.utcnow()}
    )
    session.commit()

    form = dict(
        task_id=task_id,
        dag_id="example_bash_operator",
        ignore_all_deps="false",
        ignore_ti_state="false",
        execution_date=DEFAULT_DATE,
        origin='/home',
    )
    resp = admin_client.post('run', data=form, follow_redirects=True)
    check_content_in_response('', resp)

    msg = (
        f"Task is in the &#39;{state}&#39; state which is not a valid state for "
        f"execution. The task must be cleared in order to be run"
    )
    assert not re.search(msg, resp.get_data(as_text=True))


@pytest.mark.parametrize("state", QUEUEABLE_STATES)
@unittest.mock.patch(
    'airflow.executors.executor_loader.ExecutorLoader.get_default_executor',
    return_value=CeleryExecutor(),
)
def test_run_with_not_runnable_states(_, admin_client, session, state):
    assert state not in RUNNABLE_STATES

    task_id = 'runme_0'
    session.query(TaskInstance).filter(TaskInstance.task_id == task_id).update(
        {'state': state, 'end_date': timezone.utcnow()}
    )
    session.commit()

    form = dict(
        task_id=task_id,
        dag_id="example_bash_operator",
        ignore_all_deps="false",
        ignore_ti_state="false",
        execution_date=DEFAULT_DATE,
        origin='/home',
    )
    resp = admin_client.post('run', data=form, follow_redirects=True)
    check_content_in_response('', resp)

    msg = (
        f"Task is in the &#39;{state}&#39; state which is not a valid state for "
        f"execution. The task must be cleared in order to be run"
    )
    assert re.search(msg, resp.get_data(as_text=True))


def test_refresh(admin_client):
    resp = admin_client.post('refresh?dag_id=example_bash_operator')
    check_content_in_response('', resp, resp_code=302)


def test_refresh_all(app, admin_client):
    with unittest.mock.patch.object(app.dag_bag, 'collect_dags_from_db') as collect_dags_from_db:
        resp = admin_client.post("/refresh_all", follow_redirects=True)
        check_content_in_response('', resp)
        collect_dags_from_db.assert_called_once_with()


@pytest.fixture()
def new_id_example_bash_operator():
    dag_id = 'example_bash_operator'
    test_dag_id = "non_existent_dag"
    with create_session() as session:
        dag_query = session.query(DagModel).filter(DagModel.dag_id == dag_id)
        dag_query.first().tags = []  # To avoid "FOREIGN KEY constraint" error)
    with create_session() as session:
        dag_query.update({'dag_id': test_dag_id})
    yield test_dag_id
    with create_session() as session:
        session.query(DagModel).filter(DagModel.dag_id == test_dag_id).update({'dag_id': dag_id})


def test_delete_dag_button_for_dag_on_scheduler_only(admin_client, new_id_example_bash_operator):
    # Test for JIRA AIRFLOW-3233 (PR 4069):
    # The delete-dag URL should be generated correctly for DAGs
    # that exist on the scheduler (DB) but not the webserver DagBag
    test_dag_id = new_id_example_bash_operator
    resp = admin_client.get('/', follow_redirects=True)
    check_content_in_response(f'/delete?dag_id={test_dag_id}', resp)
    check_content_in_response(f"return confirmDeleteDag(this, '{test_dag_id}')", resp)


@pytest.mark.parametrize("endpoint", ["graph", "tree"])
def test_show_external_log_redirect_link_with_local_log_handler(capture_templates, admin_client, endpoint):
    """Do not show external links if log handler is local."""
    url = f'{endpoint}?dag_id=example_bash_operator'
    with capture_templates() as templates:
        admin_client.get(url, follow_redirects=True)
        ctx = templates[0].local_context
        assert not ctx['show_external_log_redirect']
        assert ctx['external_log_name'] is None


class _ExternalHandler(ExternalLoggingMixin):
    _supports_external_link = True
    LOG_NAME = 'ExternalLog'

    @property
    def log_name(self) -> str:
        return self.LOG_NAME

    def get_external_log_url(self, *args, **kwargs) -> str:
        return 'http://external-service.com'

    @property
    def supports_external_link(self) -> bool:
        return self._supports_external_link


@pytest.mark.parametrize("endpoint", ["graph", "tree"])
@unittest.mock.patch(
    'airflow.utils.log.log_reader.TaskLogReader.log_handler',
    new_callable=unittest.mock.PropertyMock,
    return_value=_ExternalHandler(),
)
def test_show_external_log_redirect_link_with_external_log_handler(
    _, capture_templates, admin_client, endpoint
):
    """Show external links if log handler is external."""
    url = f'{endpoint}?dag_id=example_bash_operator'
    with capture_templates() as templates:
        admin_client.get(url, follow_redirects=True)
        ctx = templates[0].local_context
        assert ctx['show_external_log_redirect']
        assert ctx['external_log_name'] == _ExternalHandler.LOG_NAME


@pytest.mark.parametrize("endpoint", ["graph", "tree"])
@unittest.mock.patch(
    'airflow.utils.log.log_reader.TaskLogReader.log_handler',
    new_callable=unittest.mock.PropertyMock,
    return_value=_ExternalHandler(),
)
def test_external_log_redirect_link_with_external_log_handler_not_shown(
    _external_handler, capture_templates, admin_client, endpoint
):
    """Show external links if log handler is external."""
    _external_handler.return_value._supports_external_link = False
    url = f'{endpoint}?dag_id=example_bash_operator'
    with capture_templates() as templates:
        admin_client.get(url, follow_redirects=True)
        ctx = templates[0].local_context
        assert not ctx['show_external_log_redirect']
        assert ctx['external_log_name'] is None


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
    ],
    ids=["running", "failed", "success", "retry"],
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
