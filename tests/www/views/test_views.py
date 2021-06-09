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
import os
from unittest import mock

import pytest

from airflow.configuration import initialize_config
from airflow.plugins_manager import AirflowPlugin, EntryPointSource
from airflow.www.views import get_safe_url, truncate_task_duration
from tests.test_utils.config import conf_vars
from tests.test_utils.mock_plugins import mock_plugin_manager
from tests.test_utils.www import check_content_in_response, check_content_not_in_response


def test_configuration_do_not_expose_config(admin_client):
    with conf_vars({('webserver', 'expose_config'): 'False'}):
        resp = admin_client.get('configuration', follow_redirects=True)
    check_content_in_response(
        [
            'Airflow Configuration',
            '# Your Airflow administrator chose not to expose the configuration, '
            'most likely for security reasons.',
        ],
        resp,
    )


@mock.patch.dict(os.environ, {"AIRFLOW__CORE__UNIT_TEST_MODE": "False"})
def test_configuration_expose_config(admin_client):
    # make sure config is initialized (without unit test mote)
    conf = initialize_config()
    conf.validate()
    with conf_vars({('webserver', 'expose_config'): 'True'}):
        resp = admin_client.get('configuration', follow_redirects=True)
    check_content_in_response(['Airflow Configuration', 'Running Configuration'], resp)


def test_redoc_should_render_template(capture_templates, admin_client):
    with capture_templates() as templates:
        resp = admin_client.get('redoc')
        check_content_in_response('Redoc', resp)

    assert len(templates) == 1
    assert templates[0].name == 'airflow/redoc.html'
    assert templates[0].local_context == {
        'openapi_spec_url': '/api/v1/openapi.yaml',
        'rest_api_enabled': True,
    }


def test_plugin_should_list_on_page_with_details(admin_client):
    resp = admin_client.get('/plugin')
    check_content_in_response("test_plugin", resp)
    check_content_in_response("Airflow Plugins", resp)
    check_content_in_response("source", resp)
    check_content_in_response("<em>$PLUGINS_FOLDER/</em>test_plugin.py", resp)


def test_plugin_should_list_entrypoint_on_page_with_details(admin_client):
    mock_plugin = AirflowPlugin()
    mock_plugin.name = "test_plugin"
    mock_plugin.source = EntryPointSource(
        mock.Mock(), mock.Mock(version='1.0.0', metadata={'name': 'test-entrypoint-testpluginview'})
    )
    with mock_plugin_manager(plugins=[mock_plugin]):
        resp = admin_client.get('/plugin')

    check_content_in_response("test_plugin", resp)
    check_content_in_response("Airflow Plugins", resp)
    check_content_in_response("source", resp)
    check_content_in_response("<em>test-entrypoint-testpluginview==1.0.0:</em> <Mock id=", resp)


def test_plugin_endpoint_should_not_be_unauthenticated(app):
    resp = app.test_client().get('/plugin', follow_redirects=True)
    check_content_not_in_response("test_plugin", resp)
    check_content_in_response("Sign In - Airflow", resp)


@pytest.mark.parametrize(
    "url, content",
    [
        (
            "/taskinstance/list/?_flt_0_execution_date=2018-10-09+22:44:31",
            "List Task Instance",
        ),
        (
            "/taskreschedule/list/?_flt_0_execution_date=2018-10-09+22:44:31",
            "List Task Reschedule",
        ),
    ],
    ids=["instance", "reschedule"],
)
def test_task_start_date_filter(admin_client, url, content):
    resp = admin_client.get(url)
    # We aren't checking the logic of the date filter itself (that is built
    # in to FAB) but simply that our UTC conversion was run - i.e. it
    # doesn't blow up!
    check_content_in_response(content, resp)


@pytest.mark.parametrize(
    "test_url, expected_url",
    [
        ("", "/home"),
        ("http://google.com", "/home"),
        ("36539'%3balert(1)%2f%2f166", "/home"),
        (
            "http://localhost:8080/trigger?dag_id=test&origin=36539%27%3balert(1)%2f%2f166&abc=2",
            "/home",
        ),
        (
            "http://localhost:8080/trigger?dag_id=test_dag&origin=%2Ftree%3Fdag_id%test_dag';alert(33)//",
            "/home",
        ),
        (
            "http://localhost:8080/trigger?dag_id=test_dag&origin=%2Ftree%3Fdag_id%3Dtest_dag",
            "http://localhost:8080/trigger?dag_id=test_dag&origin=%2Ftree%3Fdag_id%3Dtest_dag",
        ),
    ],
)
@mock.patch("airflow.www.views.url_for")
def test_get_safe_url(mock_url_for, app, test_url, expected_url):
    mock_url_for.return_value = "/home"
    with app.test_request_context(base_url="http://localhost:8080"):
        assert get_safe_url(test_url) == expected_url


@pytest.mark.parametrize(
    "test_duration, expected_duration",
    [
        (0.12345, 0.123),
        (0.12355, 0.124),
        (3.12, 3.12),
        (9.99999, 10.0),
        (10.01232, 10),
    ],
)
def test_truncate_task_duration(test_duration, expected_duration):
    assert truncate_task_duration(test_duration) == expected_duration


@pytest.fixture
def test_app():
    from airflow.www import app

    return app.create_app(testing=True)


def test_mark_task_instance_state(test_app):
    """
    Test that _mark_task_instance_state() does all three things:
    - Marks the given TaskInstance as SUCCESS;
    - Clears downstream TaskInstances in FAILED/UPSTREAM_FAILED state;
    - Set DagRun to RUNNING.
    """
    from airflow.models import DAG, DagBag, TaskInstance
    from airflow.operators.dummy import DummyOperator
    from airflow.utils.session import create_session
    from airflow.utils.state import State
    from airflow.utils.timezone import datetime
    from airflow.utils.types import DagRunType
    from airflow.www.views import Airflow

    start_date = datetime(2020, 1, 1)
    with DAG("test_mark_task_instance_state", start_date=start_date) as dag:
        task_1 = DummyOperator(task_id="task_1")
        task_2 = DummyOperator(task_id="task_2")
        task_3 = DummyOperator(task_id="task_3")
        task_4 = DummyOperator(task_id="task_4")
        task_5 = DummyOperator(task_id="task_5")

        task_1 >> [task_2, task_3, task_4, task_5]

    dagrun = dag.create_dagrun(
        start_date=start_date, execution_date=start_date, state=State.FAILED, run_type=DagRunType.SCHEDULED
    )

    def get_task_instance(session, task):
        return (
            session.query(TaskInstance)
            .filter(
                TaskInstance.dag_id == dag.dag_id,
                TaskInstance.task_id == task.task_id,
                TaskInstance.execution_date == start_date,
            )
            .one()
        )

    with create_session() as session:
        get_task_instance(session, task_1).state = State.FAILED
        get_task_instance(session, task_2).state = State.SUCCESS
        get_task_instance(session, task_3).state = State.UPSTREAM_FAILED
        get_task_instance(session, task_4).state = State.FAILED
        get_task_instance(session, task_5).state = State.SKIPPED

        session.commit()

    test_app.dag_bag = DagBag(dag_folder='/dev/null', include_examples=False)
    test_app.dag_bag.bag_dag(dag=dag, root_dag=dag)

    with test_app.test_request_context():
        view = Airflow()

        view._mark_task_instance_state(
            dag_id=dag.dag_id,
            task_id=task_1.task_id,
            origin="",
            execution_date=start_date.isoformat(),
            confirmed=True,
            upstream=False,
            downstream=False,
            future=False,
            past=False,
            state=State.SUCCESS,
        )

    with create_session() as session:
        # After _mark_task_instance_state, task_1 is marked as SUCCESS
        assert get_task_instance(session, task_1).state == State.SUCCESS
        # task_2 remains as SUCCESS
        assert get_task_instance(session, task_2).state == State.SUCCESS
        # task_3 and task_4 are cleared because they were in FAILED/UPSTREAM_FAILED state
        assert get_task_instance(session, task_3).state == State.NONE
        assert get_task_instance(session, task_4).state == State.NONE
        # task_5 remains as SKIPPED
        assert get_task_instance(session, task_5).state == State.SKIPPED
        dagrun.refresh_from_db(session=session)
        # dagrun should be set to RUNNING
        assert dagrun.get_state() == State.RUNNING
