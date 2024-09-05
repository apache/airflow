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

import os
import re
from unittest import mock
from unittest.mock import patch

import pytest
from markupsafe import Markup

from airflow import __version__ as airflow_version
from airflow.configuration import (
    initialize_config,
    write_default_airflow_configuration_if_needed,
    write_webserver_configuration_if_needed,
)
from airflow.plugins_manager import AirflowPlugin, EntryPointSource
from airflow.utils.docs import get_doc_url_for_provider
from airflow.utils.task_group import TaskGroup
from airflow.www.views import (
    ProviderView,
    build_scarf_url,
    get_key_paths,
    get_safe_url,
    get_task_stats_from_query,
    get_value_from_path,
)
from tests.test_utils.compat import AIRFLOW_V_3_0_PLUS
from tests.test_utils.config import conf_vars
from tests.test_utils.mock_plugins import mock_plugin_manager
from tests.test_utils.www import check_content_in_response, check_content_not_in_response

if AIRFLOW_V_3_0_PLUS:
    from airflow.utils.types import DagRunTriggeredByType

pytestmark = pytest.mark.db_test


def test_configuration_do_not_expose_config(admin_client):
    with conf_vars({("webserver", "expose_config"): "False"}):
        resp = admin_client.get("configuration", follow_redirects=True)
    check_content_in_response(
        [
            "Airflow Configuration",
            "Your Airflow administrator chose not to expose the configuration, "
            "most likely for security reasons.",
        ],
        resp,
    )


@mock.patch.dict(os.environ, {"AIRFLOW__CORE__UNIT_TEST_MODE": "False"})
def test_configuration_expose_config(admin_client):
    # make sure config is initialized (without unit test mote)
    conf = initialize_config()
    conf.validate()
    with conf_vars({("webserver", "expose_config"): "True"}):
        resp = admin_client.get("configuration", follow_redirects=True)
    check_content_in_response(["Airflow Configuration"], resp)


@mock.patch("airflow.configuration.WEBSERVER_CONFIG")
def test_webserver_configuration_config_file(mock_webserver_config_global, admin_client, tmp_path):
    import airflow.configuration

    config_file = str(tmp_path / "my_custom_webserver_config.py")
    with mock.patch.dict(os.environ, {"AIRFLOW__WEBSERVER__CONFIG_FILE": config_file}):
        conf = write_default_airflow_configuration_if_needed()
        write_webserver_configuration_if_needed(conf)
        initialize_config()
        assert airflow.configuration.WEBSERVER_CONFIG == config_file

    assert os.path.isfile(config_file)


def test_redoc_should_render_template(capture_templates, admin_client):
    from airflow.utils.docs import get_docs_url

    with capture_templates() as templates:
        resp = admin_client.get("redoc")
        check_content_in_response("Redoc", resp)

    assert len(templates) == 1
    assert templates[0].name == "airflow/redoc.html"
    assert templates[0].local_context == {
        "config_test_connection": "Disabled",
        "openapi_spec_url": "/api/v1/openapi.yaml",
        "rest_api_enabled": True,
        "get_docs_url": get_docs_url,
        "excluded_events_raw": "",
        "included_events_raw": "",
    }


def test_plugin_should_list_on_page_with_details(admin_client):
    resp = admin_client.get("/plugin")
    check_content_in_response("test_plugin", resp)
    check_content_in_response("Airflow Plugins", resp)
    check_content_in_response("source", resp)
    check_content_in_response("<em>$PLUGINS_FOLDER/</em>test_plugin.py", resp)


def test_plugin_should_list_entrypoint_on_page_with_details(admin_client):
    mock_plugin = AirflowPlugin()
    mock_plugin.name = "test_plugin"
    mock_plugin.source = EntryPointSource(
        mock.Mock(), mock.Mock(version="1.0.0", metadata={"Name": "test-entrypoint-testpluginview"})
    )
    with mock_plugin_manager(plugins=[mock_plugin]):
        resp = admin_client.get("/plugin")

    check_content_in_response("test_plugin", resp)
    check_content_in_response("Airflow Plugins", resp)
    check_content_in_response("source", resp)
    check_content_in_response("<em>test-entrypoint-testpluginview==1.0.0:</em> <Mock id=", resp)


def test_plugin_endpoint_should_not_be_unauthenticated(app):
    resp = app.test_client().get("/plugin", follow_redirects=True)
    check_content_not_in_response("test_plugin", resp)
    check_content_in_response("Sign In - Airflow", resp)


def test_should_list_providers_on_page_with_details(admin_client):
    resp = admin_client.get("/provider")
    beam_href = '<a href="https://airflow.apache.org/docs/apache-airflow-providers-apache-beam/'
    beam_text = "apache-airflow-providers-apache-beam</a>"
    beam_description = (
        '<a href="https://beam.apache.org/" target="_blank" rel="noopener noreferrer">Apache Beam</a>'
    )
    check_content_in_response(beam_href, resp)
    check_content_in_response(beam_text, resp)
    check_content_in_response(beam_description, resp)
    check_content_in_response("Providers", resp)


@pytest.mark.parametrize(
    "provider_description, expected",
    [
        (
            "`Airbyte <https://airbyte.com/>`__",
            Markup('<a href="https://airbyte.com/" target="_blank" rel="noopener noreferrer">Airbyte</a>'),
        ),
        (
            "Amazon integration (including `Amazon Web Services (AWS) <https://aws.amazon.com/>`__).",
            Markup(
                'Amazon integration (including <a href="https://aws.amazon.com/" '
                'target="_blank" rel="noopener noreferrer">Amazon Web Services (AWS)</a>).'
            ),
        ),
        (
            "`Java Database Connectivity (JDBC) <https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc"
            "/>`__",
            Markup(
                '<a href="https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/" '
                'target="_blank" rel="noopener noreferrer">Java Database Connectivity (JDBC)</a>'
            ),
        ),
        (
            "`click me <javascript:prompt(document.domain)>`__",
            Markup("`click me &lt;javascript:prompt(document.domain)&gt;`__"),
        ),
    ],
)
def test__clean_description(admin_client, provider_description, expected):
    p = ProviderView()
    actual = p._clean_description(provider_description)
    assert actual == expected


@pytest.mark.parametrize(
    "provider_name, project_url, expected",
    [
        (
            "apache-airflow-providers-airbyte",
            "Documentation, https://airflow.apache.org/docs/apache-airflow-providers-airbyte/3.8.1/",
            "https://airflow.apache.org/docs/apache-airflow-providers-airbyte/3.8.1/",
        ),
        (
            "apache-airflow-providers-amazon",
            "Documentation, https://airflow.apache.org/docs/apache-airflow-providers-amazon/8.25.0/",
            "https://airflow.apache.org/docs/apache-airflow-providers-amazon/8.25.0/",
        ),
        (
            "apache-airflow-providers-apache-druid",
            "Documentation, javascript:prompt(document.domain)",
            # the default one is returned
            "https://airflow.apache.org/docs/apache-airflow-providers-apache-druid/1.0.0/",
        ),
    ],
)
@patch("airflow.utils.docs.get_project_url_from_metadata")
def test_get_doc_url_for_provider(
    mock_get_project_url_from_metadata, admin_client, provider_name, project_url, expected
):
    mock_get_project_url_from_metadata.return_value = [project_url]
    actual = get_doc_url_for_provider(provider_name, "1.0.0")
    assert actual == expected


def test_endpoint_should_not_be_unauthenticated(app):
    resp = app.test_client().get("/provider", follow_redirects=True)
    check_content_not_in_response("Providers", resp)
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
    "url",
    [
        "/taskinstance/list/?_flt_1_try_number=0",  # greater than
        "/taskinstance/list/?_flt_2_try_number=5",  # less than
    ],
)
def test_try_number_filter(admin_client, url):
    resp = admin_client.get(url)
    # Ensure that the taskInstance view can filter on gt / lt try_number
    check_content_in_response("List Task Instance", resp)


@pytest.mark.parametrize(
    "url, content",
    [
        (
            "/taskinstance/list/?_flt_3_dag_id=test_dag",
            "List Task Instance",
        )
    ],
    ids=["instance"],
)
def test_task_dag_id_equals_filter(admin_client, url, content):
    resp = admin_client.get(url)
    # We aren't checking the logic of the dag_id filter itself (that is built
    # in to FAB) but simply that dag_id filter was run
    check_content_in_response(content, resp)


@pytest.mark.parametrize(
    "test_url, expected_url",
    [
        ("", "/home"),
        ("javascript:alert(1)", "/home"),
        (" javascript:alert(1)", "/home"),
        ("http://google.com", "/home"),
        ("google.com", "http://localhost:8080/google.com"),
        ("\\/google.com", "http://localhost:8080/\\/google.com"),
        ("//google.com", "/home"),
        ("\\/\\/google.com", "http://localhost:8080/\\/\\/google.com"),
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


@pytest.fixture
def test_app():
    from airflow.www import app

    return app.create_app(testing=True)


def test_mark_task_instance_state(test_app):
    """
    Test that _mark_task_instance_state() does all three things:
    - Marks the given TaskInstance as SUCCESS;
    - Clears downstream TaskInstances in FAILED/UPSTREAM_FAILED state;
    - Set DagRun to QUEUED.
    """
    from airflow.models.dag import DAG
    from airflow.models.dagbag import DagBag
    from airflow.models.taskinstance import TaskInstance
    from airflow.operators.empty import EmptyOperator
    from airflow.utils.session import create_session
    from airflow.utils.state import State
    from airflow.utils.timezone import datetime
    from airflow.utils.types import DagRunType
    from airflow.www.views import Airflow
    from tests.test_utils.db import clear_db_runs

    clear_db_runs()
    start_date = datetime(2020, 1, 1)
    with DAG("test_mark_task_instance_state", start_date=start_date, schedule="0 0 * * *") as dag:
        task_1 = EmptyOperator(task_id="task_1")
        task_2 = EmptyOperator(task_id="task_2")
        task_3 = EmptyOperator(task_id="task_3")
        task_4 = EmptyOperator(task_id="task_4")
        task_5 = EmptyOperator(task_id="task_5")

        task_1 >> [task_2, task_3, task_4, task_5]

    triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
    dagrun = dag.create_dagrun(
        start_date=start_date,
        execution_date=start_date,
        data_interval=(start_date, start_date),
        state=State.FAILED,
        run_type=DagRunType.SCHEDULED,
        **triggered_by_kwargs,
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

    test_app.dag_bag = DagBag(dag_folder="/dev/null", include_examples=False)
    test_app.dag_bag.bag_dag(dag=dag)

    with test_app.test_request_context():
        view = Airflow()

        view._mark_task_instance_state(
            dag_id=dag.dag_id,
            run_id=dagrun.run_id,
            task_id=task_1.task_id,
            map_indexes=None,
            origin="",
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
        # dagrun should be set to QUEUED
        assert dagrun.get_state() == State.QUEUED


def test_mark_task_group_state(test_app):
    """
    Test that _mark_task_group_state() does all three things:
    - Marks the given TaskGroup as SUCCESS;
    - Clears downstream TaskInstances in FAILED/UPSTREAM_FAILED state;
    - Set DagRun to QUEUED.
    """
    from airflow.models.dag import DAG
    from airflow.models.dagbag import DagBag
    from airflow.models.taskinstance import TaskInstance
    from airflow.operators.empty import EmptyOperator
    from airflow.utils.session import create_session
    from airflow.utils.state import State
    from airflow.utils.timezone import datetime
    from airflow.utils.types import DagRunType
    from airflow.www.views import Airflow
    from tests.test_utils.db import clear_db_runs

    clear_db_runs()
    start_date = datetime(2020, 1, 1)
    with DAG("test_mark_task_group_state", start_date=start_date, schedule="0 0 * * *") as dag:
        start = EmptyOperator(task_id="start")

        with TaskGroup("section_1", tooltip="Tasks for section_1") as section_1:
            task_1 = EmptyOperator(task_id="task_1")
            task_2 = EmptyOperator(task_id="task_2")
            task_3 = EmptyOperator(task_id="task_3")

            task_1 >> [task_2, task_3]

        task_4 = EmptyOperator(task_id="task_4")
        task_5 = EmptyOperator(task_id="task_5")
        task_6 = EmptyOperator(task_id="task_6")
        task_7 = EmptyOperator(task_id="task_7")
        task_8 = EmptyOperator(task_id="task_8")

        start >> section_1 >> [task_4, task_5, task_6, task_7, task_8]

    triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
    dagrun = dag.create_dagrun(
        start_date=start_date,
        execution_date=start_date,
        data_interval=(start_date, start_date),
        state=State.FAILED,
        run_type=DagRunType.SCHEDULED,
        **triggered_by_kwargs,
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
        get_task_instance(session, task_4).state = State.SUCCESS
        get_task_instance(session, task_5).state = State.UPSTREAM_FAILED
        get_task_instance(session, task_6).state = State.FAILED
        get_task_instance(session, task_7).state = State.SKIPPED

        session.commit()

    test_app.dag_bag = DagBag(dag_folder="/dev/null", include_examples=False)
    test_app.dag_bag.bag_dag(dag=dag)

    with test_app.test_request_context():
        view = Airflow()

        view._mark_task_group_state(
            dag_id=dag.dag_id,
            run_id=dagrun.run_id,
            group_id=section_1.group_id,
            origin="",
            upstream=False,
            downstream=False,
            future=False,
            past=False,
            state=State.SUCCESS,
        )

    with create_session() as session:
        # After _mark_task_group_state, task_1 is marked as SUCCESS
        assert get_task_instance(session, task_1).state == State.SUCCESS
        # task_2 should remain as SUCCESS
        assert get_task_instance(session, task_2).state == State.SUCCESS
        # task_3 should be marked as SUCCESS
        assert get_task_instance(session, task_3).state == State.SUCCESS
        # task_4 should remain as SUCCESS
        assert get_task_instance(session, task_4).state == State.SUCCESS
        # task_5 and task_6 are cleared because they were in FAILED/UPSTREAM_FAILED state
        assert get_task_instance(session, task_5).state == State.NONE
        assert get_task_instance(session, task_6).state == State.NONE
        # task_7 remains as SKIPPED
        assert get_task_instance(session, task_7).state == State.SKIPPED
        dagrun.refresh_from_db(session=session)
        # dagrun should be set to QUEUED
        assert dagrun.get_state() == State.QUEUED


TEST_CONTENT_DICT = {"key1": {"key2": "val2", "key3": "val3", "key4": {"key5": "val5"}}}


@pytest.mark.parametrize(
    "test_content_dict, expected_paths", [(TEST_CONTENT_DICT, ("key1.key2", "key1.key3", "key1.key4.key5"))]
)
def test_generate_key_paths(test_content_dict, expected_paths):
    for key_path in get_key_paths(test_content_dict):
        assert key_path in expected_paths


@pytest.mark.parametrize(
    "test_content_dict, test_key_path, expected_value",
    [
        (TEST_CONTENT_DICT, "key1.key2", "val2"),
        (TEST_CONTENT_DICT, "key1.key3", "val3"),
        (TEST_CONTENT_DICT, "key1.key4.key5", "val5"),
    ],
)
def test_get_value_from_path(test_content_dict, test_key_path, expected_value):
    assert expected_value == get_value_from_path(test_key_path, test_content_dict)


def test_get_task_stats_from_query():
    query_data = [
        ["dag1", "queued", True, 1],
        ["dag1", "running", True, 2],
        ["dag1", "success", False, 3],
        ["dag2", "running", True, 4],
        ["dag2", "success", True, 5],
        ["dag3", "success", False, 6],
    ]
    expected_data = {
        "dag1": {
            "queued": 1,
            "running": 2,
        },
        "dag2": {
            "running": 4,
            "success": 5,
        },
        "dag3": {
            "success": 6,
        },
    }

    data = get_task_stats_from_query(query_data)
    assert data == expected_data


INVALID_DATETIME_RESPONSE = re.compile(r"Invalid datetime: &#x?\d+;invalid&#x?\d+;")


@pytest.mark.parametrize(
    "url, content",
    [
        (
            "/rendered-templates?execution_date=invalid",
            INVALID_DATETIME_RESPONSE,
        ),
        (
            "/log?dag_id=tutorial&execution_date=invalid",
            INVALID_DATETIME_RESPONSE,
        ),
        (
            "/redirect_to_external_log?execution_date=invalid",
            INVALID_DATETIME_RESPONSE,
        ),
        (
            "/task?execution_date=invalid",
            INVALID_DATETIME_RESPONSE,
        ),
        (
            "dags/example_bash_operator/graph?execution_date=invalid",
            INVALID_DATETIME_RESPONSE,
        ),
        (
            "dags/example_bash_operator/gantt?execution_date=invalid",
            INVALID_DATETIME_RESPONSE,
        ),
        (
            "extra_links?execution_date=invalid",
            INVALID_DATETIME_RESPONSE,
        ),
    ],
)
def test_invalid_dates(app, admin_client, url, content):
    """Test invalid date format doesn't crash page."""
    resp = admin_client.get(url, follow_redirects=True)

    assert resp.status_code == 400
    assert re.search(content, resp.get_data().decode())


@pytest.mark.parametrize("enabled", [False, True])
@patch("airflow.utils.usage_data_collection.get_platform_info", return_value=("Linux", "x86_64"))
@patch("airflow.utils.usage_data_collection.get_database_version", return_value="12.3")
@patch("airflow.utils.usage_data_collection.get_database_name", return_value="postgres")
@patch("airflow.utils.usage_data_collection.get_executor", return_value="SequentialExecutor")
@patch("airflow.utils.usage_data_collection.get_python_version", return_value="3.8")
@patch("airflow.utils.usage_data_collection.get_plugin_counts")
def test_build_scarf_url(
    get_plugin_counts,
    get_python_version,
    get_executor,
    get_database_name,
    get_database_version,
    get_platform_info,
    enabled,
):
    get_plugin_counts.return_value = {
        "plugins": 10,
        "flask_blueprints": 15,
        "appbuilder_views": 20,
        "appbuilder_menu_items": 25,
        "timetables": 30,
    }
    with patch("airflow.settings.is_usage_data_collection_enabled", return_value=enabled):
        result = build_scarf_url(5)
        expected_url = (
            "https://apacheairflow.gateway.scarf.sh/webserver/"
            f"{airflow_version}/3.8/Linux/x86_64/postgres/12.3/SequentialExecutor/1-5"
            f"/6-10/15/20/25/21-50"
        )
        if enabled:
            assert result == expected_url
        else:
            assert result == ""
