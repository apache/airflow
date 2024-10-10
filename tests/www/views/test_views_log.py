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

import copy
import logging
import logging.config
import pathlib
import shutil
import sys
import tempfile
import unittest.mock
import urllib.parse

import pytest

from airflow import settings
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.models import DagBag, DagRun
from airflow.models.tasklog import LogTemplate
from airflow.utils import timezone
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import ExternalLoggingMixin
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType
from airflow.www.app import create_app

from dev.tests_common.test_utils.compat import AIRFLOW_V_3_0_PLUS
from dev.tests_common.test_utils.config import conf_vars
from dev.tests_common.test_utils.db import clear_db_dags, clear_db_runs
from dev.tests_common.test_utils.decorators import dont_initialize_flask_app_submodules
from dev.tests_common.test_utils.www import client_with_login

if AIRFLOW_V_3_0_PLUS:
    from airflow.utils.types import DagRunTriggeredByType

pytestmark = pytest.mark.db_test

DAG_ID = "dag_for_testing_log_view"
DAG_ID_REMOVED = "removed_dag_for_testing_log_view"
TASK_ID = "task_for_testing_log_view"
DEFAULT_DATE = timezone.datetime(2017, 9, 1)
STR_DEFAULT_DATE = urllib.parse.quote(DEFAULT_DATE.strftime("%Y-%m-%dT%H:%M:%S.%f%z"))
ENDPOINT = f"log?dag_id={DAG_ID}&task_id={TASK_ID}&execution_date={STR_DEFAULT_DATE}"


@pytest.fixture(scope="module", autouse=True)
def backup_modules():
    """Make sure that the configure_logging is not cached."""
    return dict(sys.modules)


@pytest.fixture(scope="module")
def log_path(tmp_path_factory):
    return tmp_path_factory.mktemp("logs")


@pytest.fixture(scope="module")
def log_app(backup_modules, log_path):
    @dont_initialize_flask_app_submodules(
        skip_all_except=[
            "init_appbuilder",
            "init_jinja_globals",
            "init_appbuilder_views",
            "init_api_connexion",
        ]
    )
    @conf_vars(
        {
            ("logging", "logging_config_class"): "airflow_local_settings.LOGGING_CONFIG",
            ("fab", "auth_rate_limited"): "False",
        }
    )
    def factory():
        app = create_app(testing=True)
        app.config["WTF_CSRF_ENABLED"] = False
        settings.configure_orm()
        security_manager = app.appbuilder.sm
        if not security_manager.find_user(username="test"):
            security_manager.add_user(
                username="test",
                first_name="test",
                last_name="test",
                email="test@fab.org",
                role=security_manager.find_role("Admin"),
                password="test",
            )
        return app

    # Create a custom logging configuration
    logging_config = copy.deepcopy(DEFAULT_LOGGING_CONFIG)
    logging_config["handlers"]["task"]["base_log_folder"] = str(log_path)

    with tempfile.TemporaryDirectory() as settings_dir:
        local_settings = pathlib.Path(settings_dir, "airflow_local_settings.py")
        local_settings.write_text(f"LOGGING_CONFIG = {logging_config!r}")
        sys.path.append(settings_dir)
        yield factory()
        sys.path.remove(settings_dir)

    logging.config.dictConfig(DEFAULT_LOGGING_CONFIG)


@pytest.fixture(autouse=True)
def _reset_modules_after_every_test(backup_modules):
    yield
    # Remove any new modules imported during the test run.
    # This lets us import the same source files for more than one test.
    for mod in [m for m in sys.modules if m not in backup_modules]:
        del sys.modules[mod]


@pytest.fixture(autouse=True)
def dags(log_app, create_dummy_dag, session):
    dag, _ = create_dummy_dag(
        dag_id=DAG_ID,
        task_id=TASK_ID,
        start_date=DEFAULT_DATE,
        with_dagrun_type=None,
        session=session,
    )
    dag_removed, _ = create_dummy_dag(
        dag_id=DAG_ID_REMOVED,
        task_id=TASK_ID,
        start_date=DEFAULT_DATE,
        with_dagrun_type=None,
        session=session,
    )

    bag = DagBag(include_examples=False)
    bag.bag_dag(dag=dag)
    bag.bag_dag(dag=dag_removed)
    bag.sync_to_db(session=session)
    log_app.dag_bag = bag

    yield dag, dag_removed

    clear_db_dags()


@pytest.fixture(autouse=True)
def tis(dags, session):
    dag, dag_removed = dags
    triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
    dagrun = dag.create_dagrun(
        run_type=DagRunType.SCHEDULED,
        execution_date=DEFAULT_DATE,
        data_interval=(DEFAULT_DATE, DEFAULT_DATE),
        start_date=DEFAULT_DATE,
        state=DagRunState.RUNNING,
        session=session,
        **triggered_by_kwargs,
    )
    (ti,) = dagrun.task_instances
    ti.try_number = 1
    ti.hostname = "localhost"
    dagrun_removed = dag_removed.create_dagrun(
        run_type=DagRunType.SCHEDULED,
        execution_date=DEFAULT_DATE,
        data_interval=(DEFAULT_DATE, DEFAULT_DATE),
        start_date=DEFAULT_DATE,
        state=DagRunState.RUNNING,
        session=session,
        **triggered_by_kwargs,
    )
    (ti_removed_dag,) = dagrun_removed.task_instances
    ti_removed_dag.try_number = 1

    yield ti, ti_removed_dag

    clear_db_runs()


@pytest.fixture
def create_expected_log_file(log_path, tis):
    ti, _ = tis
    handler = FileTaskHandler(log_path)

    def create_expected_log_file(try_number):
        ti.try_number = 1
        handler.set_context(ti)
        handler.emit(logging.makeLogRecord({"msg": "Log for testing."}))
        handler.flush()

    yield create_expected_log_file
    # log_path fixture is used in log_app, so both have "module" scope. Because of that
    # log_path isn't deleted automatically by pytest between tests
    # We delete created log files manually to make sure tests do not reuse logs created by other tests
    for sub_path in log_path.iterdir():
        shutil.rmtree(sub_path)


@pytest.fixture
def log_admin_client(log_app):
    return client_with_login(log_app, username="test", password="test")


@pytest.mark.parametrize(
    "state, try_number, num_logs",
    [
        (None, 0, 0),
        (TaskInstanceState.UP_FOR_RETRY, 2, 2),
        (TaskInstanceState.UP_FOR_RESCHEDULE, 0, 0),
        (TaskInstanceState.UP_FOR_RESCHEDULE, 1, 1),
        (TaskInstanceState.RUNNING, 1, 1),
        (TaskInstanceState.SUCCESS, 1, 1),
        (TaskInstanceState.FAILED, 3, 3),
    ],
    ids=[
        "none",
        "up-for-retry",
        "up-for-reschedule-0",
        "up-for-reschedule-1",
        "running",
        "success",
        "failed",
    ],
)
def test_get_file_task_log(log_admin_client, tis, state, try_number, num_logs):
    ti, _ = tis
    with create_session() as session:
        ti.state = state
        ti.try_number = try_number
        session.merge(ti)

    response = log_admin_client.get(
        ENDPOINT,
        data={"username": "test", "password": "test"},
        follow_redirects=True,
    )
    assert response.status_code == 200

    data = response.data.decode()
    assert "Log by attempts" in data
    for num in range(1, num_logs + 1):
        assert f"log-group-{num}" in data
    assert "log-group-0" not in data
    assert f"log-group-{num_logs + 1}" not in data


def test_get_logs_with_metadata_as_download_file(log_admin_client, create_expected_log_file):
    url_template = (
        "get_logs_with_metadata?dag_id={}&"
        "task_id={}&execution_date={}&"
        "try_number={}&metadata={}&format=file"
    )
    try_number = 1
    create_expected_log_file(try_number)
    date = DEFAULT_DATE.isoformat()
    url = url_template.format(
        DAG_ID,
        TASK_ID,
        urllib.parse.quote_plus(date),
        try_number,
        "{}",
    )
    response = log_admin_client.get(url)

    content_disposition = response.headers["Content-Disposition"]
    assert content_disposition.startswith("attachment")
    assert (
        f"dag_id={DAG_ID}/run_id=scheduled__{date}/task_id={TASK_ID}/attempt={try_number}.log"
        in content_disposition
    )
    assert 200 == response.status_code
    content = response.data.decode("utf-8")
    assert "Log for testing." in content
    assert "localhost\n" in content


DIFFERENT_LOG_FILENAME = "{{ ti.dag_id }}/{{ ti.run_id }}/{{ ti.task_id }}/{{ try_number }}.log"


@pytest.fixture
def dag_run_with_log_filename(tis):
    run_filters = [DagRun.dag_id == DAG_ID, DagRun.execution_date == DEFAULT_DATE]
    with create_session() as session:
        log_template = session.merge(
            LogTemplate(filename=DIFFERENT_LOG_FILENAME, elasticsearch_id="irrelevant")
        )
        session.flush()  # To populate 'log_template.id'.
        run_query = session.query(DagRun).filter(*run_filters)
        run_query.update({"log_template_id": log_template.id})
        dag_run = run_query.one()
    # Dag has been updated, replace Dag in Task Instance
    ti, _ = tis
    ti.dag_run = dag_run
    yield dag_run
    with create_session() as session:
        session.query(DagRun).filter(*run_filters).update({"log_template_id": None})
        session.query(LogTemplate).filter(LogTemplate.id == log_template.id).delete()


def test_get_logs_for_changed_filename_format_db(
    log_admin_client, dag_run_with_log_filename, create_expected_log_file
):
    try_number = 1
    create_expected_log_file(try_number)
    url = (
        f"get_logs_with_metadata?dag_id={dag_run_with_log_filename.dag_id}&"
        f"task_id={TASK_ID}&"
        f"execution_date={urllib.parse.quote_plus(dag_run_with_log_filename.logical_date.isoformat())}&"
        f"try_number={try_number}&metadata={{}}&format=file"
    )
    response = log_admin_client.get(url)

    # Should find the log under corresponding db entry.
    assert 200 == response.status_code
    assert "Log for testing." in response.data.decode("utf-8")
    content_disposition = response.headers["Content-Disposition"]
    expected_filename = (
        f"{dag_run_with_log_filename.dag_id}/{dag_run_with_log_filename.run_id}/{TASK_ID}/{try_number}.log"
    )
    assert content_disposition.startswith("attachment")
    assert expected_filename in content_disposition


@unittest.mock.patch(
    "airflow.utils.log.file_task_handler.FileTaskHandler.read",
    side_effect=[
        ([[("default_log", "1st line")]], [{}]),
        ([[("default_log", "2nd line")]], [{"end_of_log": False}]),
        ([[("default_log", "3rd line")]], [{"end_of_log": True}]),
        ([[("default_log", "should never be read")]], [{"end_of_log": True}]),
    ],
)
def test_get_logs_with_metadata_as_download_large_file(_, log_admin_client):
    url_template = (
        "get_logs_with_metadata?dag_id={}&"
        "task_id={}&execution_date={}&"
        "try_number={}&metadata={}&format=file"
    )
    try_number = 1
    url = url_template.format(
        DAG_ID,
        TASK_ID,
        urllib.parse.quote_plus(DEFAULT_DATE.isoformat()),
        try_number,
        "{}",
    )
    response = log_admin_client.get(url)

    data = response.data.decode()
    assert "1st line" in data
    assert "2nd line" in data
    assert "3rd line" in data
    assert "should never be read" not in data


@pytest.mark.parametrize("metadata", ["null", "{}"])
def test_get_logs_with_metadata(log_admin_client, metadata, create_expected_log_file):
    url_template = "get_logs_with_metadata?dag_id={}&task_id={}&execution_date={}&try_number={}&metadata={}"
    try_number = 1
    create_expected_log_file(try_number)
    response = log_admin_client.get(
        url_template.format(
            DAG_ID,
            TASK_ID,
            urllib.parse.quote_plus(DEFAULT_DATE.isoformat()),
            try_number,
            metadata,
        ),
        data={"username": "test", "password": "test"},
        follow_redirects=True,
    )
    assert 200 == response.status_code

    data = response.data.decode()
    assert '"message":' in data
    assert '"metadata":' in data
    assert "Log for testing." in data


def test_get_logs_with_invalid_metadata(log_admin_client):
    """Test invalid metadata JSON returns error message"""
    metadata = "invalid"
    url_template = "get_logs_with_metadata?dag_id={}&task_id={}&execution_date={}&try_number={}&metadata={}"
    response = log_admin_client.get(
        url_template.format(
            DAG_ID,
            TASK_ID,
            urllib.parse.quote_plus(DEFAULT_DATE.isoformat()),
            1,
            metadata,
        ),
        data={"username": "test", "password": "test"},
        follow_redirects=True,
    )

    assert response.status_code == 400
    assert response.json == {"error": "Invalid JSON metadata"}


@unittest.mock.patch(
    "airflow.utils.log.file_task_handler.FileTaskHandler.read",
    return_value=(["airflow log line"], [{"end_of_log": True}]),
)
def test_get_logs_with_metadata_for_removed_dag(_, log_admin_client):
    url_template = "get_logs_with_metadata?dag_id={}&task_id={}&execution_date={}&try_number={}&metadata={}"
    response = log_admin_client.get(
        url_template.format(
            DAG_ID_REMOVED,
            TASK_ID,
            urllib.parse.quote_plus(DEFAULT_DATE.isoformat()),
            1,
            "{}",
        ),
        data={"username": "test", "password": "test"},
        follow_redirects=True,
    )
    assert 200 == response.status_code

    data = response.data.decode()
    assert '"message":' in data
    assert '"metadata":' in data
    assert "airflow log line" in data


def test_get_logs_response_with_ti_equal_to_none(log_admin_client):
    url_template = (
        "get_logs_with_metadata?dag_id={}&"
        "task_id={}&execution_date={}&"
        "try_number={}&metadata={}&format=file"
    )
    try_number = 1
    url = url_template.format(
        DAG_ID,
        "Non_Existing_ID",
        urllib.parse.quote_plus(DEFAULT_DATE.isoformat()),
        try_number,
        "{}",
    )
    response = log_admin_client.get(url)

    data = response.json
    assert "message" in data
    assert "error" in data
    assert "*** Task instance did not exist in the DB\n" == data["message"]


def test_get_logs_with_json_response_format(log_admin_client, create_expected_log_file):
    url_template = (
        "get_logs_with_metadata?dag_id={}&"
        "task_id={}&execution_date={}&"
        "try_number={}&metadata={}&format=json"
    )
    try_number = 1
    create_expected_log_file(try_number)
    url = url_template.format(
        DAG_ID,
        TASK_ID,
        urllib.parse.quote_plus(DEFAULT_DATE.isoformat()),
        try_number,
        "{}",
    )
    response = log_admin_client.get(url)
    assert 200 == response.status_code

    assert "message" in response.json
    assert "metadata" in response.json
    assert "Log for testing." in response.json["message"][0][1]


def test_get_logs_invalid_execution_data_format(log_admin_client):
    url_template = (
        "get_logs_with_metadata?dag_id={}&"
        "task_id={}&execution_date={}&"
        "try_number={}&metadata={}&format=file"
    )
    try_number = 1
    url = url_template.format(
        DAG_ID,
        TASK_ID,
        urllib.parse.quote_plus("Tuesday February 27, 2024"),
        try_number,
        "{}",
    )
    response = log_admin_client.get(url)
    assert response.status_code == 400
    assert response.json == {
        "error": (
            "Given execution date 'Tuesday February 27, 2024' could not be identified as a date. "
            "Example date format: 2015-11-16T14:34:15+00:00"
        )
    }


@unittest.mock.patch("airflow.www.views.TaskLogReader")
def test_get_logs_for_handler_without_read_method(mock_reader, log_admin_client):
    type(mock_reader.return_value).supports_read = unittest.mock.PropertyMock(return_value=False)
    url_template = (
        "get_logs_with_metadata?dag_id={}&"
        "task_id={}&execution_date={}&"
        "try_number={}&metadata={}&format=json"
    )
    try_number = 1
    url = url_template.format(
        DAG_ID,
        TASK_ID,
        urllib.parse.quote_plus(DEFAULT_DATE.isoformat()),
        try_number,
        "{}",
    )
    response = log_admin_client.get(url)
    assert 200 == response.status_code

    data = response.json
    assert "message" in data
    assert "metadata" in data
    assert "Task log handler does not support read logs." in data["message"]


@pytest.mark.parametrize("task_id", ["inexistent", TASK_ID])
def test_redirect_to_external_log_with_local_log_handler(log_admin_client, task_id):
    """Redirect to home if TI does not exist or if log handler is local"""
    url_template = "redirect_to_external_log?dag_id={}&task_id={}&execution_date={}&try_number={}"
    try_number = 1
    url = url_template.format(
        DAG_ID,
        task_id,
        urllib.parse.quote_plus(DEFAULT_DATE.isoformat()),
        try_number,
    )
    response = log_admin_client.get(url)
    assert 302 == response.status_code
    assert "/home" == response.headers["Location"]


class _ExternalHandler(ExternalLoggingMixin):
    EXTERNAL_URL = "http://external-service.com"

    @property
    def log_name(self) -> str:
        return "ExternalLog"

    def get_external_log_url(self, *args, **kwargs) -> str:
        return self.EXTERNAL_URL

    @property
    def supports_external_link(self) -> bool:
        return True


@unittest.mock.patch(
    "airflow.utils.log.log_reader.TaskLogReader.log_handler",
    new_callable=unittest.mock.PropertyMock,
    return_value=_ExternalHandler(),
)
def test_redirect_to_external_log_with_external_log_handler(_, log_admin_client):
    url_template = "redirect_to_external_log?dag_id={}&task_id={}&execution_date={}&try_number={}"
    try_number = 1
    url = url_template.format(
        DAG_ID,
        TASK_ID,
        urllib.parse.quote_plus(DEFAULT_DATE.isoformat()),
        try_number,
    )
    response = log_admin_client.get(url)
    assert 302 == response.status_code
    assert _ExternalHandler.EXTERNAL_URL == response.headers["Location"]
