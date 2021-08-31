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

from unittest import mock

import flask
import pytest

from airflow.dag_processing.processor import DagFileProcessor
from airflow.security import permissions
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.www.views import FILTER_STATUS_COOKIE, FILTER_TAGS_COOKIE
from tests.test_utils.api_connexion_utils import create_user
from tests.test_utils.db import clear_db_dags, clear_db_import_errors, clear_db_serialized_dags
from tests.test_utils.www import check_content_in_response, check_content_not_in_response, client_with_login


def clean_db():
    clear_db_dags()
    clear_db_import_errors()
    clear_db_serialized_dags()


@pytest.fixture(autouse=True)
def setup():
    clean_db()
    yield
    clean_db()


def test_home(capture_templates, admin_client):
    with capture_templates() as templates:
        resp = admin_client.get('home', follow_redirects=True)
        check_content_in_response('DAGs', resp)
        val_state_color_mapping = (
            'const STATE_COLOR = {'
            '"deferred": "lightseagreen", "failed": "red", '
            '"null": "lightblue", "queued": "gray", '
            '"removed": "lightgrey", "restarting": "violet", "running": "lime", '
            '"scheduled": "tan", "sensing": "lightseagreen", '
            '"shutdown": "blue", "skipped": "pink", '
            '"success": "green", "up_for_reschedule": "turquoise", '
            '"up_for_retry": "gold", "upstream_failed": "orange"};'
        )
        check_content_in_response(val_state_color_mapping, resp)

    assert len(templates) == 1
    assert templates[0].name == 'airflow/dags.html'
    state_color_mapping = State.state_color.copy()
    state_color_mapping["null"] = state_color_mapping.pop(None)
    assert templates[0].local_context['state_color'] == state_color_mapping


def test_home_filter_tags(admin_client):
    with admin_client:
        admin_client.get('home?tags=example&tags=data', follow_redirects=True)
        assert 'example,data' == flask.session[FILTER_TAGS_COOKIE]

        admin_client.get('home?reset_tags', follow_redirects=True)
        assert flask.session[FILTER_TAGS_COOKIE] is None


def test_home_status_filter_cookie(admin_client):
    with admin_client:
        admin_client.get('home', follow_redirects=True)
        assert 'all' == flask.session[FILTER_STATUS_COOKIE]

        admin_client.get('home?status=active', follow_redirects=True)
        assert 'active' == flask.session[FILTER_STATUS_COOKIE]

        admin_client.get('home?status=paused', follow_redirects=True)
        assert 'paused' == flask.session[FILTER_STATUS_COOKIE]

        admin_client.get('home?status=all', follow_redirects=True)
        assert 'all' == flask.session[FILTER_STATUS_COOKIE]


@pytest.fixture(scope="module")
def user_single_dag(app):
    """Create User that can only access the first DAG from TEST_FILTER_DAG_IDS"""
    return create_user(
        app,
        username="user_single_dag",
        role_name="role_single_dag",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            (permissions.ACTION_CAN_READ, permissions.resource_name_for_dag(TEST_FILTER_DAG_IDS[0])),
        ],
    )


@pytest.fixture()
def client_single_dag(app, user_single_dag):
    """Client for User that can only access the first DAG from TEST_FILTER_DAG_IDS"""
    return client_with_login(
        app,
        username="user_single_dag",
        password="user_single_dag",
    )


TEST_FILTER_DAG_IDS = ['filter_test_1', 'filter_test_2']


def _process_file(file_path, session):
    dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
    dag_file_processor.process_file(file_path, [], False, session)


@pytest.fixture()
def working_dags(tmpdir):
    dag_contents_template = "from airflow import DAG\ndag = DAG('{}')"

    with create_session() as session:
        for dag_id in TEST_FILTER_DAG_IDS:
            filename = tmpdir / f"{dag_id}.py"
            with open(filename, "w") as f:
                f.writelines(dag_contents_template.format(dag_id))
            _process_file(filename, session)


@pytest.fixture()
def broken_dags(tmpdir, working_dags):
    with create_session() as session:
        for dag_id in TEST_FILTER_DAG_IDS:
            filename = tmpdir / f"{dag_id}.py"
            with open(filename, "w") as f:
                f.writelines('airflow DAG')
            _process_file(filename, session)


def test_home_importerrors(broken_dags, user_client):
    # Users with "can read on DAGs" gets all DAG import errors
    resp = user_client.get('home', follow_redirects=True)
    check_content_in_response("Import Errors", resp)
    for dag_id in TEST_FILTER_DAG_IDS:
        check_content_in_response(f"/{dag_id}.py", resp)


def test_home_importerrors_filtered_singledag_user(broken_dags, client_single_dag):
    # Users that can only see certain DAGs get a filtered list of import errors
    resp = client_single_dag.get('home', follow_redirects=True)
    check_content_in_response("Import Errors", resp)
    # They can see the first DAGs import error
    check_content_in_response(f"/{TEST_FILTER_DAG_IDS[0]}.py", resp)
    # But not the rest
    for dag_id in TEST_FILTER_DAG_IDS[1:]:
        check_content_not_in_response(f"/{dag_id}.py", resp)


def test_home_dag_list(working_dags, user_client):
    # Users with "can read on DAGs" gets all DAGs
    resp = user_client.get('home', follow_redirects=True)
    for dag_id in TEST_FILTER_DAG_IDS:
        check_content_in_response(f"dag_id={dag_id}", resp)


def test_home_dag_list_filtered_singledag_user(working_dags, client_single_dag):
    # Users that can only see certain DAGs get a filtered list
    resp = client_single_dag.get('home', follow_redirects=True)
    # They can see the first DAG
    check_content_in_response(f"dag_id={TEST_FILTER_DAG_IDS[0]}", resp)
    # But not the rest
    for dag_id in TEST_FILTER_DAG_IDS[1:]:
        check_content_not_in_response(f"dag_id={dag_id}", resp)
