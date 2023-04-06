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
from unittest import mock

import flask
import markupsafe
import pytest

from airflow.dag_processing.processor import DagFileProcessor
from airflow.security import permissions
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.www.utils import UIAlert
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
        resp = admin_client.get("home", follow_redirects=True)
        check_content_in_response("DAGs", resp)
        val_state_color_mapping = (
            "const STATE_COLOR = {"
            '"deferred": "mediumpurple", "failed": "red", '
            '"null": "lightblue", "queued": "gray", '
            '"removed": "lightgrey", "restarting": "violet", "running": "lime", '
            '"scheduled": "tan", '
            '"shutdown": "blue", "skipped": "hotpink", '
            '"success": "green", "up_for_reschedule": "turquoise", '
            '"up_for_retry": "gold", "upstream_failed": "orange"};'
        )
        check_content_in_response(val_state_color_mapping, resp)

    assert len(templates) == 1
    assert templates[0].name == "airflow/dags.html"
    state_color_mapping = State.state_color.copy()
    state_color_mapping["null"] = state_color_mapping.pop(None)
    assert templates[0].local_context["state_color"] == state_color_mapping


def test_home_status_filter_cookie(admin_client):
    with admin_client:
        admin_client.get("home", follow_redirects=True)
        assert "all" == flask.session[FILTER_STATUS_COOKIE]

        admin_client.get("home?status=active", follow_redirects=True)
        assert "active" == flask.session[FILTER_STATUS_COOKIE]

        admin_client.get("home?status=paused", follow_redirects=True)
        assert "paused" == flask.session[FILTER_STATUS_COOKIE]

        admin_client.get("home?status=all", follow_redirects=True)
        assert "all" == flask.session[FILTER_STATUS_COOKIE]


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


@pytest.fixture(scope="module")
def user_single_dag_edit(app):
    """Create User that can edit DAG resource only a single DAG"""
    return create_user(
        app,
        username="user_single_dag_edit",
        role_name="role_single_dag",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_EDIT, permissions.resource_name_for_dag("filter_test_1")),
        ],
    )


@pytest.fixture()
def client_single_dag_edit(app, user_single_dag_edit):
    """Client for User that can only edit the first DAG from TEST_FILTER_DAG_IDS"""
    return client_with_login(
        app,
        username="user_single_dag_edit",
        password="user_single_dag_edit",
    )


TEST_FILTER_DAG_IDS = ["filter_test_1", "filter_test_2", "a_first_dag_id_asc", "filter.test"]
TEST_TAGS = ["example", "test", "team", "group"]


def _process_file(file_path, session):
    dag_file_processor = DagFileProcessor(dag_ids=[], dag_directory="/tmp", log=mock.MagicMock())
    dag_file_processor.process_file(file_path, [], False, session)


@pytest.fixture()
def working_dags(tmpdir):
    dag_contents_template = "from airflow import DAG\ndag = DAG('{}', tags=['{}'])"

    with create_session() as session:
        for dag_id, tag in list(zip(TEST_FILTER_DAG_IDS, TEST_TAGS)):
            filename = os.path.join(tmpdir, f"{dag_id}.py")
            with open(filename, "w") as f:
                f.writelines(dag_contents_template.format(dag_id, tag))
            _process_file(filename, session)


@pytest.fixture()
def working_dags_with_read_perm(tmpdir):
    dag_contents_template = "from airflow import DAG\ndag = DAG('{}', tags=['{}'])"
    dag_contents_template_with_read_perm = (
        "from airflow import DAG\ndag = DAG('{}', tags=['{}'], "
        "access_control={{'role_single_dag':{{'can_read'}}}}) "
    )
    with create_session() as session:
        for dag_id, tag in list(zip(TEST_FILTER_DAG_IDS, TEST_TAGS)):
            filename = os.path.join(tmpdir, f"{dag_id}.py")
            if dag_id == "filter_test_1":
                with open(filename, "w") as f:
                    f.writelines(dag_contents_template_with_read_perm.format(dag_id, tag))
            else:
                with open(filename, "w") as f:
                    f.writelines(dag_contents_template.format(dag_id, tag))
            _process_file(filename, session)


@pytest.fixture()
def working_dags_with_edit_perm(tmpdir):
    dag_contents_template = "from airflow import DAG\ndag = DAG('{}', tags=['{}'])"
    dag_contents_template_with_read_perm = (
        "from airflow import DAG\ndag = DAG('{}', tags=['{}'], "
        "access_control={{'role_single_dag':{{'can_edit'}}}}) "
    )
    with create_session() as session:
        for dag_id, tag in list(zip(TEST_FILTER_DAG_IDS, TEST_TAGS)):
            filename = os.path.join(tmpdir, f"{dag_id}.py")
            if dag_id == "filter_test_1":
                with open(filename, "w") as f:
                    f.writelines(dag_contents_template_with_read_perm.format(dag_id, tag))
            else:
                with open(filename, "w") as f:
                    f.writelines(dag_contents_template.format(dag_id, tag))
            _process_file(filename, session)


@pytest.fixture()
def broken_dags(tmpdir, working_dags):
    with create_session() as session:
        for dag_id in TEST_FILTER_DAG_IDS:
            filename = os.path.join(tmpdir, f"{dag_id}.py")
            with open(filename, "w") as f:
                f.writelines("airflow DAG")
            _process_file(filename, session)


@pytest.fixture()
def broken_dags_with_read_perm(tmpdir, working_dags_with_read_perm):
    with create_session() as session:
        for dag_id in TEST_FILTER_DAG_IDS:
            filename = os.path.join(tmpdir, f"{dag_id}.py")
            with open(filename, "w") as f:
                f.writelines("airflow DAG")
            _process_file(filename, session)


def test_home_filter_tags(working_dags, admin_client):
    with admin_client:
        admin_client.get("home?tags=example&tags=data", follow_redirects=True)
        assert "example,data" == flask.session[FILTER_TAGS_COOKIE]

        admin_client.get("home?reset_tags", follow_redirects=True)
        assert flask.session[FILTER_TAGS_COOKIE] is None


def test_home_importerrors(broken_dags, user_client):
    # Users with "can read on DAGs" gets all DAG import errors
    resp = user_client.get("home", follow_redirects=True)
    check_content_in_response("Import Errors", resp)
    for dag_id in TEST_FILTER_DAG_IDS:
        check_content_in_response(f"/{dag_id}.py", resp)


@pytest.mark.parametrize("page", ["home", "home?status=active", "home?status=paused", "home?status=all"])
def test_home_importerrors_filtered_singledag_user(broken_dags_with_read_perm, client_single_dag, page):
    # Users that can only see certain DAGs get a filtered list of import errors
    resp = client_single_dag.get(page, follow_redirects=True)
    check_content_in_response("Import Errors", resp)
    # They can see the first DAGs import error
    check_content_in_response(f"/{TEST_FILTER_DAG_IDS[0]}.py", resp)
    # But not the rest
    for dag_id in TEST_FILTER_DAG_IDS[1:]:
        check_content_not_in_response(f"/{dag_id}.py", resp)


def test_home_dag_list(working_dags, user_client):
    # Users with "can read on DAGs" gets all DAGs
    resp = user_client.get("home", follow_redirects=True)
    for dag_id in TEST_FILTER_DAG_IDS:
        check_content_in_response(f"dag_id={dag_id}", resp)


def test_home_dag_list_filtered_singledag_user(working_dags_with_read_perm, client_single_dag):
    # Users that can only see certain DAGs get a filtered list
    resp = client_single_dag.get("home", follow_redirects=True)
    # They can see the first DAG
    check_content_in_response(f"dag_id={TEST_FILTER_DAG_IDS[0]}", resp)
    # But not the rest
    for dag_id in TEST_FILTER_DAG_IDS[1:]:
        check_content_not_in_response(f"dag_id={dag_id}", resp)


def test_home_dag_list_search(working_dags, user_client):
    resp = user_client.get("home?search=filter_test", follow_redirects=True)
    check_content_in_response("dag_id=filter_test_1", resp)
    check_content_in_response("dag_id=filter_test_2", resp)
    check_content_not_in_response("dag_id=filter.test", resp)
    check_content_not_in_response("dag_id=a_first_dag_id_asc", resp)


def test_home_dag_edit_permissions(capture_templates, working_dags_with_edit_perm, client_single_dag_edit):
    with capture_templates() as templates:
        client_single_dag_edit.get("home", follow_redirects=True)

    dags = templates[0].local_context["dags"]
    assert len(dags) > 0
    dag_edit_perm_tuple = [(dag.dag_id, dag.can_edit) for dag in dags]
    assert ("filter_test_1", True) in dag_edit_perm_tuple
    assert ("filter_test_2", False) in dag_edit_perm_tuple


def test_home_robots_header_in_response(user_client):
    # Responses should include X-Robots-Tag header
    resp = user_client.get("home", follow_redirects=True)
    assert resp.headers["X-Robots-Tag"] == "noindex, nofollow"


@pytest.mark.parametrize(
    "client, flash_message, expected",
    [
        ("anonymous_client", UIAlert("hello world"), True),
        ("anonymous_client", UIAlert("hello world", roles=["Viewer"]), True),
        ("anonymous_client", UIAlert("hello world", roles=["User"]), False),
        ("anonymous_client", UIAlert("hello world", roles=["Viewer", "User"]), True),
        ("anonymous_client", UIAlert("hello world", roles=["Admin"]), False),
        ("user_client", UIAlert("hello world"), True),
        ("user_client", UIAlert("hello world", roles=["User"]), True),
        ("user_client", UIAlert("hello world", roles=["User", "Admin"]), True),
        ("user_client", UIAlert("hello world", roles=["Admin"]), False),
        ("admin_client", UIAlert("hello world"), True),
        ("admin_client", UIAlert("hello world", roles=["Admin"]), True),
        ("admin_client", UIAlert("hello world", roles=["User", "Admin"]), True),
    ],
)
def test_dashboard_flash_messages_role_filtering(request, client, flash_message, expected):
    with mock.patch("airflow.settings.DASHBOARD_UIALERTS", [flash_message]):
        resp = request.getfixturevalue(client).get("home", follow_redirects=True)
    if expected:
        check_content_in_response(flash_message.message, resp)
    else:
        check_content_not_in_response(flash_message.message, resp)


def test_dashboard_flash_messages_many(user_client):
    messages = [
        UIAlert("hello world"),
        UIAlert("im_not_here", roles=["Admin"]),
        UIAlert("_hello_world_"),
    ]
    with mock.patch("airflow.settings.DASHBOARD_UIALERTS", messages):
        resp = user_client.get("home", follow_redirects=True)
    check_content_in_response("hello world", resp)
    check_content_not_in_response("im_not_here", resp)
    check_content_in_response("_hello_world_", resp)


def test_dashboard_flash_messages_markup(user_client):
    link = '<a href="http://example.com">hello world</a>'
    user_input = markupsafe.Markup("Hello <em>%s</em>") % ("foo&bar",)
    messages = [
        UIAlert(link, html=True),
        UIAlert(user_input),
    ]
    with mock.patch("airflow.settings.DASHBOARD_UIALERTS", messages):
        resp = user_client.get("home", follow_redirects=True)
    check_content_in_response(link, resp)
    check_content_in_response(user_input, resp)


def test_dashboard_flash_messages_type(user_client):
    messages = [
        UIAlert("hello world", category="foo"),
    ]
    with mock.patch("airflow.settings.DASHBOARD_UIALERTS", messages):
        resp = user_client.get("home", follow_redirects=True)
    check_content_in_response("hello world", resp)
    check_content_in_response("alert-foo", resp)


def test_audit_log_view(user_client, working_dags):
    resp = user_client.get("/dags/filter_test_1/audit_log")
    check_content_in_response("Dag Audit Log", resp)


@pytest.mark.parametrize(
    "url, lower_key, greater_key",
    [
        ("home?status=all", "a_first_dag_id_asc", "filter_test_1"),
        ("home?status=all&sorting_key=dag_id&sorting_direction=asc", "filter_test_1", "filter_test_2"),
        ("home?status=all&sorting_key=dag_id&sorting_direction=desc", "filter_test_2", "filter_test_1"),
    ],
    ids=["no_order_provided", "ascending_order_on_dag_id", "descending_order_on_dag_id"],
)
def test_sorting_home_view(url, lower_key, greater_key, user_client, working_dags):
    resp = user_client.get(url, follow_redirects=True)
    resp_html = resp.data.decode("utf-8")
    lower_index = resp_html.find(lower_key)
    greater_index = resp_html.find(greater_key)
    assert lower_index < greater_index
