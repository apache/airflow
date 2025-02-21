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

from unittest import mock

import flask
import markupsafe
import pytest

from airflow.utils.state import State
from airflow.www.utils import UIAlert
from airflow.www.views import FILTER_LASTRUN_COOKIE, FILTER_STATUS_COOKIE, FILTER_TAGS_COOKIE

from tests_common.test_utils.db import clear_db_dags, clear_db_import_errors, clear_db_serialized_dags
from tests_common.test_utils.www import (
    capture_templates,  # noqa: F401
    check_content_in_response,
    check_content_not_in_response,
)

pytestmark = [pytest.mark.db_test, pytest.mark.need_serialized_dag]


def clean_db():
    clear_db_dags()
    clear_db_import_errors()
    clear_db_serialized_dags()


@pytest.fixture(autouse=True)
def _setup():
    clean_db()
    yield
    clean_db()


def test_home(
    capture_templates,  # noqa: F811
    admin_client,
):
    with capture_templates() as templates:
        resp = admin_client.get("home", follow_redirects=True)
        check_content_in_response("DAGs", resp)
        val_state_color_mapping = (
            "const STATE_COLOR = {"
            '"deferred": "mediumpurple", "failed": "red", '
            '"null": "lightblue", "queued": "gray", '
            '"removed": "lightgrey", "restarting": "violet", "running": "lime", '
            '"scheduled": "tan", '
            '"skipped": "hotpink", '
            '"success": "green", "up_for_reschedule": "turquoise", '
            '"up_for_retry": "gold", "upstream_failed": "orange"};'
        )
        check_content_in_response(val_state_color_mapping, resp)

    assert len(templates) == 1
    assert templates[0].name == "airflow/dags.html"
    state_color_mapping = State.state_color.copy()
    state_color_mapping["null"] = state_color_mapping.pop(None)
    assert templates[0].local_context["state_color"] == state_color_mapping


@mock.patch("airflow.www.views.AirflowBaseView.render_template")
def test_home_dags_count(render_template_mock, admin_client, _working_dags, session):
    from sqlalchemy import update

    from airflow.models.dag import DagModel

    def call_kwargs():
        return render_template_mock.call_args.kwargs

    admin_client.get("home", follow_redirects=True)
    assert call_kwargs()["status_count_all"] == 4

    update_stmt = update(DagModel).where(DagModel.dag_id == "filter_test_1").values(is_active=False)
    session.execute(update_stmt)

    admin_client.get("home", follow_redirects=True)
    assert call_kwargs()["status_count_all"] == 3


def test_home_status_filter_cookie(admin_client):
    with admin_client:
        admin_client.get("home", follow_redirects=True)
        assert flask.session[FILTER_STATUS_COOKIE] == "all"

        admin_client.get("home?status=active", follow_redirects=True)
        assert flask.session[FILTER_STATUS_COOKIE] == "active"

        admin_client.get("home?status=paused", follow_redirects=True)
        assert flask.session[FILTER_STATUS_COOKIE] == "paused"

        admin_client.get("home?status=all", follow_redirects=True)
        assert flask.session[FILTER_STATUS_COOKIE] == "all"

        admin_client.get("home?lastrun=running", follow_redirects=True)
        assert flask.session[FILTER_LASTRUN_COOKIE] == "running"

        admin_client.get("home?lastrun=failed", follow_redirects=True)
        assert flask.session[FILTER_LASTRUN_COOKIE] == "failed"

        admin_client.get("home?lastrun=all_states", follow_redirects=True)
        assert flask.session[FILTER_LASTRUN_COOKIE] == "all_states"


TEST_FILTER_DAG_IDS = ["filter_test_1", "filter_test_2", "a_first_dag_id_asc", "filter.test"]
TEST_TAGS = ["example", "test", "team", "group"]


@pytest.fixture
def _working_dags(dag_maker):
    for dag_id, tag in zip(TEST_FILTER_DAG_IDS, TEST_TAGS):
        with dag_maker(dag_id=dag_id, fileloc=f"/{dag_id}.py", tags=[tag]):
            # We need to enter+exit the dag maker context for it to create the dag
            pass


@pytest.fixture
def _broken_dags(session):
    from airflow.models.errors import ParseImportError

    for dag_id in TEST_FILTER_DAG_IDS:
        session.add(
            ParseImportError(
                filename=f"/{dag_id}.py", bundle_name="dag_maker", stacktrace="Some Error\nTraceback:\n"
            )
        )
    session.commit()


def test_home_filter_tags(_working_dags, admin_client):
    with admin_client:
        admin_client.get("home?tags=example&tags=data", follow_redirects=True)
        assert flask.session[FILTER_TAGS_COOKIE] == "example,data"

        admin_client.get("home?reset_tags", follow_redirects=True)
        assert flask.session[FILTER_TAGS_COOKIE] is None


@pytest.mark.usefixtures("_broken_dags", "_working_dags")
def test_home_importerrors(_broken_dags, user_client):
    # Users with "can read on DAGs" gets all DAG import errors
    resp = user_client.get("home", follow_redirects=True)
    check_content_in_response("Import Errors", resp)
    for dag_id in TEST_FILTER_DAG_IDS:
        check_content_in_response(f"/{dag_id}.py", resp)


def test_home_dag_list(_working_dags, user_client):
    # Users with "can read on DAGs" gets all DAGs
    resp = user_client.get("home", follow_redirects=True)
    for dag_id in TEST_FILTER_DAG_IDS:
        check_content_in_response(f"dag_id={dag_id}", resp)


def test_home_dag_list_search(_working_dags, user_client):
    resp = user_client.get("home?search=filter_test", follow_redirects=True)
    check_content_in_response("dag_id=filter_test_1", resp)
    check_content_in_response("dag_id=filter_test_2", resp)
    check_content_not_in_response("dag_id=filter.test", resp)
    check_content_not_in_response("dag_id=a_first_dag_id_asc", resp)


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


@pytest.mark.parametrize(
    "url, lower_key, greater_key",
    [
        ("home?status=all", "a_first_dag_id_asc", "filter_test_1"),
        ("home?status=all&sorting_key=dag_id&sorting_direction=asc", "filter_test_1", "filter_test_2"),
        ("home?status=all&sorting_key=dag_id&sorting_direction=desc", "filter_test_2", "filter_test_1"),
    ],
    ids=["no_order_provided", "ascending_order_on_dag_id", "descending_order_on_dag_id"],
)
def test_sorting_home_view(url, lower_key, greater_key, user_client, _working_dags):
    resp = user_client.get(url, follow_redirects=True)
    resp_html = resp.data.decode("utf-8")
    lower_index = resp_html.find(lower_key)
    greater_index = resp_html.find(greater_key)
    assert lower_index < greater_index


@pytest.mark.parametrize(
    "url, filter_tags_cookie_val, filter_lastrun_cookie_val, expected_filter_tags, expected_filter_lastrun",
    [
        ("home", None, None, [], None),
        # from url only
        ("home?tags=example&tags=test", None, None, ["example", "test"], None),
        ("home?lastrun=running", None, None, [], "running"),
        ("home?tags=example&tags=test&lastrun=running", None, None, ["example", "test"], "running"),
        # from cookie only
        ("home", "example,test", None, ["example", "test"], None),
        ("home", None, "running", [], "running"),
        ("home", "example,test", "running", ["example", "test"], "running"),
        # from url and cookie
        ("home?tags=example", "example,test", None, ["example"], None),
        ("home?lastrun=failed", None, "running", [], "failed"),
        ("home?tags=example", None, "running", ["example"], "running"),
        ("home?lastrun=running", "example,test", None, ["example", "test"], "running"),
        ("home?tags=example&lastrun=running", "example,test", "failed", ["example"], "running"),
    ],
)
def test_filter_cookie_eval(
    _working_dags,
    admin_client,
    url,
    filter_tags_cookie_val,
    filter_lastrun_cookie_val,
    expected_filter_tags,
    expected_filter_lastrun,
):
    with admin_client.session_transaction() as flask_session:
        flask_session[FILTER_TAGS_COOKIE] = filter_tags_cookie_val
        flask_session[FILTER_LASTRUN_COOKIE] = filter_lastrun_cookie_val

    resp = admin_client.get(url, follow_redirects=True)
    assert resp.request.args.getlist("tags") == expected_filter_tags
    assert resp.request.args.get("lastrun") == expected_filter_lastrun
