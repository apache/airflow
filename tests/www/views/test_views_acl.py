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
import urllib.parse

import pytest

from airflow.models import DagModel
from airflow.security import permissions
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from airflow.www.views import FILTER_STATUS_COOKIE
from tests.test_utils.api_connexion_utils import create_user_scope
from tests.test_utils.compat import AIRFLOW_V_3_0_PLUS
from tests.test_utils.db import clear_db_runs
from tests.test_utils.permissions import _resource_name
from tests.test_utils.www import check_content_in_response, check_content_not_in_response, client_with_login

if AIRFLOW_V_3_0_PLUS:
    from airflow.utils.types import DagRunTriggeredByType

pytestmark = pytest.mark.db_test

NEXT_YEAR = datetime.datetime.now().year + 1
DEFAULT_DATE = timezone.datetime(NEXT_YEAR, 6, 1)
DEFAULT_RUN_ID = "TEST_RUN_ID"
USER_DATA = {
    "dag_tester": (
        "dag_acl_tester",
        {
            "first_name": "dag_test",
            "last_name": "dag_test",
            "email": "dag_test@fab.org",
            "password": "dag_test",
        },
    ),
    "dag_faker": (  # User without permission.
        "dag_acl_faker",
        {
            "first_name": "dag_faker",
            "last_name": "dag_faker",
            "email": "dag_fake@fab.org",
            "password": "dag_faker",
        },
    ),
    "dag_read_only": (  # User with only read permission.
        "dag_acl_read_only",
        {
            "first_name": "dag_read_only",
            "last_name": "dag_read_only",
            "email": "dag_read_only@fab.org",
            "password": "dag_read_only",
        },
    ),
    "all_dag_user": (  # User has all dag access.
        "all_dag_role",
        {
            "first_name": "all_dag_user",
            "last_name": "all_dag_user",
            "email": "all_dag_user@fab.org",
            "password": "all_dag_user",
        },
    ),
}


@pytest.fixture(scope="module")
def acl_app(app):
    security_manager = app.appbuilder.sm
    for username, (role_name, kwargs) in USER_DATA.items():
        if not security_manager.find_user(username=username):
            role = security_manager.add_role(role_name)
            security_manager.add_user(
                role=role,
                username=username,
                **kwargs,
            )

    role_permissions = {
        "dag_acl_tester": [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            (permissions.ACTION_CAN_EDIT, "DAG:example_bash_operator"),
            (permissions.ACTION_CAN_READ, "DAG:example_bash_operator"),
        ],
        "all_dag_role": [
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ],
        "User": [
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ],
        "dag_acl_read_only": [
            (permissions.ACTION_CAN_READ, "DAG:example_bash_operator"),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ],
        "dag_acl_faker": [(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)],
    }

    for _role, _permissions in role_permissions.items():
        role = security_manager.find_role(_role)
        for _action, _perm in _permissions:
            perm = security_manager.get_permission(_action, _perm)
            security_manager.add_permission_to_role(role, perm)

    yield app

    for username in USER_DATA:
        user = security_manager.find_user(username=username)
        if user:
            security_manager.del_register_user(user)


@pytest.fixture(scope="module")
def reset_dagruns():
    """Clean up stray garbage from other tests."""
    clear_db_runs()


@pytest.fixture(autouse=True)
def init_dagruns(acl_app, reset_dagruns):
    triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
    acl_app.dag_bag.get_dag("example_bash_operator").create_dagrun(
        run_id=DEFAULT_RUN_ID,
        run_type=DagRunType.SCHEDULED,
        execution_date=DEFAULT_DATE,
        data_interval=(DEFAULT_DATE, DEFAULT_DATE),
        start_date=timezone.utcnow(),
        state=State.RUNNING,
        **triggered_by_kwargs,
    )
    acl_app.dag_bag.get_dag("example_python_operator").create_dagrun(
        run_type=DagRunType.SCHEDULED,
        execution_date=DEFAULT_DATE,
        start_date=timezone.utcnow(),
        data_interval=(DEFAULT_DATE, DEFAULT_DATE),
        state=State.RUNNING,
        **triggered_by_kwargs,
    )
    yield
    clear_db_runs()


@pytest.fixture
def dag_test_client(acl_app):
    return client_with_login(acl_app, username="dag_test", password="dag_test")


@pytest.fixture
def dag_faker_client(acl_app):
    return client_with_login(acl_app, username="dag_faker", password="dag_faker")


@pytest.fixture
def all_dag_user_client(acl_app):
    return client_with_login(
        acl_app,
        username="all_dag_user",
        password="all_dag_user",
    )


@pytest.fixture(scope="module")
def user_edit_one_dag(acl_app):
    with create_user_scope(
        acl_app,
        username="user_edit_one_dag",
        role_name="role_edit_one_dag",
        permissions=[
            (permissions.ACTION_CAN_READ, "DAG:example_bash_operator"),
            (permissions.ACTION_CAN_EDIT, "DAG:example_bash_operator"),
        ],
    ) as user:
        yield user


@pytest.mark.usefixtures("user_edit_one_dag")
def test_permission_exist(acl_app):
    perms_views = acl_app.appbuilder.sm.get_resource_permissions(
        acl_app.appbuilder.sm.get_resource("DAG:example_bash_operator"),
    )
    assert len(perms_views) == 3

    perms = {str(perm) for perm in perms_views}
    assert "can read on DAG:example_bash_operator" in perms
    assert "can edit on DAG:example_bash_operator" in perms
    assert "can delete on DAG:example_bash_operator" in perms


@pytest.mark.usefixtures("user_edit_one_dag")
def test_role_permission_associate(acl_app):
    test_role = acl_app.appbuilder.sm.find_role("role_edit_one_dag")
    perms = {str(perm) for perm in test_role.permissions}
    assert "can edit on DAG:example_bash_operator" in perms
    assert "can read on DAG:example_bash_operator" in perms


@pytest.fixture(scope="module")
def user_all_dags(acl_app):
    with create_user_scope(
        acl_app,
        username="user_all_dags",
        role_name="role_all_dags",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ],
    ) as user:
        yield user


@pytest.fixture
def client_all_dags(acl_app, user_all_dags):
    return client_with_login(
        acl_app,
        username="user_all_dags",
        password="user_all_dags",
    )


def test_index_for_all_dag_user(client_all_dags):
    # The all dag user can access/view all dags.
    resp = client_all_dags.get("/", follow_redirects=True)
    check_content_in_response("example_python_operator", resp)
    check_content_in_response("example_bash_operator", resp)


def test_index_failure(dag_test_client):
    # This user can only access/view example_bash_operator dag.
    resp = dag_test_client.get("/", follow_redirects=True)
    check_content_not_in_response("example_python_operator", resp)


def test_dag_autocomplete_success(client_all_dags):
    resp = client_all_dags.get(
        "dagmodel/autocomplete?query=flow",
        follow_redirects=False,
    )
    expected = [
        {"name": "airflow", "type": "owner", "dag_display_name": None},
        {
            "dag_display_name": None,
            "name": "asset_alias_example_alias_consumer_with_no_taskflow",
            "type": "dag",
        },
        {
            "dag_display_name": None,
            "name": "asset_alias_example_alias_producer_with_no_taskflow",
            "type": "dag",
        },
        {
            "dag_display_name": None,
            "name": "dataset_s3_bucket_consumer_with_no_taskflow",
            "type": "dag",
        },
        {
            "dag_display_name": None,
            "name": "dataset_s3_bucket_producer_with_no_taskflow",
            "type": "dag",
        },
        {
            "name": "example_dynamic_task_mapping_with_no_taskflow_operators",
            "type": "dag",
            "dag_display_name": None,
        },
        {"name": "example_setup_teardown_taskflow", "type": "dag", "dag_display_name": None},
        {"name": "test_mapped_taskflow", "type": "dag", "dag_display_name": None},
        {"name": "tutorial_taskflow_api", "type": "dag", "dag_display_name": None},
        {"name": "tutorial_taskflow_api_virtualenv", "type": "dag", "dag_display_name": None},
    ]

    assert resp.json == expected


@pytest.mark.parametrize(
    "query, expected",
    [
        (None, []),
        ("", []),
        ("no-found", []),
    ],
    ids=["none", "empty", "not-found"],
)
def test_dag_autocomplete_empty(client_all_dags, query, expected):
    url = "dagmodel/autocomplete"
    if query is not None:
        url = f"{url}?query={query}"
    resp = client_all_dags.get(url, follow_redirects=False)
    assert resp.json == expected


def test_dag_autocomplete_dag_display_name(client_all_dags):
    url = "dagmodel/autocomplete?query=Sample"
    resp = client_all_dags.get(url, follow_redirects=False)
    assert resp.json == [
        {"name": "example_display_name", "type": "dag", "dag_display_name": "Sample DAG with Display Name"}
    ]


@pytest.fixture
def setup_paused_dag():
    """Pause a DAG so we can test filtering."""
    dag_to_pause = "example_branch_operator"
    with create_session() as session:
        session.query(DagModel).filter(DagModel.dag_id == dag_to_pause).update({"is_paused": True})
    yield
    with create_session() as session:
        session.query(DagModel).filter(DagModel.dag_id == dag_to_pause).update({"is_paused": False})


@pytest.mark.parametrize(
    "status, expected, unexpected",
    [
        ("active", "example_branch_labels", "example_branch_operator"),
        ("paused", "example_branch_operator", "example_branch_labels"),
    ],
)
@pytest.mark.usefixtures("setup_paused_dag")
def test_dag_autocomplete_status(client_all_dags, status, expected, unexpected):
    with client_all_dags.session_transaction() as flask_session:
        flask_session[FILTER_STATUS_COOKIE] = status
    resp = client_all_dags.get(
        "dagmodel/autocomplete?query=example_branch_",
        follow_redirects=False,
    )
    check_content_in_response(expected, resp)
    check_content_not_in_response(unexpected, resp)


@pytest.fixture(scope="module")
def user_all_dags_dagruns(acl_app):
    with create_user_scope(
        acl_app,
        username="user_all_dags_dagruns",
        role_name="role_all_dags_dagruns",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ],
    ) as user:
        yield user


@pytest.fixture
def client_all_dags_dagruns(acl_app, user_all_dags_dagruns):
    return client_with_login(
        acl_app,
        username="user_all_dags_dagruns",
        password="user_all_dags_dagruns",
    )


def test_dag_stats_success(client_all_dags_dagruns):
    resp = client_all_dags_dagruns.post("dag_stats", follow_redirects=True)
    check_content_in_response("example_bash_operator", resp)
    assert set(next(iter(resp.json.items()))[1][0].keys()) == {"state", "count"}


def test_task_stats_failure(dag_test_client):
    resp = dag_test_client.post("task_stats", follow_redirects=True)
    check_content_not_in_response("example_python_operator", resp)


def test_dag_stats_success_for_all_dag_user(client_all_dags_dagruns):
    resp = client_all_dags_dagruns.post("dag_stats", follow_redirects=True)
    check_content_in_response("example_python_operator", resp)
    check_content_in_response("example_bash_operator", resp)


@pytest.fixture(scope="module")
def user_all_dags_dagruns_tis(acl_app):
    with create_user_scope(
        acl_app,
        username="user_all_dags_dagruns_tis",
        role_name="role_all_dags_dagruns_tis",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ],
    ) as user:
        yield user


@pytest.fixture
def client_all_dags_dagruns_tis(acl_app, user_all_dags_dagruns_tis):
    return client_with_login(
        acl_app,
        username="user_all_dags_dagruns_tis",
        password="user_all_dags_dagruns_tis",
    )


def test_task_stats_empty_success(client_all_dags_dagruns_tis):
    resp = client_all_dags_dagruns_tis.post("task_stats", follow_redirects=True)
    check_content_in_response("example_bash_operator", resp)
    check_content_in_response("example_python_operator", resp)


@pytest.mark.parametrize(
    "dags_to_run, unexpected_dag_ids",
    [
        (
            ["example_python_operator"],
            ["example_bash_operator", "example_xcom"],
        ),
        (
            ["example_python_operator", "example_bash_operator"],
            ["example_xcom"],
        ),
    ],
    ids=["single", "multi"],
)
def test_task_stats_success(
    client_all_dags_dagruns_tis,
    dags_to_run,
    unexpected_dag_ids,
):
    resp = client_all_dags_dagruns_tis.post(
        "task_stats", data={"dag_ids": dags_to_run}, follow_redirects=True
    )
    assert resp.status_code == 200
    for dag_id in unexpected_dag_ids:
        check_content_not_in_response(dag_id, resp)
    stats = json.loads(resp.data.decode())
    for dag_id in dags_to_run:
        assert dag_id in stats


@pytest.fixture(scope="module")
def user_all_dags_codes(acl_app):
    with create_user_scope(
        acl_app,
        username="user_all_dags_codes",
        role_name="role_all_dags_codes",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_CODE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ],
    ) as user:
        yield user


@pytest.fixture
def client_all_dags_codes(acl_app, user_all_dags_codes):
    return client_with_login(
        acl_app,
        username="user_all_dags_codes",
        password="user_all_dags_codes",
    )


def test_code_success(client_all_dags_codes):
    url = "code?dag_id=example_bash_operator"
    resp = client_all_dags_codes.get(url, follow_redirects=True)
    check_content_in_response("example_bash_operator", resp)


def test_code_failure(dag_test_client):
    url = "code?dag_id=example_bash_operator"
    resp = dag_test_client.get(url, follow_redirects=True)
    check_content_not_in_response("example_bash_operator", resp)


@pytest.mark.parametrize(
    "dag_id",
    ["example_bash_operator", "example_python_operator"],
)
def test_code_success_for_all_dag_user(client_all_dags_codes, dag_id):
    url = f"code?dag_id={dag_id}"
    resp = client_all_dags_codes.get(url, follow_redirects=True)
    check_content_in_response(dag_id, resp)


@pytest.mark.parametrize(
    "dag_id",
    ["example_bash_operator", "example_python_operator"],
)
def test_dag_details_success_for_all_dag_user(client_all_dags_dagruns, dag_id):
    url = f"dag_details?dag_id={dag_id}"
    resp = client_all_dags_dagruns.get(url, follow_redirects=True)
    check_content_in_response(dag_id, resp)


@pytest.fixture(scope="module")
def user_all_dags_tis(acl_app):
    with create_user_scope(
        acl_app,
        username="user_all_dags_tis",
        role_name="role_all_dags_tis",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ],
    ) as user:
        yield user


@pytest.fixture
def client_all_dags_tis(acl_app, user_all_dags_tis):
    return client_with_login(
        acl_app,
        username="user_all_dags_tis",
        password="user_all_dags_tis",
    )


@pytest.fixture(scope="module")
def user_all_dags_tis_xcom(acl_app):
    with create_user_scope(
        acl_app,
        username="user_all_dags_tis_xcom",
        role_name="role_all_dags_tis_xcom",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_XCOM),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ],
    ) as user:
        yield user


@pytest.fixture
def client_all_dags_tis_xcom(acl_app, user_all_dags_tis_xcom):
    return client_with_login(
        acl_app,
        username="user_all_dags_tis_xcom",
        password="user_all_dags_tis_xcom",
    )


@pytest.fixture(scope="module")
def user_dags_tis_logs(acl_app):
    with create_user_scope(
        acl_app,
        username="user_dags_tis_logs",
        role_name="role_dags_tis_logs",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_LOG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ],
    ) as user:
        yield user


@pytest.fixture
def client_dags_tis_logs(acl_app, user_dags_tis_logs):
    return client_with_login(
        acl_app,
        username="user_dags_tis_logs",
        password="user_dags_tis_logs",
    )


RENDERED_TEMPLATES_URL = (
    f"rendered-templates?task_id=runme_0&dag_id=example_bash_operator&"
    f"execution_date={urllib.parse.quote_plus(str(DEFAULT_DATE))}"
)
TASK_URL = (
    f"task?task_id=runme_0&dag_id=example_bash_operator&"
    f"execution_date={urllib.parse.quote_plus(str(DEFAULT_DATE))}"
)
XCOM_URL = (
    f"xcom?task_id=runme_0&dag_id=example_bash_operator&"
    f"execution_date={urllib.parse.quote_plus(str(DEFAULT_DATE))}"
)
DURATION_URL = "duration?days=30&dag_id=example_bash_operator"
TRIES_URL = "tries?days=30&dag_id=example_bash_operator"
LANDING_TIMES_URL = "landing_times?days=30&dag_id=example_bash_operator"
GANTT_URL = "gantt?dag_id=example_bash_operator"
GRID_DATA_URL = "object/grid_data?dag_id=example_bash_operator"
LOG_URL = (
    f"log?task_id=runme_0&dag_id=example_bash_operator&"
    f"execution_date={urllib.parse.quote_plus(str(DEFAULT_DATE))}"
)


@pytest.mark.parametrize(
    "client, url, expected_content",
    [
        ("client_all_dags_tis", RENDERED_TEMPLATES_URL, "Rendered Template"),
        ("all_dag_user_client", RENDERED_TEMPLATES_URL, "Rendered Template"),
        ("client_all_dags_tis", TASK_URL, "Task Instance Details"),
        ("client_all_dags_tis_xcom", XCOM_URL, "XCom"),
        ("client_all_dags_tis", DURATION_URL, "example_bash_operator"),
        ("client_all_dags_tis", TRIES_URL, "example_bash_operator"),
        ("client_all_dags_tis", LANDING_TIMES_URL, "example_bash_operator"),
        ("client_all_dags_tis", GANTT_URL, "example_bash_operator"),
        ("client_dags_tis_logs", GRID_DATA_URL, "runme_1"),
        ("viewer_client", GRID_DATA_URL, "runme_1"),
        ("client_dags_tis_logs", LOG_URL, "Log by attempts"),
        ("user_client", LOG_URL, "Log by attempts"),
    ],
    ids=[
        "rendered-templates",
        "rendered-templates-all-dag-user",
        "task",
        "xcom",
        "duration",
        "tries",
        "landing-times",
        "gantt",
        "grid-data-for-readonly-role",
        "grid-data-for-viewer",
        "log",
        "log-for-user",
    ],
)
def test_success(request, client, url, expected_content):
    resp = request.getfixturevalue(client).get(url, follow_redirects=True)
    check_content_in_response(expected_content, resp)


@pytest.mark.parametrize(
    "url, unexpected_content",
    [
        (RENDERED_TEMPLATES_URL, "Rendered Template"),
        (TASK_URL, "Task Instance Details"),
        (XCOM_URL, "XCom"),
        (DURATION_URL, "example_bash_operator"),
        (TRIES_URL, "example_bash_operator"),
        (LANDING_TIMES_URL, "example_bash_operator"),
        (GANTT_URL, "example_bash_operator"),
        (LOG_URL, "Log by attempts"),
    ],
    ids=[
        "rendered-templates",
        "task",
        "xcom",
        "duration",
        "tries",
        "landing-times",
        "gantt",
        "log",
    ],
)
def test_failure(dag_faker_client, url, unexpected_content):
    resp = dag_faker_client.get(url, follow_redirects=True)
    check_content_not_in_response(unexpected_content, resp)


def test_blocked_success(client_all_dags_dagruns):
    resp = client_all_dags_dagruns.post("blocked")
    check_content_in_response("example_bash_operator", resp)


def test_blocked_success_for_all_dag_user(all_dag_user_client):
    resp = all_dag_user_client.post("blocked")
    check_content_in_response("example_bash_operator", resp)
    check_content_in_response("example_python_operator", resp)


def test_blocked_viewer(viewer_client):
    resp = viewer_client.post("blocked")
    check_content_in_response("example_bash_operator", resp)


@pytest.mark.parametrize(
    "dags_to_block, unexpected_dag_ids",
    [
        (
            ["example_python_operator"],
            ["example_bash_operator", "example_xcom"],
        ),
        (
            ["example_python_operator", "example_bash_operator"],
            ["example_xcom"],
        ),
    ],
    ids=["single", "multi"],
)
def test_blocked_success_when_selecting_dags(
    admin_client,
    dags_to_block,
    unexpected_dag_ids,
):
    resp = admin_client.post("blocked", data={"dag_ids": dags_to_block})
    assert resp.status_code == 200
    for dag_id in unexpected_dag_ids:
        check_content_not_in_response(dag_id, resp)
    blocked_dags = {blocked["dag_id"] for blocked in json.loads(resp.data.decode())}
    for dag_id in dags_to_block:
        assert dag_id in blocked_dags


@pytest.fixture(scope="module")
def user_all_dags_edit_tis(acl_app):
    with create_user_scope(
        acl_app,
        username="user_all_dags_edit_tis",
        role_name="role_all_dags_edit_tis",
        permissions=[
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ],
    ) as user:
        yield user


@pytest.fixture
def client_all_dags_edit_tis(acl_app, user_all_dags_edit_tis):
    return client_with_login(
        acl_app,
        username="user_all_dags_edit_tis",
        password="user_all_dags_edit_tis",
    )


def test_failed_success(client_all_dags_edit_tis):
    form = dict(
        task_id="run_this_last",
        dag_id="example_bash_operator",
        dag_run_id=DEFAULT_RUN_ID,
        upstream="false",
        downstream="false",
        future="false",
        past="false",
    )
    resp = client_all_dags_edit_tis.post("failed", data=form, follow_redirects=True)
    check_content_in_response("Marked failed on 1 task instances", resp)


def test_paused_post_success(dag_test_client):
    resp = dag_test_client.post("paused?dag_id=example_bash_operator&is_paused=false", follow_redirects=True)
    check_content_in_response("OK", resp)


@pytest.fixture(scope="module")
def user_only_dags_tis(acl_app):
    with create_user_scope(
        acl_app,
        username="user_only_dags_tis",
        role_name="role_only_dags_tis",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
        ],
    ) as user:
        yield user


@pytest.fixture
def client_only_dags_tis(acl_app, user_only_dags_tis):
    return client_with_login(
        acl_app,
        username="user_only_dags_tis",
        password="user_only_dags_tis",
    )


def test_success_fail_for_read_only_task_instance_access(client_only_dags_tis):
    form = dict(
        task_id="run_this_last",
        dag_id="example_bash_operator",
        dag_run_id=DEFAULT_RUN_ID,
        upstream="false",
        downstream="false",
        future="false",
        past="false",
    )
    resp = client_only_dags_tis.post("success", data=form)
    check_content_not_in_response("Please confirm", resp, resp_code=302)


GET_LOGS_WITH_METADATA_URL = (
    f"get_logs_with_metadata?task_id=runme_0&dag_id=example_bash_operator&"
    f"execution_date={urllib.parse.quote_plus(str(DEFAULT_DATE))}&"
    f"try_number=1&metadata=null"
)


@pytest.mark.parametrize("client", ["client_dags_tis_logs", "user_client"])
def test_get_logs_with_metadata_success(request, client):
    resp = request.getfixturevalue(client).get(
        GET_LOGS_WITH_METADATA_URL,
        follow_redirects=True,
    )
    check_content_in_response('"message":', resp)
    check_content_in_response('"metadata":', resp)


def test_get_logs_with_metadata_failure(dag_faker_client):
    resp = dag_faker_client.get(
        GET_LOGS_WITH_METADATA_URL,
        follow_redirects=True,
    )
    check_content_not_in_response('"message":', resp)
    check_content_not_in_response('"metadata":', resp)


@pytest.fixture(scope="module")
def user_no_roles(acl_app):
    with create_user_scope(acl_app, username="no_roles_user", role_name="no_roles_user_role") as user:
        user.roles = []
        yield user


@pytest.fixture
def client_no_roles(acl_app, user_no_roles):
    return client_with_login(
        acl_app,
        username="no_roles_user",
        password="no_roles_user",
    )


@pytest.fixture(scope="module")
def user_no_permissions(acl_app):
    with create_user_scope(
        acl_app,
        username="no_permissions_user",
        role_name="no_permissions_role",
    ) as user:
        yield user


@pytest.fixture
def client_no_permissions(acl_app, user_no_permissions):
    return client_with_login(
        acl_app,
        username="no_permissions_user",
        password="no_permissions_user",
    )


@pytest.fixture
def client_anonymous(acl_app):
    return acl_app.test_client()


@pytest.mark.parametrize(
    "client, url, status_code, expected_content",
    [
        ["client_no_roles", "/home", 403, "Your user has no roles and/or permissions!"],
        ["client_no_permissions", "/home", 403, "Your user has no roles and/or permissions!"],
        ["client_all_dags", "/home", 200, "DAGs - Airflow"],
        ["client_anonymous", "/home", 200, "Sign In"],
    ],
)
def test_no_roles_permissions(request, client, url, status_code, expected_content):
    resp = request.getfixturevalue(client).get(url, follow_redirects=True)
    check_content_in_response(expected_content, resp, status_code)


@pytest.fixture(scope="module")
def user_dag_level_access_with_ti_edit(acl_app):
    with create_user_scope(
        acl_app,
        username="user_dag_level_access_with_ti_edit",
        role_name="role_dag_level_access_with_ti_edit",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE),
            (
                permissions.ACTION_CAN_EDIT,
                _resource_name("example_bash_operator", permissions.RESOURCE_DAG),
            ),
        ],
    ) as user:
        yield user


@pytest.fixture
def client_dag_level_access_with_ti_edit(acl_app, user_dag_level_access_with_ti_edit):
    return client_with_login(
        acl_app,
        username="user_dag_level_access_with_ti_edit",
        password="user_dag_level_access_with_ti_edit",
    )


def test_success_edit_ti_with_dag_level_access_only(client_dag_level_access_with_ti_edit):
    form = dict(
        task_id="run_this_last",
        dag_id="example_bash_operator",
        dag_run_id=DEFAULT_RUN_ID,
        upstream="false",
        downstream="false",
        future="false",
        past="false",
    )
    resp = client_dag_level_access_with_ti_edit.post("/success", data=form, follow_redirects=True)
    check_content_in_response("Marked success on 1 task instances", resp)


@pytest.fixture(scope="module")
def user_ti_edit_without_dag_level_access(acl_app):
    with create_user_scope(
        acl_app,
        username="user_ti_edit_without_dag_level_access",
        role_name="role_ti_edit_without_dag_level_access",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE),
        ],
    ) as user:
        yield user


@pytest.fixture
def client_ti_edit_without_dag_level_access(acl_app, user_ti_edit_without_dag_level_access):
    return client_with_login(
        acl_app,
        username="user_ti_edit_without_dag_level_access",
        password="user_ti_edit_without_dag_level_access",
    )


def test_failure_edit_ti_without_dag_level_access(client_ti_edit_without_dag_level_access):
    form = dict(
        task_id="run_this_last",
        dag_id="example_bash_operator",
        dag_run_id=DEFAULT_RUN_ID,
        upstream="false",
        downstream="false",
        future="false",
        past="false",
    )
    resp = client_ti_edit_without_dag_level_access.post("/success", data=form, follow_redirects=True)
    check_content_not_in_response("Marked success on 1 task instances", resp)
