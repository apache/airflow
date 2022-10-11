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

import json
from unittest import mock
from unittest.mock import PropertyMock

import pytest
from pytest import param

from airflow.models import Connection
from airflow.utils.session import create_session
from airflow.www.extensions import init_views
from airflow.www.views import ConnectionFormWidget, ConnectionModelView
from tests.test_utils.www import _check_last_log, check_content_in_response

CONNECTION = {
    "conn_id": "test_conn",
    "conn_type": "http",
    "description": "description",
    "host": "localhost",
    "port": 8080,
    "username": "root",
    "password": "admin",
}


@pytest.fixture(autouse=True)
def clear_connections():
    with create_session() as session:
        session.query(Connection).delete()


def test_create_connection(admin_client, session):
    init_views.init_connection_form()
    resp = admin_client.post("/connection/add", data=CONNECTION, follow_redirects=True)
    check_content_in_response("Added Row", resp)
    _check_last_log(session, dag_id=None, event="connection.create", execution_date=None)


def test_prefill_form_null_extra():
    mock_form = mock.Mock()
    mock_form.data = {"conn_id": "test", "extra": None, "conn_type": "test"}

    cmv = ConnectionModelView()
    cmv.prefill_form(form=mock_form, pk=1)


@pytest.mark.parametrize(
    "extras, expected",
    [
        param({"extra__test__my_param": "this_val"}, "this_val", id="conn_not_upgraded"),
        param({"my_param": "my_val"}, "my_val", id="conn_upgraded"),
        param(
            {"extra__test__my_param": "this_val", "my_param": "my_val"},
            "my_val",
            id="conn_upgraded_old_val_present",
        ),
    ],
)
def test_prefill_form_backcompat(extras, expected):
    """
    When populating custom fields in the connection form we should first check for the non-prefixed
    value (since prefixes in extra are deprecated) and then fallback to the prefixed value.

    Either way, the field is known internally to the model view as the prefixed value.
    """
    mock_form = mock.Mock()
    mock_form.data = {"conn_id": "test", "extra": json.dumps(extras), "conn_type": "test"}
    cmv = ConnectionModelView()
    cmv.extra_fields = ["extra__test__my_param"]

    # this is set by `lazy_add_provider_discovered_options_to_connection_form`
    cmv.extra_field_name_mapping["extra__test__my_param"] = "my_param"

    cmv.prefill_form(form=mock_form, pk=1)
    assert mock_form.extra__test__my_param.data == expected


@pytest.mark.parametrize("field_name", ["extra__test__custom_field", "custom_field"])
@mock.patch("airflow.utils.module_loading.import_string")
@mock.patch("airflow.providers_manager.ProvidersManager.hooks", new_callable=PropertyMock)
def test_process_form_extras_both(mock_pm_hooks, mock_import_str, field_name):
    """
    Test the handling of connection parameters set with the classic `Extra` field as well as custom fields.
    The key used in the field definition returned by `get_connection_form_widgets` is stored in
    attr `extra_field_name_mapping`.  Whatever is defined there is what should end up in `extra` when
    the form is processed.
    """
    mock_pm_hooks.get.return_value = True  # ensure that hook appears registered

    # Testing parameters set in both `Extra` and custom fields.
    mock_form = mock.Mock()
    mock_form.data = {
        "conn_type": "test",
        "conn_id": "extras_test",
        "extra": '{"param1": "param1_val"}',
        "extra__test__custom_field": "custom_field_val",
        "extra__other_conn_type__custom_field": "another_field_val",
    }

    cmv = ConnectionModelView()

    # this is set by `lazy_add_provider_discovered_options_to_connection_form`
    cmv.extra_field_name_mapping["extra__test__custom_field"] = field_name
    cmv.extra_fields = ["extra__test__custom_field"]  # Custom field
    cmv.process_form(form=mock_form, is_created=True)

    assert json.loads(mock_form.extra.data) == {
        field_name: "custom_field_val",
        "param1": "param1_val",
    }


@mock.patch("airflow.utils.module_loading.import_string")
@mock.patch("airflow.providers_manager.ProvidersManager.hooks", new_callable=PropertyMock)
def test_process_form_extras_extra_only(mock_pm_hooks, mock_import_str):
    """
    Test the handling of connection parameters set with the classic `Extra` field as well as custom fields.
    The key used in the field definition returned by `get_connection_form_widgets` is stored in
    attr `extra_field_name_mapping`.  Whatever is defined there is what should end up in `extra` when
    the form is processed.
    """
    # Testing parameters set in `Extra` field only.
    mock_form = mock.Mock()
    mock_form.data = {
        "conn_type": "test2",
        "conn_id": "extras_test2",
        "extra": '{"param2": "param2_val"}',
    }

    cmv = ConnectionModelView()

    cmv.process_form(form=mock_form, is_created=True)

    assert json.loads(mock_form.extra.data) == {"param2": "param2_val"}


@pytest.mark.parametrize("field_name", ["extra__test3__custom_field", "custom_field"])
@mock.patch("airflow.utils.module_loading.import_string")
@mock.patch("airflow.providers_manager.ProvidersManager.hooks", new_callable=PropertyMock)
def test_process_form_extras_custom_only(mock_pm_hooks, mock_import_str, field_name):
    """
    Test the handling of connection parameters set with the classic `Extra` field as well as custom fields.
    The key used in the field definition returned by `get_connection_form_widgets` is stored in
    attr `extra_field_name_mapping`.  Whatever is defined there is what should end up in `extra` when
    the form is processed.
    """

    # Testing parameters set in custom fields only.
    mock_form = mock.Mock()
    mock_form.data = {
        "conn_type": "test3",
        "conn_id": "extras_test3",
        "extra__test3__custom_field": False,
        "extra__other_conn_type__custom_field": "another_field_val",
    }

    cmv = ConnectionModelView()
    cmv.extra_fields = ["extra__test3__custom_field", "extra__test3__custom_bool_field"]  # Custom fields

    # this is set by `lazy_add_provider_discovered_options_to_connection_form`
    cmv.extra_field_name_mapping["extra__test3__custom_field"] = field_name
    cmv.process_form(form=mock_form, is_created=True)

    assert json.loads(mock_form.extra.data) == {field_name: False}


@pytest.mark.parametrize("field_name", ["extra__test4__custom_field", "custom_field"])
@mock.patch("airflow.utils.module_loading.import_string")
@mock.patch("airflow.providers_manager.ProvidersManager.hooks", new_callable=PropertyMock)
def test_process_form_extras_updates(mock_pm_hooks, mock_import_str, field_name):
    """
    Test the handling of connection parameters set with the classic `Extra` field as well as custom fields.
    The key used in the field definition returned by `get_connection_form_widgets` is stored in
    attr `extra_field_name_mapping`.  Whatever is defined there is what should end up in `extra` when
    the form is processed.
    """

    # Testing parameters set in both extra and custom fields (connection updates).
    mock_form = mock.Mock()
    mock_form.data = {
        "conn_type": "test4",
        "conn_id": "extras_test4",
        "extra": '{"extra__test4__custom_field": "custom_field_val3"}',
        "extra__test4__custom_field": "custom_field_val4",
    }

    cmv = ConnectionModelView()
    cmv.extra_fields = ["extra__test4__custom_field"]  # Custom field

    # this is set by `lazy_add_provider_discovered_options_to_connection_form`
    cmv.extra_field_name_mapping["extra__test4__custom_field"] = field_name

    cmv.process_form(form=mock_form, is_created=True)

    if field_name == "custom_field":
        assert json.loads(mock_form.extra.data) == {
            "custom_field": "custom_field_val4",
            "extra__test4__custom_field": "custom_field_val3",
        }
    else:
        assert json.loads(mock_form.extra.data) == {"extra__test4__custom_field": "custom_field_val4"}


def test_duplicate_connection(admin_client):
    """Test Duplicate multiple connection with suffix"""
    conn1 = Connection(
        conn_id="test_duplicate_gcp_connection",
        conn_type="Google Cloud",
        description="Google Cloud Connection",
    )
    conn2 = Connection(
        conn_id="test_duplicate_mysql_connection",
        conn_type="FTP",
        description="MongoDB2",
        host="localhost",
        schema="airflow",
        port=3306,
    )
    conn3 = Connection(
        conn_id="test_duplicate_postgres_connection_copy1",
        conn_type="FTP",
        description="Postgres",
        host="localhost",
        schema="airflow",
        port=3306,
    )
    with create_session() as session:
        session.query(Connection).delete()
        session.add_all([conn1, conn2, conn3])
        session.commit()

    data = {"action": "mulduplicate", "rowid": [conn1.id, conn3.id]}
    resp = admin_client.post("/connection/action_post", data=data, follow_redirects=True)
    expected_result = {
        "test_duplicate_gcp_connection",
        "test_duplicate_gcp_connection_copy1",
        "test_duplicate_mysql_connection",
        "test_duplicate_postgres_connection_copy1",
        "test_duplicate_postgres_connection_copy2",
    }
    response = {conn[0] for conn in session.query(Connection.conn_id).all()}
    assert resp.status_code == 200
    assert expected_result == response


def test_duplicate_connection_error(admin_client):
    """Test Duplicate multiple connection with suffix
    when there are already 10 copies, no new copy
    should be created"""

    connection_ids = [f"test_duplicate_postgres_connection_copy{i}" for i in range(1, 11)]
    connections = [
        Connection(
            conn_id=connection_id,
            conn_type="FTP",
            description="Postgres",
            host="localhost",
            schema="airflow",
            port=3306,
        )
        for connection_id in connection_ids
    ]

    with create_session() as session:
        session.query(Connection).delete()
        session.add_all(connections)

    data = {"action": "mulduplicate", "rowid": [connections[0].id]}
    resp = admin_client.post("/connection/action_post", data=data, follow_redirects=True)

    expected_result = {f"test_duplicate_postgres_connection_copy{i}" for i in range(1, 11)}

    assert resp.status_code == 200
    response = {conn[0] for conn in session.query(Connection.conn_id).all()}
    assert expected_result == response


@pytest.fixture()
def connection():
    connection = Connection(
        conn_id="conn1",
        conn_type="Conn 1",
        description="Conn 1 description",
    )
    with create_session() as session:
        session.add(connection)
    yield connection
    with create_session() as session:
        session.query(Connection).filter(Connection.conn_id == CONNECTION["conn_id"]).delete()


def test_connection_muldelete(admin_client, connection):
    conn_id = connection.id
    data = {"action": "muldelete", "rowid": [conn_id]}
    resp = admin_client.post("/connection/action_post", data=data, follow_redirects=True)
    assert resp.status_code == 200
    with create_session() as session:
        assert session.query(Connection).filter(Connection.id == conn_id).count() == 0


@mock.patch("airflow.providers_manager.ProvidersManager.hooks", new_callable=PropertyMock)
def test_connection_form_widgets_testable_types(mock_pm_hooks, admin_client):
    mock_pm_hooks.return_value = {
        "first": mock.MagicMock(connection_testable=True),
        "second": mock.MagicMock(connection_testable=False),
        "third": None,
    }

    assert ["first"] == ConnectionFormWidget().testable_connection_types


def test_process_form_invalid_extra_removed(admin_client):
    """
    Test that when an invalid json `extra` is passed in the form, it is removed and _not_
    saved over the existing extras.
    """
    from airflow.www.views import lazy_add_provider_discovered_options_to_connection_form

    lazy_add_provider_discovered_options_to_connection_form()

    conn_details = {"conn_id": "test_conn", "conn_type": "http"}
    conn = Connection(**conn_details, extra='{"foo": "bar"}')
    conn.id = 1

    with create_session() as session:
        session.add(conn)

    data = {**conn_details, "extra": "Invalid"}
    resp = admin_client.post("/connection/edit/1", data=data, follow_redirects=True)

    assert resp.status_code == 200
    with create_session() as session:
        conn = session.query(Connection).get(1)

    assert conn.extra == '{"foo": "bar"}'
