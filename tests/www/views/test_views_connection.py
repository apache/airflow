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
from unittest import mock

import pytest

from airflow.models import Connection
from airflow.utils.session import create_session
from airflow.www.extensions import init_views
from airflow.www.views import ConnectionModelView
from tests.test_utils.www import check_content_in_response

CONNECTION = {
    'conn_id': 'test_conn',
    'conn_type': 'http',
    'description': 'description',
    'host': 'localhost',
    'port': 8080,
    'username': 'root',
    'password': 'admin',
}


@pytest.fixture(autouse=True)
def clear_connections():
    with create_session() as session:
        session.query(Connection).delete()


def test_create_connection(admin_client):
    init_views.init_connection_form()
    resp = admin_client.post('/connection/add', data=CONNECTION, follow_redirects=True)
    check_content_in_response('Added Row', resp)


def test_prefill_form_null_extra():
    mock_form = mock.Mock()
    mock_form.data = {"conn_id": "test", "extra": None}

    cmv = ConnectionModelView()
    cmv.prefill_form(form=mock_form, pk=1)


def test_duplicate_connection(admin_client):
    """Test Duplicate multiple connection with suffix"""
    conn1 = Connection(
        conn_id='test_duplicate_gcp_connection',
        conn_type='Google Cloud',
        description='Google Cloud Connection',
    )
    conn2 = Connection(
        conn_id='test_duplicate_mysql_connection',
        conn_type='FTP',
        description='MongoDB2',
        host='localhost',
        schema='airflow',
        port=3306,
    )
    conn3 = Connection(
        conn_id='test_duplicate_postgres_connection_copy1',
        conn_type='FTP',
        description='Postgres',
        host='localhost',
        schema='airflow',
        port=3306,
    )
    with create_session() as session:
        session.query(Connection).delete()
        session.add_all([conn1, conn2, conn3])
        session.commit()

    data = {"action": "mulduplicate", "rowid": [conn1.id, conn3.id]}
    resp = admin_client.post('/connection/action_post', data=data, follow_redirects=True)
    expected_result = {
        'test_duplicate_gcp_connection',
        'test_duplicate_gcp_connection_copy1',
        'test_duplicate_mysql_connection',
        'test_duplicate_postgres_connection_copy1',
        'test_duplicate_postgres_connection_copy2',
    }
    response = {conn[0] for conn in session.query(Connection.conn_id).all()}
    assert resp.status_code == 200
    assert expected_result == response
