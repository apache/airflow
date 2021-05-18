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
