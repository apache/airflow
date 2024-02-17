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

from sqlalchemy import select

from airflow.models.log import Log


def client_with_login(app, expected_response_code=302, **kwargs):
    patch_path = "airflow.providers.fab.auth_manager.security_manager.override.check_password_hash"
    with mock.patch(patch_path) as check_password_hash:
        check_password_hash.return_value = True
        client = app.test_client()
        resp = client.post("/login/", data=kwargs)
        assert resp.status_code == expected_response_code
    return client


def client_without_login(app):
    # Anonymous users can only view if AUTH_ROLE_PUBLIC is set to non-Public
    app.config["AUTH_ROLE_PUBLIC"] = "Viewer"
    client = app.test_client()
    return client


def client_without_login_as_admin(app):
    # Anonymous users as Admin if set AUTH_ROLE_PUBLIC=Admin
    app.config["AUTH_ROLE_PUBLIC"] = "Admin"
    client = app.test_client()
    return client


def check_content_in_response(text, resp, resp_code=200):
    resp_html = resp.data.decode("utf-8")
    assert resp_code == resp.status_code
    if isinstance(text, list):
        for line in text:
            assert line in resp_html, f"Couldn't find {line!r}"
    else:
        assert text in resp_html, f"Couldn't find {text!r}"


def check_content_not_in_response(text, resp, resp_code=200):
    resp_html = resp.data.decode("utf-8")
    assert resp_code == resp.status_code
    if isinstance(text, list):
        for line in text:
            assert line not in resp_html
    else:
        assert text not in resp_html


def get_last_logs(session, dag_id, event, execution_date, limit: int = 5) -> list[Log]:
    return session.scalars(
        select(Log)
        .where(Log.dag_id == dag_id, Log.event == event, Log.execution_date == execution_date)
        .order_by(Log.dttm.desc())
        .limit(limit)
    ).all()
