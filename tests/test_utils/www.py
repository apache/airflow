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

import ast
from unittest import mock

from airflow.models import Log


def client_with_login(app, **kwargs):
    patch_path = "airflow.www.fab_security.manager.check_password_hash"
    with mock.patch(patch_path) as check_password_hash:
        check_password_hash.return_value = True
        client = app.test_client()
        resp = client.post("/login/", data=kwargs)
        assert resp.status_code == 302
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


def _check_last_log(session, dag_id, event, execution_date):
    logs = (
        session.query(
            Log.dag_id,
            Log.task_id,
            Log.event,
            Log.execution_date,
            Log.owner,
            Log.extra,
        )
        .filter(
            Log.dag_id == dag_id,
            Log.event == event,
            Log.execution_date == execution_date,
        )
        .order_by(Log.dttm.desc())
        .limit(5)
        .all()
    )
    assert len(logs) >= 1
    assert logs[0].extra
    session.query(Log).delete()


def _check_last_log_masked_connection(session, dag_id, event, execution_date):
    logs = (
        session.query(
            Log.dag_id,
            Log.task_id,
            Log.event,
            Log.execution_date,
            Log.owner,
            Log.extra,
        )
        .filter(
            Log.dag_id == dag_id,
            Log.event == event,
            Log.execution_date == execution_date,
        )
        .order_by(Log.dttm.desc())
        .limit(5)
        .all()
    )
    assert len(logs) >= 1
    extra = ast.literal_eval(logs[0].extra)
    for k, v in extra:
        if k == "password":
            assert v == "***"
        if k == "extra":
            assert v == '{"x_secret": "***", "y_secret": "***"}'


def _check_last_log_masked_variable(session, dag_id, event, execution_date):
    logs = (
        session.query(
            Log.dag_id,
            Log.task_id,
            Log.event,
            Log.execution_date,
            Log.owner,
            Log.extra,
        )
        .filter(
            Log.dag_id == dag_id,
            Log.event == event,
            Log.execution_date == execution_date,
        )
        .order_by(Log.dttm.desc())
        .limit(5)
        .all()
    )
    assert len(logs) >= 1
    extra_dict = ast.literal_eval(logs[0].extra)
    assert extra_dict == [("key", "x_secret"), ("val", "***")]
