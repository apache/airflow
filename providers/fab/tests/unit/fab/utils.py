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
import json
from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, NamedTuple
from unittest import mock

import flask
import pytest

from airflow.models import Log

if TYPE_CHECKING:
    import jinja2


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
            assert line in resp_html, f"Couldn't find {line!r}\nThe response was:\n{resp_html!r}"
    else:
        assert text in resp_html, f"Couldn't find {text!r}\nThe response was:\n{resp_html!r}"


def check_content_not_in_response(text, resp, resp_code=200):
    resp_html = resp.data.decode("utf-8")
    assert resp_code == resp.status_code
    if isinstance(text, list):
        for line in text:
            assert line not in resp_html
    else:
        assert text not in resp_html


def _check_last_log(session, dag_id, event, logical_date, expected_extra=None):
    logs = (
        session.query(
            Log.dag_id,
            Log.task_id,
            Log.event,
            Log.logical_date,
            Log.owner,
            Log.extra,
        )
        .filter(
            Log.dag_id == dag_id,
            Log.event == event,
            Log.logical_date == logical_date,
        )
        .order_by(Log.dttm.desc())
        .limit(5)
        .all()
    )
    assert len(logs) >= 1
    assert logs[0].extra
    if expected_extra:
        assert json.loads(logs[0].extra) == expected_extra
    session.query(Log).delete()


def _check_last_log_masked_connection(session, dag_id, event, logical_date):
    logs = (
        session.query(
            Log.dag_id,
            Log.task_id,
            Log.event,
            Log.logical_date,
            Log.owner,
            Log.extra,
        )
        .filter(
            Log.dag_id == dag_id,
            Log.event == event,
            Log.logical_date == logical_date,
        )
        .order_by(Log.dttm.desc())
        .limit(5)
        .all()
    )
    assert len(logs) >= 1
    extra = ast.literal_eval(logs[0].extra)
    assert extra == {
        "conn_id": "test_conn",
        "conn_type": "http",
        "description": "description",
        "host": "localhost",
        "port": "8080",
        "username": "root",
        "password": "***",
        "extra": {"x_secret": "***", "y_secret": "***"},
    }


def _check_last_log_masked_variable(session, dag_id, event, logical_date):
    logs = (
        session.query(
            Log.dag_id,
            Log.task_id,
            Log.event,
            Log.logical_date,
            Log.owner,
            Log.extra,
        )
        .filter(
            Log.dag_id == dag_id,
            Log.event == event,
            Log.logical_date == logical_date,
        )
        .order_by(Log.dttm.desc())
        .limit(5)
        .all()
    )
    assert len(logs) >= 1
    extra_dict = ast.literal_eval(logs[0].extra)
    assert extra_dict == {"key": "x_secret", "val": "***"}


class _TemplateWithContext(NamedTuple):
    template: jinja2.environment.Template
    context: dict[str, Any]

    @property
    def name(self):
        return self.template.name

    @property
    def local_context(self):
        """Returns context without global arguments."""
        result = self.context.copy()
        keys_to_delete = [
            # flask.templating._default_template_ctx_processor
            "g",
            "request",
            "session",
            # flask_wtf.csrf.CSRFProtect.init_app
            "csrf_token",
            # flask_login.utils._user_context_processor
            "current_user",
            # flask_appbuilder.baseviews.BaseView.render_template
            "appbuilder",
            "base_template",
            # airflow.www.app.py.create_app (inner method - jinja_globals)
            "server_timezone",
            "default_ui_timezone",
            "hostname",
            "navbar_color",
            "navbar_text_color",
            "navbar_hover_color",
            "navbar_text_hover_color",
            "navbar_logo_text_color",
            "log_fetch_delay_sec",
            "log_auto_tailing_offset",
            "log_animation_speed",
            "state_color_mapping",
            "airflow_version",
            "git_version",
            "k8s_or_k8scelery_executor",
            # airflow.www.static_config.configure_manifest_files
            "url_for_asset",
            # airflow.www.views.AirflowBaseView.render_template
            "scheduler_job",
            # airflow.www.views.AirflowBaseView.extra_args
            "macros",
            "auth_manager",
            "triggerer_job",
        ]
        for key in keys_to_delete:
            if key in result:
                del result[key]

        return result


@pytest.fixture(scope="module")
def capture_templates(app):
    @contextmanager
    def manager() -> Generator[list[_TemplateWithContext], None, None]:
        recorded = []

        def record(sender, template, context, **extra):
            recorded.append(_TemplateWithContext(template, context))

        flask.template_rendered.connect(record, app)  # type: ignore
        try:
            yield recorded
        finally:
            flask.template_rendered.disconnect(record, app)  # type: ignore

        assert recorded, "Failed to catch the templates"

    return manager
