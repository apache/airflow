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

import pytest
from flask import Flask, session
from werkzeug.exceptions import InternalServerError

from airflow.providers.fab.www import app as fab_app_mod

from tests_common.test_utils.config import conf_vars


@pytest.fixture
def app():
    """Create a test Flask app instance."""
    with conf_vars(
        {
            ("core", "auth_manager"): "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager",
        }
    ):
        test_app = Flask(__name__)
        test_app.secret_key = "test-secret-key"
        yield test_app


class TestCleanupSessionOnInternalError:
    """Test cleanup_session_on_internal_error error handler."""

    def test_cleanup_session_on_internal_error_with_html_accept(self, app, monkeypatch):
        """Test that session is cleared and user is redirected when Accept: text/html."""
        # Mock url_for to avoid needing full Flask route setup
        monkeypatch.setattr(fab_app_mod, "url_for", lambda endpoint: "/airflow")

        with app.test_request_context("/", headers={"Accept": "text/html"}):
            # Set up session with test data
            session["user_id"] = 123
            session["csrf_token"] = "test-token"
            assert len(session) > 0

            # Call the error handler
            err = InternalServerError("Test 500 error")
            resp = fab_app_mod.cleanup_session_on_internal_error(err)

            # Assert session was cleared
            assert len(session) == 0
            # Assert response is a redirect (301 or 302)
            assert hasattr(resp, "status_code")
            assert resp.status_code in (301, 302)
            assert "Location" in resp.headers
