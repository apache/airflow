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

import threading
from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.fab.auth_manager.api_fastapi.datamodels.roles import RoleResponse
from airflow.providers.fab.auth_manager.api_fastapi.datamodels.users import UserResponse
from airflow.providers.fab.auth_manager.api_fastapi.routes.login import _release_scoped_session

ROUTES = "airflow.providers.fab.auth_manager.api_fastapi.routes"

USER = UserResponse(
    username="alice",
    email="alice@example.com",
    first_name="Alice",
    last_name="Liddell",
    roles=None,
    active=True,
    login_count=0,
    fail_login_count=0,
)
ROLE = RoleResponse(name="Viewer", permissions=[])


class TestReleaseScopedSession:
    @patch("airflow.settings.Session")
    def test_removes_session_on_success(self, mock_session):
        with _release_scoped_session():
            pass
        mock_session.remove.assert_called_once_with()

    @patch("airflow.settings.Session")
    def test_removes_session_when_the_body_raises(self, mock_session):
        with pytest.raises(ValueError, match="boom"):
            with _release_scoped_session():
                raise ValueError("boom")
        mock_session.remove.assert_called_once_with()

    @patch("airflow.settings.Session")
    def test_a_failing_remove_does_not_mask_the_result(self, mock_session):
        mock_session.remove.side_effect = RuntimeError("connection gone")
        with _release_scoped_session():
            result = "ok"
        assert result == "ok"

    @patch("airflow.settings.Session", None)
    def test_no_scoped_session_configured(self):
        with _release_scoped_session():
            pass


@pytest.mark.db_test
class TestRoutesReleaseSessionOnHandlerThread:
    """
    The scoped session is thread-local, so it must be removed on the thread that ran the handler.

    The ``Session.remove()`` issued by ``cleanup_session_middleware`` runs on the event-loop thread
    and does not count: the assertion is on the handler thread.
    """

    @pytest.mark.parametrize(
        ("method", "path", "module", "service", "service_method", "result"),
        [
            ("get", "/fab/v1/users/alice", "users", "FABAuthManagerUsers", "get_user", USER),
            ("delete", "/fab/v1/users/alice", "users", "FABAuthManagerUsers", "delete_user", None),
            ("get", "/fab/v1/roles/Viewer", "roles", "FABAuthManagerRoles", "get_role", ROLE),
            ("delete", "/fab/v1/roles/Viewer", "roles", "FABAuthManagerRoles", "delete_role", None),
        ],
    )
    def test_session_removed_on_handler_thread(
        self, method, path, module, service, service_method, result, test_client, as_user
    ):
        handler_threads: list[int] = []
        remove_threads: list[int] = []

        def _service_call(*args, **kwargs):
            handler_threads.append(threading.get_ident())
            return result

        mock_session = MagicMock()
        mock_session.remove.side_effect = lambda: remove_threads.append(threading.get_ident())
        auth_manager = MagicMock()
        auth_manager.is_authorized_custom_view.return_value = True

        with (
            patch(f"{ROUTES}.{module}.{service}") as mock_service,
            patch(f"{ROUTES}.{module}._get_flask_app"),
            patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager") as mock_gam,
            patch("airflow.settings.Session", mock_session),
            as_user(),
        ):
            mock_gam.return_value = auth_manager
            getattr(mock_service, service_method).side_effect = _service_call
            resp = getattr(test_client, method)(path)

        assert resp.status_code < 300, resp.text
        assert len(handler_threads) == 1
        assert handler_threads[0] != threading.get_ident()
        assert handler_threads[0] in remove_threads
