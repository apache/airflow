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

from collections.abc import Callable, Iterator
from functools import partial
from pathlib import Path
from unittest import mock

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import StaleDataError

from airflow.models.connection import Connection
from airflow.models.variable import Variable
from airflow.sdk import SecretCache
from airflow.sdk.exceptions import AirflowSecretsBackendAccessDenied
from airflow.secrets.environment_variables import EnvironmentVariablesBackend
from airflow.secrets.metastore import MetastoreBackend
from airflow.utils.session import create_session

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_connections, clear_db_variables

pytestmark = pytest.mark.db_test


class TestMetastoreBackendSessionSafety:
    """MetastoreBackend must not corrupt the shared scoped session.

    Regression tests for https://github.com/apache/airflow/issues/62244.
    """

    def setup_method(self) -> None:
        clear_db_connections()
        clear_db_variables()

    def teardown_method(self) -> None:
        clear_db_connections()
        clear_db_variables()

    @pytest.mark.parametrize("conn_exists", [True, False], ids=["found", "not_found"])
    def test_get_connection_preserves_pending_session_objects(self, conn_exists):
        """get_connection must not remove unrelated pending objects from session.new."""
        if conn_exists:
            with create_session() as session:
                session.add(Connection(conn_id="target_conn", conn_type="mysql"))
                session.commit()

        with create_session() as session:
            # Simulate pending work from another function sharing the session
            pending = Connection(conn_id="pending_conn", conn_type="http")
            session.add(pending)

            # Same session passed to simulate shared scoped session behavior
            backend = MetastoreBackend()
            result = backend.get_connection("target_conn", session=session)

            if conn_exists:
                assert result is not None
                assert result.conn_id == "target_conn"
            else:
                assert result is None
            # The pending object must still be in session.new — expunge(conn) should only
            # detach the queried Connection, not wipe unrelated pending objects.
            assert pending in session.new

    @pytest.mark.parametrize("var_exists", [True, False], ids=["found", "not_found"])
    def test_get_variable_preserves_pending_session_objects(self, var_exists):
        """get_variable must not remove unrelated pending objects from session.new."""
        if var_exists:
            Variable.set(key="test_key", value="test_value")

        with create_session() as session:
            # Use any ORM model as the pending object to detect session corruption
            pending = Connection(conn_id="pending_conn", conn_type="http")
            session.add(pending)

            backend = MetastoreBackend()
            result = backend.get_variable("test_key", session=session)

            if var_exists:
                assert result == "test_value"
            else:
                assert result is None
            # The pending object must still be in session.new — expunge(var_value) should only
            # detach the queried Variable, not wipe unrelated pending objects.
            assert pending in session.new

    def test_get_connection_returns_detached_object(self):
        """Returned connection must be detached so callers can use it freely."""
        from sqlalchemy import inspect as sa_inspect

        with create_session() as session:
            session.add(Connection(conn_id="test_conn", conn_type="mysql", host="localhost"))
            session.commit()

        backend = MetastoreBackend()
        conn = backend.get_connection("test_conn")

        assert conn is not None
        # Object should be detached — not tracked by any session
        assert sa_inspect(conn).detached
        # Attributes should still be accessible
        assert conn.conn_id == "test_conn"
        assert conn.host == "localhost"


class TestMetastoreBackendErrors:
    @pytest.fixture(autouse=True)
    def reset_cache(self) -> Iterator[None]:
        SecretCache.reset()
        with conf_vars({("secrets", "use_cache"): "true"}):
            SecretCache.init()
            yield
        SecretCache.reset()

    @pytest.mark.parametrize(
        ("method", "kwargs"),
        [("get_connection", {"conn_id": "target"}), ("get_variable", {"key": "target"})],
    )
    def test_database_error_does_not_roll_back_caller_session(
        self, tmp_path: Path, method: str, kwargs: dict[str, str]
    ) -> None:
        engine = create_engine(f"sqlite:///{tmp_path / 'unmigrated.db'}")
        try:
            with Session(bind=engine, autoflush=False) as session:
                pending = Connection(conn_id="pending", conn_type="http")
                session.add(pending)
                rollback = mock.Mock()
                event.listen(session, "after_rollback", rollback)

                with pytest.raises(OperationalError, match="no such table"):
                    getattr(MetastoreBackend(), method)(**kwargs, session=session)

                rollback.assert_not_called()
                assert pending in session.new
        finally:
            engine.dispose()

    @pytest.mark.parametrize(
        "error",
        [
            OperationalError("SELECT", {}, RuntimeError("database unavailable")),
            StaleDataError("stale secret"),
        ],
    )
    @pytest.mark.parametrize("found", [False, True])
    @mock.patch.dict("sys.modules", {"airflow.sdk.execution_time.task_runner": None})
    @mock.patch("airflow.configuration.ensure_secrets_loaded", autospec=True)
    def test_connection_database_error_after_fallback(
        self, load: mock.Mock, error: OperationalError | StaleDataError, found: bool
    ) -> None:
        failed = mock.create_autospec(MetastoreBackend, instance=True)
        failed.get_connection.side_effect = error
        later = mock.create_autospec(EnvironmentVariablesBackend, instance=True)
        connection = Connection(conn_id="target", conn_type="http", host="fallback")
        later.get_connection.return_value = connection if found else None
        load.return_value = [failed, later]

        if found:
            assert Connection.get_connection_from_secrets(conn_id="target") is connection
        else:
            with pytest.raises(type(error)) as raised:
                Connection.get_connection_from_secrets(conn_id="target")
            assert raised.value is error
        later.get_connection.assert_called_once_with(conn_id="target", team_name=None)

    @pytest.mark.parametrize(
        "error",
        [
            OperationalError("SELECT", {}, RuntimeError("database unavailable")),
            StaleDataError("stale secret"),
        ],
    )
    @pytest.mark.parametrize("value", [None, "", "fallback"])
    @mock.patch("airflow.models.variable.ensure_secrets_loaded", autospec=True)
    def test_variable_database_error_after_fallback(
        self, load: mock.Mock, error: OperationalError | StaleDataError, value: str | None
    ) -> None:
        failed = mock.create_autospec(MetastoreBackend, instance=True)
        failed.get_variable.side_effect = error
        later = mock.create_autospec(EnvironmentVariablesBackend, instance=True)
        later.get_variable.return_value = value
        load.return_value = [failed, later]

        if value is not None:
            assert Variable.get_variable_from_secrets(key="target") == value
            assert SecretCache.get_variable(key="target") == value
        else:
            with pytest.raises(type(error)) as raised:
                Variable.get_variable_from_secrets(key="target")
            assert raised.value is error
            with pytest.raises(SecretCache.NotPresentException):
                SecretCache.get_variable(key="target")

            failed.get_variable.side_effect = None
            failed.get_variable.return_value = "recovered"
            assert Variable.get_variable_from_secrets(key="target") == "recovered"
        later.get_variable.assert_called_once_with(key="target", team_name=None)

    @pytest.mark.parametrize("kind", ["connection", "variable"])
    @mock.patch.dict("sys.modules", {"airflow.sdk.execution_time.task_runner": None})
    @mock.patch("airflow.configuration.ensure_secrets_loaded", autospec=True)
    @mock.patch("airflow.models.variable.ensure_secrets_loaded", autospec=True)
    def test_access_denied_after_database_error_stops_fallback(
        self, load_variables: mock.Mock, load_connections: mock.Mock, kind: str
    ) -> None:
        failed = mock.create_autospec(MetastoreBackend, instance=True)
        denied = mock.create_autospec(EnvironmentVariablesBackend, instance=True)
        later = mock.create_autospec(EnvironmentVariablesBackend, instance=True)
        getattr(failed, f"get_{kind}").side_effect = StaleDataError("stale secret")
        getattr(denied, f"get_{kind}").side_effect = AirflowSecretsBackendAccessDenied("denied")
        load_variables.return_value = load_connections.return_value = [failed, denied, later]
        lookup: Callable[[], object] = (
            partial(Connection.get_connection_from_secrets, conn_id="target")
            if kind == "connection"
            else partial(Variable.get_variable_from_secrets, key="target")
        )

        with pytest.raises(AirflowSecretsBackendAccessDenied, match="denied"):
            lookup()
        getattr(later, f"get_{kind}").assert_not_called()
