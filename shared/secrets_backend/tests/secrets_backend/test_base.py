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

from airflow_shared.secrets_backend.base import BaseSecretsBackend


class MockConnection:
    """Mock Connection class for testing deserialize_connection."""

    def __init__(self, conn_id: str, uri: str | None = None, **kwargs):
        self.conn_id = conn_id
        self.uri = uri
        self._kwargs = kwargs

    @classmethod
    def from_json(cls, value: str, conn_id: str):
        import json

        data = json.loads(value)
        return cls(conn_id=conn_id, **data)

    @classmethod
    def from_uri(cls, conn_id: str, uri: str):
        return cls(conn_id=conn_id, uri=uri)


class _TestBackend(BaseSecretsBackend):
    def __init__(self, conn_values: dict[str, str] | None = None, variables: dict[str, str] | None = None):
        self.conn_values = conn_values or {}
        self.variables = variables or {}

    def get_conn_value(self, conn_id: str) -> str | None:
        return self.conn_values.get(conn_id)

    def get_variable(self, key: str) -> str | None:
        return self.variables.get(key)


class TestBaseSecretsBackend:
    @pytest.mark.parametrize(
        ("prefix", "secret_id", "sep", "expected"),
        [
            ("prefix", "secret_id", "/", "prefix/secret_id"),
            ("prefix", "secret_id", ":", "prefix:secret_id"),
        ],
    )
    def test_build_path_with_separator(self, prefix, secret_id, sep, expected):
        path = BaseSecretsBackend.build_path(prefix, secret_id, sep=sep)
        assert path == expected

    def test_get_conn_value_not_implemented(self):
        backend = BaseSecretsBackend()
        with pytest.raises(NotImplementedError):
            backend.get_conn_value("test_conn")

    def test_get_variable_not_implemented(self):
        backend = BaseSecretsBackend()
        with pytest.raises(NotImplementedError):
            backend.get_variable("test_var")

    def test_get_config_returns_none_by_default(self):
        backend = BaseSecretsBackend()
        assert backend.get_config("test_key") is None

    def test_implementation_get_conn_value(self, sample_conn_uri):
        backend = _TestBackend(conn_values={"test_conn": sample_conn_uri})
        conn_value = backend.get_conn_value("test_conn")
        assert conn_value == sample_conn_uri

    def test_concrete_implementation_get_conn_value_missing(self):
        backend = _TestBackend(conn_values={})
        conn_value = backend.get_conn_value("missing_conn")
        assert conn_value is None

    def test_concrete_implementation_get_variable(self):
        backend = _TestBackend(variables={"test_var": "test_value"})
        var_value = backend.get_variable("test_var")
        assert var_value == "test_value"

    def test_concrete_implementation_get_variable_missing(self):
        backend = _TestBackend(variables={})
        var_value = backend.get_variable("missing_var")
        assert var_value is None

    @pytest.mark.parametrize(
        ("conn_id", "expected"),
        [
            ("simple", "simple"),
            ("with-dash", "with-dash"),
            ("with_underscore", "with_underscore"),
            ("with.dot", "with.dot"),
        ],
    )
    def test_get_conn_value_with_various_conn_ids(self, conn_id, expected):
        backend = _TestBackend(conn_values={conn_id: f"uri_{expected}"})
        conn_value = backend.get_conn_value(conn_id)
        assert conn_value == f"uri_{expected}"

    def test_deserialize_connection_json(self, sample_conn_json):
        """Test deserialize_connection with JSON format through _TestBackend."""
        backend = _TestBackend()
        backend._set_connection_class(MockConnection)

        conn = backend.deserialize_connection("test_conn", sample_conn_json)
        assert isinstance(conn, MockConnection)
        assert conn.conn_id == "test_conn"
        assert conn._kwargs["conn_type"] == "mysql"

    def test_deserialize_connection_uri(self, sample_conn_uri):
        """Test deserialize_connection with URI format through _TestBackend."""
        backend = _TestBackend()
        backend._set_connection_class(MockConnection)

        conn = backend.deserialize_connection("test_conn", sample_conn_uri)
        assert isinstance(conn, MockConnection)
        assert conn.conn_id == "test_conn"
        assert conn.uri == sample_conn_uri


class _LegacyConnValueBackend(BaseSecretsBackend):
    """Backend overriding ``get_conn_value`` with the pre-3.2 ``(self, conn_id)`` signature."""

    def __init__(self, conn_values: dict[str, str]):
        self.conn_values = conn_values
        self._set_connection_class(MockConnection)

    def get_conn_value(self, conn_id: str) -> str | None:
        return self.conn_values.get(conn_id)


class _TeamAwareConnValueBackend(BaseSecretsBackend):
    """Backend whose ``get_conn_value`` accepts ``team_name`` (3.2+ signature)."""

    def __init__(self, conn_values: dict[str, str]):
        self.conn_values = conn_values
        self.received_team_name: str | None = None
        self._set_connection_class(MockConnection)

    def get_conn_value(self, conn_id: str, team_name: str | None = None) -> str | None:
        self.received_team_name = team_name
        return self.conn_values.get(conn_id)


class _KwargsConnValueBackend(BaseSecretsBackend):
    """Backend whose ``get_conn_value`` swallows extra kwargs via ``**kwargs``."""

    def __init__(self, conn_values: dict[str, str]):
        self.conn_values = conn_values
        self.received_kwargs: dict = {}
        self._set_connection_class(MockConnection)

    def get_conn_value(self, conn_id: str, **kwargs) -> str | None:
        self.received_kwargs = kwargs
        return self.conn_values.get(conn_id)


class _TeamAwareRaisingBackend(BaseSecretsBackend):
    """``get_conn_value`` declares ``team_name`` but its body raises ``TypeError``."""

    def __init__(self):
        self.call_count = 0
        self._set_connection_class(MockConnection)

    def get_conn_value(self, conn_id: str, team_name: str | None = None) -> str | None:
        self.call_count += 1
        raise TypeError("boom from inside the backend body")


class TestTeamNameBackwardCompat:
    """``get_connection`` must not forward ``team_name`` to overrides that predate it (issue #1333)."""

    @pytest.mark.parametrize("team_name", [None, "team_a"])
    def test_legacy_get_conn_value_signature_does_not_break(self, sample_conn_uri, team_name):
        backend = _LegacyConnValueBackend(conn_values={"test_conn": sample_conn_uri})

        conn = backend.get_connection(conn_id="test_conn", team_name=team_name)

        assert isinstance(conn, MockConnection)
        assert conn.conn_id == "test_conn"

    def test_team_name_forwarded_when_override_accepts_it(self, sample_conn_uri):
        backend = _TeamAwareConnValueBackend(conn_values={"test_conn": sample_conn_uri})

        conn = backend.get_connection(conn_id="test_conn", team_name="team_a")

        assert isinstance(conn, MockConnection)
        assert backend.received_team_name == "team_a"

    def test_team_name_forwarded_to_kwargs_override(self, sample_conn_uri):
        backend = _KwargsConnValueBackend(conn_values={"test_conn": sample_conn_uri})

        conn = backend.get_connection(conn_id="test_conn", team_name="team_a")

        assert isinstance(conn, MockConnection)
        assert backend.received_kwargs == {"team_name": "team_a"}

    def test_team_aware_backend_typeerror_not_masked(self):
        # A TypeError from inside a team_name-aware backend must propagate, not be
        # retried without team_name (which would hide the error and could resolve the
        # lookup against the global scope instead of the requested team).
        backend = _TeamAwareRaisingBackend()

        with pytest.raises(TypeError, match="boom from inside the backend body"):
            backend.get_connection(conn_id="test_conn", team_name="team_a")

        assert backend.call_count == 1

    @pytest.mark.parametrize("team_name", [None, "team_a"])
    def test_legacy_backend_missing_conn_returns_none(self, team_name):
        backend = _LegacyConnValueBackend(conn_values={})

        assert backend.get_connection(conn_id="missing", team_name=team_name) is None
