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
