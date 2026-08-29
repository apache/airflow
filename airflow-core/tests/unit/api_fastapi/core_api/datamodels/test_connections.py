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
from pydantic import ValidationError

from airflow.api_fastapi.core_api.datamodels.connections import ConnectionBody, ConnectionResponse


def _payload(**overrides):
    base = {
        "conn_id": "conn_1",
        "conn_type": "generic",
        "description": None,
        "host": None,
        "login": None,
        "schema": None,
        "port": None,
        "password": None,
        "extra": None,
        "team_name": None,
    }
    return {**base, **overrides}


@pytest.mark.parametrize(
    "extra",
    [
        pytest.param("bearer-token", id="raw-token"),
        pytest.param("key=value&other=x", id="query-string"),
        pytest.param("<xml/>", id="xml"),
    ],
)
def test_redact_extra_rejects_non_json(extra):
    """
    The DB layer (``Connection._validate_extra``) is meant to guarantee ``extra``
    is valid JSON, so this response-model branch is defense-in-depth. If a row
    ever slips past the DB guard (direct SQL, buggy migration, legacy data), we
    must fail closed rather than return the raw value — that was the leak
    described in #63160 before the safeguard.
    """
    with pytest.raises(ValidationError):
        ConnectionResponse.model_validate(_payload(extra=extra))


@pytest.mark.parametrize("port", [0, 80, 5432, 65535, None])
def test_connection_body_valid_port(port):
    conn = ConnectionBody(connection_id="test_conn", conn_type="http", port=port)
    assert conn.port == port


@pytest.mark.parametrize("port", [-1, 65536, 99999999, "invalid"])
def test_connection_body_invalid_port(port):
    with pytest.raises(ValidationError):
        ConnectionBody(connection_id="test_conn", conn_type="http", port=port)
