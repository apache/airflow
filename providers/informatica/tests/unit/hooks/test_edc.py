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

from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.informatica.hooks.edc import InformaticaEDCHook


@pytest.fixture
def hook():
    return InformaticaEDCHook(informatica_edc_conn_id="test_conn")


@patch("airflow.providers.informatica.hooks.edc.HttpHook.get_connection")
def test_config_property_and_build_connection_config(mock_get_connection, hook):
    """Test config property and _build_connection_config method."""
    mock_conn = MagicMock()
    mock_conn.host = "testhost"
    mock_conn.schema = "https"
    mock_conn.port = 443
    mock_conn.login = "user"
    mock_conn.password = "pass"
    mock_conn.extra_dejson = {
        "verify_ssl": True,
        "provider_id": "test_provider",
        "modified_by": "tester",
        "security_domain": "domain",
    }
    mock_get_connection.return_value = mock_conn
    config = hook.config
    assert config.base_url == "https://testhost:443"
    assert config.username == "user"
    assert config.password == "pass"
    assert config.security_domain == "domain"
    assert config.provider_id == "test_provider"
    assert config.modified_by == "tester"
    assert config.verify_ssl is True
    assert isinstance(config.request_timeout, int)
    assert config.auth_header.startswith("Basic ")


@patch("airflow.providers.informatica.hooks.edc.HttpHook.get_connection")
@patch("airflow.providers.informatica.hooks.edc.HttpHook.get_conn")
def test_get_conn_headers_and_verify(mock_get_conn, mock_get_connection, hook):
    """Test get_conn sets headers and verify."""
    mock_conn = MagicMock()
    mock_conn.host = "testhost"
    mock_conn.schema = "https"
    mock_conn.port = 443
    mock_conn.login = "user"
    mock_conn.password = "pass"
    mock_conn.extra_dejson = {"verify_ssl": True}
    mock_get_connection.return_value = mock_conn
    mock_session = MagicMock()
    mock_session.headers = {}
    mock_get_conn.return_value = mock_session
    session = hook.get_conn()
    assert "Accept" in session.headers
    assert "Content-Type" in session.headers
    assert "Authorization" in session.headers
    assert session.verify is True


def test_build_url(hook):
    """Test _build_url method."""
    hook._config = MagicMock(base_url="http://test")
    url = hook._build_url("endpoint")
    assert url == "http://test/endpoint"
    url2 = hook._build_url("/endpoint")
    assert url2 == "http://test/endpoint"


@patch("airflow.providers.informatica.hooks.edc.InformaticaEDCHook.get_conn")
def test_request_success_and_error(mock_get_conn, hook):
    """Test _request method for success and error cases."""
    mock_session = MagicMock()
    mock_response = MagicMock()
    mock_response.ok = True
    mock_response.status_code = 200
    mock_response.text = ""
    mock_response.json.return_value = {"result": "ok"}
    mock_session.request.return_value = mock_response
    mock_get_conn.return_value = mock_session
    hook._config = MagicMock(base_url="http://test", request_timeout=10)
    resp = hook._request("GET", "endpoint")
    assert resp.json() == {"result": "ok"}

    # Error case
    mock_response.ok = False
    mock_response.status_code = 400
    mock_response.text = "Bad Request"
    mock_session.request.return_value = mock_response
    try:
        hook._request("GET", "endpoint")
        pytest.fail("Expected exception was not raised")
    except Exception:
        pass


def test_encode_id(hook):
    """Test _encode_id method for tilde and percent encoding."""
    # ID with unsafe chars
    unsafe_id = "table:___name/unsafe"
    encoded = hook._encode_id(unsafe_id, tilde=True)
    assert "~" in encoded
    encoded_percent = hook._encode_id(unsafe_id, tilde=False)
    assert "%" in encoded_percent


@patch("airflow.providers.informatica.hooks.edc.InformaticaEDCHook._request")
def test_get_object(mock_request, hook):
    """Test get_object method."""
    mock_request.return_value.json.return_value = {"id": "table://database/schema/safe", "name": "test"}
    hook._config = MagicMock(base_url="http://test", request_timeout=10)
    obj = hook.get_object("table://database/schema/safe")
    assert obj["id"] == "table://database/schema/safe"
    assert obj["name"] == "test"


@patch("airflow.providers.informatica.hooks.edc.InformaticaEDCHook._request")
def test_create_lineage_link(mock_request, hook):
    """Test create_lineage_link method and error for same source/target."""
    hook._config = MagicMock(
        base_url="http://test", provider_id="prov", modified_by="mod", request_timeout=10
    )
    mock_request.return_value.content = b'{"success": true}'
    mock_request.return_value.json.return_value = {"success": True}
    result = hook.create_lineage_link("src_id", "tgt_id")
    assert result["success"] is True
    # Error for same source/target
    try:
        hook.create_lineage_link("same_id", "same_id")
        pytest.fail("Expected exception was not raised")
    except Exception:
        pass


def test_close_session(hook):
    """Test close_session does nothing (no-op)."""
    assert hook.close_session() is None
