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

import json
import uuid
from unittest.mock import Mock, patch

import pytest

from airflow.providers.common.compat.sdk import Connection
from airflow.providers.vespa.hooks.vespa import VespaHook


def _make_connection(
    *,
    host: str,
    schema: str | None,
    port: int | None = None,
    extra: dict | None = None,
) -> Connection:
    return Connection(
        conn_id="vespa_test",
        conn_type="vespa",
        host=host,
        schema=schema,
        port=port,
        extra=json.dumps(extra or {}),
    )


class TestGetField:
    """Tests for the _get_field and _get_int_field helpers."""

    def test_bare_key_preferred(self):
        extra = {"namespace": "bare", "extra__vespa__namespace": "prefixed"}
        assert VespaHook._get_field(extra, "namespace") == "bare"

    def test_prefixed_fallback(self):
        extra = {"extra__vespa__namespace": "prefixed"}
        assert VespaHook._get_field(extra, "namespace") == "prefixed"

    def test_missing_returns_none(self):
        assert VespaHook._get_field({}, "namespace") is None

    def test_int_field_from_string(self):
        assert VespaHook._get_int_field({"max_queue_size": "500"}, "max_queue_size") == 500

    def test_int_field_from_int(self):
        assert VespaHook._get_int_field({"max_queue_size": 500}, "max_queue_size") == 500

    def test_int_field_empty_string(self):
        assert VespaHook._get_int_field({"max_queue_size": ""}, "max_queue_size") is None

    def test_int_field_missing(self):
        assert VespaHook._get_int_field({}, "max_queue_size") is None

    def test_int_field_invalid_raises(self):
        with pytest.raises(ValueError, match="must be an integer"):
            VespaHook._get_int_field({"max_queue_size": "abc"}, "max_queue_size")


class TestVespaHook:
    @patch("airflow.providers.vespa.hooks.vespa.BaseHook.get_connection")
    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_init_default(self, mock_vespa, mock_get_connection):
        mock_get_connection.return_value = _make_connection(
            host="https://vespa.example.com:8080",
            schema="test_schema",
            extra={"namespace": "default"},
        )

        hook = VespaHook()

        assert hook.namespace == "default"
        assert hook.schema == "test_schema"

    @patch("airflow.providers.vespa.hooks.vespa.BaseHook.get_connection")
    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_init_with_params(self, mock_vespa, mock_get_connection):
        mock_get_connection.return_value = _make_connection(
            host="https://vespa.example.com:8080",
            schema="conn_schema",
            extra={"namespace": "conn_ns"},
        )

        hook = VespaHook(conn_id="custom_conn", namespace="test_ns", schema="test_schema")

        assert hook.namespace == "test_ns"
        assert hook.schema == "test_schema"

    @patch("airflow.providers.vespa.hooks.vespa.BaseHook.get_connection")
    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_init_schema_resolution_chain(self, mock_vespa, mock_get_connection):
        """Schema resolves: explicit arg > conn.schema > extra['schema']."""
        mock_get_connection.return_value = _make_connection(
            host="https://vespa.example.com:8080",
            schema="",
            extra={"schema": "from_extra"},
        )

        hook = VespaHook()
        assert hook.schema == "from_extra"

    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_from_resolved_connection(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="document",
            namespace="prod",
            extra={},
        )

        assert hook.namespace == "prod"
        assert hook.schema == "document"

    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_from_resolved_connection_schema_fallback_to_extra(self, mock_vespa):
        """When schema arg is empty, falls back to extra['schema']."""
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="",
            extra={"schema": "doc_from_extra"},
        )
        assert hook.schema == "doc_from_extra"

    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_normalise_already_vespa_format(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        docs = [
            {"id": "doc1", "fields": {"title": "Test 1"}},
            {"id": "doc2", "fields": {"title": "Test 2", "content": "Content"}},
        ]
        result = hook._normalise(docs)

        assert len(result) == 2
        assert result[0] == {"id": "doc1", "fields": {"title": "Test 1"}}
        assert result[1] == {"id": "doc2", "fields": {"title": "Test 2", "content": "Content"}}

    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_normalise_raw_fields(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        docs = [
            {"id": "doc1", "title": "Test 1"},
            {"id": "doc2", "title": "Test 2", "content": "Content"},
        ]
        result = hook._normalise(docs)

        assert len(result) == 2
        assert result[0] == {"id": "doc1", "fields": {"title": "Test 1"}}
        assert result[1] == {"id": "doc2", "fields": {"title": "Test 2", "content": "Content"}}

    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_normalise_does_not_mutate_input(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        original = {"id": "doc1", "title": "Test 1"}
        docs = [original]
        hook._normalise(docs)

        assert "id" in original
        assert original == {"id": "doc1", "title": "Test 1"}

    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_normalise_generate_missing_ids_for_feed(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        docs = [{"title": "Test 1"}, {"title": "Test 2"}]
        result = hook._normalise(docs, operation_type="feed")

        assert len(result) == 2
        assert result[0]["fields"] == {"title": "Test 1"}
        assert result[1]["fields"] == {"title": "Test 2"}
        uuid.UUID(result[0]["id"])
        uuid.UUID(result[1]["id"])

    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_normalise_update_requires_id(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        with pytest.raises(ValueError, match="missing required 'id'.*update"):
            hook._normalise([{"title": "no id"}], operation_type="update")

    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_normalise_delete_requires_id(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        with pytest.raises(ValueError, match="missing required 'id'.*delete"):
            hook._normalise([{"fields": {}}], operation_type="delete")

    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_normalise_update_with_vespa_format_requires_id(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        with pytest.raises(ValueError, match="missing required 'id'.*update"):
            hook._normalise([{"fields": {"title": {"assign": "New"}}}], operation_type="update")

    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_default_callback_successful_response(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        mock_response = Mock(spec=["is_successful", "status_code", "get_json"])
        mock_response.is_successful.return_value = True

        hook.default_callback(mock_response, "doc1")
        assert hook.feed_errors_queue.empty()

    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_default_callback_failed_response_with_json(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        mock_response = Mock(spec=["is_successful", "status_code", "get_json"])
        mock_response.is_successful.return_value = False
        mock_response.status_code = 400
        mock_response.get_json.return_value = {"error": "Invalid document format"}

        hook.default_callback(mock_response, "doc1")

        assert not hook.feed_errors_queue.empty()
        error = hook.feed_errors_queue.get()
        assert error["id"] == "doc1"
        assert error["status"] == 400
        assert error["reason"] == {"error": "Invalid document format"}

    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_default_callback_failed_response_json_parse_error(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        mock_response = Mock(spec=["is_successful", "status_code", "get_json"])
        mock_response.is_successful.return_value = False
        mock_response.status_code = 500
        mock_response.get_json.side_effect = ValueError("Invalid JSON")

        hook.default_callback(mock_response, "doc2")

        error = hook.feed_errors_queue.get()
        assert error["id"] == "doc2"
        assert error["status"] == 500
        assert "json_parse_error" in error["reason"]
        assert "Invalid JSON" in error["reason"]["json_parse_error"]
        assert error["reason"]["status_code"] == 500

    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_feed_iterable_success_default_operation(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )
        mock_app = Mock(spec=["feed_async_iterable"])
        hook.vespa_app = mock_app

        docs = [
            {"id": "doc1", "title": "Test 1"},
            {"id": "doc2", "title": "Test 2"},
        ]
        result = hook.feed_iterable(docs)

        mock_app.feed_async_iterable.assert_called_once()
        call_kwargs = mock_app.feed_async_iterable.call_args[1]

        assert call_kwargs["operation_type"] == "feed"
        assert call_kwargs["schema"] == "doc"
        assert call_kwargs["namespace"] == "test"
        assert call_kwargs["callback"] == hook.default_callback
        assert len(call_kwargs["iter"]) == 2
        assert call_kwargs["iter"][0] == {"id": "doc1", "fields": {"title": "Test 1"}}

        assert result["sent"] == 2
        assert result["errors"] == 0

    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_feed_iterable_invalid_operation_type(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        with pytest.raises(ValueError, match="Invalid operation_type"):
            hook.feed_iterable([{"id": "1"}], operation_type="upsert")

    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_feed_iterable_custom_operation_type(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )
        mock_app = Mock(spec=["feed_async_iterable"])
        hook.vespa_app = mock_app

        docs = [{"id": "doc1", "fields": {"title": {"assign": "Updated Title"}}}]
        hook.feed_iterable(docs, operation_type="update")

        call_kwargs = mock_app.feed_async_iterable.call_args[1]
        assert call_kwargs["operation_type"] == "update"
        assert call_kwargs["iter"][0] == {"id": "doc1", "fields": {"title": {"assign": "Updated Title"}}}

    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_feed_iterable_forwards_kwargs(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )
        mock_app = Mock(spec=["feed_async_iterable"])
        hook.vespa_app = mock_app

        docs = [{"id": "doc1", "fields": {"title": {"assign": "New"}}}]
        hook.feed_iterable(docs, operation_type="update", auto_assign=False, create=True)

        call_kwargs = mock_app.feed_async_iterable.call_args[1]
        assert call_kwargs["auto_assign"] is False
        assert call_kwargs["create"] is True

    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_feed_iterable_custom_callback(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )
        mock_app = Mock(spec=["feed_async_iterable"])
        hook.vespa_app = mock_app

        def custom_callback(*args, **kwargs):
            return None

        docs = [{"id": "doc1", "title": "Test"}]
        hook.feed_iterable(docs, callback=custom_callback)

        call_kwargs = mock_app.feed_async_iterable.call_args[1]
        assert call_kwargs["callback"] == custom_callback

    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_feed_iterable_with_optional_params(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={
                "max_queue_size": 1000,
                "max_workers": 8,
                "max_connections": 20,
            },
        )
        mock_app = Mock(spec=["feed_async_iterable"])
        hook.vespa_app = mock_app

        docs = [{"id": "doc1", "title": "Test"}]
        hook.feed_iterable(docs)

        call_kwargs = mock_app.feed_async_iterable.call_args[1]
        assert call_kwargs["max_queue_size"] == 1000
        assert call_kwargs["max_workers"] == 8
        assert call_kwargs["max_connections"] == 20

    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_feed_iterable_with_errors(self, mock_vespa):
        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )
        mock_app = Mock(spec=["feed_async_iterable"])
        hook.vespa_app = mock_app

        def mock_feed_async(**kwargs):
            hook.feed_errors_queue.put({"id": "doc1", "status": 400, "reason": {"error": "Bad request"}})
            hook.feed_errors_queue.put({"id": "doc3", "status": 500, "reason": {"error": "Server error"}})

        mock_app.feed_async_iterable.side_effect = mock_feed_async

        docs = [
            {"id": "doc1", "title": "Bad doc"},
            {"id": "doc2", "title": "Good doc"},
            {"id": "doc3", "title": "Another bad doc"},
        ]
        result = hook.feed_iterable(docs)

        assert result["sent"] == 3
        assert result["errors"] == 2
        assert result["error_details"][0]["id"] == "doc1"
        assert result["error_details"][1]["id"] == "doc3"

    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_authentication_mtls_with_certificates(self, mock_vespa_class):
        VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={
                "client_cert_path": "/path/to/client.pem",
                "client_key_path": "/path/to/client.key",
                "vespa_cloud_secret_token": "unused_token",
            },
        )

        call_kwargs = mock_vespa_class.call_args[1]
        assert call_kwargs["cert"] == "/path/to/client.pem"
        assert call_kwargs["key"] == "/path/to/client.key"
        assert call_kwargs["vespa_cloud_secret_token"] == "unused_token"

    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_authentication_mtls_with_prefixed_keys(self, mock_vespa_class):
        """Backward compatibility: prefixed keys still work."""
        VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={
                "extra__vespa__client_cert_path": "/path/to/client.pem",
                "extra__vespa__client_key_path": "/path/to/client.key",
            },
        )

        call_kwargs = mock_vespa_class.call_args[1]
        assert call_kwargs["cert"] == "/path/to/client.pem"
        assert call_kwargs["key"] == "/path/to/client.key"

    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_authentication_token_fallback(self, mock_vespa_class):
        VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={"vespa_cloud_secret_token": "secret_token_123"},
        )

        call_kwargs = mock_vespa_class.call_args[1]
        assert call_kwargs["vespa_cloud_secret_token"] == "secret_token_123"
        assert call_kwargs["cert"] is None
        assert call_kwargs["key"] is None

    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_authentication_partial_certificates_fallback_to_token(self, mock_vespa_class):
        VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={
                "client_cert_path": "/path/to/client.pem",
                "vespa_cloud_secret_token": "fallback_token",
            },
        )

        call_kwargs = mock_vespa_class.call_args[1]
        assert call_kwargs["cert"] is None
        assert call_kwargs["key"] is None
        assert call_kwargs["vespa_cloud_secret_token"] == "fallback_token"

    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_authentication_no_credentials(self, mock_vespa_class):
        VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        call_kwargs = mock_vespa_class.call_args[1]
        assert call_kwargs["cert"] is None
        assert call_kwargs["key"] is None
        assert call_kwargs["vespa_cloud_secret_token"] is None


class TestVespaHookTestConnection:
    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_test_connection_success(self, mock_vespa_class):
        mock_vespa_instance = mock_vespa_class.return_value
        mock_resp = Mock(spec=["status_code"], status_code=200)
        mock_vespa_instance.get_application_status.return_value = mock_resp

        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        ok, msg = hook.test_connection()

        assert ok is True
        assert msg == "Connection successful"
        mock_vespa_instance.get_application_status.assert_called_once()

    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_test_connection_failure(self, mock_vespa_class):
        mock_vespa_instance = mock_vespa_class.return_value
        mock_resp = Mock(spec=["status_code"], status_code=503)
        mock_vespa_instance.get_application_status.return_value = mock_resp

        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        ok, msg = hook.test_connection()

        assert ok is False
        assert "503" in msg

    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_test_connection_none_response(self, mock_vespa_class):
        mock_vespa_instance = mock_vespa_class.return_value
        mock_vespa_instance.get_application_status.return_value = None

        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        ok, msg = hook.test_connection()

        assert ok is False
        assert "no response" in msg

    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_test_connection_exception(self, mock_vespa_class):
        mock_vespa_instance = mock_vespa_class.return_value
        mock_vespa_instance.get_application_status.side_effect = Exception("Connection refused")

        hook = VespaHook.from_resolved_connection(
            host="https://vespa.example.com:8080",
            schema="doc",
            namespace="test",
            extra={},
        )

        ok, msg = hook.test_connection()

        assert ok is False
        assert "Connection refused" in msg


class TestVespaHookConfiguration:
    """Tests for Vespa client construction through both __init__ and from_resolved_connection."""

    @patch("airflow.providers.vespa.hooks.vespa.BaseHook.get_connection")
    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_conn_id_stored_on_instance(self, mock_vespa, mock_get_connection):
        """Regression: conn_name_attr='conn_id' requires self.conn_id to be set."""
        mock_get_connection.return_value = _make_connection(host="https://vespa.example.com", schema="doc")

        hook = VespaHook(conn_id="custom_conn")

        assert hook.conn_id == "custom_conn"

    @patch("airflow.providers.vespa.hooks.vespa.BaseHook.get_connection")
    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_init_bare_host_gets_protocol_prepended(self, mock_vespa, mock_get_connection):
        mock_get_connection.return_value = _make_connection(
            host="vespa.example.com",
            schema="doc",
            extra={"protocol": "https"},
        )

        VespaHook(conn_id="test")

        assert mock_vespa.call_args[1]["url"] == "https://vespa.example.com"

    @patch("airflow.providers.vespa.hooks.vespa.BaseHook.get_connection")
    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_init_host_with_scheme_ignores_protocol(self, mock_vespa, mock_get_connection):
        mock_get_connection.return_value = _make_connection(
            host="https://vespa.example.com",
            schema="doc",
            extra={"protocol": "http"},
        )

        VespaHook(conn_id="test")

        assert mock_vespa.call_args[1]["url"] == "https://vespa.example.com"

    @patch("airflow.providers.vespa.hooks.vespa.BaseHook.get_connection")
    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_init_port_appended_when_missing(self, mock_vespa, mock_get_connection):
        mock_get_connection.return_value = _make_connection(
            host="https://vespa.example.com",
            schema="doc",
            port=4080,
        )

        VespaHook(conn_id="test")

        assert mock_vespa.call_args[1]["url"] == "https://vespa.example.com:4080"

    @patch("airflow.providers.vespa.hooks.vespa.BaseHook.get_connection")
    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_init_port_not_duplicated(self, mock_vespa, mock_get_connection):
        mock_get_connection.return_value = _make_connection(
            host="https://vespa.example.com:4080",
            schema="doc",
            port=4080,
        )

        VespaHook(conn_id="test")

        assert mock_vespa.call_args[1]["url"] == "https://vespa.example.com:4080"

    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_from_resolved_token_passed_through(self, mock_vespa):
        VespaHook.from_resolved_connection(
            host="https://vespa.example.com",
            schema="doc",
            extra={"vespa_cloud_secret_token": "tok_123"},
        )

        call_kwargs = mock_vespa.call_args[1]
        assert call_kwargs["vespa_cloud_secret_token"] == "tok_123"
        assert call_kwargs["cert"] is None
        assert call_kwargs["key"] is None

    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_from_resolved_cert_and_key_passed_through(self, mock_vespa):
        VespaHook.from_resolved_connection(
            host="https://vespa.example.com",
            schema="doc",
            extra={
                "client_cert_path": "/certs/client.pem",
                "client_key_path": "/certs/client.key",
            },
        )

        call_kwargs = mock_vespa.call_args[1]
        assert call_kwargs["cert"] == "/certs/client.pem"
        assert call_kwargs["key"] == "/certs/client.key"

    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_from_resolved_partial_cert_falls_back_to_no_cert(self, mock_vespa):
        VespaHook.from_resolved_connection(
            host="https://vespa.example.com",
            schema="doc",
            extra={"client_cert_path": "/certs/client.pem"},
        )

        call_kwargs = mock_vespa.call_args[1]
        assert call_kwargs["cert"] is None
        assert call_kwargs["key"] is None

    @patch("airflow.providers.vespa.hooks.vespa.Vespa")
    def test_from_resolved_port_appended(self, mock_vespa):
        VespaHook.from_resolved_connection(
            host="https://vespa.example.com",
            port=19071,
            schema="doc",
            extra={},
        )

        assert mock_vespa.call_args[1]["url"] == "https://vespa.example.com:19071"
