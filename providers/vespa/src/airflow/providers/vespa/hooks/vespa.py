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

import uuid
from collections.abc import Callable, Iterable
from queue import Empty, Queue
from typing import TYPE_CHECKING, Any

from vespa.application import Vespa

from airflow.providers.common.compat.sdk import BaseHook

if TYPE_CHECKING:
    from vespa.io import VespaResponse

    from airflow.providers.common.compat.sdk import Connection

VALID_OPERATION_TYPES = frozenset({"feed", "update", "delete"})


class VespaHook(BaseHook):
    """
    Hook for interacting with a Vespa cluster via pyvespa.

    Supports document feed, update, and delete operations through
    ``feed_async_iterable``. Uses a custom ``vespa`` connection type.

    Connection extras (bare keys canonical, ``extra__vespa__`` prefix
    accepted for backward compatibility):

    - ``namespace`` -- Vespa namespace (default: ``"default"``)
    - ``max_queue_size`` / ``max_workers`` / ``max_connections`` -- feed tuning
    - ``vespa_cloud_secret_token`` -- token auth
    - ``client_cert_path`` / ``client_key_path`` -- mTLS auth
    - ``protocol`` -- ``http`` or ``https`` (default: ``http``)
    """

    conn_name_attr = "conn_id"
    default_conn_name = "vespa_default"
    conn_type = "vespa"
    hook_name = "Vespa"

    @staticmethod
    def _get_field(extra: dict[str, Any], field: str) -> Any:
        """Return an extra-field value, preferring bare key over legacy prefix."""
        if field in extra:
            return extra[field]
        return extra.get(f"extra__vespa__{field}")

    @staticmethod
    def _get_int_field(extra: dict[str, Any], field: str) -> int | None:
        """Return an extra field cast to ``int``, or *None* if absent or empty."""
        val = VespaHook._get_field(extra, field)
        if val is None or val == "":
            return None
        try:
            return int(val)
        except (TypeError, ValueError):
            raise ValueError(f"Connection extra '{field}' must be an integer, got {val!r}")

    def __init__(
        self,
        conn_id: str = default_conn_name,
        *,
        namespace: str | None = None,
        schema: str | None = None,
    ) -> None:
        """
        Create a VespaHook.

        Parameters ``namespace`` and ``schema`` override values stored in the
        Airflow ``Connection``. This is convenient when you want to share the
        same endpoint across multiple DAGs but write to different schemas or
        namespaces without creating separate connections.
        """
        super().__init__()
        self.conn_id = conn_id
        conn: Connection = self.get_connection(conn_id)
        self.conn = conn
        extra = self.conn.extra_dejson or {}
        host = self.conn.host

        if not host:
            raise ValueError("Vespa connection requires a host")

        resolved_schema = schema or self.conn.schema or self._get_field(extra, "schema")
        resolved_namespace = namespace or self._get_field(extra, "namespace") or "default"

        self._configure_from_connection(
            host=host,
            port=self.conn.port,
            namespace=resolved_namespace,
            schema=resolved_schema,
            extra=extra,
        )

    @classmethod
    def from_resolved_connection(
        cls,
        *,
        host: str,
        port: int | None = None,
        schema: str | None = None,
        namespace: str | None = None,
        extra: dict[str, Any],
    ) -> VespaHook:
        """
        Instantiate without querying Airflow's metadata database.

        Intended for trigger processes where synchronous DB access is
        prohibited. The caller must supply the already-resolved connection
        parameters.
        """
        self = cls.__new__(cls)
        BaseHook.__init__(self)

        resolved_schema = schema or cls._get_field(extra, "schema")
        resolved_namespace = namespace or cls._get_field(extra, "namespace") or "default"

        self._configure_from_connection(
            host=host,
            port=port,
            namespace=resolved_namespace,
            schema=resolved_schema,
            extra=extra,
        )
        return self

    # On Airflow 3 the YAML metadata in provider.yaml takes precedence and these
    # methods are skipped.  They are kept for Airflow 2.11 backward compatibility.

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        try:
            from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget
            from wtforms import PasswordField, StringField
        except ImportError:
            return {}

        return {
            "namespace": StringField("Namespace"),
            "protocol": StringField("Protocol"),
            "max_queue_size": StringField("Max feed queue size"),
            "max_workers": StringField("Max feed workers"),
            "max_connections": StringField("Max feed connections"),
            "vespa_cloud_secret_token": PasswordField(
                "Vespa Cloud secret token", widget=BS3PasswordFieldWidget()
            ),
            "client_cert_path": StringField("Client certificate file path"),
            "client_key_path": StringField("Client key file path"),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["login", "password", "extra"],
            "relabeling": {
                "host": "Endpoint",
                "schema": "Document Type",
            },
            "placeholders": {
                "host": "vespa-endpoint.example.com",
                "schema": "my_document_type",
                "protocol": "https",
                "namespace": "my_namespace",
                "client_cert_path": "/path/to/client.pem",
                "client_key_path": "/path/to/client.key",
            },
        }

    def _configure_from_connection(
        self,
        *,
        host: str,
        port: int | None = None,
        namespace: str,
        schema: str | None,
        extra: dict[str, Any],
    ) -> None:
        """Populate instance attributes shared by both constructors."""
        cert_file = self._get_field(extra, "client_cert_path")
        key_file = self._get_field(extra, "client_key_path")
        token = self._get_field(extra, "vespa_cloud_secret_token")

        # mTLS requires both cert and key; if either is missing, discard both
        # and fall through to token-based auth.
        if not cert_file or not key_file:
            self.log.info("No client certificate or key found, trying token authentication")
            cert_file = None
            key_file = None

            if not token:
                self.log.info("No token found either. Not authenticating.")
            else:
                self.log.info("Token authentication available")
        else:
            self.log.info("Using mTLS authentication with certificate files")

        url = host.rstrip("/")
        protocol = self._get_field(extra, "protocol") or "http"

        if host.startswith("http://") or host.startswith("https://"):
            url = host
        else:
            url = f"{protocol}://{host}"

        if port and not url.endswith(f":{port}"):
            url = f"{url}:{port}"

        self.log.info("Connecting to Vespa at %s", url)

        self.vespa_app = Vespa(
            url=url,
            vespa_cloud_secret_token=token,
            cert=cert_file,
            key=key_file,
        )

        self.namespace = namespace
        self.schema = schema
        self.max_queue_size = self._get_int_field(extra, "max_queue_size")
        self.max_workers = self._get_int_field(extra, "max_workers")
        self.max_connections = self._get_int_field(extra, "max_connections")
        # pyvespa fires per-document callbacks from worker threads; a Queue
        # collects errors safely so feed_iterable() can aggregate them.
        self.feed_errors_queue: Queue = Queue()

    def _normalise(
        self,
        bodies: Iterable[dict[str, Any]],
        *,
        operation_type: str = "feed",
    ) -> list[dict[str, Any]]:
        """
        Convert documents to Vespa ``{id, fields}`` format.

        Accepts both the Vespa-native ``{"id": ..., "fields": {...}}`` form
        and a flat convenience form ``{"id": ..., "key": "val", ...}`` where
        all non-``id`` keys become fields.  Missing ``id`` on feed operations
        is auto-generated; on update/delete it is an error.
        """
        norm: list[dict[str, Any]] = []
        for i, body in enumerate(bodies):
            if "fields" in body:
                if operation_type in ("update", "delete") and "id" not in body:
                    raise ValueError(
                        f"Document at index {i} is missing required 'id' for {operation_type} operation"
                    )
                norm.append(body)
                continue

            doc = dict(body)
            doc_id = doc.pop("id", None)

            if doc_id is None and operation_type in ("update", "delete"):
                raise ValueError(
                    f"Document at index {i} is missing required 'id' for {operation_type} operation"
                )
            if doc_id is None:
                doc_id = str(uuid.uuid4())

            norm.append({"id": doc_id, "fields": doc})
        return norm

    def default_callback(self, response: VespaResponse, doc_id: str) -> None:
        """Collect non-success responses from Vespa feed calls."""
        if not response.is_successful():
            try:
                reason = response.get_json()
            except Exception as err:
                reason = {
                    "json_parse_error": str(err),
                    "status_code": response.status_code,
                    "response_type": type(response).__name__,
                }

            self.feed_errors_queue.put({"id": doc_id, "status": response.status_code, "reason": reason})

    def feed_iterable(
        self,
        bodies: Iterable[dict[str, Any]],
        callback: Callable[..., Any] | None = None,
        operation_type: str = "feed",
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Feed, update, or delete documents via pyvespa."""
        if operation_type not in VALID_OPERATION_TYPES:
            raise ValueError(
                f"Invalid operation_type {operation_type!r}. Must be one of {sorted(VALID_OPERATION_TYPES)}"
            )

        if callback is None:
            callback = self.default_callback

        docs = self._normalise(bodies, operation_type=operation_type)

        feed_kwargs: dict[str, Any] = {
            "iter": docs,
            "operation_type": operation_type,
            "schema": self.schema,
            "namespace": self.namespace,
            "callback": callback,
        }

        for key in ("max_queue_size", "max_workers", "max_connections"):
            val = getattr(self, key, None)
            if val is not None:
                feed_kwargs[key] = val

        feed_kwargs.update(kwargs)

        self.feed_errors_queue = Queue()

        self.log.info("Starting %s operation for %s documents", operation_type, len(docs))
        self.vespa_app.feed_async_iterable(**feed_kwargs)

        feed_errors: list[dict[str, Any]] = []
        while not self.feed_errors_queue.empty():
            try:
                feed_errors.append(self.feed_errors_queue.get_nowait())
            except Empty:
                break

        return {
            "sent": len(docs),
            "errors": len(feed_errors),
            "error_details": feed_errors,
        }

    def test_connection(self) -> tuple[bool, str]:
        """Test connectivity to the Vespa endpoint using pyvespa's own session."""
        try:
            resp = self.vespa_app.get_application_status()
            if resp is not None and resp.status_code == 200:
                return True, "Connection successful"
            status = getattr(resp, "status_code", "unknown") if resp else "no response"
            return False, f"Vespa returned HTTP {status}"
        except Exception as err:
            return False, str(err)
