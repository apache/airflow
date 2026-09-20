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
"""Hook that resolves a ``modal`` connection to an authenticated Modal client."""

from __future__ import annotations

import os
from functools import cached_property
from typing import TYPE_CHECKING, Any

import modal

from airflow.providers.common.compat.sdk import AirflowNotFoundException, BaseHook
from airflow.providers.modal.exceptions import ModalConnectionError

if TYPE_CHECKING:
    from airflow.sdk import Connection


class ModalHook(BaseHook):
    """
    Hook for `Modal <https://modal.com/>`__.

    Turns an Airflow connection of type ``modal`` into a :class:`modal.Client`, so every piece
    of Airflow code that talks to Modal (operators, the Common AI sandbox toolset backend, a
    future executor) reads credentials the same way instead of each one reaching for the
    worker's environment.

    Connection fields:

    - **Login**: Modal token id (``ak-...``).
    - **Password**: Modal token secret (``as-...``).
    - **Extra** ``environment`` (optional): Modal environment name to scope app lookups and
      sandboxes to. When unset, the Modal SDK resolves it from ``MODAL_ENVIRONMENT`` or the
      active profile in ``~/.modal.toml``.

    Credential precedence:

    1. Token id and secret on the connection: the client is built from exactly those.
    2. A connection with **neither** token field set, or no connection at all for the default
       connection id: the Modal SDK's own resolution applies (``MODAL_TOKEN_ID`` /
       ``MODAL_TOKEN_SECRET``, then ``~/.modal.toml``). This keeps a worker that already has
       ``modal token new`` run on it working without any Airflow configuration.
    3. Exactly one of the two token fields set: an error. A half-filled connection is a
       misconfiguration, and silently falling back to ambient credentials would hide it.

    Workspace is not a connection field because Modal derives it from the token itself.

    :param modal_conn_id: :ref:`Modal connection id <howto/connection:modal>`. Pass ``None``
        to skip connection lookup entirely and use ambient credentials.
    """

    conn_name_attr = "modal_conn_id"
    default_conn_name = "modal_default"
    conn_type = "modal"
    hook_name = "Modal"

    def __init__(self, modal_conn_id: str | None = default_conn_name, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.modal_conn_id = modal_conn_id
        self._client: modal.Client | None = None
        self._client_pid: int | None = None

    @cached_property
    def _connection(self) -> Connection | None:
        """
        The Airflow connection, or ``None`` when ambient credentials should be used.

        A missing *default* connection is not an error: most Modal installs already carry
        credentials on the host, and requiring an ``modal_default`` row just to relay them
        would be friction for no safety. A missing connection under any *other* id is an
        error, because the author named it on purpose.
        """
        if self.modal_conn_id is None:
            return None
        try:
            return self.get_connection(self.modal_conn_id)
        except AirflowNotFoundException:
            if self.modal_conn_id != self.default_conn_name:
                raise
            self.log.info(
                "Connection %r not found; using ambient Modal credentials (MODAL_TOKEN_ID / "
                "MODAL_TOKEN_SECRET or ~/.modal.toml).",
                self.modal_conn_id,
            )
            return None

    @cached_property
    def credentials(self) -> tuple[str, str] | None:
        """Token id and secret from the connection, or ``None`` to defer to the Modal SDK."""
        conn = self._connection
        if conn is None:
            return None
        token_id, token_secret = conn.login or "", conn.password or ""
        if bool(token_id) != bool(token_secret):
            missing = "Token Secret (password)" if token_id else "Token ID (login)"
            raise ModalConnectionError(
                f"Connection {self.modal_conn_id!r} sets one Modal token field but not the other: "
                f"{missing} is empty. Set both, or clear both to use ambient credentials."
            )
        if not token_id:
            return None
        return token_id, token_secret

    @cached_property
    def environment_name(self) -> str | None:
        """Modal environment from the connection extra, or ``None`` to defer to the Modal SDK."""
        conn = self._connection
        if conn is None:
            return None
        environment = conn.extra_dejson.get("environment")
        return environment or None

    def get_conn(self) -> modal.Client:
        """Build the Modal client for this connection."""
        credentials = self.credentials
        if credentials is None:
            return modal.Client.from_env()
        return modal.Client.from_credentials(*credentials)

    @property
    def client(self) -> modal.Client:
        """
        The Modal client, built once per hook instance and per process.

        A client owns a gRPC transport, and the Modal SDK documents its recovery of a client
        inherited across ``os.fork()`` as best-effort, recommending a fresh client instead. So
        the cache is keyed on the PID: a hook whose client was populated before a fork (for
        example one held by an executor or a long-lived plugin) rebuilds it on first use in the
        child instead of reusing the parent's transport.
        """
        pid = os.getpid()
        if self._client is None or self._client_pid != pid:
            self._client = self.get_conn()
            self._client_pid = pid
        return self._client

    @property
    def client_kwargs(self) -> dict[str, Any]:
        """
        Keyword arguments that route a Modal SDK call through this connection.

        Every Modal entry point that reaches the API (``modal.App.lookup``,
        ``modal.Sandbox.create``, ``modal.Sandbox.list``, ``modal.Sandbox.from_id``,
        ``modal.Secret.from_name``) accepts ``client=``; splat this into the call::

            modal.Sandbox.create(app=app, image=image, **hook.client_kwargs)

        The environment is deliberately not included: a sandbox takes its environment from
        the app it belongs to, and ``Sandbox.create(environment_name=...)`` is deprecated in
        the Modal SDK. Resolve the app with :meth:`lookup_app`, which applies the connection's
        environment.
        """
        return {"client": self.client}

    def lookup_app(self, name: str, *, create_if_missing: bool = False) -> modal.App:
        """
        Look up a Modal app by name through this connection.

        :param name: App name.
        :param create_if_missing: Create the app when it does not exist yet.
        """
        return modal.App.lookup(
            name,
            client=self.client,
            environment_name=self.environment_name,
            create_if_missing=create_if_missing,
        )

    def test_connection(self) -> tuple[bool, str]:
        """
        Authenticate against Modal and report the outcome for the connection form.

        Explicit credentials are checked with :meth:`modal.Client.verify`, which opens a
        throwaway client, sends one ``ClientHello`` and closes it again, so repeated tests from
        the API server do not accumulate transports. The ambient path checks the SDK's shared
        ``from_env`` client, which the hook does not own and therefore does not close.
        """
        try:
            credentials = self.credentials
            if credentials is None:
                modal.Client.from_env().hello()
            else:
                modal.Client.verify(modal.config.config.get("server_url"), credentials)
        except Exception as e:
            return False, str(e)
        return True, "Connection established!"
