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

    def _build_client(self) -> modal.Client:
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
            self._client = self._build_client()
            self._client_pid = pid
        return self._client

    def get_conn(self) -> modal.Client:
        """Return the Modal client for this connection (same cached object as :attr:`client`)."""
        return self.client

    @property
    def client_kwargs(self) -> dict[str, Any]:
        """
        Keyword arguments that route a Modal SDK call through this connection's credentials.

        For calls that also take ``environment_name`` (``Function.from_name``, ``Cls.from_name``,
        ``Secret.from_name``, ``Volume.from_name``, ``App.lookup``) prefer the matching hook
        method, which applies the connection's environment as well. Use this for calls that only
        take a client, such as ``modal.Sandbox.list(**hook.client_kwargs)``.
        """
        return {"client": self.client}

    def lookup_app(self, name: str, *, create_if_missing: bool = False) -> modal.App:
        """
        Look up a Modal app by name in the connection's environment.

        :param name: App name.
        :param create_if_missing: Create the app when it does not exist yet.
        """
        return modal.App.lookup(
            name,
            client=self.client,
            environment_name=self.environment_name,
            create_if_missing=create_if_missing,
        )

    def get_function(self, app_name: str, name: str, *, version: int | None = None) -> modal.Function:
        """
        Return a handle to a deployed Modal function in the connection's environment.

        Call ``.remote(...)``, ``.spawn(...)`` or ``.map(...)`` on the result as with any Modal
        function handle.

        :param app_name: Name of the deployed app.
        :param name: Function name within the app.
        :param version: Pin a specific deployment version; latest when omitted.
        """
        return modal.Function.from_name(
            app_name, name, version=version, environment_name=self.environment_name, client=self.client
        )

    def get_cls(self, app_name: str, name: str, *, version: int | None = None) -> modal.Cls:
        """
        Return a handle to a deployed Modal class in the connection's environment.

        :param app_name: Name of the deployed app.
        :param name: Class name within the app.
        :param version: Pin a specific deployment version; latest when omitted.
        """
        return modal.Cls.from_name(
            app_name, name, version=version, environment_name=self.environment_name, client=self.client
        )

    def get_secret(self, name: str, *, required_keys: list[str] | None = None) -> modal.Secret:
        """
        Return a named Modal secret from the connection's environment.

        :param name: Secret name.
        :param required_keys: Keys the secret must contain; the SDK raises when one is missing.
        """
        return modal.Secret.from_name(
            name,
            environment_name=self.environment_name,
            required_keys=required_keys or [],
            client=self.client,
        )

    def get_volume(self, name: str, *, create_if_missing: bool = False) -> modal.Volume:
        """
        Return a named Modal volume from the connection's environment.

        :param name: Volume name.
        :param create_if_missing: Create the volume when it does not exist yet.
        """
        return modal.Volume.from_name(
            name,
            environment_name=self.environment_name,
            create_if_missing=create_if_missing,
            client=self.client,
        )

    def create_sandbox(
        self, *entrypoint: str, app_name: str, create_app_if_missing: bool = False, **sandbox_kwargs: Any
    ) -> modal.Sandbox:
        """
        Create a Modal sandbox under a named app, through this connection.

        The app is resolved with :meth:`lookup_app`, so the sandbox lands in the connection's
        environment (Modal derives a sandbox's environment from its app). Remaining keyword
        arguments go to ``modal.Sandbox.create`` unchanged: ``image``, ``gpu``, ``cpu``,
        ``memory``, ``timeout``, ``secrets``, ``volumes`` and so on.

        :param entrypoint: Command to run, as separate arguments.
        :param app_name: App the sandbox belongs to.
        :param create_app_if_missing: Create the app when it does not exist yet.
        """
        app = self.lookup_app(app_name, create_if_missing=create_app_if_missing)
        return modal.Sandbox.create(*entrypoint, app=app, client=self.client, **sandbox_kwargs)

    def get_sandbox(self, sandbox_id: str) -> modal.Sandbox:
        """
        Reattach to an existing sandbox by id, through this connection.

        :param sandbox_id: The ``sb-...`` id returned by ``Sandbox.object_id``.
        """
        return modal.Sandbox.from_id(sandbox_id, client=self.client)

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
