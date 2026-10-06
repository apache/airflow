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

import typing
from dataclasses import dataclass
from typing import cast
from urllib.parse import urlsplit, urlunsplit

from jupyter_client import AsyncKernelManager, KernelConnectionInfo
from papermill.clientwrap import PapermillNotebookClient
from papermill.engines import NBClientEngine
from papermill.utils import merge_kwargs, remove_args
from traitlets import Unicode

from airflow.providers.common.compat.sdk import BaseHook

JUPYTER_KERNEL_SHELL_PORT = 60316
JUPYTER_KERNEL_IOPUB_PORT = 60317
JUPYTER_KERNEL_STDIN_PORT = 60318
JUPYTER_KERNEL_CONTROL_PORT = 60319
JUPYTER_KERNEL_HB_PORT = 60320
REMOTE_KERNEL_ENGINE = "remote_kernel_engine"
REMOTE_GATEWAY_KERNEL_ENGINE = "remote_gateway_kernel_engine"


def _coerce_bool(value: typing.Any, default: bool) -> bool:
    """Interpret connection-extra booleans that may arrive as strings (UI/URI-defined extras)."""
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip().lower() in ("true", "t", "yes", "y", "1")
    return bool(value)


class KernelConnection:
    """Class to represent kernel connection object."""

    ip: str
    shell_port: int
    iopub_port: int
    stdin_port: int
    control_port: int
    hb_port: int
    session_key: str


@dataclass
class GatewayKernelConnection:
    """Connection details for kernels behind a Jupyter server or kernel gateway reached over HTTP(S)."""

    url: str
    token: str | None = None
    auth_scheme: str = "token"
    auth_header_key: str = "Authorization"
    verify_ssl: bool = True
    ca_certs: str | None = None
    client_cert: str | None = None
    client_key: str | None = None
    request_timeout: float | None = None
    connect_timeout: float | None = None
    kernel_id: str | None = None
    headers: dict | None = None


class KernelHook(BaseHook):
    """
    The KernelHook can be used to interact with remote jupyter kernel.

    Takes kernel host/ip from connection and refers to jupyter kernel ports and session_key
     from ``extra`` field.

    Alternatively, when the connection host is an ``http://`` or ``https://`` URL (or the
    ``use_gateway`` extra is set), the connection describes a Jupyter server or kernel gateway
    (e.g. JupyterHub user server, Jupyter Kernel Gateway, Enterprise Gateway) reached over
    HTTP(S) with token authentication.

    :param kernel_conn_id: connection that has kernel host/ip
    """

    conn_name_attr = "kernel_conn_id"
    default_conn_name = "jupyter_kernel_default"
    conn_type = "jupyter_kernel"
    hook_name = "Jupyter Kernel"

    def __init__(self, kernel_conn_id: str = default_conn_name, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.kernel_conn = self.get_connection(kernel_conn_id)
        if self.use_gateway:
            register_remote_gateway_kernel_engine()
        else:
            register_remote_kernel_engine()

    @property
    def use_gateway(self) -> bool:
        """Whether the connection points at a Jupyter server/gateway over HTTP(S) rather than raw ZMQ."""
        host = self.kernel_conn.host or ""
        return host.startswith(("http://", "https://")) or _coerce_bool(
            self.kernel_conn.extra_dejson.get("use_gateway"), False
        )

    def get_conn(self) -> KernelConnection | GatewayKernelConnection:
        if self.use_gateway:
            return self._get_gateway_conn()
        kernel_connection = KernelConnection()
        kernel_connection.ip = cast("str", self.kernel_conn.host)
        kernel_connection.shell_port = self.kernel_conn.extra_dejson.get(
            "shell_port", JUPYTER_KERNEL_SHELL_PORT
        )
        kernel_connection.iopub_port = self.kernel_conn.extra_dejson.get(
            "iopub_port", JUPYTER_KERNEL_IOPUB_PORT
        )
        kernel_connection.stdin_port = self.kernel_conn.extra_dejson.get(
            "stdin_port", JUPYTER_KERNEL_STDIN_PORT
        )
        kernel_connection.control_port = self.kernel_conn.extra_dejson.get(
            "control_port", JUPYTER_KERNEL_CONTROL_PORT
        )
        kernel_connection.hb_port = self.kernel_conn.extra_dejson.get("hb_port", JUPYTER_KERNEL_HB_PORT)
        kernel_connection.session_key = self.kernel_conn.extra_dejson.get("session_key", "")
        return kernel_connection

    def _get_gateway_conn(self) -> GatewayKernelConnection:
        extra = self.kernel_conn.extra_dejson
        host = cast("str", self.kernel_conn.host)
        url = host if "://" in host else f"https://{host}"
        parts = urlsplit(url)
        if self.kernel_conn.port and not parts.port:
            parts = parts._replace(netloc=f"{parts.netloc}:{self.kernel_conn.port}")
        # urlunsplit also normalizes the scheme to lowercase.
        url = urlunsplit(parts)
        return GatewayKernelConnection(
            url=url,
            token=self.kernel_conn.password or extra.get("token"),
            auth_scheme=extra.get("auth_scheme", "token"),
            auth_header_key=extra.get("auth_header_key", "Authorization"),
            verify_ssl=_coerce_bool(extra.get("verify_ssl"), True),
            ca_certs=extra.get("ca_certs"),
            client_cert=extra.get("client_cert"),
            client_key=extra.get("client_key"),
            request_timeout=extra.get("request_timeout"),
            connect_timeout=extra.get("connect_timeout"),
            kernel_id=extra.get("kernel_id"),
            headers=extra.get("headers"),
        )


def register_remote_kernel_engine():
    """Register ``RemoteKernelEngine`` papermill engine."""
    from papermill.engines import papermill_engines

    papermill_engines.register(REMOTE_KERNEL_ENGINE, RemoteKernelEngine)


def register_remote_gateway_kernel_engine():
    """Register ``GatewayKernelEngine`` papermill engine."""
    from papermill.engines import papermill_engines

    from airflow.providers.papermill.hooks.gateway_kernel import GatewayKernelEngine

    papermill_engines.register(REMOTE_GATEWAY_KERNEL_ENGINE, GatewayKernelEngine)


class RemoteKernelManager(AsyncKernelManager):
    """Jupyter kernel manager that connects to a remote kernel."""

    session_key = Unicode("", config=True, help="Session key to connect to remote kernel")

    @property
    def has_kernel(self) -> bool:
        return True

    async def _async_is_alive(self) -> bool:
        return True

    def shutdown_kernel(self, now: bool = False, restart: bool = False):
        pass

    def client(self, **kwargs: typing.Any):
        """Create a client configured to connect to our kernel."""
        kernel_client = super().client(**kwargs)
        # load connection info to set session_key
        config = KernelConnectionInfo(
            ip=self.ip,
            shell_port=self.shell_port,
            iopub_port=self.iopub_port,
            stdin_port=self.stdin_port,
            control_port=self.control_port,
            hb_port=self.hb_port,
            key=self.session_key,
            transport="tcp",
            signature_scheme="hmac-sha256",
        )
        kernel_client.load_connection_info(config)
        return kernel_client


class RemoteKernelEngine(NBClientEngine):
    """Papermill engine to use ``RemoteKernelManager`` to connect to remote kernel and execute notebook."""

    @classmethod
    def execute_managed_notebook(
        cls,
        nb_man,
        kernel_name,
        log_output=False,
        stdout_file=None,
        stderr_file=None,
        start_timeout=60,
        execution_timeout=None,
        **kwargs,
    ):
        """Perform the actual execution of the parameterized notebook locally."""
        km = RemoteKernelManager()
        km.ip = kwargs["kernel_ip"]
        km.shell_port = kwargs["kernel_shell_port"]
        km.iopub_port = kwargs["kernel_iopub_port"]
        km.stdin_port = kwargs["kernel_stdin_port"]
        km.control_port = kwargs["kernel_control_port"]
        km.hb_port = kwargs["kernel_hb_port"]
        km.ip = kwargs["kernel_ip"]
        km.session_key = kwargs["kernel_session_key"]

        # Exclude parameters that named differently downstream
        safe_kwargs = remove_args(["timeout", "startup_timeout"], **kwargs)

        final_kwargs = merge_kwargs(
            safe_kwargs,
            timeout=execution_timeout if execution_timeout else kwargs.get("timeout"),
            startup_timeout=start_timeout,
            log_output=False,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
        )

        return PapermillNotebookClient(nb_man, km=km, **final_kwargs).execute()
