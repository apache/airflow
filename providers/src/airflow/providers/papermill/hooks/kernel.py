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

from jupyter_client import AsyncKernelManager
from papermill.clientwrap import PapermillNotebookClient
from papermill.engines import NBClientEngine
from papermill.utils import merge_kwargs, remove_args
from traitlets import Unicode

from airflow.hooks.base import BaseHook

JUPYTER_KERNEL_SHELL_PORT = 60316
JUPYTER_KERNEL_IOPUB_PORT = 60317
JUPYTER_KERNEL_STDIN_PORT = 60318
JUPYTER_KERNEL_CONTROL_PORT = 60319
JUPYTER_KERNEL_HB_PORT = 60320
REMOTE_KERNEL_ENGINE = "remote_kernel_engine"


class KernelConnection:
    """Class to represent kernel connection object."""

    ip: str
    shell_port: int
    iopub_port: int
    stdin_port: int
    control_port: int
    hb_port: int
    session_key: str


class KernelHook(BaseHook):
    """
    The KernelHook can be used to interact with remote jupyter kernel.

    Takes kernel host/ip from connection and refers to jupyter kernel ports and session_key
     from ``extra`` field.

    :param kernel_conn_id: connection that has kernel host/ip
    """

    conn_name_attr = "kernel_conn_id"
    default_conn_name = "jupyter_kernel_default"
    conn_type = "jupyter_kernel"
    hook_name = "Jupyter Kernel"

    def __init__(self, kernel_conn_id: str = default_conn_name, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.kernel_conn = self.get_connection(kernel_conn_id)
        register_remote_kernel_engine()

    def get_conn(self) -> KernelConnection:
        kernel_connection = KernelConnection()
        kernel_connection.ip = self.kernel_conn.host
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
        kernel_connection.hb_port = self.kernel_conn.extra_dejson.get(
            "hb_port", JUPYTER_KERNEL_HB_PORT
        )
        kernel_connection.session_key = self.kernel_conn.extra_dejson.get(
            "session_key", ""
        )
        return kernel_connection


def register_remote_kernel_engine():
    """Register ``RemoteKernelEngine`` papermill engine."""
    from papermill.engines import papermill_engines

    papermill_engines.register(REMOTE_KERNEL_ENGINE, RemoteKernelEngine)


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
        config: dict[str, int | str | bytes] = dict(
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
