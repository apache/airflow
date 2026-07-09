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
"""This module contains SFTP operator."""

from __future__ import annotations

import socket
from collections.abc import Sequence
from typing import Any

import paramiko

from airflow.providers.common.compat.sdk import AirflowException, BaseOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook


class SFTPOperator(BaseOperator):
    """
    SFTPOperator for transferring files from remote host to local or vice a versa.

    This operator uses sftp_hook to open an SFTP transport channel that serves as
    the basis for file transfer. All transfer logic is delegated to
    ``SFTPHook.transfer()`` so that both the synchronous and deferrable code paths
    share a single, authoritative implementation.

    :param ssh_conn_id: :ref:`ssh connection id<howto/connection:ssh>`
        from airflow Connections.
    :param sftp_hook: predefined SFTPHook to use.
        Either `sftp_hook` or `ssh_conn_id` needs to be provided.
    :param remote_host: remote host to connect (templated).
        Nullable. If provided, it will replace the `remote_host` which was
        defined in `sftp_hook` or predefined in the connection of `ssh_conn_id`.
    :param local_filepath: local file path or list of local file paths to get or put. (templated)
    :param remote_filepath: remote file path or list of remote file paths to get, put, or delete. (templated)
    :param operation: specify operation ``'get'``, ``'put'``, or ``'delete'``. Defaults to ``'put'``.
    :param confirm: specify if the SFTP operation should be confirmed. Defaults to True.
    :param create_intermediate_dirs: create missing intermediate directories when
        copying from remote to local and vice-versa. Default is False.

        Example: The following task would copy ``file.txt`` to the remote host
        at ``/tmp/tmp1/tmp2/`` while creating ``tmp``,``tmp1`` and ``tmp2`` if they
        don't exist. If the parameter is not passed it would error as the directory
        does not exist. ::

            put_file = SFTPOperator(
                task_id="test_sftp",
                ssh_conn_id="ssh_default",
                local_filepath="/tmp/file.txt",
                remote_filepath="/tmp/tmp1/tmp2/file.txt",
                operation="put",
                create_intermediate_dirs=True,
                dag=dag,
            )

    :param concurrency: number of threads when transferring directories. Each thread opens
        a new SFTP connection. Only applies to directory transfers. (Default: 1)
    :param prefetch: controls whether prefetch is performed on GET transfers. (Default: True)
    :param deferrable: run the operator in deferrable mode. When True, the worker slot is
        freed during the transfer and reclaimed only when the transfer completes.
        Best suited for single large file transfers. For bulk directory transfers involving
        many files, consider using ``async PythonOperator`` with ``SFTPClientPool`` instead,
        which provides true async multiplexing via a single event loop. (Default: False)
    """

    template_fields: Sequence[str] = ("local_filepath", "remote_filepath", "remote_host")

    def __init__(
        self,
        *,
        sftp_hook: SFTPHook | None = None,
        ssh_conn_id: str | None = None,
        remote_host: str | None = None,
        local_filepath: str | list[str] | None = None,
        remote_filepath: str | list[str],
        operation: str = SFTPOperation.PUT,
        confirm: bool = True,
        create_intermediate_dirs: bool = False,
        concurrency: int = 1,
        prefetch: bool = True,
        deferrable: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sftp_hook = sftp_hook
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.operation = operation
        self.confirm = confirm
        self.create_intermediate_dirs = create_intermediate_dirs
        self.local_filepath = local_filepath
        self.remote_filepath = remote_filepath
        self.concurrency = concurrency
        self.prefetch = prefetch
        self.deferrable = deferrable

    def execute(self, context: Any) -> str | list[str] | None:
        local_filepath_array: list[str] = []
        if self.local_filepath is None:
            local_filepath_array = []
        elif isinstance(self.local_filepath, str):
            local_filepath_array = [self.local_filepath]
        else:
            local_filepath_array = list(self.local_filepath)

        remote_filepath_array: list[str] = (
            [self.remote_filepath] if isinstance(self.remote_filepath, str) else list(self.remote_filepath)
        )

        # ------------------------------------------------------------------ #
        # Input validation                                                     #
        # ------------------------------------------------------------------ #
        if self.operation in (SFTPOperation.GET, SFTPOperation.PUT) and len(local_filepath_array) != len(
            remote_filepath_array
        ):
            raise ValueError(
                f"{len(local_filepath_array)} paths in local_filepath "
                f"!= {len(remote_filepath_array)} paths in remote_filepath"
            )

        if self.operation == SFTPOperation.DELETE and local_filepath_array:
            raise ValueError("local_filepath should not be provided for delete operation")

        if self.operation not in (SFTPOperation.GET, SFTPOperation.PUT, SFTPOperation.DELETE):
            raise TypeError(
                f"Unsupported operation value {self.operation}, "
                f"expected {SFTPOperation.GET!r}, {SFTPOperation.PUT!r}, "
                f"or {SFTPOperation.DELETE!r}."
            )

        if self.concurrency < 1:
            raise ValueError(f"concurrency should be >= 1, got {self.concurrency}")

        # ------------------------------------------------------------------ #
        # Synchronous path — delegate all transfer logic to the hook          #
        # ------------------------------------------------------------------ #
        if self.remote_host is not None:
            self.log.info(
                "remote_host is provided explicitly. "
                "It will replace the remote_host which was defined "
                "in sftp_hook or predefined in connection of ssh_conn_id."
            )

        if self.ssh_conn_id:
            if self.sftp_hook and isinstance(self.sftp_hook, SFTPHook):
                self.log.info("ssh_conn_id is ignored when sftp_hook is provided.")
            else:
                self.log.info("sftp_hook not provided or invalid. Trying ssh_conn_id to create SFTPHook.")
                self.sftp_hook = SFTPHook(
                    ssh_conn_id=self.ssh_conn_id,
                    remote_host=self.remote_host or "",
                )

        if not self.sftp_hook:
            raise AirflowException("Cannot operate without sftp_hook or ssh_conn_id.")

        try:
            for idx, remote_fp in enumerate(remote_filepath_array):
                local_fp = local_filepath_array[idx] if local_filepath_array else ""
                self.sftp_hook.transfer(
                    local_filepath=local_fp,
                    remote_filepath=remote_fp,
                    operation=self.operation,
                    confirm=self.confirm,
                    create_intermediate_dirs=self.create_intermediate_dirs,
                    concurrency=self.concurrency,
                    prefetch=self.prefetch,
                )
        except Exception as e:
            raise AirflowException(
                f"Error while processing {self.operation.upper()} operation, error: {e}"
            ) from e

        return self.local_filepath

    def execute_complete(self, context: Any, event: dict[str, Any]) -> str | list[str] | None:
        """
        Handle completion from ``SFTPOperatorTrigger``.

        :param context: Airflow task context
        :param event: trigger result dict with ``status`` and ``message`` keys
        :raises AirflowException: if the trigger reported an error
        """
        if event.get("status") == "error":
            raise AirflowException(event.get("message", "Unknown error during deferrable SFTP transfer"))
        self.log.info("Deferrable SFTP transfer completed: %s", event.get("message"))
        return self.local_filepath

    def get_openlineage_facets_on_start(self):
        """
        Return OpenLineage datasets.

        Dataset will have the following structure:
            input: file://<local_host>/path
            output: file://<remote_host>:<remote_port>/path.
        """
        from airflow.providers.common.compat.openlineage.facet import Dataset
        from airflow.providers.openlineage.extractors import OperatorLineage

        scheme = "file"
        local_host = socket.gethostname()
        try:
            local_host = socket.gethostbyname(local_host)
        except Exception as e:
            self.log.warning(
                "Failed to resolve local hostname. "
                "Using the hostname got by socket.gethostbyname() without resolution. %s",
                e,
                exc_info=True,
            )

        hook = self.sftp_hook or SFTPHook(ssh_conn_id=self.ssh_conn_id)

        if self.remote_host is not None:
            remote_host = self.remote_host
        else:
            remote_host = hook.get_connection(hook.ssh_conn_id).host

        try:
            remote_host = socket.gethostbyname(remote_host)
        except OSError as e:
            self.log.warning(
                "Failed to resolve remote hostname. Using the provided hostname without resolution. %s",
                e,
                exc_info=True,
            )

        if hasattr(hook, "port"):
            remote_port = hook.port

        if isinstance(self.local_filepath, str):
            local_filepath = [self.local_filepath]
        else:
            local_filepath = self.local_filepath
        if isinstance(self.remote_filepath, str):
            remote_filepath = [self.remote_filepath]
        else:
            remote_filepath = self.remote_filepath

        local_datasets = [
            Dataset(namespace=self._get_namespace(scheme, local_host, None, path), name=path)
            for path in local_filepath
        ]
        remote_datasets = [
            Dataset(namespace=self._get_namespace(scheme, remote_host, remote_port, path), name=path)
            for path in remote_filepath
        ]

        if self.operation == SFTPOperation.GET:
            inputs = remote_datasets
            outputs = local_datasets
        else:
            inputs = local_datasets
            outputs = remote_datasets

        return OperatorLineage(
            inputs=inputs,
            outputs=outputs,
        )

    def _get_namespace(self, scheme, host, port, path) -> str:
        port = port or paramiko.config.SSH_PORT
        authority = f"{host}:{port}"
        return f"{scheme}://{authority}"
