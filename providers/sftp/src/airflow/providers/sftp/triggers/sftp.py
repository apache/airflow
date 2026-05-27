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

import asyncio
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any

from dateutil.parser import parse as parse_date

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.sftp.hooks.sftp import SFTPHookAsync
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.timezone import convert_to_utc


class SFTPTrigger(BaseTrigger):
    """
    SFTPTrigger that fires in below listed scenarios.

    1. The path on the SFTP server does not exist
    2. The pattern do not match

    :param path: The path on the SFTP server to search for a file matching the file pattern.
                Authentication method used in the SFTP connection must have access to this path
    :param file_pattern: Pattern to be used for matching against the list of files at the path above.
                Uses the fnmatch module from std library to perform the matching.

    :param sftp_conn_id: SFTP connection ID to be used for connecting to SFTP server
    :param poke_interval: How often, in seconds, to check for the existence of the file on the SFTP server
    """

    def __init__(
        self,
        path: str,
        file_pattern: str = "",
        sftp_conn_id: str = "sftp_default",
        newer_than: datetime | str | None = None,
        poke_interval: float = 5,
    ) -> None:
        super().__init__()
        self.path = path
        self.file_pattern = file_pattern
        self.sftp_conn_id = sftp_conn_id
        self.newer_than = newer_than
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize SFTPTrigger arguments and classpath."""
        return (
            "airflow.providers.sftp.triggers.sftp.SFTPTrigger",
            {
                "path": self.path,
                "file_pattern": self.file_pattern,
                "sftp_conn_id": self.sftp_conn_id,
                "newer_than": self.newer_than,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Make a series of asynchronous calls to sftp servers via async sftp hook. It yields a Trigger.

        - If file matching file pattern exists at the specified path return it,
        - If file pattern was not provided, it looks directly into the specific path which was provided.
        - If newer then datetime was provided it looks for the file path last modified time and
          check whether the last modified time is greater, if true return file if false it polls again.
        """
        hook = self._get_async_hook()

        if isinstance(self.newer_than, str):
            self.newer_than = parse_date(self.newer_than)
        _newer_than = convert_to_utc(self.newer_than) if self.newer_than else None
        while True:
            try:
                if self.file_pattern:
                    files_returned_by_hook = await hook.get_files_and_attrs_by_pattern(
                        path=self.path, fnmatch_pattern=self.file_pattern
                    )
                    files_sensed = []
                    for file in files_returned_by_hook:
                        if _newer_than:
                            if file.attrs.mtime is None:
                                continue
                            mod_time = datetime.fromtimestamp(float(file.attrs.mtime)).strftime(
                                "%Y%m%d%H%M%S"
                            )
                            mod_time_utc = convert_to_utc(datetime.strptime(mod_time, "%Y%m%d%H%M%S"))
                            if _newer_than <= mod_time_utc:
                                files_sensed.append(file.filename)
                        else:
                            files_sensed.append(file.filename)
                    if files_sensed:
                        yield TriggerEvent(
                            {
                                "status": "success",
                                "message": f"Sensed {len(files_sensed)} files: {files_sensed}",
                            }
                        )
                        return
                else:
                    mod_time = await hook.get_mod_time(self.path)
                    if _newer_than:
                        mod_time_utc = convert_to_utc(datetime.strptime(mod_time, "%Y%m%d%H%M%S"))
                        if _newer_than <= mod_time_utc:
                            yield TriggerEvent({"status": "success", "message": f"Sensed file: {self.path}"})
                            return
                    else:
                        yield TriggerEvent({"status": "success", "message": f"Sensed file: {self.path}"})
                        return
                await asyncio.sleep(self.poke_interval)
            except AirflowException:
                await asyncio.sleep(self.poke_interval)
            except FileNotFoundError:
                await asyncio.sleep(self.poke_interval)
            except Exception as e:
                exc = e
                # Break loop to avoid infinite retries on terminal failure
                break

        yield TriggerEvent({"status": "error", "message": str(exc)})

    def _get_async_hook(self) -> SFTPHookAsync:
        return SFTPHookAsync(sftp_conn_id=self.sftp_conn_id)


class SFTPOperatorTrigger(BaseTrigger):
    """
    Trigger for SFTPOperator deferrable mode.

    Fires when a file transfer (PUT, GET, or DELETE) completes
    on the SFTP server, freeing the worker slot during the transfer.

    :param ssh_conn_id: The SSH connection ID to use.
    :param local_filepath: Local file path(s) to transfer.
    :param remote_filepath: Remote file path(s) on the SFTP server.
    :param operation: The SFTP operation - put, get, or delete.
    :param confirm: Whether to confirm the file transfer.
    :param create_intermediate_dirs: Whether to create intermediate dirs.
    :param remote_host: Remote host to connect to (overrides connection).
    :param concurrency: Number of threads for directory transfers.
    :param prefetch: Whether to prefetch during file retrieval.
    """

    def __init__(
        self,
        ssh_conn_id: str | None = None,
        local_filepath: str | list[str] | None = None,
        remote_filepath: str | list[str] = "",
        operation: str = "put",
        confirm: bool = True,
        create_intermediate_dirs: bool = False,
        remote_host: str | None = None,
        concurrency: int = 1,
        prefetch: bool = True,
    ) -> None:
        super().__init__()
        self.ssh_conn_id = ssh_conn_id
        self.local_filepath = local_filepath
        self.remote_filepath = remote_filepath
        self.operation = operation
        self.confirm = confirm
        self.create_intermediate_dirs = create_intermediate_dirs
        self.remote_host = remote_host
        self.concurrency = concurrency
        self.prefetch = prefetch

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize the trigger for storage in the database."""
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "ssh_conn_id": self.ssh_conn_id,
                "local_filepath": self.local_filepath,
                "remote_filepath": self.remote_filepath,
                "operation": self.operation,
                "confirm": self.confirm,
                "create_intermediate_dirs": self.create_intermediate_dirs,
                "remote_host": self.remote_host,
                "concurrency": self.concurrency,
                "prefetch": self.prefetch,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Run the file transfer asynchronously and yield a TriggerEvent when done."""
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                self._do_transfer,
            )
            yield TriggerEvent(
                {
                    "status": "success",
                    "local_filepath": self.local_filepath,
                }
            )
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})

    def _do_transfer(self) -> None:
        """Run the actual synchronous SFTP transfer in a thread executor."""
        import os
        from pathlib import Path

        from airflow.providers.sftp.constants import SFTPOperation
        from airflow.providers.sftp.hooks.sftp import SFTPHook

        sftp_hook = SFTPHook(
            ssh_conn_id=self.ssh_conn_id,
            remote_host=self.remote_host or "",
        )

        if isinstance(self.local_filepath, str):
            local_filepath_array = [self.local_filepath] if self.local_filepath else []
        else:
            local_filepath_array = self.local_filepath or []

        if isinstance(self.remote_filepath, str):
            remote_filepath_array = [self.remote_filepath]
        else:
            remote_filepath_array = list(self.remote_filepath)

        if self.operation.lower() == SFTPOperation.GET:
            for local, remote in zip(local_filepath_array, remote_filepath_array):
                if self.create_intermediate_dirs:
                    Path(os.path.dirname(local)).mkdir(parents=True, exist_ok=True)
                if sftp_hook.isdir(remote):
                    if self.concurrency > 1:
                        sftp_hook.retrieve_directory_concurrently(
                            remote, local, workers=self.concurrency, prefetch=self.prefetch
                        )
                    else:
                        sftp_hook.retrieve_directory(remote, local)
                else:
                    sftp_hook.retrieve_file(remote, local, prefetch=self.prefetch)
        elif self.operation.lower() == SFTPOperation.PUT:
            for local, remote in zip(local_filepath_array, remote_filepath_array):
                if self.create_intermediate_dirs:
                    sftp_hook.create_directory(os.path.dirname(remote))
                if os.path.isdir(local):
                    if self.concurrency > 1:
                        sftp_hook.store_directory_concurrently(
                            remote, local, confirm=self.confirm, workers=self.concurrency
                        )
                    else:
                        sftp_hook.store_directory(remote, local, confirm=self.confirm)
                else:
                    sftp_hook.store_file(remote, local, confirm=self.confirm)
        elif self.operation.lower() == SFTPOperation.DELETE:
            for remote in remote_filepath_array:
                if sftp_hook.isdir(remote):
                    sftp_hook.delete_directory(remote, include_files=True)
                else:
                    sftp_hook.delete_file(remote)
