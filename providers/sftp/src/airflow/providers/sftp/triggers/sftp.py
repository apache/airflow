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
import os
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any

from dateutil.parser import parse as parse_date

from airflow.providers.common.compat.sdk import AirflowException, timezone
from airflow.providers.sftp.hooks.sftp import SFTPHookAsync
from airflow.triggers.base import BaseTrigger, TriggerEvent


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
        _newer_than = timezone.convert_to_utc(self.newer_than) if self.newer_than else None
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
                            mod_time_utc = timezone.convert_to_utc(
                                datetime.strptime(mod_time, "%Y%m%d%H%M%S")
                            )
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
                        mod_time_utc = timezone.convert_to_utc(datetime.strptime(mod_time, "%Y%m%d%H%M%S"))
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


class SFTPTransferTrigger(BaseTrigger):
    """
    Trigger that performs a single-file (or file-pair list) SFTP get/put transfer.

    Used by :class:`~airflow.providers.sftp.operators.sftp.SFTPOperator` in deferrable
    mode so the worker slot is released while the transfer runs on the Triggerer.
    Directory transfers, delete, and concurrent directory copies are not supported.
    ``confirm`` and ``prefetch`` are not applied because
    :class:`~airflow.providers.sftp.hooks.sftp.SFTPHookAsync` does not accept them.
    """

    def __init__(
        self,
        ssh_conn_id: str,
        remote_filepath: str | list[str],
        operation: str,
        local_filepath: str | list[str] | None = None,
        create_intermediate_dirs: bool = False,
        remote_host: str | None = None,
    ) -> None:
        super().__init__()
        self.ssh_conn_id = ssh_conn_id
        self.local_filepath = local_filepath
        self.remote_filepath = remote_filepath
        self.operation = operation
        self.create_intermediate_dirs = create_intermediate_dirs
        self.remote_host = remote_host

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize SFTPTransferTrigger arguments and classpath."""
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "ssh_conn_id": self.ssh_conn_id,
                "local_filepath": self.local_filepath,
                "remote_filepath": self.remote_filepath,
                "operation": self.operation,
                "create_intermediate_dirs": self.create_intermediate_dirs,
                "remote_host": self.remote_host,
            },
        )

    def _local_paths(self) -> list[str]:
        if self.local_filepath is None:
            return []
        if isinstance(self.local_filepath, str):
            return [self.local_filepath]
        return list(self.local_filepath)

    def _remote_paths(self) -> list[str]:
        if isinstance(self.remote_filepath, str):
            return [self.remote_filepath]
        return list(self.remote_filepath)

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Run the file transfer through :class:`SFTPHookAsync` and yield when done."""
        try:
            hook = SFTPHookAsync(sftp_conn_id=self.ssh_conn_id, host=self.remote_host)
            operation = self.operation.lower()
            if operation not in ("get", "put"):
                raise ValueError(f"Unsupported operation value {self.operation}")
            local_paths = self._local_paths()
            remote_paths = self._remote_paths()
            if not local_paths:
                raise ValueError("local_filepath is required for get/put")
            for local_path, remote_path in zip(local_paths, remote_paths, strict=True):
                if operation == "get":
                    if self.create_intermediate_dirs:
                        parent_dir = os.path.dirname(local_path)
                        if parent_dir:
                            os.makedirs(parent_dir, exist_ok=True)
                    await hook.retrieve_file(remote_path, local_path)
                else:
                    await hook.store_file(remote_path, local_path)
            yield TriggerEvent({"status": "success", "local_filepath": self.local_filepath})
        except Exception as exc:
            yield TriggerEvent({"status": "error", "message": str(exc)})
