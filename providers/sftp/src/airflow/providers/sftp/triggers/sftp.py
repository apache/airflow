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
from functools import cached_property
from typing import Any

from dateutil.parser import parse as parse_date

from airflow.providers.common.compat.sdk import AirflowException, timezone
from airflow.providers.sftp.hooks.sftp import SFTPHookAsync, SFTPOperation
from airflow.triggers.base import BaseTrigger, TriggerEvent


class BaseSFTPTrigger(BaseTrigger):
    """Base class for SFTP triggers, providing shared async hook construction."""

    def __init__(self, sftp_conn_id: str = "sftp_default") -> None:
        super().__init__()
        self.sftp_conn_id = sftp_conn_id

    def _get_async_hook(self) -> SFTPHookAsync:
        return SFTPHookAsync(sftp_conn_id=self.sftp_conn_id)


class SFTPTrigger(BaseSFTPTrigger):
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
        super().__init__(sftp_conn_id=sftp_conn_id)
        self.path = path
        self.file_pattern = file_pattern
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

        while True:
            try:
                if self.file_pattern:
                    files_sensed = await hook.sense_files_by_pattern(
                        path=self.path, fnmatch_pattern=self.file_pattern, newer_than=self.newer_than_utc
                    )
                    if files_sensed:
                        yield TriggerEvent(
                            {
                                "status": "success",
                                "message": f"Sensed {len(files_sensed)} files: {files_sensed}",
                            }
                        )
                        return
                elif await hook.sense_path(path=self.path, newer_than=self.newer_than_utc):
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

    @cached_property
    def newer_than_utc(self) -> datetime | None:
        """Parse and convert ``newer_than`` to a UTC datetime once, without mutating the original value."""
        if not self.newer_than:
            return None
        newer_than = parse_date(self.newer_than) if isinstance(self.newer_than, str) else self.newer_than
        return timezone.convert_to_utc(newer_than)


class SFTPTransferTrigger(BaseSFTPTrigger):
    """
    Trigger for SFTPOperator deferrable mode.

    Fires when a file transfer (PUT, GET, or DELETE) completes
    on the SFTP server, freeing the worker slot during the transfer.

    :param sftp_conn_id: The SFTP connection ID to use.
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
        sftp_conn_id: str = "sftp_default",
        local_filepath: str | list[str] | None = None,
        remote_filepath: str | list[str] = "",
        operation: str = SFTPOperation.PUT,
        confirm: bool = True,
        create_intermediate_dirs: bool = False,
        remote_host: str | None = None,
        concurrency: int = 1,
        prefetch: bool = True,
    ) -> None:
        super().__init__(sftp_conn_id=sftp_conn_id)
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
                "sftp_conn_id": self.sftp_conn_id,
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
            hook = self._get_async_hook()
            await hook.transfer(
                operation=self.operation,
                local_filepath=self.local_filepath,
                remote_filepath=self.remote_filepath,
                confirm=self.confirm,
                create_intermediate_dirs=self.create_intermediate_dirs,
                concurrency=self.concurrency,
                prefetch=self.prefetch,
            )
            yield TriggerEvent(
                {
                    "status": "success",
                    "local_filepath": self.local_filepath,
                }
            )
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
