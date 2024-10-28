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
from datetime import datetime
from typing import Any, AsyncIterator

from dateutil.parser import parse as parse_date

from airflow.exceptions import AirflowException
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
        exc = None
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
                            mod_time = datetime.fromtimestamp(
                                float(file.attrs.mtime)
                            ).strftime("%Y%m%d%H%M%S")
                            mod_time_utc = convert_to_utc(
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
                        mod_time_utc = convert_to_utc(
                            datetime.strptime(mod_time, "%Y%m%d%H%M%S")
                        )
                        if _newer_than <= mod_time_utc:
                            yield TriggerEvent(
                                {
                                    "status": "success",
                                    "message": f"Sensed file: {self.path}",
                                }
                            )
                            return
                    else:
                        yield TriggerEvent(
                            {"status": "success", "message": f"Sensed file: {self.path}"}
                        )
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
