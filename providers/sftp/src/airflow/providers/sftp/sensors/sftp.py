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
"""This module contains SFTP sensor."""

from __future__ import annotations

import os
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from paramiko.sftp import SFTP_NO_SUCH_FILE

from airflow.providers.common.compat.sdk import AirflowException, BaseSensorOperator, PokeReturnValue, conf
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.sftp.triggers.sftp import SFTPTrigger
from airflow.utils.timezone import convert_to_utc, parse

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class SFTPSensor(BaseSensorOperator):
    """
    Waits for a file or directory to be present on SFTP.

    :param path: Remote file or directory path
    :param file_pattern: The pattern that will be used to match the file (fnmatch format)
    :param sftp_conn_id: The connection to run the sensor against
    :param newer_than: DateTime for which the file or file path should be newer than, comparison is inclusive
    :param deferrable: If waiting for completion, whether to defer the task until done, default is ``False``.
    """

    template_fields: Sequence[str] = (
        "path",
        "file_pattern",
        "newer_than",
    )

    def __init__(
        self,
        *,
        path: str,
        file_pattern: str = "",
        newer_than: datetime | str | None = None,
        sftp_conn_id: str = "sftp_default",
        python_callable: Callable | None = None,
        op_args: list | None = None,
        op_kwargs: dict[str, Any] | None = None,
        use_managed_conn: bool = True,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.path = path
        self.file_pattern = file_pattern
        self.hook: SFTPHook | None = None
        self.sftp_conn_id = sftp_conn_id
        self.newer_than: datetime | str | None = newer_than
        self.use_managed_conn = use_managed_conn
        self.python_callable: Callable | None = python_callable
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        self.deferrable = deferrable

    def _get_files(self) -> list[str]:
        files_from_pattern: list[str] = []
        files_found: list[str] = []

        if self.file_pattern:
            files_from_pattern = self.hook.get_files_by_pattern(self.path, self.file_pattern)  # type: ignore[union-attr]
            if files_from_pattern:
                actual_files_present = [
                    os.path.join(self.path, file_from_pattern) for file_from_pattern in files_from_pattern
                ]
            else:
                return files_found
        else:
            try:
                # If a file is present, it is the single element added to the actual_files_present list to be
                # processed. If the file is a directory, actual_file_present will be assigned an empty list,
                # since SFTPHook.isfile(...) returns False
                actual_files_present = [self.path] if self.hook.isfile(self.path) else []  # type: ignore[union-attr]
            except Exception as e:
                raise AirflowException from e

        if self.newer_than:
            for actual_file_present in actual_files_present:
                try:
                    mod_time = self.hook.get_mod_time(actual_file_present)  # type: ignore[union-attr]
                    self.log.info("Found File %s last modified: %s", actual_file_present, mod_time)
                except OSError as e:
                    if e.errno != SFTP_NO_SUCH_FILE:
                        raise AirflowException from e
                    continue

                if isinstance(self.newer_than, str):
                    self.newer_than = parse(self.newer_than)
                _mod_time = convert_to_utc(datetime.strptime(mod_time, "%Y%m%d%H%M%S"))
                _newer_than = convert_to_utc(self.newer_than)
                if _newer_than <= _mod_time:
                    files_found.append(actual_file_present)
                    self.log.info(
                        "File %s has modification time: '%s', which is newer than: '%s'",
                        actual_file_present,
                        str(_mod_time),
                        str(_newer_than),
                    )
                else:
                    self.log.info(
                        "File %s has modification time: '%s', which is older than: '%s'",
                        actual_file_present,
                        str(_mod_time),
                        str(_newer_than),
                    )
        else:
            files_found = actual_files_present

        return files_found

    def poke(self, context: Context) -> PokeReturnValue | bool:
        self.hook = SFTPHook(self.sftp_conn_id, use_managed_conn=self.use_managed_conn)

        self.log.info("Poking for %s, with pattern %s", self.path, self.file_pattern)

        if self.use_managed_conn:
            files_found = self._get_files()
        else:
            with self.hook.get_managed_conn():
                files_found = self._get_files()

        if not len(files_found):
            return False

        if self.python_callable is not None:
            if self.op_kwargs:
                self.op_kwargs["files_found"] = files_found
            callable_return = self.python_callable(*self.op_args, **self.op_kwargs)
            return PokeReturnValue(
                is_done=True,
                xcom_value={"files_found": files_found, "decorator_return_value": callable_return},
            )
        return True

    def execute(self, context: Context) -> Any:
        # Unlike other async sensors, we do not follow the pattern of calling the synchronous self.poke()
        # method before deferring here. This is due to the current limitations we have in the synchronous
        # SFTPHook methods. They are as follows:
        #
        # For file_pattern sensing, the hook implements list_directory() method which returns a list of
        # filenames only without the attributes like modified time which is required for the file_pattern
        # sensing when newer_than is supplied. This leads to intermittent failures potentially due to
        # throttling by the SFTP server as the hook makes multiple calls to the server to get the
        # attributes for each of the files in the directory.This limitation is resolved here by instead
        # calling the read_directory() method which returns a list of files along with their attributes
        # in a single call. We can add back the call to self.poke() before deferring once the above
        # limitations are resolved in the sync sensor.
        if self.deferrable:
            self.defer(
                timeout=timedelta(seconds=self.timeout),
                trigger=SFTPTrigger(
                    path=self.path,
                    file_pattern=self.file_pattern,
                    sftp_conn_id=self.sftp_conn_id,
                    poke_interval=self.poke_interval,
                    newer_than=self.newer_than,
                ),
                method_name="execute_complete",
            )
        else:
            return super().execute(context=context)

    def execute_complete(self, context: dict[str, Any], event: Any = None) -> None:
        """
        Execute callback when the trigger fires; returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event is not None:
            if "status" in event and event["status"] == "error":
                raise AirflowException(event["message"])

            if "status" in event and event["status"] == "success":
                self.log.info("%s completed successfully.", self.task_id)
                self.log.info(event["message"])
                return None

        raise AirflowException("No event received in trigger callback")
