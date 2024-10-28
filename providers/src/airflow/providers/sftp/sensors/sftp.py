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
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Callable, Sequence

from paramiko.sftp import SFTP_NO_SUCH_FILE

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.sftp.triggers.sftp import SFTPTrigger
from airflow.sensors.base import BaseSensorOperator, PokeReturnValue
from airflow.utils.timezone import convert_to_utc, parse

if TYPE_CHECKING:
    from airflow.utils.context import Context


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
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.path = path
        self.file_pattern = file_pattern
        self.hook: SFTPHook | None = None
        self.sftp_conn_id = sftp_conn_id
        self.newer_than: datetime | str | None = newer_than
        self.python_callable: Callable | None = python_callable
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        self.deferrable = deferrable

    def poke(self, context: Context) -> PokeReturnValue | bool:
        self.hook = SFTPHook(self.sftp_conn_id)
        self.log.info("Poking for %s, with pattern %s", self.path, self.file_pattern)
        files_found = []

        if self.file_pattern:
            files_from_pattern = self.hook.get_files_by_pattern(
                self.path, self.file_pattern
            )
            if files_from_pattern:
                actual_files_to_check = [
                    os.path.join(self.path, file_from_pattern)
                    for file_from_pattern in files_from_pattern
                ]
            else:
                return False
        else:
            actual_files_to_check = [self.path]

        for actual_file_to_check in actual_files_to_check:
            try:
                mod_time = self.hook.get_mod_time(actual_file_to_check)
                self.log.info(
                    "Found File %s last modified: %s", actual_file_to_check, mod_time
                )
            except OSError as e:
                if e.errno != SFTP_NO_SUCH_FILE:
                    raise AirflowException from e
                continue

            if self.newer_than:
                if isinstance(self.newer_than, str):
                    self.newer_than = parse(self.newer_than)
                _mod_time = convert_to_utc(datetime.strptime(mod_time, "%Y%m%d%H%M%S"))
                _newer_than = convert_to_utc(self.newer_than)
                if _newer_than <= _mod_time:
                    files_found.append(actual_file_to_check)
                    self.log.info(
                        "File %s has modification time: '%s', which is newer than: '%s'",
                        actual_file_to_check,
                        str(_mod_time),
                        str(_newer_than),
                    )
                else:
                    self.log.info(
                        "File %s has modification time: '%s', which is older than: '%s'",
                        actual_file_to_check,
                        str(_mod_time),
                        str(_newer_than),
                    )
            else:
                files_found.append(actual_file_to_check)

        self.hook.close_conn()
        if not len(files_found):
            return False

        if self.python_callable is not None:
            if self.op_kwargs:
                self.op_kwargs["files_found"] = files_found
            callable_return = self.python_callable(*self.op_args, **self.op_kwargs)
            return PokeReturnValue(
                is_done=True,
                xcom_value={
                    "files_found": files_found,
                    "decorator_return_value": callable_return,
                },
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
