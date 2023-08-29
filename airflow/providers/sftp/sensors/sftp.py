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
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Sequence

from paramiko.sftp import SFTP_NO_SUCH_FILE

from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.sensors.base import BaseSensorOperator, PokeReturnValue
from airflow.utils.timezone import convert_to_utc

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SFTPSensor(BaseSensorOperator):
    """
    Waits for a file or directory to be present on SFTP.

    :param path: Remote file or directory path
    :param file_pattern: The pattern that will be used to match the file (fnmatch format)
    :param sftp_conn_id: The connection to run the sensor against
    :param newer_than: DateTime for which the file or file path should be newer than, comparison is inclusive
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
        newer_than: datetime | None = None,
        sftp_conn_id: str = "sftp_default",
        python_callable: Callable | None = None,
        op_args: list | None = None,
        op_kwargs: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.path = path
        self.file_pattern = file_pattern
        self.hook: SFTPHook | None = None
        self.sftp_conn_id = sftp_conn_id
        self.newer_than: datetime | None = newer_than
        self.python_callable: Callable | None = python_callable
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}

    def poke(self, context: Context) -> PokeReturnValue | bool:
        self.hook = SFTPHook(self.sftp_conn_id)
        self.log.info("Poking for %s, with pattern %s", self.path, self.file_pattern)
        files_found = []

        if self.file_pattern:
            files_from_pattern = self.hook.get_files_by_pattern(self.path, self.file_pattern)
            if files_from_pattern:
                actual_files_to_check = [
                    os.path.join(self.path, file_from_pattern) for file_from_pattern in files_from_pattern
                ]
            else:
                return False
        else:
            actual_files_to_check = [self.path]
        for actual_file_to_check in actual_files_to_check:
            try:
                mod_time = self.hook.get_mod_time(actual_file_to_check)
                self.log.info("Found File %s last modified: %s", actual_file_to_check, mod_time)
            except OSError as e:
                if e.errno != SFTP_NO_SUCH_FILE:
                    raise e
                continue
            if self.newer_than:
                _mod_time = convert_to_utc(datetime.strptime(mod_time, "%Y%m%d%H%M%S"))
                _newer_than = convert_to_utc(self.newer_than)
                if _newer_than <= _mod_time:
                    files_found.append(actual_file_to_check)
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
                xcom_value={"files_found": files_found, "decorator_return_value": callable_return},
            )
        return True
