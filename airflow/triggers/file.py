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
import datetime
import os
import typing
from glob import glob
from typing import Any

from airflow.triggers.base import BaseTrigger, TriggerEvent


class FileTrigger(BaseTrigger):
    """
    A trigger that fires exactly once after it finds the requested file or folder.

    :param filepath: File or folder name (relative to the base path set within the connection), can
        be a glob.
    :param recursive: when set to ``True``, enables recursive directory matching behavior of
        ``**`` in glob filepath parameter. Defaults to ``False``.
    """

    def __init__(
        self,
        filepath: str,
        recursive: bool = False,
        poll_interval: float = 5.0,
    ):
        super().__init__()
        self.filepath = filepath
        self.recursive = recursive
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize FileTrigger arguments and classpath."""
        return (
            "airflow.triggers.file.FileTrigger",
            {
                "filepath": self.filepath,
                "recursive": self.recursive,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> typing.AsyncIterator[TriggerEvent]:
        """Loop until the relevant files are found."""
        while True:
            for path in glob(self.filepath, recursive=self.recursive):
                if os.path.isfile(path):
                    mod_time_f = os.path.getmtime(path)
                    mod_time = datetime.datetime.fromtimestamp(mod_time_f).strftime("%Y%m%d%H%M%S")
                    self.log.info("Found File %s last modified: %s", str(path), str(mod_time))
                    yield TriggerEvent(True)
                for _, _, files in os.walk(self.filepath):
                    if len(files) > 0:
                        yield TriggerEvent(True)
            await asyncio.sleep(self.poll_interval)
