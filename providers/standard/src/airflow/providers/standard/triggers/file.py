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
from collections.abc import AsyncIterator
from glob import glob
from typing import Any

from airflow.providers.standard.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.triggers.base import BaseEventTrigger, BaseTrigger, TriggerEvent
else:
    from airflow.triggers.base import (  # type: ignore
        BaseTrigger,
        BaseTrigger as BaseEventTrigger,
        TriggerEvent,
    )


class FileTrigger(BaseTrigger):
    """
    A trigger that fires exactly once after it finds the requested file or folder.

    :param filepath: File or folder name (relative to the base path set within the connection), can
        be a glob.
    :param recursive: when set to ``True``, enables recursive directory matching behavior of
        ``**`` in glob filepath parameter. Defaults to ``False``.
    :param poke_interval: Time that the job should wait in between each try
    """

    def __init__(
        self,
        filepath: str,
        recursive: bool = False,
        poke_interval: float = 5.0,
        **kwargs,
    ):
        super().__init__()
        self.filepath = filepath
        self.recursive = recursive
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize FileTrigger arguments and classpath."""
        return (
            "airflow.providers.standard.triggers.file.FileTrigger",
            {
                "filepath": self.filepath,
                "recursive": self.recursive,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Loop until the relevant files are found."""
        while True:
            for path in glob(self.filepath, recursive=self.recursive):
                if os.path.isfile(path):
                    mod_time_f = os.path.getmtime(path)
                    mod_time = datetime.datetime.fromtimestamp(mod_time_f).strftime("%Y%m%d%H%M%S")
                    self.log.info("Found File %s last modified: %s", path, mod_time)
                    yield TriggerEvent(True)
                    return
                for _, _, files in os.walk(path):
                    if files:
                        yield TriggerEvent(True)
                        return
            await asyncio.sleep(self.poke_interval)


class FileDeleteTrigger(BaseEventTrigger):
    """
    A trigger that fires exactly once after it finds the requested file and then delete the file.

    The difference between ``FileTrigger`` and ``FileDeleteTrigger`` is ``FileDeleteTrigger`` can only find a
    specific file.

    :param filepath: File (relative to the base path set within the connection).
    :param poke_interval: Time that the job should wait in between each try
    """

    def __init__(
        self,
        filepath: str,
        poke_interval: float = 5.0,
        **kwargs,
    ):
        super().__init__()
        self.filepath = filepath
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize FileDeleteTrigger arguments and classpath."""
        return (
            "airflow.providers.standard.triggers.file.FileDeleteTrigger",
            {
                "filepath": self.filepath,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Loop until the relevant file is found."""
        while True:
            if os.path.isfile(self.filepath):
                mod_time_f = os.path.getmtime(self.filepath)
                mod_time = datetime.datetime.fromtimestamp(mod_time_f).strftime("%Y%m%d%H%M%S")
                self.log.info("Found file %s last modified: %s", self.filepath, mod_time)
                os.remove(self.filepath)
                self.log.info("File %s has been deleted", self.filepath)
                yield TriggerEvent(True)
                return
            await asyncio.sleep(self.poke_interval)
