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
import logging
import os
from collections.abc import AsyncIterator, Hashable
from glob import glob
from typing import Any

import anyio

from airflow.providers.standard.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.triggers.base import BaseEventTrigger, BaseTrigger, TriggerEvent
else:
    from airflow.triggers.base import (  # type: ignore
        BaseTrigger,
        BaseTrigger as BaseEventTrigger,
        TriggerEvent,
    )

log = logging.getLogger(__name__)


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
                if await anyio.Path(path).is_file():
                    mod_time_f = (await anyio.Path(path).stat()).st_mtime
                    mod_time = datetime.datetime.fromtimestamp(mod_time_f).strftime("%Y%m%d%H%M%S")
                    self.log.info("Found File %s last modified: %s", path, mod_time)
                    yield TriggerEvent(True)
                    return
                for _, _, files in await anyio.to_thread.run_sync(lambda: list(os.walk(path))):
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
            filepath = anyio.Path(self.filepath)
            if await filepath.is_file():
                mod_time_f = (await filepath.stat()).st_mtime
                mod_time = datetime.datetime.fromtimestamp(mod_time_f).strftime("%Y%m%d%H%M%S")
                self.log.info("Found file %s last modified: %s", self.filepath, mod_time)
                await filepath.unlink()
                self.log.info("File %s has been deleted", self.filepath)
                yield TriggerEvent(True)
                return
            await asyncio.sleep(self.poke_interval)


class DirectoryFileDeleteTrigger(BaseEventTrigger):
    """
    Fire once when ``filename`` appears in ``directory``, then delete it.

    Functionally equivalent to ``FileDeleteTrigger`` for a single file, but
    sibling triggers that point at the same ``directory`` and ``poke_interval``
    share a single underlying directory scan in the triggerer; each instance
    only fires for its own ``filename``. This is useful when many assets are
    driven by per-flag-file events landing in a shared inbox directory.

    :param directory: Directory to scan.
    :param filename: File name (without directory) whose appearance fires this
        trigger. The matched file is deleted before the event is yielded.
    :param poke_interval: Time to wait between scans.
    """

    def __init__(self, *, directory: str, filename: str, poke_interval: float = 5.0) -> None:
        super().__init__()
        self.directory = directory
        self.filename = filename
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize DirectoryFileDeleteTrigger arguments and classpath."""
        return (
            "airflow.providers.standard.triggers.file.DirectoryFileDeleteTrigger",
            {
                "directory": self.directory,
                "filename": self.filename,
                "poke_interval": self.poke_interval,
            },
        )

    def shared_stream_key(self) -> Hashable | None:
        """All triggers on the same directory + cadence share one scan."""
        # Use realpath so trivial path variants all resolve to the same canonical
        # path: trailing slashes (``/tmp/flags`` vs ``/tmp/flags/``), relative vs
        # absolute paths (``./flags`` vs ``/tmp/flags``), and symlinks vs their
        # targets all key to the same group instead of running N independent scans.
        return ("directory-scan", os.path.realpath(self.directory), self.poke_interval)

    @classmethod
    async def open_shared_stream(cls, kwargs: dict[str, Any]) -> AsyncIterator[Any]:
        """
        Drive one directory-listing loop and broadcast each snapshot.

        Missing directories yield an empty snapshot so subscribers keep
        polling for the file to appear. Configuration-class failures
        (``PermissionError``, ``NotADirectoryError``, ``IsADirectoryError``)
        propagate — these are almost always permanent (wrong mount, wrong
        mode, path points at a file), so silently retrying just hides the
        misconfiguration from the operator; surfacing them as a
        ``_PollFailure`` makes the trigger visibly fail in the UI, where it
        can be diagnosed and restarted after the operator corrects the
        config. Other ``OSError`` subclasses (transient I/O blips, NFS
        hiccups, etc.) are logged at warning and the snapshot is skipped for
        this cadence, since those may self-heal.
        """
        directory = anyio.Path(kwargs["directory"])
        poke_interval: float = kwargs["poke_interval"]
        while True:
            try:
                names = {p.name async for p in directory.iterdir()}
            except FileNotFoundError:
                names = set()
            except (PermissionError, NotADirectoryError, IsADirectoryError):
                raise
            except OSError:
                log.warning(
                    "Failed to list %s; retrying after %ss",
                    directory,
                    poke_interval,
                    exc_info=True,
                )
                await asyncio.sleep(poke_interval)
                continue
            yield {"directory": str(directory), "names": names}
            await asyncio.sleep(poke_interval)

    async def filter_shared_stream(self, shared_stream: AsyncIterator[Any]) -> AsyncIterator[TriggerEvent]:
        """Fire once for this instance's own filename and delete the file."""
        async for snapshot in shared_stream:
            if self.filename not in snapshot["names"]:
                continue
            filepath = anyio.Path(snapshot["directory"]) / self.filename
            try:
                await filepath.unlink()
            except FileNotFoundError:
                # Lost a race with a sibling, or the file disappeared between
                # snapshot and unlink. Wait for the next scan.
                continue
            self.log.info("File %s has been deleted", filepath)
            yield TriggerEvent({"filepath": str(filepath)})
            return

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Standalone fallback when the shared-stream manager is unavailable.

        Mirrors the shared path so the trigger remains usable in unit tests
        and on Airflow versions without the manager wired in. It does not
        deduplicate I/O — that requires the triggerer to drive the shared
        stream.
        """
        kwargs = self.serialize()[1]
        async for event in self.filter_shared_stream(type(self).open_shared_stream(kwargs)):
            yield event
