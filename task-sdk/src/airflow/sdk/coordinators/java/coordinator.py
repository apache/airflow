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
"""Java runtime coordinator that launches a JVM subprocess for Dag file processing and task execution."""

from __future__ import annotations

import os
import pathlib
import re
import stat
import zipfile
from typing import TYPE_CHECKING

import attrs
import structlog

from airflow.sdk.coordinators._bundle_metadata import validate_schema_version
from airflow.sdk.coordinators._subprocess import SubprocessCoordinator

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence

    from structlog.typing import FilteringBoundLogger
    from typing_extensions import Self

    from airflow.sdk.api.datamodels._generated import TaskInstance

log: FilteringBoundLogger = structlog.get_logger(logger_name="coordinators.java")


def _find_jars(items: Iterable[pathlib.Path]) -> Iterator[pathlib.Path]:
    """
    Yield JAR files under *items*, descending into directories.

    A symlink loop or a directory that hardlinks into one of its ancestors
    would otherwise recurse until the interpreter stack is exhausted, so
    directories are deduplicated by ``(st_dev, st_ino)`` for the duration
    of a single scan.
    """
    seen_dirs: set[tuple[int, int]] = set()
    yield from _walk_jars(items, seen_dirs)


def _walk_jars(items: Iterable[pathlib.Path], seen_dirs: set[tuple[int, int]]) -> Iterator[pathlib.Path]:
    for item in items:
        try:
            st = item.stat()
        except OSError:
            continue
        if stat.S_ISDIR(st.st_mode):
            key = (st.st_dev, st.st_ino)
            if key in seen_dirs:
                log.debug("Skipping already-visited directory", path=item)
                continue
            seen_dirs.add(key)
            yield from _walk_jars(_iter_dir(item), seen_dirs)
        elif stat.S_ISREG(st.st_mode) and item.suffix == ".jar":
            yield item


def _iter_dir(directory: pathlib.Path) -> Iterator[pathlib.Path]:
    # iterdir() is lazy, so an unreadable directory raises only once iteration
    # starts; swallow it here so a single bad directory does not abort the scan.
    try:
        yield from directory.iterdir()
    except OSError:
        return


def _calculate_classpath(roots: Sequence[pathlib.Path]) -> str:
    jars = (p.as_posix() for p in _find_jars(roots))
    return os.pathsep.join(sorted(jars))  # Keep output deterministic.


def _parse_manifest(data: bytes) -> dict[str, str]:
    """
    Return the main-section attributes of a JAR manifest, keyed by lowercased name.

    A line holds at most 72 bytes, so a longer value continues on lines starting with one space,
    which are joined without it.
    """
    attributes: dict[str, str] = {}
    name: str | None = None
    for line in re.split(r"\r\n|\r|\n", data.decode("utf-8", errors="replace")):
        if not line:
            break  # The main section ends at the first blank line.
        if line.startswith(" "):
            if name is not None:
                attributes[name] += line[1:]
            continue
        key, sep, value = line.partition(":")
        name = key.lower() if sep else None
        if name is not None:
            attributes[name] = value.removeprefix(" ")
    return attributes


@attrs.define
class _JarMetadata:
    main_class: str | None
    schema_version: str | None

    @classmethod
    def from_jar(cls, path: pathlib.Path) -> Self | None:
        try:
            with zipfile.ZipFile(path) as zf:
                try:
                    manifest_info = zf.getinfo("META-INF/MANIFEST.MF")
                except KeyError:
                    log.debug("JAR does not contain META-INF/MANIFEST.MF; ignored", path=path)
                    return None
                manifest = _parse_manifest(zf.read(manifest_info))
            return cls(manifest.get("main-class"), manifest.get("airflow-supervisor-schema-version"))
        except zipfile.BadZipFile:
            log.exception("Cannot read JAR; ignored", path=path)
            return None


@attrs.define
class _JarInfo:
    main_class: str
    schema_version: str = attrs.field(validator=validate_schema_version)

    @attrs.define
    class _Progress:
        main_class: str | None = attrs.field(init=False, default=None)
        schema_version: str | None = attrs.field(init=False, default=None)

        def collect(self) -> _JarInfo | None:
            if self.main_class is None or self.schema_version is None:
                return None
            return _JarInfo(self.main_class, self.schema_version)

    @classmethod
    def find(cls, roots: Sequence[pathlib.Path], main_class: str) -> _JarInfo:
        log.debug("Finding JARs recursively", roots=roots)
        progress = cls._Progress()
        for p in _find_jars(roots):
            if (metadata := _JarMetadata.from_jar(p)) is None:
                continue
            if metadata.main_class and ((main_class == metadata.main_class) or not main_class):
                log.debug("JAR located with Main-Class metadata", path=p, main_class=metadata.main_class)
                progress.main_class = metadata.main_class
            if metadata.schema_version:
                log.debug(
                    "JAR located with Airflow-Supervisor-Schema-Version metadata",
                    path=p,
                    schema_version=metadata.schema_version,
                )
                progress.schema_version = metadata.schema_version
            if (result := progress.collect()) is not None:
                return result
        if progress.main_class is not None:
            tp = "cannot find a JAR with Airflow-Supervisor-Schema-Version metadata in {1}"
        elif main_class:
            tp = "cannot find a JAR with Main-Class matching {0!r} in {1}"
        else:
            tp = "cannot find a JAR with Main-Class metadata in {1}"
        raise FileNotFoundError(tp.format(main_class, os.pathsep.join(os.fspath(p.resolve()) for p in roots)))


@attrs.define(kw_only=True)
class JavaCoordinator(SubprocessCoordinator):
    """
    Coordinator that launches a JVM subprocess for DAG parsing and task execution.

    Configuration is taken from the ``[sdk] coordinators`` entry that constructs
    this instance::

        "jdk-17": {
            "classpath": "airflow.sdk.coordinators.java.JavaCoordinator",
            "kwargs": {
                "task_handler_bundle_name": "java-task-handlers",
                "java_executable": "/usr/lib/jvm/java-17-openjdk/bin/java",
                "jvm_args": ["-Xmx1024m"]
            }
        }

    :param java_executable: Path to the ``java`` command (defaults to
        ``"java"``, which relies on ``$PATH``).
    :param jvm_args: Extra arguments passed to the JVM (e.g. ``["-Xmx512m"]``).
    :param task_handler_bundle_name: Name of the Dag bundle holding the JARs. It
        must be registered in ``[dag_processor] dag_bundle_config_list``. If
        unset, the task's own Dag bundle is used.
    :param main_class: Explicit entry point to execute with *java_executable*.
    :param task_startup_timeout: Maximum time the coordinator waits for a task
        process to start, in seconds. The default is 10 seconds.

    Every JAR in the bundle goes on the classpath, so one bundle is one
    classpath. Handlers that need conflicting dependency versions belong in
    separate bundles, each served by its own coordinator and queue.

    If *main_class* is not explicitly set, JavaCoordinator scans the bundle to
    find an executable JAR (one with Main-Class set in its metadata). If more
    than one executable JAR is found, it may be nondeterministic which one ends
    up being executed, so set *main_class* when more than one JAR in the bundle
    declares Main-Class.

    To report the task handlers a JAR registers, the coordinator runs that JAR's
    own Main-Class with the whole bundle on the classpath. *main_class* applies
    to task execution only.

    A JAR containing metadata *Airflow-Supervisor-Schema-Version* should also be
    available to specify the wire schema version. The JAR containing the Java
    SDK automatically sets this, so you don't generally need to do anything if
    dependency JARs are deployed as-is. If you repackage the dependencies,
    however, you must also reproduce the metadata entry in one of the JARs.

    The default *task_startup_timeout* should plenty long enough since a task-
    containing JAR is not supposed to consume significant time to perform setup
    (it should happen in individual tasks instead). However, if the launch time
    has to be so slow, you can increase the timeout to give the JAR more time.
    Note that decreasing the value is generally not meaningful since the
    coordinator does not need to wait for the full period.
    """

    java_executable: str = "java"
    jvm_args: list[str] = attrs.field(factory=list)
    main_class: str = ""

    def _build_execute_task_command(self, *, what: TaskInstance) -> tuple[list[str], str | None]:
        # Without main_class, the first executable JAR in walk order wins; tracked at
        # https://github.com/apache/airflow/issues/71134
        roots = self._get_scan_roots()
        jar = _JarInfo.find(roots, self.main_class)
        command = [
            self.java_executable,
            "-classpath",
            _calculate_classpath(roots),
            *self.jvm_args,
            jar.main_class,
        ]
        return command, jar.schema_version

    def _build_parse_task_handler_command(self, *, path: pathlib.Path) -> tuple[list[str], str | None]:
        metadata = _JarMetadata.from_jar(path)
        if metadata is None or not metadata.main_class:
            raise ValueError(f"{path} is not an executable JAR: its manifest sets no Main-Class")
        roots = self._get_scan_roots()
        # A thin JAR leaves the version to the airflow-sdk JAR beside it, where execution finds it too.
        schema_version = metadata.schema_version or _JarInfo.find(roots, metadata.main_class).schema_version
        command = [
            self.java_executable,
            "-classpath",
            _calculate_classpath(roots),
            *self.jvm_args,
            metadata.main_class,
        ]
        return command, schema_version
