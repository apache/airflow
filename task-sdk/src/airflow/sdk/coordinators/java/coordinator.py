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

import email
import os
import pathlib
import stat
import zipfile
from typing import TYPE_CHECKING

import attrs
import structlog

from airflow.sdk.coordinators._subprocess import SubprocessCoordinator
from airflow.sdk.execution_time.schema import get_schema_version_migrator

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


def _calculate_classpath(jars_root: Sequence[pathlib.Path]) -> str:
    jars = (p.as_posix() for p in _find_jars(jars_root))
    return os.pathsep.join(sorted(jars))  # Keep output deterministic.


@attrs.define
class _JarMetadata:
    main_class: str
    schema_version: str

    @classmethod
    def from_jar(cls, path: pathlib.Path) -> Self | None:
        try:
            with zipfile.ZipFile(path) as zf:
                try:
                    manifest_info = zf.getinfo("META-INF/MANIFEST.MF")
                except KeyError:
                    log.debug("JAR does not contain META-INF/MANIFEST.MF; ignored", path=path)
                    return None
                with zf.open(manifest_info) as f:
                    manifest = email.message_from_binary_file(f)
            return cls(manifest["Main-Class"], manifest["Airflow-Supervisor-Schema-Version"])
        except zipfile.BadZipFile:
            log.exception("Cannot read JAR; ignored", path=path)
            return None


def _validate_schema_version(instance, _, value) -> str:
    return get_schema_version_migrator().resolve_version(str(value))


@attrs.define
class _JarInfo:
    main_class: str
    schema_version: str = attrs.field(validator=_validate_schema_version)

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


def _convert_jars_root(
    value: None | os.PathLike[str] | pathlib.Path | list[os.PathLike[str] | pathlib.Path],
) -> list[pathlib.Path]:
    if value is None:
        return []
    if isinstance(value, (str, os.PathLike, pathlib.Path)):
        return [pathlib.Path(value).expanduser()]
    return [pathlib.Path(v).expanduser() for v in value]


@attrs.define(kw_only=True)
class JavaCoordinator(SubprocessCoordinator):
    """
    Coordinator that launches a JVM subprocess for DAG parsing and task execution.

    Configuration is taken from the ``[sdk] coordinators`` entry that constructs
    this instance::

        "jdk-17": {
            "classpath": "airflow.sdk.coordinators.java.JavaCoordinator",
            "kwargs": {
                "jars_root": ["~/airflow/jars"],
                "java_executable": "/usr/lib/jvm/java-17-openjdk/bin/java",
                "jvm_args": ["-Xmx1024m"]
            }
        }

    :param java_executable: Path to the ``java`` command (defaults to
        ``"java"``, which relies on ``$PATH``).
    :param jvm_args: Extra arguments passed to the JVM (e.g. ``["-Xmx512m"]``).
    :param jars_root: A list of directories scanned for JAR bundles.
    :param main_class: Explicit entry point to execute with *java_executable*.
    :param task_startup_timeout: Maximum time the coordinator waits for a task
        process to start, in seconds. The default is 10 seconds.

    If *main_class* is not explicitly set, JavaCoordinator scans *jars_root* to
    find an executable JAR (one with Main-Class set in its metadata). If more
    than one executable JAR is found, it may be nondeterministic which one ends
    up being executed.

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
    jars_root: list[pathlib.Path] = attrs.field(
        converter=_convert_jars_root,
        validator=attrs.validators.min_len(1),
    )
    main_class: str = ""

    def _build_execute_task_command(self, *, what: TaskInstance) -> tuple[list[str], str | None]:
        jar = _JarInfo.find(self.jars_root, self.main_class)
        command = [
            self.java_executable,
            "-classpath",
            _calculate_classpath(self.jars_root),
            *self.jvm_args,
            jar.main_class,
        ]
        return command, jar.schema_version
