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
import stat
import zipfile
from typing import TYPE_CHECKING, Final

import attrs
import structlog

from airflow.sdk.coordinators._bundle_metadata import (
    validate_schema_version,
)
from airflow.sdk.coordinators._subprocess import SubprocessCoordinator
from airflow.sdk.coordinators.java._jar_manifest import (
    MAIN_CLASS,
    SUPERVISOR_SCHEMA_VERSION,
    read_main_attributes,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence

    from structlog.typing import FilteringBoundLogger
    from typing_extensions import Self

    from airflow.sdk.api.datamodels._generated import TaskInstance

log: FilteringBoundLogger = structlog.get_logger(logger_name="coordinators.java")

# The first supervisor schema version whose runtime can answer a Dag parse request.
_DAG_PARSING_SCHEMA_VERSION: Final = "2026-10-30"


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
    # Sorted, so the JAR a scan picks does not depend on filesystem order.
    try:
        children = sorted(directory.iterdir())
    except OSError:
        return
    yield from children


def _calculate_classpath(roots: Sequence[pathlib.Path]) -> str:
    jars = (p.as_posix() for p in _find_jars(roots))
    return os.pathsep.join(sorted(jars))  # Keep output deterministic.


@attrs.define
class _JarMetadata:
    main_class: str | None
    schema_version: str | None

    @classmethod
    def from_jar(cls, path: pathlib.Path) -> Self | None:
        try:
            with zipfile.ZipFile(path) as zf:
                attributes = read_main_attributes(zf)
        except (FileNotFoundError, IsADirectoryError, zipfile.BadZipFile):
            log.exception("Cannot read JAR; ignored", path=path)
            return None
        if attributes is None:
            log.debug("JAR does not contain META-INF/MANIFEST.MF; ignored", path=path)
            return None
        return cls(attributes.get(MAIN_CLASS), attributes.get(SUPERVISOR_SCHEMA_VERSION))


def _read_executable_jar(path: pathlib.Path) -> tuple[str, str | None]:
    """Return the Main-Class and schema version of the JAR at *path*, raising ``ValueError`` if it cannot run."""
    try:
        with zipfile.ZipFile(path) as zf:
            attributes = read_main_attributes(zf)
    except zipfile.BadZipFile as e:
        raise ValueError(f"{path} is not a valid JAR: {e}") from e
    if attributes is None:
        raise ValueError(f"{path} has no META-INF/MANIFEST.MF")
    if not (main_class := attributes.get(MAIN_CLASS)):
        raise ValueError(f"{path} is not an executable JAR: its manifest sets no Main-Class")
    return main_class, attributes.get(SUPERVISOR_SCHEMA_VERSION)


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
    def for_jar(
        cls, roots: Sequence[pathlib.Path], jar: pathlib.Path, main_class: str, schema_version: str | None
    ) -> _JarInfo:
        """
        Return how to run *jar*, whose manifest sets *main_class* and *schema_version*.

        Another JAR under *roots* that sets the same Main-Class is rejected, because the JVM would
        load the classes of whichever comes first on the classpath. Without its own schema version,
        *jar* takes the first one another JAR sets.
        """
        target = jar.resolve()
        same_main_class = [jar]
        for p in _find_jars(roots):
            if p.resolve() == target or (metadata := _JarMetadata.from_jar(p)) is None:
                continue
            if metadata.main_class == main_class:
                same_main_class.append(p)
            schema_version = schema_version or metadata.schema_version
        if len(same_main_class) > 1:
            paths = ", ".join(os.fspath(p) for p in same_main_class)
            raise ValueError(
                f"These JARs all set Main-Class {main_class!r}: {paths}. Keep one in the bundle."
            )
        if schema_version is None:
            raise FileNotFoundError(
                "cannot find a JAR with Airflow-Supervisor-Schema-Version metadata in "
                + os.pathsep.join(os.fspath(p.resolve()) for p in roots)
            )
        return cls(main_class, schema_version)

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
                "java_executable": "/usr/lib/jvm/java-17-openjdk/bin/java",
                "jvm_args": ["-Xmx1024m"]
            }
        }

    :param java_executable: Path to the ``java`` command (defaults to
        ``"java"``, which relies on ``$PATH``).
    :param jvm_args: Extra arguments passed to the JVM (e.g. ``["-Xmx512m"]``).
    :param main_class: Explicit entry point to execute with *java_executable*.
    :param task_startup_timeout: Maximum time the coordinator waits for a task
        process to start, in seconds. The default is 10 seconds.

    If *main_class* is not explicitly set, JavaCoordinator scans the Dag bundle to
    find an executable JAR (one with Main-Class set in its metadata). If more
    than one executable JAR is found, the first by path is executed. A task of a
    native Java Dag does not scan: it runs the JAR the Dag was parsed from.

    A JAR containing metadata *Airflow-Supervisor-Schema-Version* should also be
    available to specify the wire schema version. The JAR containing the Java
    SDK automatically sets this, so you don't generally need to do anything if
    dependency JARs are deployed as-is. If you repackage the dependencies,
    however, you must also reproduce the metadata entry in one of the JARs.

    The coordinator also parses native Java Dags: every JAR whose manifest sets
    Main-Class (matching *main_class* when that is set) is run to list the Dags
    its main class declares. With one JavaCoordinator configured, it parses the
    JARs of every Dag bundle. With several, ``[sdk] dag_bundle_to_coordinator``
    picks the one that parses a bundle. A JAR whose Main-Class another JAR in the
    bundle also sets is rejected.

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

    def _build_command(self, roots: Sequence[pathlib.Path], main_class: str) -> list[str]:
        return [self.java_executable, "-classpath", _calculate_classpath(roots), *self.jvm_args, main_class]

    def _build_execute_task_command(self, *, what: TaskInstance) -> tuple[list[str], str | None]:
        # Without main_class, the first executable JAR in path order wins; tracked at
        # https://github.com/apache/airflow/issues/71134
        roots = self._get_scan_roots()
        jar = _JarInfo.find(roots, self.main_class)
        return self._build_command(roots, jar.main_class), jar.schema_version

    def _build_jar_command(self, path: pathlib.Path) -> tuple[list[str], str]:
        """
        Build the command that runs the executable JAR at *path*, and return its schema version.

        The JAR's own ``Main-Class`` runs, so *main_class* must match it when set. The bundle's other
        JARs go on the classpath, so another JAR that sets the same ``Main-Class`` is rejected.

        :raises ValueError: when the JAR cannot run.
        """
        main_class, schema_version = _read_executable_jar(path)
        if self.main_class and main_class != self.main_class:
            raise ValueError(
                f"{path} runs {main_class!r}, but this coordinator's main_class is {self.main_class!r}"
            )
        roots = self._get_scan_roots()
        jar = _JarInfo.for_jar(roots, path, main_class, schema_version)
        return self._build_command(roots, jar.main_class), jar.schema_version

    def _build_dag_file_command(
        self, *, what: TaskInstance, path: pathlib.Path
    ) -> tuple[list[str], str | None]:
        return self._build_jar_command(path)

    def _build_parse_dag_command(self, *, path: pathlib.Path) -> tuple[list[str], str | None]:
        command, schema_version = self._build_jar_command(path)
        if schema_version < _DAG_PARSING_SCHEMA_VERSION:
            raise ValueError(
                f"{path} uses supervisor schema {schema_version}, which cannot parse Dags; "
                "rebuild it with a newer Java SDK or list it in .airflowignore"
            )
        return command, schema_version
