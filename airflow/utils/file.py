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
from __future__ import annotations

import io
import logging
import os
import re
import zipfile
from collections import OrderedDict
from pathlib import Path
from typing import TYPE_CHECKING, Generator, NamedTuple, Pattern, overload

from pathspec.patterns import GitWildMatchPattern
from typing_extensions import Protocol

from airflow.configuration import conf
from airflow.exceptions import RemovedInAirflow3Warning

if TYPE_CHECKING:
    import pathlib

log = logging.getLogger(__name__)


class _IgnoreRule(Protocol):
    """Interface for ignore rules for structural subtyping"""

    @staticmethod
    def compile(pattern: str, base_dir: Path, definition_file: Path) -> _IgnoreRule | None:
        """
        Build an ignore rule from the supplied pattern where base_dir
        and definition_file should be absolute paths.
        """

    @staticmethod
    def match(path: Path, rules: list[_IgnoreRule]) -> bool:
        """Match a candidate absolute path against a list of rules"""


class _RegexpIgnoreRule(NamedTuple):
    """Typed namedtuple with utility functions for regexp ignore rules"""

    pattern: Pattern
    base_dir: Path

    @staticmethod
    def compile(pattern: str, base_dir: Path, definition_file: Path) -> _IgnoreRule | None:
        """Build an ignore rule from the supplied regexp pattern and log a useful warning if it is invalid"""
        try:
            return _RegexpIgnoreRule(re.compile(pattern), base_dir)
        except re.error as e:
            log.warning("Ignoring invalid regex '%s' from %s: %s", pattern, definition_file, e)
            return None

    @staticmethod
    def match(path: Path, rules: list[_IgnoreRule]) -> bool:
        """Match a list of ignore rules against the supplied path"""
        for rule in rules:
            if not isinstance(rule, _RegexpIgnoreRule):
                raise ValueError(f"_RegexpIgnoreRule cannot match rules of type: {type(rule)}")
            if rule.pattern.search(str(path.relative_to(rule.base_dir))) is not None:
                return True
        return False


class _GlobIgnoreRule(NamedTuple):
    """Typed namedtuple with utility functions for glob ignore rules"""

    pattern: Pattern
    raw_pattern: str
    include: bool | None = None
    relative_to: Path | None = None

    @staticmethod
    def compile(pattern: str, _, definition_file: Path) -> _IgnoreRule | None:
        """Build an ignore rule from the supplied glob pattern and log a useful warning if it is invalid"""
        relative_to: Path | None = None
        if pattern.strip() == "/":
            # "/" doesn't match anything in gitignore
            log.warning("Ignoring no-op glob pattern '/' from %s", definition_file)
            return None
        if pattern.startswith("/") or "/" in pattern.rstrip("/"):
            # See https://git-scm.com/docs/gitignore
            # > If there is a separator at the beginning or middle (or both) of the pattern, then the
            # > pattern is relative to the directory level of the particular .gitignore file itself.
            # > Otherwise the pattern may also match at any level below the .gitignore level.
            relative_to = definition_file.parent
        ignore_pattern = GitWildMatchPattern(pattern)
        return _GlobIgnoreRule(ignore_pattern.regex, pattern, ignore_pattern.include, relative_to)

    @staticmethod
    def match(path: Path, rules: list[_IgnoreRule]) -> bool:
        """Match a list of ignore rules against the supplied path"""
        matched = False
        for r in rules:
            if not isinstance(r, _GlobIgnoreRule):
                raise ValueError(f"_GlobIgnoreRule cannot match rules of type: {type(r)}")
            rule: _GlobIgnoreRule = r  # explicit typing to make mypy play nicely
            rel_path = str(path.relative_to(rule.relative_to) if rule.relative_to else path.name)
            if rule.raw_pattern.endswith("/") and path.is_dir():
                # ensure the test path will potentially match a directory pattern if it is a directory
                rel_path += "/"
            if rule.include is not None and rule.pattern.match(rel_path) is not None:
                matched = rule.include
        return matched


def TemporaryDirectory(*args, **kwargs):
    """This function is deprecated. Please use `tempfile.TemporaryDirectory`"""
    import warnings
    from tempfile import TemporaryDirectory as TmpDir

    warnings.warn(
        "This function is deprecated. Please use `tempfile.TemporaryDirectory`",
        RemovedInAirflow3Warning,
        stacklevel=2,
    )

    return TmpDir(*args, **kwargs)


def mkdirs(path, mode):
    """
    Creates the directory specified by path, creating intermediate directories
    as necessary. If directory already exists, this is a no-op.

    :param path: The directory to create
    :param mode: The mode to give to the directory e.g. 0o755, ignores umask
    """
    import warnings

    warnings.warn(
        f"This function is deprecated. Please use `pathlib.Path({path}).mkdir`",
        RemovedInAirflow3Warning,
        stacklevel=2,
    )
    Path(path).mkdir(mode=mode, parents=True, exist_ok=True)


ZIP_REGEX = re.compile(rf"((.*\.zip){re.escape(os.sep)})?(.*)")


@overload
def correct_maybe_zipped(fileloc: None) -> None:
    ...


@overload
def correct_maybe_zipped(fileloc: str | Path) -> str | Path:
    ...


def correct_maybe_zipped(fileloc: None | str | Path) -> None | str | Path:
    """
    If the path contains a folder with a .zip suffix, then
    the folder is treated as a zip archive and path to zip is returned.
    """
    if not fileloc:
        return fileloc
    search_ = ZIP_REGEX.search(str(fileloc))
    if not search_:
        return fileloc
    _, archive, _ = search_.groups()
    if archive and zipfile.is_zipfile(archive):
        return archive
    else:
        return fileloc


def open_maybe_zipped(fileloc, mode="r"):
    """
    Opens the given file. If the path contains a folder with a .zip suffix, then
    the folder is treated as a zip archive, opening the file inside the archive.

    :return: a file object, as in `open`, or as in `ZipFile.open`.
    """
    _, archive, filename = ZIP_REGEX.search(fileloc).groups()
    if archive and zipfile.is_zipfile(archive):
        return io.TextIOWrapper(zipfile.ZipFile(archive, mode=mode).open(filename))
    else:

        return open(fileloc, mode=mode)


def _find_path_from_directory(
    base_dir_path: str,
    ignore_file_name: str,
    ignore_rule_type: type[_IgnoreRule],
) -> Generator[str, None, None]:
    """
    Recursively search the base path and return the list of file paths that should not be ignored by
    regular expressions in any ignore files at each directory level.
    :param base_dir_path: the base path to be searched
    :param ignore_file_name: the file name containing regular expressions for files that should be ignored.
    :param ignore_rule_type: the concrete class for ignore rules, which implements the _IgnoreRule interface.

    :return: a generator of file paths which should not be ignored.
    """
    # A Dict of patterns, keyed using resolved, absolute paths
    patterns_by_dir: dict[Path, list[_IgnoreRule]] = {}

    for root, dirs, files in os.walk(base_dir_path, followlinks=True):
        patterns: list[_IgnoreRule] = patterns_by_dir.get(Path(root).resolve(), [])

        ignore_file_path = Path(root) / ignore_file_name
        if ignore_file_path.is_file():
            with open(ignore_file_path) as ifile:
                lines_no_comments = [re.sub(r"\s*#.*", "", line) for line in ifile.read().split("\n")]
                # append new patterns and filter out "None" objects, which are invalid patterns
                patterns += [
                    p
                    for p in [
                        ignore_rule_type.compile(line, Path(base_dir_path), ignore_file_path)
                        for line in lines_no_comments
                        if line
                    ]
                    if p is not None
                ]
                # evaluation order of patterns is important with negation
                # so that later patterns can override earlier patterns
                patterns = list(OrderedDict.fromkeys(patterns).keys())

        dirs[:] = [subdir for subdir in dirs if not ignore_rule_type.match(Path(root) / subdir, patterns)]

        # explicit loop for infinite recursion detection since we are following symlinks in this walk
        for sd in dirs:
            dirpath = (Path(root) / sd).resolve()
            if dirpath in patterns_by_dir:
                raise RuntimeError(
                    "Detected recursive loop when walking DAG directory "
                    f"{base_dir_path}: {dirpath} has appeared more than once."
                )
            patterns_by_dir.update({dirpath: patterns.copy()})

        for file in files:
            if file == ignore_file_name:
                continue
            abs_file_path = Path(root) / file
            if ignore_rule_type.match(abs_file_path, patterns):
                continue
            yield str(abs_file_path)


def find_path_from_directory(
    base_dir_path: str,
    ignore_file_name: str,
    ignore_file_syntax: str = conf.get_mandatory_value("core", "DAG_IGNORE_FILE_SYNTAX", fallback="regexp"),
) -> Generator[str, None, None]:
    """
    Recursively search the base path and return the list of file paths that should not be ignored.
    :param base_dir_path: the base path to be searched
    :param ignore_file_name: the file name in which specifies the patterns of files/dirs to be ignored
    :param ignore_file_syntax: the syntax of patterns in the ignore file: regexp or glob

    :return: a generator of file paths.
    """
    if ignore_file_syntax == "glob":
        return _find_path_from_directory(base_dir_path, ignore_file_name, _GlobIgnoreRule)
    elif ignore_file_syntax == "regexp" or not ignore_file_syntax:
        return _find_path_from_directory(base_dir_path, ignore_file_name, _RegexpIgnoreRule)
    else:
        raise ValueError(f"Unsupported ignore_file_syntax: {ignore_file_syntax}")


def list_py_file_paths(
    directory: str | pathlib.Path,
    safe_mode: bool = conf.getboolean("core", "DAG_DISCOVERY_SAFE_MODE", fallback=True),
    include_examples: bool | None = None,
) -> list[str]:
    """
    Traverse a directory and look for Python files.

    :param directory: the directory to traverse
    :param safe_mode: whether to use a heuristic to determine whether a file
        contains Airflow DAG definitions. If not provided, use the
        core.DAG_DISCOVERY_SAFE_MODE configuration setting. If not set, default
        to safe.
    :param include_examples: include example DAGs
    :return: a list of paths to Python files in the specified directory
    """
    if include_examples is None:
        include_examples = conf.getboolean("core", "LOAD_EXAMPLES")
    file_paths: list[str] = []
    if directory is None:
        file_paths = []
    elif os.path.isfile(directory):
        file_paths = [str(directory)]
    elif os.path.isdir(directory):
        file_paths.extend(find_dag_file_paths(directory, safe_mode))
    if include_examples:
        from airflow import example_dags

        example_dag_folder = example_dags.__path__[0]  # type: ignore
        file_paths.extend(list_py_file_paths(example_dag_folder, safe_mode, include_examples=False))
    return file_paths


def find_dag_file_paths(directory: str | pathlib.Path, safe_mode: bool) -> list[str]:
    """Finds file paths of all DAG files."""
    file_paths = []

    for file_path in find_path_from_directory(str(directory), ".airflowignore"):
        try:
            if not os.path.isfile(file_path):
                continue
            _, file_ext = os.path.splitext(os.path.split(file_path)[-1])
            if file_ext != ".py" and not zipfile.is_zipfile(file_path):
                continue
            if not might_contain_dag(file_path, safe_mode):
                continue

            file_paths.append(file_path)
        except Exception:
            log.exception("Error while examining %s", file_path)

    return file_paths


COMMENT_PATTERN = re.compile(r"\s*#.*")


def might_contain_dag(file_path: str, safe_mode: bool, zip_file: zipfile.ZipFile | None = None):
    """
    Heuristic that guesses whether a Python file contains an Airflow DAG definition.

    :param file_path: Path to the file to be checked.
    :param safe_mode: Is safe mode active?. If no, this function always returns True.
    :param zip_file: if passed, checks the archive. Otherwise, check local filesystem.
    :return: True, if file might contain DAGs.
    """
    if not safe_mode:
        return True
    if zip_file:
        with zip_file.open(file_path) as current_file:
            content = current_file.read()
    else:
        if zipfile.is_zipfile(file_path):
            return True
        with open(file_path, "rb") as dag_file:
            content = dag_file.read()
    content = content.lower()
    return all(s in content for s in (b"dag", b"airflow"))
