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

import ast
import hashlib
import logging
import os
import re
import zipfile
from collections.abc import Generator
from io import TextIOWrapper
from pathlib import Path
from re import Pattern
from typing import NamedTuple, Protocol, overload

from pathspec.patterns import GitWildMatchPattern

from airflow.configuration import conf

log = logging.getLogger(__name__)

MODIFIED_DAG_MODULE_NAME = "unusual_prefix_{path_hash}_{module_name}"


class _IgnoreRule(Protocol):
    """Interface for ignore rules for structural subtyping."""

    @staticmethod
    def compile(pattern: str, base_dir: Path, definition_file: Path) -> _IgnoreRule | None:
        """
        Build an ignore rule from the supplied pattern.

        ``base_dir`` and ``definition_file`` should be absolute paths.
        """

    @staticmethod
    def match(path: Path, rules: list[_IgnoreRule]) -> bool:
        """Match a candidate absolute path against a list of rules."""


class _RegexpIgnoreRule(NamedTuple):
    """Typed namedtuple with utility functions for regexp ignore rules."""

    pattern: Pattern
    base_dir: Path

    @staticmethod
    def compile(pattern: str, base_dir: Path, definition_file: Path) -> _IgnoreRule | None:
        """Build an ignore rule from the supplied regexp pattern and log a useful warning if it is invalid."""
        try:
            return _RegexpIgnoreRule(re.compile(pattern), base_dir)
        except re.error as e:
            log.warning("Ignoring invalid regex '%s' from %s: %s", pattern, definition_file, e)
            return None

    @staticmethod
    def match(path: Path, rules: list[_IgnoreRule]) -> bool:
        """Match a list of ignore rules against the supplied path."""
        for rule in rules:
            if not isinstance(rule, _RegexpIgnoreRule):
                raise ValueError(f"_RegexpIgnoreRule cannot match rules of type: {type(rule)}")
            if rule.pattern.search(str(path.relative_to(rule.base_dir))) is not None:
                return True
        return False


class _GlobIgnoreRule(NamedTuple):
    """Typed namedtuple with utility functions for glob ignore rules."""

    pattern: Pattern
    raw_pattern: str
    include: bool | None = None
    relative_to: Path | None = None

    @staticmethod
    def compile(pattern: str, _, definition_file: Path) -> _IgnoreRule | None:
        """Build an ignore rule from the supplied glob pattern and log a useful warning if it is invalid."""
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
        """Match a list of ignore rules against the supplied path."""
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


ZIP_REGEX = re.compile(rf"((.*\.zip){re.escape(os.sep)})?(.*)")


@overload
def correct_maybe_zipped(fileloc: None) -> None: ...


@overload
def correct_maybe_zipped(fileloc: str | Path) -> str | Path: ...


def correct_maybe_zipped(fileloc: None | str | Path) -> None | str | Path:
    """If the path contains a folder with a .zip suffix, treat it as a zip archive and return path."""
    if not fileloc:
        return fileloc
    search_ = ZIP_REGEX.search(str(fileloc))
    if not search_:
        return fileloc
    _, archive, _ = search_.groups()
    if archive and zipfile.is_zipfile(archive):
        return archive
    return fileloc


def open_maybe_zipped(fileloc, mode="r"):
    """
    Open the given file.

    If the path contains a folder with a .zip suffix, then the folder
    is treated as a zip archive, opening the file inside the archive.

    :return: a file object, as in `open`, or as in `ZipFile.open`.
    """
    _, archive, filename = ZIP_REGEX.search(fileloc).groups()
    if archive and zipfile.is_zipfile(archive):
        return TextIOWrapper(zipfile.ZipFile(archive, mode=mode).open(filename))
    return open(fileloc, mode=mode)


def _find_path_from_directory(
    base_dir_path: str | os.PathLike[str],
    ignore_file_name: str,
    ignore_rule_type: type[_IgnoreRule],
) -> Generator[str, None, None]:
    """
    Recursively search the base path and return the list of file paths that should not be ignored.

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
                patterns = list(dict.fromkeys(patterns))

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
            if file != ignore_file_name:
                abs_file_path = Path(root) / file
                if not ignore_rule_type.match(abs_file_path, patterns):
                    yield str(abs_file_path)


def find_path_from_directory(
    base_dir_path: str | os.PathLike[str],
    ignore_file_name: str,
    ignore_file_syntax: str = conf.get_mandatory_value("core", "DAG_IGNORE_FILE_SYNTAX", fallback="glob"),
) -> Generator[str, None, None]:
    """
    Recursively search the base path for a list of file paths that should not be ignored.

    :param base_dir_path: the base path to be searched
    :param ignore_file_name: the file name in which specifies the patterns of files/dirs to be ignored
    :param ignore_file_syntax: the syntax of patterns in the ignore file: regexp or glob

    :return: a generator of file paths.
    """
    if ignore_file_syntax == "glob" or not ignore_file_syntax:
        return _find_path_from_directory(base_dir_path, ignore_file_name, _GlobIgnoreRule)
    if ignore_file_syntax == "regexp":
        return _find_path_from_directory(base_dir_path, ignore_file_name, _RegexpIgnoreRule)
    raise ValueError(f"Unsupported ignore_file_syntax: {ignore_file_syntax}")


def list_py_file_paths(
    directory: str | os.PathLike[str] | None,
    safe_mode: bool = conf.getboolean("core", "DAG_DISCOVERY_SAFE_MODE", fallback=True),
) -> list[str]:
    """
    Traverse a directory and look for Python files.

    :param directory: the directory to traverse
    :param safe_mode: whether to use a heuristic to determine whether a file
        contains Airflow DAG definitions. If not provided, use the
        core.DAG_DISCOVERY_SAFE_MODE configuration setting. If not set, default
        to safe.
    :return: a list of paths to Python files in the specified directory
    """
    file_paths: list[str] = []
    if directory is None:
        file_paths = []
    elif os.path.isfile(directory):
        file_paths = [str(directory)]
    elif os.path.isdir(directory):
        file_paths.extend(find_dag_file_paths(directory, safe_mode))
    return file_paths


def find_dag_file_paths(directory: str | os.PathLike[str], safe_mode: bool) -> list[str]:
    """Find file paths of all DAG files."""
    file_paths = []

    for file_path in find_path_from_directory(directory, ".airflowignore"):
        path = Path(file_path)
        try:
            if path.is_file() and (path.suffix == ".py" or zipfile.is_zipfile(path)):
                if might_contain_dag(file_path, safe_mode):
                    file_paths.append(file_path)
        except Exception:
            log.exception("Error while examining %s", file_path)

    return file_paths


COMMENT_PATTERN = re.compile(r"\s*#.*")


def might_contain_dag(file_path: str, safe_mode: bool, zip_file: zipfile.ZipFile | None = None) -> bool:
    """
    Check whether a Python file contains Airflow DAGs.

    When safe_mode is off (with False value), this function always returns True.

    If might_contain_dag_callable isn't specified, it uses airflow default heuristic
    """
    if not safe_mode:
        return True

    might_contain_dag_callable = conf.getimport(
        "core",
        "might_contain_dag_callable",
        fallback="airflow.utils.file.might_contain_dag_via_default_heuristic",
    )
    return might_contain_dag_callable(file_path=file_path, zip_file=zip_file)


def might_contain_dag_via_default_heuristic(file_path: str, zip_file: zipfile.ZipFile | None = None) -> bool:
    """
    Heuristic that guesses whether a Python file contains an Airflow DAG definition.

    :param file_path: Path to the file to be checked.
    :param zip_file: if passed, checks the archive. Otherwise, check local filesystem.
    :return: True, if file might contain DAGs.
    """
    if zip_file:
        with zip_file.open(file_path) as current_file:
            content = current_file.read()
    else:
        if zipfile.is_zipfile(file_path):
            return True
        with open(file_path, "rb") as dag_file:
            content = dag_file.read()
    content = content.lower()
    if b"airflow" not in content:
        return False
    return any(s in content for s in (b"dag", b"asset"))


def _find_imported_modules(module: ast.Module) -> Generator[str, None, None]:
    for st in module.body:
        if isinstance(st, ast.Import):
            for n in st.names:
                yield n.name
        elif isinstance(st, ast.ImportFrom) and st.module is not None:
            yield st.module


def iter_airflow_imports(file_path: str) -> Generator[str, None, None]:
    """Find Airflow modules imported in the given file."""
    try:
        parsed = ast.parse(Path(file_path).read_bytes())
    except Exception:
        return
    for m in _find_imported_modules(parsed):
        if m.startswith("airflow."):
            yield m


def get_unique_dag_module_name(file_path: str) -> str:
    """Return a unique module name in the format unusual_prefix_{sha1 of module's file path}_{original module name}."""
    if isinstance(file_path, str):
        path_hash = hashlib.sha1(file_path.encode("utf-8"), usedforsecurity=False).hexdigest()
        org_mod_name = re.sub(r"[.-]", "_", Path(file_path).stem)
        return MODIFIED_DAG_MODULE_NAME.format(path_hash=path_hash, module_name=org_mod_name)
    raise ValueError("file_path should be a string to generate unique module name")
