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
"""File discovery utilities for finding files while respecting ignore patterns."""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Generator
from pathlib import Path
from re import Pattern
from typing import NamedTuple, Protocol

from pathspec.patterns import GitWildMatchPattern

log = logging.getLogger(__name__)


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

    wild_match_pattern: GitWildMatchPattern
    relative_to: Path | None = None

    @staticmethod
    def compile(pattern: str, base_dir: Path, definition_file: Path) -> _IgnoreRule | None:
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
        return _GlobIgnoreRule(wild_match_pattern=ignore_pattern, relative_to=relative_to)

    @staticmethod
    def match(path: Path, rules: list[_IgnoreRule]) -> bool:
        """Match a list of ignore rules against the supplied path, accounting for exclusion rules and ordering."""
        matched = False
        for rule in rules:
            if not isinstance(rule, _GlobIgnoreRule):
                raise ValueError(f"_GlobIgnoreRule cannot match rules of type: {type(rule)}")
            rel_obj = path.relative_to(rule.relative_to) if rule.relative_to else Path(path.name)
            if path.is_dir():
                rel_path = f"{rel_obj.as_posix()}/"
            else:
                rel_path = rel_obj.as_posix()
            if (
                rule.wild_match_pattern.include is not None
                and rule.wild_match_pattern.match_file(rel_path) is not None
            ):
                matched = rule.wild_match_pattern.include

        return matched


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
                patterns_to_match_excluding_comments = [
                    re.sub(r"\s*#.*", "", line) for line in ifile.read().split("\n")
                ]
                # append new patterns and filter out "None" objects, which are invalid patterns
                patterns += [
                    p
                    for p in [
                        ignore_rule_type.compile(pattern, Path(base_dir_path), ignore_file_path)
                        for pattern in patterns_to_match_excluding_comments
                        if pattern
                    ]
                    if p is not None
                ]
                # evaluation order of patterns is important with negation
                # so that later patterns can override earlier patterns

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
    ignore_file_syntax: str = "glob",
) -> Generator[str, None, None]:
    """
    Recursively search the base path for a list of file paths that should not be ignored.

    :param base_dir_path: the base path to be searched
    :param ignore_file_name: the file name in which specifies the patterns of files/dirs to be ignored
    :param ignore_file_syntax: the syntax of patterns in the ignore file: regexp or glob (default: glob)

    :return: a generator of file paths.
    """
    if ignore_file_syntax == "glob" or not ignore_file_syntax:
        return _find_path_from_directory(base_dir_path, ignore_file_name, _GlobIgnoreRule)
    if ignore_file_syntax == "regexp":
        return _find_path_from_directory(base_dir_path, ignore_file_name, _RegexpIgnoreRule)
    raise ValueError(f"Unsupported ignore_file_syntax: {ignore_file_syntax}")
