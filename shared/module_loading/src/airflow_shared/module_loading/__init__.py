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

import functools
import logging
import pkgutil
import sys
from collections import defaultdict
from collections.abc import Callable, Iterator
from importlib import import_module
from typing import TYPE_CHECKING

if sys.version_info >= (3, 12):
    from importlib import metadata
else:
    import importlib_metadata as metadata

log = logging.getLogger(__name__)

EPnD = tuple[metadata.EntryPoint, metadata.Distribution]

if TYPE_CHECKING:
    from types import ModuleType


def import_string(dotted_path: str):
    """
    Import a dotted module path and return the attribute/class designated by the last name in the path.

    Raise ImportError if the import failed.
    """
    # TODO: Add support for nested classes. Currently, it only works for top-level classes.
    try:
        module_path, class_name = dotted_path.rsplit(".", 1)
    except ValueError:
        raise ImportError(f"{dotted_path} doesn't look like a module path")

    module = import_module(module_path)

    try:
        return getattr(module, class_name)
    except AttributeError:
        raise ImportError(f'Module "{module_path}" does not define a "{class_name}" attribute/class')


def qualname(o: object | Callable) -> str:
    """Convert an attribute/class/function to a string importable by ``import_string``."""
    if callable(o) and hasattr(o, "__module__") and hasattr(o, "__name__"):
        return f"{o.__module__}.{o.__name__}"

    cls = o

    if not isinstance(cls, type):  # instance or class
        cls = type(cls)

    name = cls.__qualname__
    module = cls.__module__

    if module and module != "__builtin__":
        return f"{module}.{name}"

    return name


def iter_namespace(ns: ModuleType):
    return pkgutil.iter_modules(ns.__path__, ns.__name__ + ".")


def is_valid_dotpath(path: str) -> bool:
    """
    Check if a string follows valid dotpath format (ie: 'package.subpackage.module').

    :param path: String to check
    """
    import re

    if not isinstance(path, str):
        return False

    # Pattern explanation:
    # ^            - Start of string
    # [a-zA-Z_]    - Must start with letter or underscore
    # [a-zA-Z0-9_] - Following chars can be letters, numbers, or underscores
    # (\.[a-zA-Z_][a-zA-Z0-9_]*)*  - Can be followed by dots and valid identifiers
    # $            - End of string
    pattern = r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$"

    return bool(re.match(pattern, path))


@functools.cache
def _get_grouped_entry_points() -> dict[str, list[EPnD]]:
    mapping: dict[str, list[EPnD]] = defaultdict(list)
    for dist in metadata.distributions():
        try:
            for e in dist.entry_points:
                mapping[e.group].append((e, dist))
        except Exception as e:
            log.warning("Error when retrieving package metadata (skipping it): %s, %s", dist, e)
    return mapping


def entry_points_with_dist(group: str) -> Iterator[EPnD]:
    """
    Retrieve entry points of the given group.

    This is like the ``entry_points()`` function from ``importlib.metadata``,
    except it also returns the distribution the entry point was loaded from.

    Note that this may return multiple distributions to the same package if they
    are loaded from different ``sys.path`` entries. The caller site should
    implement appropriate deduplication logic if needed.

    :param group: Filter results to only this entrypoint group
    :return: Generator of (EntryPoint, Distribution) objects for the specified groups
    """
    return iter(_get_grouped_entry_points()[group])
