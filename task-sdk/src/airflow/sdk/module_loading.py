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

from collections.abc import Callable
from importlib import import_module
from typing import Any

def import_string(dotted_path: str) -> Any:
    """
    Import `dotted_path` and return the attribute designated by the tail of the path.
    Supports nested attributes (e.g., "pkg.mod.Class.Nested.attr").

    Raises ImportError if resolution fails.
    """
    if not dotted_path or "." not in dotted_path:
        raise ImportError(f"{dotted_path!r} doesn't look like a module path")

    parts: list[str] = dotted_path.split(".")
    n = len(parts)

    module = None
    module_idx = None

    for i in range(n, 0, -1):
        mod_path = ".".join(parts[:i])
        try:
            module = import_module(mod_path)
            module_idx = i
            break
        except Exception:
            continue

    if module is None or module_idx is None:
        raise ImportError(f"Could not import any module from {dotted_path!r}")

    if module_idx == n:
        raise ImportError(
            f'{dotted_path!r} resolved to a module. Provide an attribute/class after the module path.'
        )

    obj: Any = module
    for name in parts[module_idx:]:
        try:
            obj = getattr(obj, name)
        except AttributeError as e:
            raise ImportError(
                f'Module/object "{ ".".join(parts[:module_idx]) }" has no attribute "{name}" '
                f'while resolving {dotted_path!r}'
            ) from e

    return obj

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
