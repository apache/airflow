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

import sys
from collections import ChainMap
from functools import lru_cache
from importlib import import_module
from importlib.metadata import entry_points
from typing import TYPE_CHECKING, MutableMapping

import upath

if TYPE_CHECKING:
    import airflow.io.path

_ENTRY_POINT_GROUP = "airflow_pathlib.implementations"


class _Registry(MutableMapping[str, "type[airflow.io.path.ObjectStoragePath]"]):
    """Registry for ObjectStoragePath subclasses."""

    # _default_ set for serialization
    known_implementations: dict[str, str] = {"_default_": "airflow.io.implementations.AirflowCloudPath"}

    def __init__(self) -> None:
        if sys.version_info >= (3, 10):
            eps = entry_points(group=_ENTRY_POINT_GROUP)
        else:
            eps = entry_points().get(_ENTRY_POINT_GROUP, [])
        self._entries = {ep.name: ep for ep in eps}
        self._m = ChainMap({}, self.known_implementations)  # type: ignore

    def __getitem__(self, item: str) -> type[airflow.io.path.ObjectStoragePath]:
        fqn = self._m.get(item)
        if fqn is None:
            if item in self._entries:
                fqn = self._m[item] = self._entries[item].load()
        if fqn is None:
            raise KeyError(f"{item} not in registry")
        if isinstance(fqn, str):
            module_name, name = fqn.rsplit(".", 1)
            mod = import_module(module_name)
            cls = getattr(mod, name)  # type: ignore
        else:
            cls = fqn
        return cls

    def __setitem__(self, item: str, value: type[upath.core.UPath] | str) -> None:
        if not ((isinstance(value, type) and issubclass(value, upath.core.UPath)) or isinstance(value, str)):
            raise ValueError(f"expected ObjectStorage subclass or FQN-string, got: {type(value).__name__!r}")
        self._m[item] = value

    def __delitem__(self, __v: str) -> None:
        raise NotImplementedError("removal is unsupported")

    def __len__(self) -> int:
        return len(set().union(self._m, self._entries))

    def __iter__(self):
        return iter(set().union(self._m, self._entries))


registry = _Registry()


@lru_cache
def get_path_class(
    protocol: str, *, default: str | None = None
) -> type[upath.core.UPath | airflow.io.path.ObjectStoragePath]:
    """Return the path cls for the given protocol.

    Returns `None` or ``default`` if no matching protocol can be found.

    Parameters
    ----------
    protocol:
        The protocol string
    default:
        returned if no matching protocol can be found
    """
    try:
        return registry[protocol]
    except KeyError:
        if default:
            return registry[default]
        else:
            raise
