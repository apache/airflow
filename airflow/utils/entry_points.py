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

from typing import Iterator

from packaging.utils import canonicalize_name

try:
    import importlib_metadata as metadata
except ImportError:
    from importlib import metadata  # type: ignore[no-redef]


def entry_points_with_dist(group: str) -> Iterator[tuple[metadata.EntryPoint, metadata.Distribution]]:
    """Retrieve entry points of the given group.

    This is like the ``entry_points()`` function from importlib.metadata,
    except it also returns the distribution the entry_point was loaded from.

    :param group: Filter results to only this entrypoint group
    :return: Generator of (EntryPoint, Distribution) objects for the specified groups
    """
    loaded: set[str] = set()
    for dist in metadata.distributions():
        key = canonicalize_name(dist.metadata["Name"])
        if key in loaded:
            continue
        loaded.add(key)
        for e in dist.entry_points:
            if e.group != group:
                continue
            yield e, dist
