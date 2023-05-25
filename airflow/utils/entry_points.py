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

import collections
import functools
import logging
from typing import Iterator, Tuple

try:
    import importlib_metadata as metadata
except ImportError:
    from importlib import metadata  # type: ignore[no-redef]

log = logging.getLogger(__name__)

EPnD = Tuple[metadata.EntryPoint, metadata.Distribution]


@functools.lru_cache(maxsize=None)
def _get_grouped_entry_points() -> dict[str, list[EPnD]]:
    mapping: dict[str, list[EPnD]] = collections.defaultdict(list)
    for dist in metadata.distributions():
        try:
            for e in dist.entry_points:
                mapping[e.group].append((e, dist))
        except Exception as e:
            log.warning("Error when retrieving package metadata (skipping it): %s, %s", dist, e)
    return mapping


def entry_points_with_dist(group: str) -> Iterator[EPnD]:
    """Retrieve entry points of the given group.

    This is like the ``entry_points()`` function from ``importlib.metadata``,
    except it also returns the distribution the entry point was loaded from.

    Note that this may return multiple distributions to the same package if they
    are loaded from different ``sys.path`` entries. The caller site should
    implement appropriate deduplication logic if needed.

    :param group: Filter results to only this entrypoint group
    :return: Generator of (EntryPoint, Distribution) objects for the specified groups
    """
    return iter(_get_grouped_entry_points()[group])
