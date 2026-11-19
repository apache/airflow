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
"""Walk a pydantic-ai toolset and the toolsets it wraps or combines."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic_ai.toolsets.combined import CombinedToolset
from pydantic_ai.toolsets.wrapper import WrapperToolset

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from pydantic_ai.toolsets.abstract import AbstractToolset


def iter_toolsets(toolset: AbstractToolset[Any]) -> Iterator[AbstractToolset[Any]]:
    """
    Yield ``toolset`` and every toolset nested inside it, outermost first.

    pydantic-ai composes toolsets by wrapping: ``.prefixed()``, ``.filtered()``,
    ``.prepared()`` and ``.renamed()`` each return a :class:`WrapperToolset` around
    the original, and passing several toolsets together produces a
    :class:`CombinedToolset`. A check that only looks at the top level therefore
    misses a toolset the moment an author composes it, which the documentation
    recommends for running two sandboxes on one agent. This walks both shapes.

    A toolset resolved per run from a callable (``DynamicToolset``) has nothing
    to walk until the run starts, so it is yielded as itself and its contents are
    not inspected.
    """
    stack: list[AbstractToolset[Any]] = [toolset]
    while stack:
        current = stack.pop()
        yield current
        if isinstance(current, WrapperToolset):
            stack.append(current.wrapped)
        elif isinstance(current, CombinedToolset):
            stack.extend(reversed(current.toolsets))


def find_toolset(toolsets: Iterable[AbstractToolset[Any]], cls: type) -> AbstractToolset[Any] | None:
    """Return the first toolset of type ``cls`` in ``toolsets``, looking inside wrappers, or ``None``."""
    for toolset in toolsets:
        for nested in iter_toolsets(toolset):
            if isinstance(nested, cls):
                return nested
    return None
